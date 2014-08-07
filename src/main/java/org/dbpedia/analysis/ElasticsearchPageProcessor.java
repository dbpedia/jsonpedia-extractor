package org.dbpedia.analysis;

import com.machinelinking.enricher.WikiEnricher;
import com.machinelinking.enricher.WikiEnricherFactory;
import com.machinelinking.parser.DocumentSource;
import com.machinelinking.serializer.JSONSerializer;
import com.machinelinking.util.JSONUtils;
import com.machinelinking.wikimedia.PageProcessor;
import com.machinelinking.wikimedia.WikiPage;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ElasticsearchPageProcessor implements PageProcessor {

    private static String indexName;
    private static String sectionTypeName;
    private static String pageTypeName;

    private long processedPages = 0;
    private long errorPages = 0;
    private final ObjectMapper mapper = new ObjectMapper();

    private Client client;

    public ElasticsearchPageProcessor(Client client, String indexName, String pageTypeName, String sectionTypeName){
        this.pageTypeName = pageTypeName;
        this.sectionTypeName = sectionTypeName;
        this.indexName = indexName;
        this.client = client;
    }

    /**
     * Get all the links from the given json node
     * Both external links and internal references in wikipedia have the same json format in jsonpedia.
     */
    private Link[] getLinks(JsonNode l) throws IOException {
        if(l.isMissingNode()){
            return new Link[]{};
        }

        Link[] links = mapper.readValue(l, Link[].class);
        if(links == null){
            return new Link[]{};
        }
        return links;
    }

    /**
     * addLinks add every link in l that belongs to the current section in the json builder
     * @param outName name of the key for the json document (e.g.: links or references)
     * @param l list of all links
     * @param currentSection current section
     * @param b json builder
     */
    private void addLinks(String outName, Link[] l, int currentSection, XContentBuilder b) throws IOException {
        b.startArray(outName);
        for(Link li: l){
            if(li.section_idx != currentSection){
                continue;
            }
            b.startObject();
            if(li.description == null || li.description == ""){
                if(outName == "links"){
                    li.description = "__MISSING__";
                } else {
                    li.description = li.url;
                }
            }
            b.field("name", li.description);
            b.field("url", li.url);
            b.endObject();
        }
        b.endArray();
    }


    /**
     * Get all the ancestors of the given section.
     * @return an array containing the names of all the ancestors
     */
    private String[] getAncestors(JsonNode sections, JsonNode currentSection) throws IOException {
        Integer[] ancestors = mapper.readValue(currentSection.path("ancestors"), Integer[].class);
        if(ancestors == null){
            return new String[]{};
        }
        String[] ancestors_str = new String[ancestors.length];
        for(int i = 0; i < ancestors.length; i++){
            ancestors_str[i] = sections.path(ancestors[i]).path("title").getTextValue().trim();
        }
        return ancestors_str;
    }

    private boolean indexPage(JsonNode root, String pageTitle) throws IOException {
        XContentBuilder b = jsonBuilder()
                .startObject()
                    .field("title", pageTitle)
                    .field("content", root.asText())
                .endObject();
        return client.prepareIndex(indexName, pageTypeName).setSource(b).execute().actionGet().isCreated();
    }

    /**
     * indexes a single page into elasticsearch
     * @param root root of the json tree
     * @throws java.io.IOException
     */
    private boolean indexSections(JsonNode root, String pageTitle) throws IOException {
        JsonNode sections = root.path("sections");
        Link[] links = getLinks(root.path("links"));
        Link[] references = getLinks(root.path("references"));

        if (sections.isMissingNode()) {
            throw new IOException("sections were expected in the json document"); //TODO: specify some other type of exception
        }

        String[] cats = mapper.readValue(root.path("categories").path("content"), String[].class);
        if(cats == null){
            cats = new String[]{};
        }

        XContentBuilder b;
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        JsonNode currentSection = sections.path(0);

        int currentSectionIdx = 1;
        while(!currentSection.isMissingNode()){
            b = jsonBuilder().startObject();
            b.field("wikipedia_page", pageTitle);
            b.field("wikipedia_categories", cats);
            b.field("section", currentSection.path("title").getTextValue());
            String[] ancestors = getAncestors(sections, currentSection);
            b.field("ancestors", ancestors);
            addLinks("links", links, currentSectionIdx, b);
            addLinks("references", references, currentSectionIdx, b);
            currentSection = sections.path(currentSectionIdx);
            b.endObject();
            bulkRequest.add(client.prepareIndex(indexName, sectionTypeName).setSource(b));

            // next section
            currentSectionIdx++;
        }

        if(bulkRequest.numberOfActions() > 1){
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            return !bulkResponse.hasFailures();
        }
        return true;
    }

    @Override
    public void processPage(String pagePrefix, String threadId, WikiPage page) {
        try {
            TokenBuffer buffer = JSONUtils.createJSONBuffer();
            final JSONSerializer serializer = new JSONSerializer(buffer);

            final WikiEnricher enricher = WikiEnricherFactory.getInstance().createFullyConfiguredInstance(
                    WikiEnricherFactory.Extractors
            );

            enricher.enrichEntity(
                    new DocumentSource(
                            new URL(pagePrefix), // not used
                            page.getContent()
                    ),
                    serializer
            );
            final JsonNode root = JSONUtils.bufferToJSONNode(buffer);
            boolean success = indexPage(root, page.getTitle());
            boolean success2 = indexSections(root, page.getTitle());
            if(success){
                processedPages++;
            } else {
                errorPages++;
            }
            if((processedPages + errorPages) % 100 == 0){
                System.out.println("*************************************************");
            }
        } catch(Exception e) {
            e.printStackTrace();
            errorPages++;
        }
    }

    @Override
    public long getProcessedPages() {
        return processedPages;
    }

    @Override
    public long getErrorPages() {
        return errorPages;
    }
}
