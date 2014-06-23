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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;

public class ElasticsearchPageProcessor implements PageProcessor {

    private static String indexName;
    private static String typeName;

    private long processedPages = 0;
    private long errorPages = 0;
    private final ObjectMapper mapper = new ObjectMapper();

    private Client client;

    public ElasticsearchPageProcessor(Client client, String indexName, String typeName){
        this.typeName = typeName;
        this.indexName = indexName;
        this.client = client;
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

    /**
     * indexes a single page into elasticsearch
     * @param root root of the json tree
     * @throws java.io.IOException
     */
    private boolean indexSections(JsonNode root, String pageTitle) throws IOException {
        JsonNode sections = root.path("sections");

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
        int section_idx = 1;
        while(!currentSection.isMissingNode()){
            b = jsonBuilder().startObject();
            b.field("wikipedia_page", pageTitle);
            b.field("wikipedia_categories", cats);
            b.field("section", currentSection.path("title").getTextValue());
            String[] ancestors = getAncestors(sections, currentSection);
            b.field("ancestors", ancestors);
            currentSection = sections.path(section_idx);
            b.endObject();
            bulkRequest.add(client.prepareIndex(indexName, typeName).setSource(b));

            // next section
            section_idx++;
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
            final ByteArrayOutputStream jsonBuffer = new ByteArrayOutputStream();
            final JSONSerializer serializer = new JSONSerializer(jsonBuffer);

            final WikiEnricher enricher = WikiEnricherFactory.getInstance().createFullyConfiguredInstance(
                    WikiEnricherFactory.Extractors
            );

            enricher.enrichEntity(
                    new DocumentSource(
                            new URL("http://en.wikipedia.org/"),
                            page.getContent()
                    ),
                    serializer
            );

            final JsonNode root = JSONUtils.parseJSON(jsonBuffer.toString()); // TODO: optimize
            boolean success = indexSections(root, page.getTitle());
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
