package org.dbpedia.analysis;

import com.machinelinking.enricher.WikiEnricher;
import com.machinelinking.enricher.WikiEnricherFactory;
import com.machinelinking.parser.DocumentSource;
import com.machinelinking.serializer.JSONSerializer;
import com.machinelinking.util.JSONUtils;
import com.machinelinking.wikimedia.PageProcessor;
import com.machinelinking.wikimedia.WikiPage;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.util.TokenBuffer;

import java.io.*;
import java.net.URL;

public class LuceneIndexPageProcessor implements PageProcessor {
    private long processedPages = 0;
    private long errorPages = 0;

    private final IndexWriter indexWriter;
    private final ObjectMapper mapper = new ObjectMapper();

    public LuceneIndexPageProcessor(IndexWriter indexWriter){
        this.indexWriter = indexWriter;
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

        ArrayNode arr = (ArrayNode)sections.path(0);
        for(JsonNode currentSection: arr){
            Document doc = new Document();
            doc.add(new StringField("wikipedia_page", pageTitle, Field.Store.YES));
            for(String cat: cats){
                doc.add(new StringField("wikipedia_categories", cat, Field.Store.YES));
            }
            doc.add(new StringField("section", currentSection.path("title").getTextValue(), Field.Store.YES));
            for(String ancestor: getAncestors(sections, currentSection)){
                doc.add(new StringField("ancestors", ancestor, Field.Store.YES));
            }
            indexWriter.addDocument(doc);
        }


//        JsonNode currentSection = sections.path(0);
//        int currentSectionIdx = 1;
//        while(!currentSection.isMissingNode()){
//            b = jsonBuilder().startObject();
//            b.field("wikipedia_page", pageTitle);
//            b.field("wikipedia_categories", cats);
//            b.field("section", currentSection.path("title").getTextValue());
//            String[] ancestors = getAncestors(sections, currentSection);
//            b.field("ancestors", ancestors);
//            addLinks("links", links, currentSectionIdx, b);
//            addLinks("references", references, currentSectionIdx, b);
//            currentSection = sections.path(currentSectionIdx);
//            b.endObject();
//            bulkRequest.add(client.prepareIndex(indexName, typeName).setSource(b));
//
//            // next section
//            currentSectionIdx++;
//        }
//
//        if(bulkRequest.numberOfActions() > 1){
//            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//            return !bulkResponse.hasFailures();
//        }
        return true;
    }

    @Override
    public void processPage(String pagePrefix, String threadId, WikiPage page) {
        try {
            TokenBuffer buffer = JSONUtils.createJSONBuffer();
//            final ByteArrayOutputStream jsonBuffer = new ByteArrayOutputStream();
            final JSONSerializer serializer = new JSONSerializer(buffer);

//            final WikiTextParser parser = new WikiTextParser(
//                    WikiTextSerializerHandlerFactory.getInstance().createSerializerHandler(serializer)
//            );
            final WikiEnricher enricher = WikiEnricherFactory.getInstance().createFullyConfiguredInstance(
                    WikiEnricherFactory.Extractors
            );

            enricher.enrichEntity(
                    new DocumentSource(
                            new URL(pagePrefix),
                            page.getContent()
                    ),
                    serializer
            );

            final JsonNode root = JSONUtils.bufferToJSONNode(buffer);
            indexSections(root, page.getTitle());
            processedPages++;
//        } catch (IOException|WikiTextParserException|RuntimeException exc) {
        } catch(Exception e) {
//            e.printStackTrace();
            errorPages++;
        }

        if((processedPages + errorPages) % 100 == 0){
            System.out.println("*************************************************");
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
