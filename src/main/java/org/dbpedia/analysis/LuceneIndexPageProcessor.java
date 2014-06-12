package org.dbpedia.analysis;

import com.google.common.base.Joiner;
import com.machinelinking.enricher.WikiEnricher;
import com.machinelinking.enricher.WikiEnricherFactory;
import com.machinelinking.pagestruct.WikiTextSerializerHandlerFactory;
import com.machinelinking.parser.DocumentSource;
import com.machinelinking.parser.WikiTextParser;
import com.machinelinking.parser.WikiTextParserException;
import com.machinelinking.serializer.JSONSerializer;
import com.machinelinking.util.JSONUtils;
import com.machinelinking.wikimedia.PageProcessor;
import com.machinelinking.wikimedia.WikiPage;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.common.inject.internal.Join;

import java.io.*;
import java.net.URL;
import java.util.Arrays;

public class LuceneIndexPageProcessor implements PageProcessor {
    private long processedPages = 0;
    private long errorPages = 0;

    private final IndexWriter indexWriter;
    private final TaxonomyWriter taxonomyWriter;
    private final FacetsConfig config;
    private final ObjectMapper mapper = new ObjectMapper();

    public LuceneIndexPageProcessor(IndexWriter indexWriter, TaxonomyWriter taxonomyWriter, FacetsConfig config){
        this.taxonomyWriter = taxonomyWriter;
        this.indexWriter = indexWriter;
        this.config = config;
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
     * indexes a single page into the faceted index
     * @param root root of the json tree.
     * @throws IOException
     */
    private void indexSections(JsonNode root, String pageTitle) throws IOException {
        JsonNode sections = root.path("sections");

        if (sections.isMissingNode()) {
            throw new IOException("sections were expected in the json document"); //TODO: specify some other type of exception
        }

        String[] cats = mapper.readValue(root.path("categories").path("content"), String[].class);
        String categories = cats == null ? "" : Joiner.on(' ').join(cats);

        JsonNode currentSection = sections.path(0);
        int section_idx = 1;
        while(!currentSection.isMissingNode()){
            Document doc = new Document();
            doc.add(new FacetField("wikipedia_page", pageTitle));
            doc.add(new FacetField("section", currentSection.path("title").getTextValue()));
            String[] ancestors = getAncestors(sections, currentSection);

            if(categories != ""){
                doc.add(new FacetField("wikipedia_categories", categories));

            }
            if(ancestors.length > 0){
                doc.add(new FacetField("ancestors", ancestors));
            }
            //TODO: links and refs
            try {
                indexWriter.addDocument(config.build(taxonomyWriter, doc));
            } catch (Exception e) {
                e.printStackTrace();
            }
            currentSection = sections.path(section_idx);
            section_idx++;
        }

    }

    @Override
    public void processPage(String pagePrefix, String threadId, WikiPage page) {
        try {
            final ByteArrayOutputStream jsonBuffer = new ByteArrayOutputStream();
            final JSONSerializer serializer = new JSONSerializer(jsonBuffer);

//            final WikiTextParser parser = new WikiTextParser(
//                    WikiTextSerializerHandlerFactory.getInstance().createSerializerHandler(serializer)
//            );
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
            indexSections(root, page.getTitle());
            processedPages++;
            if(processedPages % 100 == 0){
                System.out.println("*************************************************");
            }
//        } catch (IOException|WikiTextParserException|RuntimeException exc) {
        } catch(Exception e) {
            e.printStackTrace();
            errorPages++;
            return;
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
