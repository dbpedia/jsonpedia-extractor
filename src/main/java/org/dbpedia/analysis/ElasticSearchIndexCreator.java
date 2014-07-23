package org.dbpedia.analysis;

import com.machinelinking.util.FileUtil;
import com.machinelinking.wikimedia.ProcessorReport;
import com.machinelinking.wikimedia.WikiDumpMultiThreadProcessor;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * <strong>IndexCreator</strong> populates an elasticsearch index with sections data from wikipedia
 */
public class ElasticSearchIndexCreator extends WikiDumpMultiThreadProcessor<ElasticsearchPageProcessor> {

    private static final String indexName = "jsonpedia";
    private static final String typeName = "category";

    private final Client client;

    public ProcessorReport export(URL pagePrefix, InputStream is) throws IOException {
        final BufferedInputStream bis =
                is instanceof BufferedInputStream ? (BufferedInputStream) is: new BufferedInputStream(is);

        try {
            final ProcessorReport report = super.process(
                    pagePrefix,
                    bis,
                    super.getBestNumberOfThreads()
            );
            return report;
        } catch (SAXException|IOException exc){
            throw new RuntimeException(exc);
        }
    }

    public ProcessorReport export(URL pagePrefix, File input) throws IOException {
        return this.export(pagePrefix, FileUtil.openDecompressedInputStream(input));
    }

    public String getFromClasspath(String resourcePath) throws IOException {
        return IOUtils.toString(
                getClass().getClassLoader().getResourceAsStream(resourcePath),
                Charset.defaultCharset()
        );
    }


    /**
     * @param elasticsearchCluster list of host:port urls for the elasticsearch machines
     * @param append append to the index instead of deleting it
     */
    public ElasticSearchIndexCreator(String[] elasticsearchCluster, boolean append) throws IOException {
        super();
        TransportClient cl = new TransportClient();
        for(String c: elasticsearchCluster){
            String[] hostport = c.split(":");
            Integer port;
            if(hostport.length != 2){
                System.err.println(c + " is not a valid elasticsearch url");
                continue;
            }

            try{
                port = Integer.parseInt(hostport[1]);
            } catch (NumberFormatException exc){
                System.err.println(hostport[1] + " is not a valid port number");
                continue;
            }
            cl.addTransportAddress(new InetSocketTransportAddress(hostport[0], port));
        }
        this.client = cl;

        if(!append){
//            this.client.admin()
//                    .indices()
//                    .prepareAnalyze("lowercase_analyzer")
//                    .setAnalyzer("keyword")
//                    .setTokenFilters("whitespace");

            final IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute().actionGet();
            if (res.isExists()) {
                this.client.admin().indices()
                        .prepareDelete(indexName)
                        .execute()
                        .actionGet();
            }

            String settings = getFromClasspath("org/dbpedia/analysis/settings.json");

            this.client.admin().indices()
                    .prepareCreate(indexName)
                    .setSource(settings)
                    .execute()
                    .actionGet();
            this.client.admin().indices()
                    .prepareFlush(indexName)
                    .execute()
                    .actionGet();
        }
    }

    @Override
    public void initProcess() {

    }

    @Override
    public ElasticsearchPageProcessor initProcessor(int i) {
        return new ElasticsearchPageProcessor(client, indexName, typeName);
    }

    @Override
    public void finalizeProcessor(ElasticsearchPageProcessor pageProcessor) {

    }

    @Override
    public void finalizeProcess(ProcessorReport processorReport) {
        client.close();
    }
}
