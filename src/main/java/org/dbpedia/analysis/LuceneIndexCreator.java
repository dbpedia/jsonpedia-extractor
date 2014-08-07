package org.dbpedia.analysis;

import com.machinelinking.util.FileUtil;
import com.machinelinking.wikimedia.ProcessorReport;
import com.machinelinking.wikimedia.WikiDumpMultiThreadProcessor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.dbpedia.analysis.analyzers.KStemAnalyzer;
import org.dbpedia.analysis.analyzers.LowercaseAnalyzer;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * <strong>IndexCreator</strong> populates a lucene index with data coming form the jsonpedia processing of a wikipedia dump.
 */
public class LuceneIndexCreator extends WikiDumpMultiThreadProcessor<LuceneIndexPageProcessor> {

    private IndexWriter indexWriter;


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

    /**
     *
     * @param indexPath path to the index folder
     * @param appendToIndex if set to false this will delete the old index instead of appending to it
     * @throws java.io.IOException if indexPath is not a valid folder
     */
    public LuceneIndexCreator(String indexPath, boolean appendToIndex) throws IOException {
        super();
        Map<String,Analyzer> perField = new HashMap<>();
        perField.put("wikipedia_page", new LowercaseAnalyzer());
        perField.put("wikipedia_category", new LowercaseAnalyzer());
        PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(new KStemAnalyzer(), perField);

        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, aWrapper);
        File f = new File(indexPath);
        if(!f.isDirectory()){
            throw new IOException(String.format("%s is not a valid location for a index directory", indexPath));
        }

        IndexWriterConfig.OpenMode op;
        Directory dir = FSDirectory.open(f);
        if(appendToIndex){
            op = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
        } else {
            op = IndexWriterConfig.OpenMode.CREATE;
        }
        iwc.setOpenMode(op);
        indexWriter = new IndexWriter(dir, iwc);
    }

    @Override
    public void initProcess() {

    }

    @Override
    public LuceneIndexPageProcessor initProcessor(int i) {
        return new LuceneIndexPageProcessor(indexWriter);
    }

    @Override
    public void finalizeProcessor(LuceneIndexPageProcessor luceneIndexPageProcessor) {

    }

    @Override
    public void finalizeProcess(ProcessorReport processorReport) {
        try{
            indexWriter.commit();
            indexWriter.close();
        } catch(IOException e) {}
    }
}
