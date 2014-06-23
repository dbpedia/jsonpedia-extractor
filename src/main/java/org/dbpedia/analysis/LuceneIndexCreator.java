package org.dbpedia.analysis;

import com.machinelinking.util.FileUtil;
import com.machinelinking.wikimedia.ProcessorReport;
import com.machinelinking.wikimedia.WikiDumpMultiThreadProcessor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * <strong>IndexCreator</strong> populates a lucene faceted index with data coming form the jsonpedia processing of a wikipedia dump.
 */
public class LuceneIndexCreator extends WikiDumpMultiThreadProcessor<LuceneIndexPageProcessor> {

    private final FacetsConfig config = new FacetsConfig();
    private IndexWriter indexWriter;
    private TaxonomyWriter taxonomyWriter;


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
     * @param taxonomyPath path to the taxonomy folder
     * @param indexPath path to the index folder
     * @param appendToIndex if set to false this will delete the old index instead of appending to it
     * @throws java.io.IOException if taxonomyPath or indexPath are not valid folders
     */
    public LuceneIndexCreator(String indexPath, String taxonomyPath, boolean appendToIndex) throws IOException {
        super();
        Analyzer an = new StandardAnalyzer(Version.LUCENE_48);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, an);
        File f = new File(indexPath);
        if(!f.isDirectory()){
            throw new IOException(String.format("%s is not a valid location for a index directory", indexPath));
        }

        File f2 = new File(taxonomyPath);
        if(!f2.isDirectory()){
            throw new IOException(String.format("%s is not a valid location for a taxonomy directory", taxonomyPath));
        }

        IndexWriterConfig.OpenMode op;
        Directory dir = FSDirectory.open(f);
        Directory dir2 = new MMapDirectory(f2);
        if(appendToIndex){
            op = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
        } else {
            op = IndexWriterConfig.OpenMode.CREATE;
        }
        config.setHierarchical("ancestors", true);
        taxonomyWriter = new DirectoryTaxonomyWriter(dir2, op);
        iwc.setOpenMode(op);
        indexWriter = new IndexWriter(dir, iwc);
    }

    @Override
    public void initProcess() {

    }

    @Override
    public LuceneIndexPageProcessor initProcessor(int i) {
        return new LuceneIndexPageProcessor(indexWriter, taxonomyWriter, config);
    }

    @Override
    public void finalizeProcessor(LuceneIndexPageProcessor luceneIndexPageProcessor) {

    }

    @Override
    public void finalizeProcess(ProcessorReport processorReport) {
        try{
            indexWriter.commit();
            indexWriter.close();
            taxonomyWriter.commit();
            taxonomyWriter.close();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

    }
}
