package org.dbpedia.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * <strong>IndexCreator</strong> populates a lucene faceted index with data coming form the jsonpedia processing of a wikipedia dump.
 */
public class IndexCreator implements Closeable {
    private IndexWriter writer;

    /**
     *
     * @param indexPath path to the index folder
     * @param eraseOld if set to true this will delete the old index instead of appending to it
     * @throws IOException
     */
    public IndexCreator(String indexPath, boolean eraseOld) throws IOException{
        Analyzer an = new StandardAnalyzer(Version.LUCENE_48);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, an);
        File f = new File(indexPath);
        if(!f.isDirectory()){
            throw new IOException(String.format("%s is not a valid index directory", indexPath));
        }

        Directory dir = FSDirectory.open(f);
        if(eraseOld){
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        } else {
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        }
        writer = new IndexWriter(dir, iwc);
    }

    @Override
    public void close(){
        try{
            writer.close();
        } catch(IOException e) {}
    }

    public static void main(String args[]){
        IndexCreator ic;
        try {
            ic = new IndexCreator("index-folder", true);
            ic.close();
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}
