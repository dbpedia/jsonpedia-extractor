package org.dbpedia.analysis.analyzers;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import java.io.Reader;

/**
 * KStemAnalyzer mirrors the one in the elasticsearch config.
 */
public class KStemAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader){
        Tokenizer source = new StandardTokenizer(Version.LUCENE_48, reader);
        TokenStream filter = new LowerCaseFilter(Version.LUCENE_48, source);
        filter = new StopFilter(Version.LUCENE_48, filter, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        filter = new KStemFilter(filter);
        return new TokenStreamComponents(source, filter);
    }
}
