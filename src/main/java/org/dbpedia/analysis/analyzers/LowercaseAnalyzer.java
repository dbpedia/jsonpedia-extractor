package org.dbpedia.analysis.analyzers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.util.Version;

import java.io.Reader;

/**
 * LowercaseAnalyzer mirrors the analyzer found in the elasticsearch config
 */
public class LowercaseAnalyzer extends Analyzer{
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer source = new KeywordTokenizer(reader);
        TokenStream filter = new LowerCaseFilter(Version.LUCENE_48, source);
        return new TokenStreamComponents(source, filter);
    }
}
