package org.dbpedia.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BooleanConverter;
import com.beust.jcommander.converters.FileConverter;
import com.machinelinking.wikimedia.ProcessorReport;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 *
 */
public class ElasticSearchMain {
    @Parameter(
            names = {"--elasticsearch", "-e"},
            description = "host:port elasticsearch machine",
            required = true
    )
    private String elasticsearchMachine = "localhost:9300";

    @Parameter(
            names = {"--input", "-i"},
            description = "path to the input dump",
            converter = FileConverter.class,
            required = true
    )
    private File inputPath;

    @Parameter(
            names = {"--append", "-a"},
            description = "append to the index instead of starting from scratch",
            converter = BooleanConverter.class
    )
    private boolean append = false;

    public int run(String[] args) {
        final JCommander commander = new JCommander(this);
        int exitCode = 0;
        try {
            commander.parse(args);
            final ElasticSearchIndexCreator creator = new ElasticSearchIndexCreator(
                    new String[]{this.elasticsearchMachine},
                    append
            );
            final ProcessorReport rep = creator.export(new URL("http://en.wikipedia.org/wiki/"), inputPath); // careful, needs trailing slash
            System.out.println(rep);
            exitCode = 0;
        } catch(IOException ie) {
            System.err.println(ie.getMessage());
            commander.usage();
            exitCode = 1;
        } catch(ParameterException px) {
            System.err.println(px.getMessage());
            commander.usage();
            exitCode = 2;
        }

        return exitCode;
    }

    public static void main(String[] args){
        try {
            System.exit(new ElasticSearchMain().run(args));
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}
