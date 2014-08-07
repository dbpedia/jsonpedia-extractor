package org.dbpedia.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;
import com.beust.jcommander.converters.BooleanConverter;
import com.machinelinking.wikimedia.ProcessorReport;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class LuceneIndexMain {
    @Parameter(
            names = {"--input", "-i"},
            description = "path to the input dump",
            converter = FileConverter.class,
            required = true
    )
    private File inputPath;

    @Parameter(
            names = {"--output", "-o"},
            description = "path to the folder where the index will be stored",
            converter = FileConverter.class,
            required = true
    )
    private File outputPath;

    @Parameter(
            names = {"--append", "-a"},
            description = "append to the index instead of erasing it",
            converter = BooleanConverter.class
    )
    private boolean append = false;


    public int run(String[] args) {
        final JCommander commander = new JCommander(this);
        int exitCode = 0;
        try {
            commander.parse(args);
            final LuceneIndexCreator c = new LuceneIndexCreator(
                    this.outputPath.getPath(),
                    append
            );
            final ProcessorReport rep = c.export(new URL("http://en.wikipedia.org/"), inputPath);
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
        } catch(Throwable trx) {
            trx.printStackTrace();
            System.exit(1);
        }

        return exitCode;
    }


    public static void main(String[] args){
        System.exit(new LuceneIndexMain().run(args));
    }
}
