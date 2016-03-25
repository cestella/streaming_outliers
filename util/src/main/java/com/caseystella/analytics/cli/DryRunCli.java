package com.caseystella.analytics.cli;

import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.util.JSONUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import org.apache.commons.cli.*;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class DryRunCli {
    private static abstract class OptionHandler implements Function<String, Option> {}
    public static enum DryRunOptions {
        HELP("h", new OptionHandler() {

            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                return new Option(s, "help", false, "Generate Help screen");
            }
        })
        ,EXTRACTOR_CONFIG("e", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "extractor_config", true, "JSON Document describing the extractor for this input data");
                o.setArgName("JSON_FILE");
                o.setRequired(true);
                return o;
            }
        })
        ,STREAM_OUTLIER_CONFIG("s", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "sketchy_outlier_config", true, "JSON Document describing the config for the sketchy outlier detector");
                o.setArgName("JSON_FILE");
                o.setRequired(true);
                return o;
            }
        })
        , FILTER("f", new OptionHandler() {

            @Override
            public Option apply(@Nullable String s) {
                return OptionBuilder.withArgName( "property=value" )
                        .hasArgs(2)
                        .withValueSeparator()
                        .withDescription( "use value for given property" )
                        .withLongOpt("filter")
                        .create( s );
            }
        })
        ,INPUT("i", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "input", true, "Input file");
                o.setArgName("CSV_FILE");
                o.setRequired(true);
                return o;
            }
        })
        ,OUTPUT("u", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "output", true, "output path");
                o.setArgName("FILE");
                o.setRequired(true);
                return o;
            }
        });
        Option option;
        String shortCode;
        DryRunOptions(String shortCode, OptionHandler optionHandler) {
            this.shortCode = shortCode;
            this.option = optionHandler.apply(shortCode);
        }

        public boolean has(CommandLine cli) {
            return cli.hasOption(shortCode);
        }

        public Properties getProperties(CommandLine cli) {
            return cli.getOptionProperties(shortCode);
        }
        public String get(CommandLine cli) {
            return cli.getOptionValue(shortCode);
        }

        public static CommandLine parse(CommandLineParser parser, String[] args) {
            try {
                CommandLine cli = parser.parse(getOptions(), args);
                if(DryRunOptions.HELP.has(cli)) {
                    printHelp();
                    System.exit(0);
                }
                return cli;
            } catch (ParseException e) {
                System.err.println("Unable to parse args: " + Joiner.on(' ').join(args));
                e.printStackTrace(System.err);
                printHelp();
                System.exit(-1);
                return null;
            }
        }

        public static void printHelp() {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "OutlierAnalysis", getOptions());
        }

        public static Options getOptions() {
            Options ret = new Options();
            for(DryRunOptions o : DryRunOptions.values()) {
                ret.addOption(o.option);
            }
            return ret;
        }
    }
    public static void main(String... argv) throws IOException {
        CommandLine cli = DryRunOptions.parse(new PosixParser(), argv);

        DataPointExtractorConfig extractorConfig = JSONUtil.INSTANCE.load( new File(DryRunOptions.EXTRACTOR_CONFIG.get(cli))
                                                                         , DataPointExtractorConfig.class
                                                                         );
        com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                = JSONUtil.INSTANCE.load( new File(DryRunOptions.STREAM_OUTLIER_CONFIG.get(cli))
                                        , com.caseystella.analytics.outlier.streaming.OutlierConfig.class
                                        );

        File inputFile = new File(DryRunOptions.INPUT.get(cli));
        File outputTS = new File(DryRunOptions.OUTPUT.get(cli)+ ".ts");
        File sketchyTS = new File(DryRunOptions.OUTPUT.get(cli)+ ".sketchy");
        File realTS = new File(DryRunOptions.OUTPUT.get(cli)+ ".real");
        Properties filter = DryRunOptions.FILTER.getProperties(cli);
        DryRun dryRun = new DryRun(extractorConfig, streamingOutlierConfig, filter);
        System.out.println("Running with filter: \n" + filter);
        dryRun.run(inputFile, outputTS, sketchyTS, realTS);
    }
}
