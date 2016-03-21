package com.caseystella.analytics.cli;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Sink;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.commons.cli.*;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.io.*;
import java.text.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CSVProjector {
    private static abstract class OptionHandler implements Function<String, Option> {}
    public static enum CSVOptions {
        HELP("h", new OptionHandler() {

            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                return new Option(s, "help", false, "Generate Help screen");
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
        ,OUTPUT("o", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "output", true, "Output file");
                o.setArgName("CSV_FILE");
                o.setRequired(true);
                return o;
            }
        })
        ,COLUMNS("c", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "columns", true, "Columns to project");
                o.setArgName("COL[,COL]*");
                o.setRequired(true);
                return o;
            }
        })
        ,DATE_FORMAT("d", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "date_format", true, "Date Format");
                o.setArgName("FORMAT");
                o.setRequired(true);
                return o;
            }
        })
        ;Option option;
        String shortCode;
        CSVOptions(String shortCode, OptionHandler optionHandler) {
            this.shortCode = shortCode;
            this.option = optionHandler.apply(shortCode);
        }

        public boolean has(CommandLine cli) {
            return cli.hasOption(shortCode);
        }

        public String get(CommandLine cli) {
            return cli.getOptionValue(shortCode);
        }

        public static CommandLine parse(CommandLineParser parser, String[] args) {
            try {
                CommandLine cli = parser.parse(getOptions(), args);
                if(HELP.has(cli)) {
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
            for(CSVOptions o : CSVOptions.values()) {
                ret.addOption(o.option);
            }
            return ret;
        }
    }

    public static class CollisionHandler {
        public List<BloomFilter<Long>> filters;
        public CollisionHandler(int numFilters, int size) {
            filters = new ArrayList<>();
            for(int i = 0;i < numFilters;++i) {
                BloomFilter<Long> collisionFilter = BloomFilter.create(new Funnel<Long>() {

                    /**
                     * Sends a stream of data from the {@code from} object into the sink {@code into}. There
                     * is no requirement that this data be complete enough to fully reconstitute the object
                     * later.
                     *
                     * @param from
                     * @param into
                     */
                    @Override
                    public void funnel(Long from, Sink into) {
                        into.putLong(from);
                    }
                }, size);
                filters.add(collisionFilter);
            }
        }

        public boolean has(long l) {
            return filters.get((int) (l % filters.size())).mightContain(l);
        }
        public void put(long l) {
            filters.get((int) (l % filters.size())).put(l);
        }
    }

    public static long getUncollidedTs(CollisionHandler handler,  long ts) {
        int collisions = 0;
        for(long l = ts;collisions < 20000;l++,collisions++) {
            if(!handler.has(l)) {
                handler.put(l);
                return l;
            }
        }
        throw new RuntimeException("Something very bad happened and I couldn't find any free timestamps for " + ts);
    }

    public static void main(String... argv) throws IOException{
        CommandLine cli = CSVOptions.parse(new PosixParser(), argv);
        SimpleDateFormat sdf = new SimpleDateFormat(CSVOptions.DATE_FORMAT.get(cli));
        BufferedReader in = new BufferedReader(new FileReader(new File(CSVOptions.INPUT.get(cli))));
        PrintWriter out = new PrintWriter(new File(CSVOptions.OUTPUT.get(cli)));
        CSVParser parser = new CSVParserBuilder().build();
        CSVWriter writer = new CSVWriter(out);
        List<Integer> cols = new ArrayList<>();
        for(String col : Splitter.on(',').split(CSVOptions.COLUMNS.get(cli))) {
            cols.add(Integer.parseInt(col));
        }
        //CollisionHandler collisionHandler = new CollisionHandler(150, 2000000);

        int lineNo = 1;
        for(String line = null;(line = in.readLine()) != null;lineNo++) {
            if(lineNo % 100 == 0) {
                System.out.print(".");
            }
            if(lineNo % 1000 == 0) {
                System.out.println(" -- Completed " + lineNo);
            }
            String[] tokens = parser.parseLine(line);
            String[] projectedTokens = new String[cols.size()];
            int i = 0;
            boolean write = true;
            for(Integer col : cols) {
                if(i == 0) {
                    try {
                        Date d = sdf.parse(tokens[col]);
                        long ts = d.getTime();//getUncollidedTs(collisionHandler, d.getTime());
                        projectedTokens[i++] = ts + "";
                    } catch (java.text.ParseException e) {
                        write = false;
                        break;
                    }
                }
                else {
                    projectedTokens[i++] = tokens[col];
                }
            }
            if(write) {
                writer.writeNext(projectedTokens);
            }
        }
        writer.close();
    }
}
