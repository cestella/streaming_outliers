package com.caseystella.analytics.outlier;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.timeseries.TSConstants;
import com.caseystella.analytics.util.JSONUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import storm.kafka.CallbackKafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class Topology {
    private static abstract class OptionHandler implements Function<String, Option> {}
    private enum OutlierOptions {
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
        ,BATCH_OUTLIER_CONFIG("o", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "outlier_config", true, "JSON Document describing the config for the real outlier detector");
                o.setArgName("JSON_FILE");
                o.setRequired(true);
                return o;
            }
        })
        ,TIMESERIES_DB_CONFIG("d", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "tsdb_config", true, "JSON Document describing the config for the timeseries database");
                o.setArgName("JSON_FILE");
                o.setRequired(true);
                return o;
            }
        })
        ,TOPIC("t", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "kafka_topic", true, "Kafka Topic to be used");
                o.setArgName("TOPIC");
                o.setRequired(true);
                return o;
            }
        })
        ,NUM_WORKERS("n", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "num_workers", true, "Number of workers");
                o.setArgName("N");
                o.setRequired(true);
                return o;
            }
        })
        ,FROM_BEGINNING("b", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "from_beginning", false, "Run from the beginning of the queue.");
                o.setRequired(false);
                return o;
            }
        })
        ;
        Option option;
        String shortCode;
        OutlierOptions(String shortCode, OptionHandler optionHandler) {
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
                if(OutlierOptions.HELP.has(cli)) {
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
            for(OutlierOptions o : OutlierOptions.values()) {
                ret.addOption(o.option);
            }
            return ret;
        }
    }

    public static TopologyBuilder createTopology( DataPointExtractorConfig extractorConfig
                                                , com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                                                , com.caseystella.analytics.outlier.batch.OutlierConfig batchOutlierConfig
                                                , PersistenceConfig persistenceConfig
                                                , String kafkaTopic
                                                , String zkQuorum
                                                , int numWorkers
                                                , boolean startAtBeginning
                                         )
    {
        TopologyBuilder builder = new TopologyBuilder();
        String spoutId = "outlier_filter";
        String boltId= "outlier";
        OutlierKafkaSpout spout = null;
        {
            //BrokerHosts hosts, String topic, String zkRoot, String id
            SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkQuorum)
                                                     , kafkaTopic
                                                     , "/kafka"
                                                     , "streaming_outlier"
                                                     );
            spoutConfig.maxOffsetBehind = startAtBeginning?-2:-1;
            /*SpoutConfig spoutConfig
                            , OutlierConfig outlierConfig
                            , DataPointExtractorConfig extractorConfig
                            , PersistenceConfig persistenceConfig
                            , String zkConnectString*/
            spout = new OutlierKafkaSpout(spoutConfig
                                         , streamingOutlierConfig
                                         , extractorConfig
                                         , persistenceConfig
                                         , zkQuorum
                                         );
        }
        OutlierBolt bolt = null;
        {
            bolt = new OutlierBolt(batchOutlierConfig, persistenceConfig);
        }
        builder.setSpout(spoutId, spout, numWorkers);
        builder.setBolt(boltId, bolt, 1).shuffleGrouping(spoutId);
        return builder;
    }

    public static void main(String... argv) throws Exception {
        CommandLine cli = OutlierOptions.parse(new PosixParser(), argv);
        DataPointExtractorConfig extractorConfig = JSONUtil.INSTANCE.load(new FileInputStream(new File(OutlierOptions.EXTRACTOR_CONFIG.get(cli)))
                                                                         , DataPointExtractorConfig.class
                                                                         );
        com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig = JSONUtil.INSTANCE.load(new FileInputStream(new File(OutlierOptions.STREAM_OUTLIER_CONFIG.get(cli)))
                                                                         , com.caseystella.analytics.outlier.streaming.OutlierConfig.class
                                                                         );
        com.caseystella.analytics.outlier.batch.OutlierConfig batchOutlierConfig = JSONUtil.INSTANCE.load(new FileInputStream(new File(OutlierOptions.BATCH_OUTLIER_CONFIG.get(cli)))
                                                                         , com.caseystella.analytics.outlier.batch.OutlierConfig.class
                                                                         );
        PersistenceConfig persistenceConfig = JSONUtil.INSTANCE.load(new FileInputStream(new File(OutlierOptions.TIMESERIES_DB_CONFIG.get(cli)))
                                                                         , PersistenceConfig.class
                                                                         );
        int numWorkers = 6;
        if(OutlierOptions.NUM_WORKERS.has(cli)) {
            numWorkers = Integer.parseInt(OutlierOptions.NUM_WORKERS.get(cli));
        }
        Map clusterConf = Utils.readStormConfig();
        Config config = new Config();
        config.setNumWorkers(numWorkers);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);

        Configuration hadoopConfig = HBaseConfiguration.create();
        clusterConf.put(TSConstants.HBASE_CONFIG_KEY, hadoopConfig);
        config.put(TSConstants.HBASE_CONFIG_KEY, hadoopConfig);
        String topicName = OutlierOptions.TOPIC.get(cli);
        String topologyName = "streaming_outliers_" + topicName;
        String zkConnectString = hadoopConfig.get(HConstants.ZOOKEEPER_QUORUM) + ":" + hadoopConfig.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        /*DataPointExtractorConfig extractorConfig
                                                , com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                                                , com.caseystella.analytics.outlier.batch.OutlierConfig batchOutlierConfig
                                                , PersistenceConfig persistenceConfig
                                                , String kafkaTopic
                                                , String zkQuorum
                                                , int numWorkers*/
        boolean startAtBeginning = OutlierOptions.FROM_BEGINNING.has(cli);
        TopologyBuilder topology = createTopology(extractorConfig, streamingOutlierConfig, batchOutlierConfig, persistenceConfig, topicName, zkConnectString, numWorkers, startAtBeginning);
        StormSubmitter.submitTopologyWithProgressBar( topologyName, clusterConf, topology.createTopology());
        //Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
    }
}
