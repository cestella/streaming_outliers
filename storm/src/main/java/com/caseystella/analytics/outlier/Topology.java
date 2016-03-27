package com.caseystella.analytics.outlier;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.util.JSONUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.storm.EsBolt;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
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
                o.setRequired(false);
                return o;
            }
        })
        ,NUM_INDEXING_WORKERS("g", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "num_indexing_workers", true, "Number of indexing workers");
                o.setArgName("N");
                o.setRequired(false);
                return o;
            }
        })
        ,NUM_SPOUTS("x", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "num_spouts", true, "Number of spouts");
                o.setArgName("N");
                o.setRequired(false);
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
        ,ZK_QUORUM("z", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "zkquorum", true, "Zookeeper Quorum");
                o.setArgName("host:port[,host:port]");
                o.setRequired(true);
                return o;
            }
        })
        ,ES_NODE("q", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "es_node", true, "Elastic search node");
                o.setArgName("host:port");
                o.setRequired(true);
                return o;
            }
        })
        ,INDEX("i", new OptionHandler() {
            @Nullable
            @Override
            public Option apply(@Nullable String s) {
                Option o = new Option(s, "index_name", true, "Elastic search index name");
                o.setArgName("NAME");
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
                                                , PersistenceConfig persistenceConfig
                                                , String kafkaTopic
                                                , String zkQuorum
                                                , String esNode
                                                , int numWorkers
                                                , int numSpouts
                                                , int numIndexers
                                                , String indexName
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
            spoutConfig.startOffsetTime = startAtBeginning?kafka.api.OffsetRequest.EarliestTime()
                                                          :kafka.api.OffsetRequest.LatestTime()
                                                          ;
            if(startAtBeginning) {
                spoutConfig.ignoreZkOffsets = true;
            }
            spout = new OutlierKafkaSpout(spoutConfig
                                         , extractorConfig
                                         , streamingOutlierConfig.getGroupingKeys()
                                         , zkQuorum
                                         );
        }
        OutlierBolt bolt = null;
        {
            bolt = new OutlierBolt(kafkaTopic, streamingOutlierConfig, persistenceConfig);
        }
        builder.setSpout(spoutId, spout, numSpouts);
        builder.setBolt(boltId, bolt, numWorkers).fieldsGrouping(spoutId, new Fields(Constants.GROUP_ID));
        {
            Map conf = new HashMap();
            //conf.put(ConfigurationOptions.ES_INPUT_JSON, "yes");
            if(esNode != null) {
                /*if(esNode.contains(":")) {
                    Iterable<String> tokens = Splitter.on(':').split(esNode);
                    String host = Iterables.getFirst(tokens, ConfigurationOptions.ES_NODES_DEFAULT);
                    String port = Iterables.getLast(tokens, ConfigurationOptions.ES_PORT_DEFAULT);
                    conf.put(ConfigurationOptions.ES_NODES, host);
                    conf.put(ConfigurationOptions.ES_PORT, port);
                }
                else {
                    conf.put(ConfigurationOptions.ES_NODES, esNode);
                }*/
                conf.put(ConfigurationOptions.ES_NODES, esNode);
            }
            conf.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, true);
            builder.setBolt("es_bolt", new EsBolt(indexName, conf), numIndexers)
                   .shuffleGrouping(boltId, OutlierBolt.STREAM_ID);
        }
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

        PersistenceConfig persistenceConfig = JSONUtil.INSTANCE.load(new FileInputStream(new File(OutlierOptions.TIMESERIES_DB_CONFIG.get(cli)))
                                                                         , PersistenceConfig.class
                                                                         );
        int numSpouts = 1;
        int numWorkers = 10;
        if(OutlierOptions.NUM_WORKERS.has(cli)) {
            numWorkers = Integer.parseInt(OutlierOptions.NUM_WORKERS.get(cli));
        }
        if(OutlierOptions.NUM_SPOUTS.has(cli)) {
            numSpouts = Integer.parseInt(OutlierOptions.NUM_SPOUTS.get(cli));
        }
        Map clusterConf = Utils.readStormConfig();
        clusterConf.put("topology.max.spout.pending", 100);
        Config config = new Config();
        config.put("topology.max.spout.pending", 100);
        config.setNumWorkers(numWorkers);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);

        String topicName = OutlierOptions.TOPIC.get(cli);
        String topologyName = "streaming_outliers_" + topicName;
        String zkConnectString = OutlierOptions.ZK_QUORUM.get(cli);
        /*DataPointExtractorConfig extractorConfig
                                                , com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                                                , com.caseystella.analytics.outlier.batch.OutlierConfig batchOutlierConfig
                                                , PersistenceConfig persistenceConfig
                                                , String kafkaTopic
                                                , String zkQuorum
                                                , int numWorkers*/
        boolean startAtBeginning = OutlierOptions.FROM_BEGINNING.has(cli);
        TopologyBuilder topology = createTopology( extractorConfig
                                                 , streamingOutlierConfig
                                                 , persistenceConfig
                                                 , topicName
                                                 , zkConnectString
                                                 , OutlierOptions.ES_NODE.get(cli)
                                                 , numWorkers
                                                 , numSpouts
                                                 , OutlierOptions.NUM_INDEXING_WORKERS.has(cli)?
                                                   Integer.parseInt(OutlierOptions.NUM_INDEXING_WORKERS.get(cli)):
                                                   5
                                                 , OutlierOptions.INDEX.has(cli)?
                                                   OutlierOptions.INDEX.get(cli):
                                                   "{source}/outlier"
                                                 , startAtBeginning
                                                 );
        StormSubmitter.submitTopologyWithProgressBar( topologyName, clusterConf, topology.createTopology());
        //Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
    }
}
