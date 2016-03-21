package com.caseystella.analytics.outlier;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.timeseries.inmemory.InMemoryTimeSeriesDB;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.integration.ComponentRunner;
import com.caseystella.analytics.integration.Processor;
import com.caseystella.analytics.integration.ReadinessState;
import com.caseystella.analytics.integration.UnableToStartException;
import com.caseystella.analytics.integration.components.KafkaWithZKComponent;
import com.caseystella.analytics.integration.components.StormTopologyComponent;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.caseystella.analytics.util.JSONUtil;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.adrianwalker.multilinestring.Multiline;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class StreamingOutlierIntegrationTest {
    /**
    {
         "valueConverter" : "CSVConverter"
       , "valueConverterConfig" : {
                                    "columnMap" : {
                                                  "timestamp" : 0
                                                 ,"data" : 1
                                                  }
                                  }
       , "measurements" : [
            {
             "source" : "benchmark"
            ,"timestampField" : "timestamp"
            ,"timestampConverter" : "DateConverter"
            ,"timestampConverterConfig" : {
                                            "format" : "yyyy-MM-dd HH:mm:ss"
                                          }
            ,"measurementField" : "data"
            }
                          ]
     }
     */
    @Multiline
    public static String extractorConfigStr;

    /**
   {
     "rotationPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 100
                       ,"unit" : "POINTS"
                        }
     ,"chunkingPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 10
                       ,"unit" : "POINTS"
                         }
     ,"globalStatistics" : {
                         }
     ,"outlierAlgorithm" : "SKETCHY_MOVING_MAD"
     ,"config" : {
                 "minAmountToPredict" : 100
                ,"zscoreCutoffs" : {
                                    "NORMAL" : 3.5
                                   ,"MODERATE_OUTLIER" : 5
                                   }
                 }
     }
     */
    @Multiline
    public static String streamingOutlierConfigStr;

    /**
     {
      "algorithm" : "RAD"
     ,"headStart" : 0
     , "config" : {}
     }
     */
    @Multiline
    public static String batchOutlierConfigStr;
    /**
     {
     "databaseHandler" : "com.caseystella.analytics.timeseries.inmemory.InMemoryTimeSeriesDB"
     ,"config" : {}
     }
     */
    @Multiline
    public static String persistenceConfigStr;
    public static String KAFKA_TOPIC = "topic";
    public final static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Function<DataPoint, byte[]> TO_STR = new Function<DataPoint, byte[]>() {
        @Nullable
        @Override
        public byte[] apply(@Nullable DataPoint dataPoint) {
            String ts = format.format(new Date(dataPoint.getTimestamp()));
            return ( ts + "," + dataPoint.getValue()).getBytes();
        }
    };

    public Iterable<byte[]> getMessages() {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        int i = 0;
        for(i = 0; i < 1000*1000;i += 1000) {
            double val = r.nextDouble()*1000;
            DataPoint dp = (new DataPoint(i, val, null, "foo"));
            points.add(dp);
        }
        points.add(new DataPoint(i, 10000, null, "foo"));
        return Iterables.transform(points, TO_STR);
    }
    @Test
    public void test() throws IOException, UnableToStartException {
        DataPointExtractorConfig extractorConfig = JSONUtil.INSTANCE.load(extractorConfigStr
                                                                         , DataPointExtractorConfig.class
                                                                         );
        com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig = JSONUtil.INSTANCE.load(streamingOutlierConfigStr
                                                                         , com.caseystella.analytics.outlier.streaming.OutlierConfig.class
                                                                         );
        com.caseystella.analytics.outlier.batch.OutlierConfig batchOutlierConfig = JSONUtil.INSTANCE.load(batchOutlierConfigStr
                                                                         , com.caseystella.analytics.outlier.batch.OutlierConfig.class
                                                                         );
        PersistenceConfig persistenceConfig = JSONUtil.INSTANCE.load(persistenceConfigStr
                                                                    , PersistenceConfig.class
                                                                    );
        final StringBuffer zkQuorum = new StringBuffer();
        final KafkaWithZKComponent kafkaComponent = new KafkaWithZKComponent().withTopics(new ArrayList<KafkaWithZKComponent.Topic>() {{
            add(new KafkaWithZKComponent.Topic(KAFKA_TOPIC, 1));
        }})
                .withPostStartCallback(new Function<KafkaWithZKComponent, Void>() {
                    @Nullable
                    @Override
                    public Void apply(@Nullable KafkaWithZKComponent kafkaWithZKComponent) {
                        zkQuorum.append(kafkaWithZKComponent.getZookeeperConnect());
                        return null;
                    }
                });
        final StormTopologyComponent stormComponent = new StormTopologyComponent.Builder()
                                                                                .withTopologyName("streaming_outliers")
                                                                                .build();
        ComponentRunner runner = new ComponentRunner.Builder()
                .withComponent("kafka", kafkaComponent)
                .withComponent("storm", stormComponent)
                .withMillisecondsBetweenAttempts(10000)
                .withNumRetries(10)
                .build();
        runner.start();
        try {

            TopologyBuilder topology = Topology.createTopology(extractorConfig
                                                              , streamingOutlierConfig
                                                              , batchOutlierConfig
                                                              , persistenceConfig
                                                              , KAFKA_TOPIC
                                                              , zkQuorum.toString()
                                                              , 1
                                                              , true);
            Config config = new Config();
            stormComponent.submitTopology(topology,config );
            kafkaComponent.writeMessages(KAFKA_TOPIC, getMessages());
            List<DataPoint> outliers =
            runner.process(new Processor<List<DataPoint>>() {
                List<DataPoint> outliers = new ArrayList<>();
                @Override
                public ReadinessState process(ComponentRunner runner) {
                    Collection<DataPoint> points = InMemoryTimeSeriesDB.getAllPoints(TimeseriesDatabaseHandlers.getBatchOutlierMetric(KAFKA_TOPIC + ".benchmark"));
                    if(points.size() > 0) {
                        outliers.addAll(points);
                        return ReadinessState.READY;
                    }
                    return ReadinessState.NOT_READY;
                }

                @Override
                public List<DataPoint> getResult() {
                    return outliers;
                }
            });
            Assert.assertEquals(outliers.size(), 1);
            Assert.assertEquals(outliers.get(0).getTimestamp(), 1000*1000);
        }
        finally {
            runner.stop();
            InMemoryTimeSeriesDB.clear();
        }


    }
}
