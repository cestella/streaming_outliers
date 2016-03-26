package com.caseystella.analytics.outlier;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.integration.components.ElasticSearchComponent;
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
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.adrianwalker.multilinestring.Multiline;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
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
     ,"sketchyOutlierAlgorithm" : "SKETCHY_MOVING_MAD"
     ,"batchOutlierAlgorithm" : "RAD"
     ,"config" : {
                 "minAmountToPredict" : 100
                ,"reservoirSize" : 100
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
     "databaseHandler" : "com.caseystella.analytics.timeseries.inmemory.InMemoryTimeSeriesDB"
     ,"config" : {}
     }
     */
    @Multiline
    public static String persistenceConfigStr;
    public static String KAFKA_TOPIC = "topic";
    private static String indexDir = "target/elasticsearch";
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
        final ElasticSearchComponent esComponent = new ElasticSearchComponent.Builder()
                .withHttpPort(9211)
                .withIndexDir(new File(indexDir))
                .build();
        ComponentRunner runner = new ComponentRunner.Builder()
                .withComponent("es", esComponent)
                .withComponent("kafka", kafkaComponent)
                .withComponent("storm", stormComponent)
                .withMillisecondsBetweenAttempts(10000)
                .withNumRetries(100)
                .build();
        runner.start();
        try {

            TopologyBuilder topology = Topology.createTopology(extractorConfig
                                                              , streamingOutlierConfig
                                                              , persistenceConfig
                                                              , KAFKA_TOPIC
                                                              , zkQuorum.toString()
                                                              , "localhost:9211"
                                                              , 1
                                                              , 1
                                                              , 1
                                                              , "outliers"
                                                              , true);
            Config config = new Config();
            stormComponent.submitTopology(topology,config );
            kafkaComponent.writeMessages(KAFKA_TOPIC, getMessages());
            final String indexName = KAFKA_TOPIC + ".benchmark";
            List<DataPoint> outliers =
            runner.process(new Processor<List<DataPoint>>() {
                List<DataPoint> outliers = new ArrayList<>();
                @Override
                public ReadinessState process(ComponentRunner runner) {
                    Collection<DataPoint> allPoints = InMemoryTimeSeriesDB.getAllPoints(KAFKA_TOPIC + ".benchmark");
                    Iterable<DataPoint> outlierPoints = Iterables.filter(allPoints, new Predicate<DataPoint>() {
                        @Override
                        public boolean apply(@Nullable DataPoint dataPoint) {
                            String type = dataPoint.getMetadata().get(TimeseriesDatabaseHandlers.TYPE_KEY);
                            return type != null && type.equals(TimeseriesDatabaseHandlers.OUTLIER_TYPE);

                        }
                    });
                    boolean isDone = false;
                    try {
                        isDone = esComponent.hasIndex(indexName)
                                && esComponent.getAllIndexedDocs(indexName).size() > 0;
                    } catch (IOException e) {
                        //swallow
                        e.printStackTrace();
                    }
                    if(isDone && Iterables.size(outlierPoints) > 0) {
                        Iterables.addAll(outliers, outlierPoints);
                        return ReadinessState.READY;
                    }
                    return ReadinessState.NOT_READY;
                }

                @Override
                public List<DataPoint> getResult() {
                    return outliers;
                }
            });
            List<Map<String, Object>> outlierIndex = esComponent.getAllIndexedDocs(indexName);
            for(Map<String, Object> o : outlierIndex) {
                System.out.println(o);
            }
            Assert.assertEquals(outlierIndex.size(),1);
            Assert.assertEquals(outliers.size(), 1);
            Assert.assertEquals(outliers.get(0).getTimestamp(), 1000*1000);
        }
        finally {
            runner.stop();
            InMemoryTimeSeriesDB.clear();
        }


    }
}
