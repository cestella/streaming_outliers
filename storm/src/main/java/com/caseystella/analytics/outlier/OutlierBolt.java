package com.caseystella.analytics.outlier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.streaming.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.timeseries.TSConstants;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class OutlierBolt implements IRichBolt {
    public static String STREAM_ID = "outliers";
    private static final Logger LOG = Logger.getLogger(OutlierBolt.class);
    OutputCollector _collector;
    OutlierConfig outlierConfig;
    OutlierAlgorithm sketchyOutlierAlgorithm;
    com.caseystella.analytics.outlier.batch.OutlierAlgorithm batchOutlierAlgorithm;
    TimeseriesDatabaseHandler tsdbHandler;
    PersistenceConfig persistenceConfig;
    String topic;
    public OutlierBolt(String topic, OutlierConfig outlierConfig, PersistenceConfig persistenceConfig) {
        this.outlierConfig = outlierConfig;
        this.persistenceConfig = persistenceConfig;
        this.topic = topic;
    }

    public static String getMeasureId(String topic, String source) {
        if(source == null || source.length() == 0) {
            return topic;
        }
        else {
            return topic + "." + source;
        }
    }

    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     * <p/>
     * <p>This includes the:</p>
     *
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context   This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        sketchyOutlierAlgorithm = outlierConfig.getSketchyOutlierAlgorithm();
        sketchyOutlierAlgorithm.configure(outlierConfig);
        batchOutlierAlgorithm = outlierConfig.getBatchOutlierAlgorithm();
        batchOutlierAlgorithm.configure(outlierConfig);
        tsdbHandler = persistenceConfig.getDatabaseHandler();
        tsdbHandler.configure(persistenceConfig.getConfig());
    }

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. The IBolt does not have to process the Tuple
     * immediately. It is perfectly fine to hang onto a tuple and process it later
     * (for instance, to do an aggregation or join).
     * <p/>
     * <p>Tuples should be emitted using the OutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some point using the OutputCollector.
     * Otherwise, Storm will be unable to determine when tuples coming off the spouts
     * have been completed.</p>
     * <p/>
     * <p>For the common case of acking an input tuple at the end of the execute method,
     * see IBasicBolt which automates this.</p>
     *
     * @param input The input tuple to be processed.
     */
    @Override
    public void execute(Tuple input) {
        DataPoint dp = (DataPoint)input.getValueByField(Constants.DATA_POINT);
        String measureId = getMeasureId(topic, dp.getSource());
        dp.setSource(measureId);
        //this guy gets persisted to TSDB
        tsdbHandler.persist(dp.getSource()
                , dp
                , TimeseriesDatabaseHandlers.getBasicTags(dp, TimeseriesDatabaseHandlers.RAW_TYPE)
                , TimeseriesDatabaseHandlers.EMPTY_CALLBACK
        );
        //now let's look for outliers
        Outlier outlier = sketchyOutlierAlgorithm.analyze(dp);
        if(outlier.getSeverity() == Severity.SEVERE_OUTLIER) {
            outlier = batchOutlierAlgorithm.analyze(outlier, outlier.getSample(), dp);
            if(outlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                tsdbHandler.persist(dp.getSource()
                        , dp
                        , TimeseriesDatabaseHandlers.getOutlierTags(dp
                                , outlier.getSeverity()
                                , TimeseriesDatabaseHandlers.OUTLIER_TYPE
                        )
                        , TimeseriesDatabaseHandlers.EMPTY_CALLBACK
                );
                try {
                    String json = OutlierHelper.INSTANCE.toJson(dp);
                    LOG.info(json);
                    _collector.emit(STREAM_ID, ImmutableList.<Object>of(json));
                } catch (RuntimeException e) {
                    LOG.error(e.getMessage(), e);
                }
            }

        }
        _collector.ack(input);

    }

    /**
     * Called when an IBolt is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     * <p/>
     * <p>The one context where cleanup is guaranteed to be called is when a topology
     * is killed when running Storm in local mode.</p>
     */
    @Override
    public void cleanup() {

    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_ID, new Fields(ImmutableList.of("outlier")));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
