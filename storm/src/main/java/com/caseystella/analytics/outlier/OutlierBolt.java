package com.caseystella.analytics.outlier;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.batch.OutlierAlgorithm;
import com.caseystella.analytics.outlier.batch.OutlierConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.timeseries.TSConstants;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.caseystella.analytics.timeseries.tsdb.TSDBHandler;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

public class OutlierBolt extends BaseRichBolt {
    private OutlierConfig outlierConfig;
    private PersistenceConfig persistenceConfig;
    private transient OutlierAlgorithm outlierAlgorithm;
    private transient TimeseriesDatabaseHandler tsdbHandler;
    private transient OutputCollector collector;
    boolean isFirst = true;
    private int headStart = 0;
    public OutlierBolt() {

    }
    public OutlierBolt(OutlierConfig outlierConfig, PersistenceConfig persistenceConfig) {
        withOutlierConfig(outlierConfig);
        withPersistenceConfig(persistenceConfig);
    }
    public OutlierBolt withOutlierConfig(OutlierConfig config) {
        this.outlierConfig = config;
        return this;
    }
    public OutlierBolt withPersistenceConfig(PersistenceConfig config) {
        this.persistenceConfig = config;
        return this;
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
        this.collector = collector;
        outlierAlgorithm = outlierConfig.getAlgorithm();
        outlierAlgorithm.configure(outlierConfig);

        if(!persistenceConfig.getConfig().containsKey(TSConstants.HBASE_CONFIG_KEY)
        && stormConf.containsKey(TSConstants.HBASE_CONFIG_KEY)
          )
        {
            Configuration config = (Configuration) stormConf.get(TSConstants.HBASE_CONFIG_KEY);
            persistenceConfig.getConfig().put(TSConstants.HBASE_CONFIG_KEY, config);
        }
        headStart = outlierConfig.getHeadStart();
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
        if(isFirst) {
            //because our timeseries databases often optimize for throughput rather than latency
            //we could be getting our outliers before they show up in the TSDB due to async puts in
            //the outliercallback.  Thus, we're going to give TSDB a head start;
            try {
                Thread.sleep(headStart);
            } catch (InterruptedException e) {
            }
        }
        Outlier outlier = (Outlier) input.getValueByField(Constants.OUTLIER);
        DataPoint dp = outlier.getDataPoint();
        List<DataPoint> context = tsdbHandler.retrieve(dp.getSource(), dp, outlier.getRange());
        if(context.size() > 0) {
            Outlier realOutlier = outlierAlgorithm.analyze(outlier, context, dp);
            if (realOutlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                //write out to tsdb
                tsdbHandler.persist(TimeseriesDatabaseHandlers.getBatchOutlierMetric(dp.getSource())
                        , dp
                        , TimeseriesDatabaseHandlers.getOutlierTags(realOutlier.getSeverity())
                        , TimeseriesDatabaseHandlers.EMPTY_CALLBACK
                );
                //emit the outlier for downstream processing if necessary.
                collector.emit(ImmutableList.of(input.getValueByField(Constants.MEASUREMENT_ID)
                        , realOutlier
                        )
                );
            }
        }
        collector.ack(input);
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.MEASUREMENT_ID, Constants.OUTLIER));
    }
}
