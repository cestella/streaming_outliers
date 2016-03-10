package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.streaming.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.caseystella.analytics.timeseries.TSConstants;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.caseystella.analytics.timeseries.tsdb.TSDBHandler;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import storm.kafka.Callback;
import storm.kafka.EmitContext;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutlierCallback implements Callback {
    OutlierConfig outlierConfig;
    OutlierAlgorithm outlierAlgorithm;
    TimeseriesDatabaseHandler tsdbHandler;
    PersistenceConfig persistenceConfig;

    public OutlierCallback(OutlierConfig outlierConfig, PersistenceConfig persistenceConfig) {
        this.outlierConfig = outlierConfig;
        this.persistenceConfig = persistenceConfig;
    }

    @Override
    public Iterable<List<Object>> apply(List<Object> tuple, EmitContext context) {
        List<List<Object>> ret = new ArrayList<>();
        for(Object o : tuple) {
            DataPoint dp = (DataPoint)o;
            String measureId = context.get(EmitContext.Type.TOPIC) + "." + dp.getSource();
            dp.setSource(measureId);
            //this guy gets persisted to TSDB
            tsdbHandler.persist(dp.getSource()
                           , dp
                           , TimeseriesDatabaseHandlers.getBasicTags(dp)
                           , TimeseriesDatabaseHandlers.EMPTY_CALLBACK
                           );
            //now let's look for outliers
            Outlier outlier = outlierAlgorithm.analyze(dp);
            if(outlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                ret.add(ImmutableList.of(measureId, outlier));
                tsdbHandler.persist(TimeseriesDatabaseHandlers.getStreamingOutlierMetric(dp.getSource())
                           , dp
                           , TimeseriesDatabaseHandlers.getOutlierTags(outlier.getSeverity())
                           , TimeseriesDatabaseHandlers.EMPTY_CALLBACK
                           );
            }

        }
        return ret;
    }

    @Override
    public void initialize(EmitContext context) {
        outlierAlgorithm = outlierConfig.getOutlierAlgorithm();
        outlierAlgorithm.configure(outlierConfig);
        Map stormConf = context.get(EmitContext.Type.STORM_CONFIG);
        if(!persistenceConfig.getConfig().containsKey(TSConstants.HBASE_CONFIG_KEY) && stormConf.containsKey(TSConstants.HBASE_CONFIG_KEY)) {
            Configuration config = (Configuration) stormConf.get(TSConstants.HBASE_CONFIG_KEY);
            persistenceConfig.getConfig().put(TSConstants.HBASE_CONFIG_KEY, config);
        }
        tsdbHandler = persistenceConfig.getDatabaseHandler();
        tsdbHandler.configure(persistenceConfig.getConfig());
        tsdbHandler = persistenceConfig.getDatabaseHandler();
        tsdbHandler.configure(persistenceConfig.getConfig());
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p/>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p/>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p/>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p/>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p/>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p/>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}
