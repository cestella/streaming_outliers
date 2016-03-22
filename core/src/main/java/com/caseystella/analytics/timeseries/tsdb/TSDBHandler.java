package com.caseystella.analytics.timeseries.tsdb;

import com.caseystella.analytics.timeseries.TSConstants;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;
import com.caseystella.analytics.util.ConfigUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.*;
import net.opentsdb.utils.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TSDBHandler implements TimeseriesDatabaseHandler {
    protected static final Logger LOG = LoggerFactory.getLogger(TSDBHandler.class);
    public static final String DOWNSAMPLE_AGGREGATOR_CONFIG = "downsample_aggregator";
    public static final String DOWNSAMPLE_INTERVAL_CONFIG = "downsample_interval";
    private TSDB tsdb;
    private Aggregator aggregator = null;
    private long sampleInterval;


    public void persist(String metric, DataPoint dp, Map<String, String> tags) {
        persist(metric, dp, tags , null);
    }
    @Override
    public void persist(String metric, DataPoint dp, Map<String, String> tags, final Function<Object, Void> callback) {
        try {
            if(callback == null) {
                tsdb.addPoint(metric, dp.getTimestamp(), dp.getValue(), tags).joinUninterruptibly();
            }
            else {

                Deferred<Object> ret = tsdb.addPoint(metric, dp.getTimestamp(), dp.getValue(), tags);
                ret.addCallback(new Callback<Object, Object>() {
                    @Override
                    public Object call(Object o) throws Exception {
                        return callback.apply(o);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public List<DataPoint> retrieve(String metric, DataPoint pt, TimeRange range, Map<String, String> filter) {
        Query q = tsdb.newQuery();
        long start = range.getBegin();
        long end = pt.getTimestamp();
        if(range.getBegin() == pt.getTimestamp()) {
            start = range.getBegin()-1;
        }
        q.setStartTime(start);
        q.setEndTime(end);
        Map<String, String> tags =
                new HashMap<String, String>(filter == null?new HashMap<String, String>():filter) {{
                            put(TimeseriesDatabaseHandlers.TYPE_KEY, TimeseriesDatabaseHandlers.RAW_TYPE);
                        }};
        q.setTimeSeries(metric
                       , tags
                       , Aggregators.AVG
                       , false
                       );
        if(aggregator != null && sampleInterval > 0) {
            q.downsample(sampleInterval, aggregator);
        }
        net.opentsdb.core.DataPoints[] datapoints = q.run();
        if(datapoints.length == 0) {
            throw new RuntimeException("Unable to retrieve points (empty set)");
        }
        List<DataPoint> ret = new ArrayList<>();
        int total =0;
        for(int j = 0;j < datapoints.length;++j) {
            DataPoints dp = datapoints[j];
            if(LOG.isDebugEnabled() && dp.size() == 0) {
               LOG.debug("Returned 0 sized query.");
            }
            for (int i = 0; i < dp.size(); ++i,++total) {
                double val = dp.doubleValue(i);
                long ts = dp.timestamp(i);
                if(ts <= end && ts >= start) {
                    if(ts != pt.getTimestamp() || val != pt.getValue()) {
                        ret.add(new DataPoint(ts, val, dp.getTags(), metric));
                    }
                }
            }
        }
        {
            String reason = " with range: (" + start + "," + end + ") and grouping: "
                          + Joiner.on(",").join(filter.entrySet());
            LOG.info("Found " + total + " and returned " + ret.size() + reason);
        }
        /*if(ret.size() == 0) {
            throw new RuntimeException("Unable to find any datapoints on " + range + " in " + metric + ": " + tags);
        }*/
        return ret;
    }

    public static class TSDBConfig extends Config {
        public TSDBConfig(Map<String, String> props) throws IOException {
            this(props.entrySet());
        }
        public TSDBConfig(Iterable<Map.Entry<String, String>> props) throws IOException {
            super(false);
            for(Map.Entry<String, String> prop : props) {
                properties.put(prop.getKey(), prop.getValue());
            }
        }
    }

    @Override
    public void configure(Map<String, Object> config) {
        try {
            Map<String, String> s = new HashMap<>();
            for(Map.Entry<String, Object> o : config.entrySet()) {
                if(o.getValue() instanceof String) {
                    s.put(o.getKey(), (String) o.getValue());
                }
            }
            tsdb = new TSDB(new TSDBConfig(s));
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize TSDB connector.", e);
        }
        {
            Object aggObj = config.get(DOWNSAMPLE_AGGREGATOR_CONFIG);
            if(aggObj != null) {
                aggregator = Aggregators.get(aggObj.toString());
            }
        }
        {
            Object aggObj = config.get(DOWNSAMPLE_INTERVAL_CONFIG);
            if(aggObj != null) {
                sampleInterval = ConfigUtil.INSTANCE.coerceLong(DOWNSAMPLE_INTERVAL_CONFIG, aggObj);
            }
        }
    }
}
