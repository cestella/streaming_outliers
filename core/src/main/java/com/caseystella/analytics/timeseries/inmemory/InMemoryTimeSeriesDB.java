package com.caseystella.analytics.timeseries.inmemory;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;

import java.util.*;

public class InMemoryTimeSeriesDB implements TimeseriesDatabaseHandler {
    public static Map<String, NavigableSet<DataPoint>> backingStore = new HashMap<>();

    public final static Object _sync = new Object();
    public static final Comparator<DataPoint> COMPARATOR = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint o1, DataPoint o2) {
            return  ComparisonChain.start()
                                   .compare(o1.getTimestamp(), o2.getTimestamp())
                                   .compare(o1.getValue(), o2.getValue())
                                   .result() ;
        }
    };
    @Override
    public void persist( String metric
                                    , DataPoint pt
                                    , Map<String, String> tags
                                    , Function<Object, Void> callback
                                    )
    {
        synchronized(_sync) {
            NavigableSet<DataPoint> tsMap = backingStore.get(metric);
            if (tsMap == null) {
                tsMap = Collections.synchronizedNavigableSet(new TreeSet<>(COMPARATOR));
                backingStore.put(metric, tsMap);
            }
            Map<String, String> metadata = pt.getMetadata();
            if(metadata == null) {
                metadata = new HashMap<>();
            }
            if(tags != null) {
                metadata.putAll(tags);
            }
            pt.setMetadata(metadata);
            tsMap.add(pt);
        }
    }

    public static DataPoint getRightEndpoint(long ts) {
        DataPoint dp = new DataPoint();
        dp.setTimestamp(ts);
        dp.setValue(Long.MAX_VALUE);
        return dp;
    }
    public static DataPoint getLeftEndpoint(long ts) {
        DataPoint dp = new DataPoint();
        dp.setTimestamp(ts);
        dp.setValue(Long.MIN_VALUE);
        return dp;
    }
    private static boolean mapContains(Map<String, String> tags, Map<String, String> filter) {
        if(filter != null) {
            for (Map.Entry<String, String> kv : filter.entrySet()) {
                Object o = tags.get(kv.getKey());
                if (o == null || !o.equals(kv.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }
    @Override
    public List<DataPoint> retrieve(String metric, DataPoint pt, TimeRange range, Map<String, String> filter, int maxPts) {
        synchronized(_sync) {
            NavigableSet<DataPoint> tsMap = backingStore.get(metric);
            List<DataPoint> ret = new ArrayList<>();
            for (DataPoint pts : tsMap.subSet(getLeftEndpoint(range.getBegin())
                                                              , true
                                                              , getRightEndpoint(pt.getTimestamp())
                                                              , true
                                                              )) {
                if(pts.getTimestamp() != pt.getTimestamp() && pts.getValue() != pt.getValue() && mapContains(pts.getMetadata(), filter)) {
                    ret.add(pts);
                }
            }
            return ret;
        }
    }

    public static Collection<DataPoint> getAllPoints(String metric) {
        synchronized (_sync) {
            NavigableSet<DataPoint> metricMap = backingStore.get(metric);
            if(metricMap != null) {
                return metricMap;
            }
            else {
                return Collections.EMPTY_LIST;
            }
        }
    }
    public static void clear() {
        synchronized(_sync) {
            backingStore.clear();
        }
    }

    @Override
    public void configure(Map<String, Object> config) {

    }
}
