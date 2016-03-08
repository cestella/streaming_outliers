package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.google.common.base.Function;

import java.util.*;

public class InMemoryTimeSeriesDB implements TimeseriesDatabaseHandler {
    public static Map<String, NavigableMap<Long, DataPoint>> backingStore = new HashMap<>();
    public final static Object _sync = new Object();
    @Override
    public void persist( String metric
                                    , DataPoint pt
                                    , Map<String, String> tags
                                    , Function<Object, Void> callback
                                    )
    {
        synchronized(_sync) {
            NavigableMap<Long, DataPoint> tsMap = backingStore.get(metric);
            if (tsMap == null) {
                tsMap = Collections.synchronizedNavigableMap(new TreeMap<Long, DataPoint>());
                backingStore.put(metric, tsMap);
            }
            tsMap.put(pt.getTimestamp(), pt);
        }
    }

    @Override
    public List<DataPoint> retrieve(String metric, DataPoint pt, TimeRange range) {
        synchronized(_sync) {
            NavigableMap<Long, DataPoint> tsMap = backingStore.get(metric);
            List<DataPoint> ret = new ArrayList<>();
            for (Map.Entry<Long, DataPoint> pts : tsMap.subMap(range.getBegin(), true, pt.getTimestamp(), false).entrySet()) {
                ret.add(pts.getValue());
            }
            return ret;
        }
    }

    public static Collection<DataPoint> getAllPoints(String metric) {
        synchronized (_sync) {
            NavigableMap<Long, DataPoint> metricMap = backingStore.get(metric);
            if(metricMap != null) {
                return metricMap.values();
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
