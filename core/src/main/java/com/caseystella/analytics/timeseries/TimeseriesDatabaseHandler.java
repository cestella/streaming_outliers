package com.caseystella.analytics.timeseries;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;
import com.google.common.base.Function;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface TimeseriesDatabaseHandler extends Serializable {
    void persist(String metric, DataPoint pt, Map<String, String> tags, Function<Object, Void> callback);
    List<DataPoint> retrieve(String metric, DataPoint pt, TimeRange range, Map<String, String> filter);
    void configure(Map<String, Object> config);
}
