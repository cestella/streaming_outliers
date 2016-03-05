package com.caseystella.analytics.timeseries;

import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;

import java.util.Map;

public interface OutlierPersister {
    void persist(Outlier outlier);
    void configure(OutlierConfig config, Map<String, Object> context);
}
