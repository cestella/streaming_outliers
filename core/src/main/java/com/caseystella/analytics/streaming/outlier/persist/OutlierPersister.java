package com.caseystella.analytics.streaming.outlier.persist;

import com.caseystella.analytics.Outlier;
import com.caseystella.analytics.streaming.outlier.OutlierConfig;

import java.util.Map;

public interface OutlierPersister {
    void persist(Outlier outlier);
    void configure(OutlierConfig config, Map<String, Object> context);
}
