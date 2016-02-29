package com.caseystella.analytics.streaming.outlier;

import com.caseystella.analytics.DataPoint;

/**
 * Created by cstella on 2/28/16.
 */
public interface OutlierAlgorithm {
    Severity analyze(DataPoint dp);
    void configure(String configStr);

}
