package com.caseystella.analytics.outlier.streaming;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;

/**
 * Created by cstella on 2/28/16.
 */
public interface OutlierAlgorithm {
    Outlier analyze(DataPoint dp);
    void configure(OutlierConfig configStr);

}
