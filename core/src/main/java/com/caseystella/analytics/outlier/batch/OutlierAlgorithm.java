package com.caseystella.analytics.outlier.batch;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;

import java.util.List;

/**
 * Created by cstella on 3/5/16.
 */
public interface OutlierAlgorithm {
    Outlier analyze(Outlier outlierCandidate, List<DataPoint> context, DataPoint dp);
    void configure(OutlierConfig configStr);
}
