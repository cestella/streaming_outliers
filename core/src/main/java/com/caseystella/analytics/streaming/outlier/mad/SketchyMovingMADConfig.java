package com.caseystella.analytics.streaming.outlier.mad;

import com.caseystella.analytics.streaming.outlier.OutlierConfig;
import com.caseystella.analytics.streaming.outlier.Severity;

import java.util.EnumMap;
import java.util.Map;

public class SketchyMovingMADConfig extends OutlierConfig {
    private Map<Severity, Double> zScoreCutoffs = new EnumMap<>(Severity.class);
    private long minAmountToPredict;

    public long getMinAmountToPredict() {
        return minAmountToPredict;
    }

    public void setMinAmountToPredict(long minAmountToPredict) {
        this.minAmountToPredict = minAmountToPredict;
    }

    public Map<Severity, Double> getzScoreCutoffs() {
        return zScoreCutoffs;
    }

    public void setzScoreCutoffs(Map<String, Double> zScoreCutoffs) {
        for(Map.Entry<String, Double> kv : zScoreCutoffs.entrySet()) {
            this.zScoreCutoffs.put(Severity.valueOf(kv.getKey()), kv.getValue());
        }
    }
}
