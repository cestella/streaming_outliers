package com.caseystella.analytics.outlier.streaming;

import com.caseystella.analytics.distribution.GlobalStatistics;
import com.caseystella.analytics.distribution.config.RotationConfig;
import com.caseystella.analytics.distribution.scaling.ScalingFunctions;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandler;
import com.caseystella.analytics.timeseries.TimeseriesDatabaseHandlers;

import java.util.HashMap;
import java.util.Map;

public class OutlierConfig {
    private RotationConfig rotationPolicy;
    private RotationConfig chunkingPolicy;
    private GlobalStatistics globalStatistics;
    private OutlierAlgorithm outlierAlgorithm;
    private ScalingFunctions scalingFunction = null;
    private Map<String, Object> config = new HashMap<>();


    public Map<String, Object> getConfig() {
        return config;
    }

    public GlobalStatistics getGlobalStatistics() {
        return globalStatistics;
    }

    public OutlierAlgorithm getOutlierAlgorithm() {
        return outlierAlgorithm;
    }

    public void setOutlierAlgorithm(String outlierAlgorithm) {
        this.outlierAlgorithm = OutlierAlgorithms.newInstance(outlierAlgorithm);
    }

    public void setGlobalStatistics(GlobalStatistics globalStatistics) {
        this.globalStatistics = globalStatistics;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public ScalingFunctions getScalingFunction() {
        if(scalingFunction != null) {
            return scalingFunction;
        }
        else {
            if(globalStatistics != null && globalStatistics.getMin() != null && globalStatistics.getMin() < 0) {
                scalingFunction = ScalingFunctions.SHIFT_TO_POSITIVE;
            }
            else {
                scalingFunction = ScalingFunctions.NONE;
            }
            return scalingFunction;
        }
    }

    public void setScalingFunction(ScalingFunctions scalingFunction) {
        this.scalingFunction = scalingFunction;
    }

    public RotationConfig getRotationPolicy() {
        return rotationPolicy;
    }

    public void setRotationPolicy(RotationConfig rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
    }

    public RotationConfig getChunkingPolicy() {
        return chunkingPolicy;
    }

    public void setChunkingPolicy(RotationConfig chunkingPolicy) {
        this.chunkingPolicy = chunkingPolicy;
    }

}
