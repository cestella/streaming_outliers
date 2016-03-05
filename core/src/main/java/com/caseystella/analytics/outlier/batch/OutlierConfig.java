package com.caseystella.analytics.outlier.batch;

import com.caseystella.analytics.distribution.scaling.ScalingFunctions;

import java.util.HashMap;
import java.util.Map;

public class OutlierConfig {
    private OutlierAlgorithm algorithm;
    private ScalingFunctions scalingFunction = ScalingFunctions.NONE;
    private Map<String, Object> config = new HashMap<>();

    public OutlierAlgorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = OutlierAlgorithms.newInstance(algorithm);
    }

    public ScalingFunctions getScalingFunction() {
        return scalingFunction;
    }

    public void setScalingFunction(ScalingFunctions scalingFunction) {
        this.scalingFunction = scalingFunction;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
