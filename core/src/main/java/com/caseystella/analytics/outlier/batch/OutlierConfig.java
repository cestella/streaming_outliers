package com.caseystella.analytics.outlier.batch;

import com.caseystella.analytics.distribution.scaling.ScalingFunctions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OutlierConfig implements Serializable {
    private OutlierAlgorithm algorithm;
    private ScalingFunctions scalingFunction = ScalingFunctions.NONE;
    private int headStart = 30*1000;
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

    public int getHeadStart() {
        return headStart;
    }

    public void setHeadStart(int headStart) {
        this.headStart = headStart;
    }
}
