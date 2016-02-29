package com.caseystella.analytics.streaming.outlier;

import com.caseystella.analytics.distribution.RotationConfig;

public class OutlierConfig {
    private RotationConfig rotationPolicy;
    private RotationConfig chunkingPolicy;

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
