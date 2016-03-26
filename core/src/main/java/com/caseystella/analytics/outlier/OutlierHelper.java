package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.util.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.*;

public enum OutlierHelper {
    INSTANCE;
    public String toJson(DataPoint dp) {
        Map<String, Object> json = new HashMap<>();
        json.put("timestamp", dp.getTimestamp());
        json.put("value", dp.getValue());
        json.put("source", dp.getSource());
        Set<String> constants = new HashSet<>();
        for(OutlierMetadataConstants constant : OutlierMetadataConstants.values()) {
            constants.add(constant.toString());
            json.put(constant.toString(), Double.valueOf(dp.getMetadata().get(constant.toString())));
        }
        for(Map.Entry<String, String> kv : dp.getMetadata().entrySet()) {
            if(!constants.contains(kv.getKey())) {
                json.put(kv.getKey(), kv.getValue());
            }
        }
        try {
            return JSONUtil.INSTANCE.toJSON(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to convert to json: " + Joiner.on(',').join(json.entrySet()), e);
        }
    }
}
