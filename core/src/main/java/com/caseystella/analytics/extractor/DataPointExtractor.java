package com.caseystella.analytics.extractor;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.Extractor;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataPointExtractor implements Extractor {
    DataPointExtractorConfig config = null;
    public DataPointExtractor() {

    }

    public DataPointExtractor(DataPointExtractorConfig config) {
        this.config =  config;
    }

    public DataPointExtractor withConfig(DataPointExtractorConfig config) {
        this.config = config;
        return this;
    }

    @Override
    public Iterable<DataPoint> extract(byte[] key, byte[] value) {

        Map<String, Object> unionMap = new HashMap<>();
        {
            Map<String, Object> keyMap = config.getKeyConverter().convert(key, config.getKeyConverterConfig());
            Map<String, Object> valueMap = config.getValueConverter().convert(value, config.getValueConverterConfig());
            if (keyMap != null) {
                unionMap.putAll(keyMap);
            }
            if (valueMap != null) {
                unionMap.putAll(valueMap);
            }
        }
        List<DataPoint> ret = new ArrayList<>();
        if(unionMap.size() > 0) {
            for (DataPointExtractorConfig.Measurement measurement : config.getMeasurements()) {
                DataPoint dp = new DataPoint();
                if (measurement.getSource() != null) {
                    dp.setSource(measurement.getSource());
                } else {
                    List<String> sources = new ArrayList<>();
                    for (String sourceField : measurement.getSourceFields()) {
                        sources.add(unionMap.get(sourceField).toString());
                    }
                    dp.setSource(Joiner.on("/").join(sources));
                }
                Object tsObj = unionMap.get(measurement.getTimestampField());
                if(tsObj == null) {
                    throw new RuntimeException("Unable to find " + measurement.getTimestampField() + " in " + unionMap);
                }
                dp.setTimestamp(measurement.getTimestampConverter().convert(tsObj, measurement.getTimestampConverterConfig()));

                Object measurementObj = unionMap.get(measurement.getMeasurementField());
                if(measurementObj == null) {
                    throw new RuntimeException("Unable to find " + measurement.getMeasurementField() + " in " + unionMap);
                }
                dp.setValue(measurement.getMeasurementConverter().convert(measurementObj, measurement.getMeasurementConverterConfig()));

                Map<String, String> metadata = new HashMap<>();
                if (measurement.getMetadataFields() != null && measurement.getMetadataFields().size() > 0) {
                    for (String field : measurement.getMetadataFields()) {
                        metadata.put(field, unionMap.get(field).toString());
                    }
                } else {
                    for (Map.Entry<String, Object> kv : unionMap.entrySet()) {
                        if (!kv.getKey().equals(measurement.getMeasurementField()) && !kv.getKey().equals(measurement.getTimestampField())) {
                            metadata.put(kv.getKey(), kv.getValue().toString());
                        }
                    }
                }
                dp.setMetadata(metadata);
                ret.add(dp);
            }
        }
        return ret;
    }

}
