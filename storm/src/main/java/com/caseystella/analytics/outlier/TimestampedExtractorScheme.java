package com.caseystella.analytics.outlier;

import backtype.storm.tuple.Fields;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.extractor.DataPointExtractor;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.google.common.collect.Iterables;
import storm.kafka.KeyValueScheme;

import java.util.ArrayList;
import java.util.List;

public class TimestampedExtractorScheme implements KeyValueScheme {

    private DataPointExtractor extractor = null;
    public TimestampedExtractorScheme(DataPointExtractorConfig config) {
        extractor = new DataPointExtractor(config);
    }
    @Override
    public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        Iterable<DataPoint> dataPoints = extractor.extract(key, value, false);
        List<Object> ret = new ArrayList<>();
        Iterables.addAll(ret, dataPoints);
        return ret;
    }

    @Override
    public List<Object> deserialize(byte[] value) {
        Iterable<DataPoint> dataPoints = extractor.extract(new byte[] {}, value, false);
        List<Object> ret = new ArrayList<>();
        Iterables.addAll(ret, dataPoints);
        return ret;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(Constants.MEASUREMENT_ID, Constants.OUTLIER);
    }
}
