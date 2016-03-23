package com.caseystella.analytics.outlier;

import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.extractor.DataPointExtractor;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class TimestampedExtractorScheme implements MultiScheme{

    private DataPointExtractor extractor = null;
    private List<String> groupingKeys;
    public TimestampedExtractorScheme(DataPointExtractorConfig config
                                     , List<String> groupingKeys
                                     ) {
        extractor = new DataPointExtractor(config);
        this.groupingKeys = groupingKeys;
    }

    @Override
    public Iterable<List<Object>> deserialize(byte[] value) {
        List<List<Object>> ret = new ArrayList<>();
        Iterable<DataPoint> dataPoints = extractor.extract(new byte[] {}, value, false);
        for(DataPoint dp : dataPoints) {
            String groupId = Outlier.groupingKey(dp, groupingKeys);
            ret.add(ImmutableList.of(groupId, dp));
        }
        return ret;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(Constants.GROUP_ID, Constants.DATA_POINT);
    }
}
