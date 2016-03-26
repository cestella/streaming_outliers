package com.caseystella.analytics.outlier;

import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import storm.kafka.*;

import java.util.ArrayList;
import java.util.List;

public class OutlierKafkaSpout extends KafkaSpout{
    public OutlierKafkaSpout( SpoutConfig spoutConfig
                            , DataPointExtractorConfig extractorConfig
                            , List<String> groupingKeys
                            , String zkConnectString
                            )
    {
        super(spoutConfig);
        spoutConfig.scheme = new TimestampedExtractorScheme(extractorConfig, groupingKeys);
        if(zkConnectString != null && zkConnectString.length() > 0) {
            boolean isFirst = true;
            for (String hostPort : Splitter.on(',').split(zkConnectString)) {
                Iterable<String> hp = Splitter.on(':').split(hostPort);
                String host = Iterables.getFirst(hp, null);
                if (host != null) {
                    if (isFirst) {
                        spoutConfig.zkServers = new ArrayList<>();
                        spoutConfig.zkPort = Integer.parseInt(Iterables.getLast(hp));
                        isFirst = false;
                    }
                    spoutConfig.zkServers.add(host);
                }
            }
        }
    }

}
