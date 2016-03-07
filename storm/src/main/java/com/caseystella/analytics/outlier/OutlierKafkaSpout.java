package com.caseystella.analytics.outlier;

import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.timeseries.PersistenceConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import storm.kafka.Callback;
import storm.kafka.CallbackKafkaSpout;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.SpoutConfig;

import java.util.ArrayList;

public class OutlierKafkaSpout extends CallbackKafkaSpout {
    private OutlierConfig outlierConfig;
    private PersistenceConfig persistenceConfig;
    public OutlierKafkaSpout( SpoutConfig spoutConfig
                            , OutlierConfig outlierConfig
                            , DataPointExtractorConfig extractorConfig
                            , PersistenceConfig persistenceConfig
                            , String zkConnectString
                            )
    {
        super(spoutConfig, OutlierCallback.class);
        this.persistenceConfig = persistenceConfig;
        this.outlierConfig = outlierConfig;
        spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TimestampedExtractorScheme(extractorConfig));
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



    @Override
    protected Callback createCallback(Class<? extends Callback> callbackClass) {
        return new OutlierCallback(outlierConfig, persistenceConfig);
    }
}
