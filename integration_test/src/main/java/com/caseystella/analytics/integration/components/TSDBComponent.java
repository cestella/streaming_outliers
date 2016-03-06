package com.caseystella.analytics.integration.components;

import com.caseystella.analytics.integration.UnableToStartException;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TSDBComponent extends HBaseComponent {
    private TSDB tsdb;
    private List<String> metrics = new ArrayList<>();
    public TSDB getTSDB() {
        return tsdb;
    }
    public TSDBComponent withMetrics(String... metrics) {
        for(String metric : metrics) {
            this.metrics.add(metric);
        }
        return this;
    }

    static class TSDBConfig extends Config {
        public TSDBConfig(Map<String, String> props) throws IOException {
            this(props.entrySet());
        }
        public TSDBConfig(Iterable<Map.Entry<String, String>> props) throws IOException {
            super(false);
            for(Map.Entry<String, String> prop : props) {
                properties.put(prop.getKey(), prop.getValue());
            }
        }
    }
    @Override
    public void start() throws UnableToStartException {
        super.start();
        String zkConnectString = getConfig().get(HConstants.ZOOKEEPER_QUORUM) + ":" + getConfig().get(HConstants.ZOOKEEPER_CLIENT_PORT);
        getConfig().set("tsd.storage.hbase.zk_quorum", zkConnectString);
        getConfig().set("tsd.http.cachedir", "target/tsdb/cache");
        getConfig().set("tsd.http.staticroot", "target/tsdb/staticroot");
        getConfig().set("tsd.core.auto_create_metrics", "true");
        //now we have to create tables
        try {
            getTestingUtility().createTable(Bytes.toBytes("tsdb"), Bytes.toBytes("t"));
            getTestingUtility().createTable(TableName.valueOf("tsdb-uid"), new String[] {"id", "name"});
            getTestingUtility().createTable(Bytes.toBytes("tsdb-tree"), Bytes.toBytes("t"));
            getTestingUtility().createTable(Bytes.toBytes("tsdb-meta"), Bytes.toBytes("name"));
            tsdb = new TSDB(new TSDBConfig(getConfig()));
            if(metrics.size() > 0) {
                final UniqueId uid = new UniqueId(tsdb.getClient(), tsdb.uidTable(), "metrics", (int) 3);
                for (String metric : metrics) {
                    byte[] u = uid.getOrCreateId(metric);
                }
            }
        } catch (IOException e) {
            throw new UnableToStartException("Unable to create TSDB tables", e);
        }


    }
    public void addMetric(String metric) throws IOException {
        final UniqueId uid = new UniqueId(tsdb.getClient(), tsdb.uidTable(), "metric", (int) 3);
        uid.getOrCreateId(metric);
    }
}
