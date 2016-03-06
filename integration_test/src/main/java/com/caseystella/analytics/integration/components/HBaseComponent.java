package com.caseystella.analytics.integration.components;

import com.caseystella.analytics.integration.InMemoryComponent;
import com.caseystella.analytics.integration.UnableToStartException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import java.io.IOException;

public class HBaseComponent implements InMemoryComponent{

    private boolean startMR = false;
    private Configuration config;
    private HBaseTestingUtility testingUtility;

    public HBaseComponent withMR() {
        startMR = true;
        return this;
    }

    public Configuration getConfig() {
        return config;
    }

    public HBaseTestingUtility getTestingUtility() {
        return testingUtility;
    }

    @Override
    public void start() throws UnableToStartException {
        config = HBaseConfiguration.create();
        config.set("hbase.master.hostname", "localhost");
        config.set("hbase.regionserver.hostname", "localhost");
        testingUtility = new HBaseTestingUtility(config);

        try {
            testingUtility.startMiniCluster(1);
        } catch (Exception e) {
            throw new UnableToStartException(e.getMessage(), e);
        }
        if(startMR) {
            try {
                testingUtility.startMiniMapReduceCluster();
            } catch (IOException e) {
                throw new UnableToStartException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void stop() {
        testingUtility.shutdownMiniMapReduceCluster();
        try {
            testingUtility.shutdownMiniCluster();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        try {
            testingUtility.cleanupTestDir();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
