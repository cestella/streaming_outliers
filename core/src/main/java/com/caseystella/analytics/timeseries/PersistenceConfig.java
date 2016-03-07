package com.caseystella.analytics.timeseries;

import java.util.Map;

public class PersistenceConfig {
    TimeseriesDatabaseHandler databaseHandler;
    Map<String, Object> config;

    public TimeseriesDatabaseHandler getDatabaseHandler() {
        return databaseHandler;
    }

    public void setDatabaseHandler(String databaseHandler) {
        this.databaseHandler = TimeseriesDatabaseHandlers.newInstance(databaseHandler);
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
