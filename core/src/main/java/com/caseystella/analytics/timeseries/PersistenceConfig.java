package com.caseystella.analytics.timeseries;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PersistenceConfig  implements Serializable {
    TimeseriesDatabaseHandler databaseHandler;
    Map<String, Object> config;
    List<String> tags = new ArrayList<>();
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

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
