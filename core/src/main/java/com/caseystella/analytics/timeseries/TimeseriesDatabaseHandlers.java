package com.caseystella.analytics.timeseries;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.timeseries.tsdb.TSDBHandler;
import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum TimeseriesDatabaseHandlers {
    TSDB(TSDBHandler.class)
    ;
    public static final String TYPE_KEY = "type";
    public static final String RAW_TYPE = "raw";
    public static final String PROSPECTIVE_TYPE = "prospective_outlier";
    public static final String OUTLIER_TYPE = "outlier";
    public static final String SEVERITY_TAG_KEY = "severity";
    Class<? extends TimeseriesDatabaseHandler> clazz;
    TimeseriesDatabaseHandlers(Class<? extends TimeseriesDatabaseHandler> clazz) {
        this.clazz = clazz;
    }

    public static Map<String, String> getBasicTags( DataPoint dp, String type) {
        Map<String, String> tags= new HashMap<>();
        tags.putAll(dp.getMetadata());
        tags.put(TYPE_KEY, type);
        return tags;
    }

    public static Function<Object, Void> EMPTY_CALLBACK  = new Function<Object, Void>() {
        @Override
        public Void apply(@Nullable Object input) {
            return null;
        }
    };

    public static Map<String, String> getOutlierTags(final DataPoint dp, final Severity outlierSeverity, final String type) {
        HashMap<String, String> ret = new HashMap<>(dp.getMetadata());
        ret.put(SEVERITY_TAG_KEY, outlierSeverity.toString());
        ret.put(TYPE_KEY, type);
        return ret;
    }




    public TimeseriesDatabaseHandler newInstance()  {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        }
    }
    public static TimeseriesDatabaseHandler newInstance(String outlierPersister) {
        try {
            return TimeseriesDatabaseHandlers.valueOf(outlierPersister).newInstance();
        }
        catch(Throwable t) {
            try {
                return (TimeseriesDatabaseHandler) TimeseriesDatabaseHandler.class.forName(outlierPersister).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the TimeseriesDatabaseHandlers enum", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the TimeseriesDatabaseHandlers enum", e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the TimeseriesDatabaseHandlers enum", e);
            }
        }
    }

}
