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
    public static final String SERIES_TAG_KEY = "series";
    public static final String SERIES_TAG_VALUE = "true";
    public static final String SEVERITY_TAG_KEY = "severity";
    public static final String STREAMING_OUTLIER_SUFFIX = ".outlier.prospective";
    public static final String REAL_OUTLIER_SUFFIX = ".outlier.real";
    Class<? extends TimeseriesDatabaseHandler> clazz;
    TimeseriesDatabaseHandlers(Class<? extends TimeseriesDatabaseHandler> clazz) {
        this.clazz = clazz;
    }

    public static Map<String, String> getBasicTags( DataPoint dp) {
        Map<String, String> tags= new HashMap<>();
        tags.putAll(dp.getMetadata());
        tags.put(SERIES_TAG_KEY, SERIES_TAG_VALUE);
        return tags;
    }

    public static Function<Object, Void> EMPTY_CALLBACK  = new Function<Object, Void>() {
        @Override
        public Void apply(@Nullable Object input) {
            return null;
        }
    };

    public static String getStreamingOutlierMetric(String baseMetric) {
        return baseMetric + STREAMING_OUTLIER_SUFFIX;
    }
    public static String getBatchOutlierMetric(String baseMetric) {
        return baseMetric + STREAMING_OUTLIER_SUFFIX;
    }
    public static Map<String, String> getOutlierTags(final Severity outlierSeverity) {

        if(outlierSeverity == Severity.SEVERE_OUTLIER || outlierSeverity == Severity.MODERATE_OUTLIER)
        {
            return new HashMap<String, String>() {{
                put(SEVERITY_TAG_KEY, outlierSeverity.toString());
            }};
        }
        return null;


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
