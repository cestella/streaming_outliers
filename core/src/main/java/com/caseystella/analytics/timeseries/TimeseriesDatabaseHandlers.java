package com.caseystella.analytics.timeseries;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.timeseries.tsdb.TSDBHandler;
import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
    Class<? extends TimeseriesDatabaseHandler> clazz;
    TimeseriesDatabaseHandlers(Class<? extends TimeseriesDatabaseHandler> clazz) {
        this.clazz = clazz;
    }


    public static Function<Object, Void> EMPTY_CALLBACK  = new Function<Object, Void>() {
        @Override
        public Void apply(@Nullable Object input) {
            return null;
        }
    };

    public static Map<String, String> getTags(final DataPoint dp, final String type, List<String> tags) {
        HashMap<String, String> ret = null;
        if(tags == null ) {
            tags = new ArrayList<>();
            //ret = new HashMap<>(dp.getMetadata());
        }
        ret = new HashMap<>();
        for(String tag : tags) {
            String val = dp.getMetadata().get(tag);
            if(val != null) {
                ret.put(tag, val);
            }
        }
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
