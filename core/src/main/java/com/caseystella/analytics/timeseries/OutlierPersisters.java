package com.caseystella.analytics.timeseries;

public enum OutlierPersisters {
    ;
    Class<? extends OutlierPersister> clazz;
    OutlierPersisters(Class<? extends OutlierPersister> clazz) {
        this.clazz = clazz;
    }

    public OutlierPersister newInstance()  {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate outlier algorithm.", e);
        }
    }
    public static OutlierPersister newInstance(String outlierPersister) {
        try {
            return OutlierPersisters.valueOf(outlierPersister).newInstance();
        }
        catch(Throwable t) {
            try {
                return (OutlierPersister) OutlierPersister.class.forName(outlierPersister).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the OutlierPersisters enum", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the OutlierPersisters enum", e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to instantiate " + outlierPersister + " or find it in the OutlierPersisters enum", e);
            }
        }
    }

}
