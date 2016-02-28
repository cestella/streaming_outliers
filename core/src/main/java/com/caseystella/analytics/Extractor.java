package com.caseystella.analytics;

/**
 * Created by cstella on 2/27/16.
 */
public interface Extractor {
    Iterable<DataPoint> extract(byte[] key, byte[] value);
}
