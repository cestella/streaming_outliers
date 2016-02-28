package com.caseystella.analytics.converters;

import java.util.Map;

public interface Converter<T, S> {
    T convert(S in, Map<String, Object> config );
}
