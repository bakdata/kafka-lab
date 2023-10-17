package com.bakdata.uni.range;

import java.util.Collection;
import java.util.stream.Collectors;

public interface RangeKeyBuilder<K> {
    default String build(final Collection<K> rangeKeys) {
        return rangeKeys.stream()
            .map(Object::toString)
            .collect(Collectors.joining("_"));
    }
}
