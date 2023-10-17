package com.bakdata.uni.range;


import com.bakdata.uni.range.extractor.type.FieldType;

public record RangeField(
    FieldType fieldType,
    String fieldName,
    boolean isKey
) {
}
