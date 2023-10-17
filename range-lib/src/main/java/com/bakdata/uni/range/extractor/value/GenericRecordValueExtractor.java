/*
 *    Copyright 2022 bakdata GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.bakdata.uni.range.extractor.value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.KafkaException;


/**
 * Implements the value extraction logic for an Avro's Generic Record.
 */
@Slf4j
public class GenericRecordValueExtractor<V> implements FieldValueExtractor<V> {

    /**
     * Extracts the value from an Avro record for a given field name.
     *
     * @param complexValue The Avro record
     * @param fieldName The name of the field to get extracted
     * @param fieldClass The class of the field
     * @return The field value
     */
    @Override
    public <F> F extract(final V complexValue, final String fieldName, final Class<F> fieldClass) {
        try {
            final GenericRecord record = (GenericRecord) complexValue;
            log.trace("Record value of type Avro Generic Record");
            final Object rangeFieldValue = record.get(fieldName);
            log.trace("Extracted field value is: {}", rangeFieldValue);
            return fieldClass.cast(rangeFieldValue);
        } catch (final AvroRuntimeException exception) {
            final String errorMessage = String.format("Could not find field with name %s", fieldName);
            throw new KafkaException(errorMessage);
        }
    }

    @Override
    public String flatFields(final V complexValue, final String fieldName) {
        try {
            final Collection<Object> stringBuilder = new ArrayList<>();
            final GenericRecord genericRecord = (GenericRecord) complexValue;
            for (final Field field : genericRecord.getSchema().getFields()) {
                if (!field.name().equals(fieldName)) {
                    stringBuilder.add(genericRecord.get(field.name()));
                }
            }
            return stringBuilder.stream()
                .map(Object::toString)
                .collect(Collectors.joining("_"));
        } catch (final AvroRuntimeException exception) {
            final String errorMessage = String.format("Could not find field with name %s", fieldName);
            throw new KafkaException(errorMessage);
        }
    }
}
