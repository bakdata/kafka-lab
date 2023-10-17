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

package com.bakdata.uni.range.indexer.ordered;

import com.bakdata.uni.range.extractor.type.FieldType;
import com.bakdata.uni.range.extractor.value.FieldValueExtractor;
import com.bakdata.uni.range.extractor.value.GenericRecordValueExtractor;
import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.padder.ZeroPadder;
import org.apache.avro.generic.GenericContainer;

/**
 * Implements the logic for range index for incoming data.
 */
public final class WriteRangeIndexer<K, V, F> implements RangeIndexer<K, V> {
    private final FieldValueExtractor<K> keyFieldValueExtractor;
    private final FieldValueExtractor<V> fieldValueExtractor;
    private final ZeroPadder<F> zeroPadder;
    private final String rangeField;
    private final boolean isKey;

    private WriteRangeIndexer(final ZeroPadder<F> zeroPadder,
        final FieldValueExtractor<K> keyFieldValueExtractor,
        final FieldValueExtractor<V> fieldValueExtractor,
        final String rangeField, final boolean isKey) {
        this.keyFieldValueExtractor = keyFieldValueExtractor;
        this.zeroPadder = zeroPadder;
        this.fieldValueExtractor = fieldValueExtractor;
        this.rangeField = rangeField;
        this.isKey = isKey;
    }

    /**
     * Creates the zero padder for the key and sets the range field value extractor based on the
     * schema type.
     */
    public static <K, V, F> WriteRangeIndexer<K, V, F> create(final FieldType fieldType,
        final String rangeField, final boolean isKey) {
        final ZeroPadder<F> zeroPadder = (ZeroPadder<F>) fieldType.getZeroPadder();
        return new WriteRangeIndexer<>(zeroPadder, new GenericRecordValueExtractor<>(),
            new GenericRecordValueExtractor<>(),
            rangeField, isKey);
    }

    /**
     * Creates the range index for a given key over a specific range
     * field.
     *
     * <p>
     * First the value is converted to Avro generic record or Protobuf message. Then the value is extracted from the
     * schema. Depending on the type (integer or long) of the key and value zero paddings are appended to the left side
     * of the key and value, and they are contaminated with an <b>_</b>.
     *
     * <p>
     * Imagine the incoming record has a key of type integer with the value 1. The value is a proto schema with the
     * following schema:
     *
     * <pre>{@code
     * message ProtoRangeQueryTest {
     *   int32 userId = 1;
     *   int32 timestamp = 2;
     * }
     *  }</pre>
     *
     * <p>
     * And the <i>range field</i> is <i>timestamp</i> with the value of 5. The returned value
     * would be 1_0000000005
     */
    @Override
    public String createIndex(final K key, final V value) {
        if (this.isKey) {
            final GenericContainer specificRecord = (GenericContainer) key;
            specificRecord.getSchema().getFields();
            final F number =
                this.keyFieldValueExtractor.extract(key, this.rangeField, this.zeroPadder.getPadderClass());

            final String flatFields = this.keyFieldValueExtractor.flatFields(key, this.rangeField);
            final String paddedValue = this.zeroPadder.padZero(number);

            return this.createRangeIndexFormat(flatFields, paddedValue);
        } else {
            final F number =
                this.fieldValueExtractor.extract(value, this.rangeField, this.zeroPadder.getPadderClass());
            final String paddedValue = this.zeroPadder.padZero(number);
            return this.createRangeIndexFormat(key, paddedValue);
        }
    }
}
