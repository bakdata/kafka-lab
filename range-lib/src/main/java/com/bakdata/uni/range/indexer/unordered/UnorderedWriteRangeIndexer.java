package com.bakdata.uni.range.indexer.unordered;

import com.bakdata.uni.range.extractor.type.FieldType;
import com.bakdata.uni.range.extractor.value.FieldValueExtractor;
import com.bakdata.uni.range.extractor.value.GenericRecordValueExtractor;
import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.padder.ZeroPadder;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;

@RequiredArgsConstructor
public class UnorderedWriteRangeIndexer<K, V, F> implements RangeIndexer<K, V> {
    private final ZeroPadder<F> zeroPadder;
    private final FieldValueExtractor<K> keyFieldValueExtractor;
    private final FieldValueExtractor<V> fieldValueExtractor;
    private final String rangeField;
    private final boolean isKey;

    /**
     * Creates the zero padder for the key and sets the range field value extractor based on the
     * schema type.
     */
    public static <K, V, F> UnorderedWriteRangeIndexer<K, V, F> create(final FieldType fieldType,
        final String rangeField, final boolean isKey) {
        final ZeroPadder<F> zeroPadder = (ZeroPadder<F>) fieldType.getZeroPadder();

        return new UnorderedWriteRangeIndexer<>(zeroPadder,
            new GenericRecordValueExtractor<>(),
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
            final SpecificRecord s = (SpecificRecord) key;
            s.getSchema().getFields();
            final F number =
                this.keyFieldValueExtractor.extract(key, this.rangeField, this.zeroPadder.getPadderClass());

            final String s1 = this.keyFieldValueExtractor.flatFields(key, this.rangeField);

            return this.createRangeIndexFormat(s1, number.toString());
        } else {
            final F number =
                this.fieldValueExtractor.extract(value, this.rangeField, this.zeroPadder.getPadderClass());
            return this.createRangeIndexFormat(key, number.toString());
        }
    }
}
