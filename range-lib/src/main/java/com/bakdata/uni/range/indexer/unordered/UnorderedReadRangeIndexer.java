package com.bakdata.uni.range.indexer.unordered;

import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.padder.ZeroPadder;
import com.google.common.primitives.Ints;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.KafkaException;

@RequiredArgsConstructor
public class UnorderedReadRangeIndexer<K, F> implements RangeIndexer<K, String> {

    private final ZeroPadder<F> zeroPadder;

    @Override
    public String createIndex(final K key, final String value) {
        if (Ints.tryParse(value) == null) {
            throw new KafkaException("The string value should be a series of digits");
        }
        final F number = this.zeroPadder.getEndOfRange(value);

        return this.createRangeIndexFormat(key, number.toString());
    }
}
