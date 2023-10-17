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


import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.padder.ZeroPadder;
import com.google.common.primitives.Ints;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.KafkaException;

/**
 * An indexer for string values. This indexer is used to build the default range index pattern when querying for
 * ranges.
 */
@RequiredArgsConstructor
public final class ReadRangeIndexer<K, F> implements RangeIndexer<K, String> {

    private final ZeroPadder<F> zeroPadder;

    @Override
    public String createIndex(final K key, final String value) {
        if (Ints.tryParse(value) == null) {
            throw new KafkaException("The string value should be a series of digits");
        }
        final F number = this.zeroPadder.getEndOfRange(value);
        final String paddedValue = this.zeroPadder.padZero(number);

        return this.createRangeIndexFormat(key, paddedValue);
    }
}
