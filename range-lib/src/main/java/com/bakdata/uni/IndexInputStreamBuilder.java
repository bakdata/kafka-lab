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

package com.bakdata.uni;

import com.bakdata.uni.range.RangeKeyBuilder;
import com.bakdata.uni.range.extractor.value.FieldValueExtractor;
import com.bakdata.uni.range.extractor.value.GenericRecordValueExtractor;
import java.util.ArrayList;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Repartitioned;

/**
 * Contains the logic of consuming the input topic and repartitioning of the topic if a rangeKey field is given.
 */
@RequiredArgsConstructor
public final class IndexInputStreamBuilder {
    private final RangeKeyBuilder<String> rangeKeyBuilder;

    public <K, V> KStream<String, V> repartitionOnRangeKey(final KStream<K, V> inputStream,
        final Collection<String> rangeKey) {
        if (rangeKey.isEmpty()) {
            throw new KafkaException("The range key list should not be empty");
        }
        return inputStream.selectKey(
            (key, value) -> this.extractRangeKeys(rangeKey, value), Named.as("rangeKeySelector")
        ).repartition(Repartitioned.with(new StringSerde(), null));
    }

    /**
     * Extracts the value of a given field.
     */
    private <V> String extractRangeKeys(final Iterable<String> fieldNameList, final V value) {
        if (value != null) {
            final Collection<String> stringBuilder = new ArrayList<>();
            final FieldValueExtractor<V> fieldValueExtractor = new GenericRecordValueExtractor<>();
            for (final String fieldName : fieldNameList) {
                stringBuilder.add(fieldValueExtractor.extract(value, fieldName, String.class));
            }
            return this.rangeKeyBuilder.build(stringBuilder);
        }
        throw new KafkaException("The value should not be null. Check you input topic data.");
    }
}
