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

package com.bakdata.uni.topology;

import com.bakdata.uni.range.RangeField;
import com.bakdata.uni.range.RangeProcessor;
import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.indexer.ordered.WriteRangeIndexer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;

/**
 * Crates the range topology.
 */
@Slf4j
@RequiredArgsConstructor
public class RangeTopology<V> implements TopologyStrategy {
    private static final String RANGE_PROCESSOR_NAME = "range-processor";
    private final StreamsBuilder streamsBuilder;
    private final Serde<V> valueSerde;
    private final RangeField rangeField;
    private final String storeName;

    /**
     * Creates a range topology.
     */
    @Override
    public <K, V> void create(final KStream<K, V> stream) {
        log.info("Setting up the range topology.");

        // key serde is string because the store saves zero padded range index string as keys
        final Serde<String> keySerde = Serdes.String();
        this.streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(this.storeName), keySerde, this.valueSerde)
        );

        final RangeIndexer<K, V> rangeIndexer = WriteRangeIndexer.create(
            this.rangeField.fieldType(),
            this.rangeField.fieldName(),
            this.rangeField.isKey()
        );

        final Named named = Named.as(RANGE_PROCESSOR_NAME + "_" + this.storeName);
        stream.process(() -> new RangeProcessor<>(this.storeName, rangeIndexer),
            named, this.storeName);
    }
}
