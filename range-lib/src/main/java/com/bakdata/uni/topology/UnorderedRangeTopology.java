package com.bakdata.uni.topology;


import com.bakdata.uni.range.RangeField;
import com.bakdata.uni.range.RangeProcessor;
import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.indexer.unordered.UnorderedWriteRangeIndexer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;

@Slf4j
@RequiredArgsConstructor
public class UnorderedRangeTopology<V> implements TopologyStrategy {
    private static final String RANGE_PROCESSOR_NAME = "unordered-range-processor";
    private final StreamsBuilder streamsBuilder;
    private final Serde<V> valueSerde;
    private final RangeField rangeField;
    private final String storeName;

    @Override
    public <K, V> void create(final KStream<K, V> stream) {
        log.info("Setting up the range topology.");

        final Serde<String> keySerde = Serdes.String();
        this.streamsBuilder.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(this.storeName), keySerde, this.valueSerde)
        );

        final RangeIndexer<K, V> rangeIndexer = UnorderedWriteRangeIndexer.create(
            this.rangeField.fieldType(),
            this.rangeField.fieldName(),
            this.rangeField.isKey()
        );

        final Named named = Named.as(RANGE_PROCESSOR_NAME + "_" + this.storeName);
        stream.process(() -> new RangeProcessor<>(this.storeName, rangeIndexer), named, this.storeName);
    }
}
