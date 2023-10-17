package com.bakdata.uni.range.store;


import com.bakdata.uni.range.RangeKeyBuilder;
import com.bakdata.uni.range.indexer.RangeIndexer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Slf4j
@RequiredArgsConstructor
public class RangeKeyValueStore<K, V> {
    private final KafkaStreams streams;
    private final RangeIndexer<String, String> rangeIndexer;
    private final String storeName;
    private final RangeKeyBuilder<K> rangeKeyBuilder;

    public List<V> query(final Collection<K> rangeKeys, final String from, final String to) {
        final String key = this.rangeKeyBuilder.build(rangeKeys);
        final String fromIndex = this.rangeIndexer.createIndex(key, from);
        final String toIndex = this.rangeIndexer.createIndex(key, to);

        log.debug("Index from is: {}", fromIndex);
        log.debug("Index to is: {}", toIndex);

        final List<V> values = new ArrayList<>();

        try (final KeyValueIterator<String, V> iterator = this.getStore().range(fromIndex, toIndex)) {
            while (iterator.hasNext()) {
                values.add(iterator.next().value);
            }
        }
        return values;
    }

    private ReadOnlyKeyValueStore<String, V> getStore() {
        return this.streams.store(
            StoreQueryParameters.fromNameAndType(
                this.storeName,
                QueryableStoreTypes.keyValueStore()
            )
        );
    }
}
