package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaStreamsApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.VoidSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import picocli.CommandLine;

@Slf4j
@Setter
public class WindowingApplication extends KafkaStreamsApplication {
    @CommandLine.Option(names = "--window-duration",
            description = "The size of the window in seconds. Must be larger than zero.")
    private Duration windowSize = Duration.ofSeconds(30);

    @CommandLine.Option(names = "--grace-period",
            description = "The grace period in millis to admit out-of-order events to a window. Must be non-negative.")
    private Duration gracePeriod = Duration.ofMillis(500);


    public static void main(final String[] args) {
        startApplication(new WindowingApplication(), args);
    }


    @Override
    public void buildTopology(final StreamsBuilder streamsBuilder) {
        final TimeWindows windows = TimeWindows.ofSizeAndGrace(this.windowSize, this.gracePeriod);

        final KStream<Void, RunnersStatus> inputStream = streamsBuilder
                .stream(this.getInputTopics(),
                        Consumed.with(new VoidSerde(), this.getRunnerStatusSerde())
                                .withTimestampExtractor(new RunTimeExtractor()));

        final SpecificAvroSerde<CountAndSum> countAndSumSerde = this.getCountAndSumSerde();
        final KTable<String, CountAndSum> heartRateCountAndSum =
                inputStream.selectKey((key, value) -> value.getRunnerId() + "_" + value.getSession())
                        .peek((key, value) -> log.error("key={}, value={}", key, value))
                        .groupByKey(Grouped.with(Serdes.String(), this.getRunnerStatusSerde()))
                        .windowedBy(windows)
                        .aggregate(() -> new CountAndSum(0L, 0.0),
                                (key, value, aggregate) -> getCountAndSumOfHeartRate(value, aggregate),
                                Materialized.with(Serdes.String(), countAndSumSerde)
                        )
                        .toStream()

                        .map((Windowed<String> key, CountAndSum count) ->
                                new KeyValue<>("%s_%s".formatted(key.key(), key.window().start()), count))

                        .toTable(Materialized.with(Serdes.String(), countAndSumSerde));

        final KTable<String, Double> averageHeartRate =
                heartRateCountAndSum.mapValues(value -> value.getSum() / value.getCount(),
                        Named.as("Average-Heart-Rate"),
                        Materialized.with(Serdes.String(), Serdes.Double()));

        averageHeartRate.toStream()
                .peek(
                        (key, value) -> log.error("Aggregated value key is ={}, value={}", key, value)
                )
                .to(this.getOutputTopic(), Produced.with(Serdes.String(), Serdes.Double()));
    }

    private static CountAndSum getCountAndSumOfHeartRate(final RunnersStatus value, final CountAndSum aggregate) {
        aggregate.setCount(aggregate.getCount() + 1);
        aggregate.setSum(aggregate.getSum() + value.getHeartRate());
        return aggregate;
    }

    private SpecificAvroSerde<CountAndSum> getCountAndSumSerde() {
        final SpecificAvroSerde<CountAndSum> serde = new SpecificAvroSerde<>();
        serde.configure(this.getSerdeConfig(), false);
        return serde;
    }

    private SpecificAvroSerde<RunnersStatus> getRunnerStatusSerde() {
        final SpecificAvroSerde<RunnersStatus> serde = new SpecificAvroSerde<>();
        serde.configure(this.getSerdeConfig(), false);
        return serde;
    }

    private Map<String, String> getSerdeConfig() {
        return Map.of(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
    }


    @Override
    public Topology createTopology() {
        final Topology topology = super.createTopology();
        log.info("The topology is: \n {}", topology.describe());
        return topology;
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaConfig = super.createKafkaProperties();
        kafkaConfig.setProperty(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        return kafkaConfig;
    }

    @Override
    public String getUniqueAppId() {
        return String.format("windowing-app-%s", this.getOutputTopic());
    }

}
