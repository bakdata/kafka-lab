package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaStreamsApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.VoidSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
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

    private static CountAndSum getCountAndSumOfHeartRate(final RunnersStatus value, final CountAndSum aggregate) {
        aggregate.setCount(aggregate.getCount() + 1);
        aggregate.setSum(aggregate.getSum() + value.getHeartRate());
        return aggregate;
    }

    @Override
    public void buildTopology(final StreamsBuilder streamsBuilder) {
        // create TimeWindow
        final TimeWindows windows = TimeWindows.ofSizeAndGrace(this.windowSize, this.gracePeriod);

        // Read the input stream from the input topic and define the time stamp
        final KStream<Void, RunnersStatus> inputStream = streamsBuilder
                .stream(this.getInputTopics(),
                        Consumed.with(new VoidSerde(), this.getRunnerStatusSerde())
                                .withTimestampExtractor(new RunTimeExtractor()));

        final SpecificAvroSerde<CountAndSum> countAndSumSerde = this.getCountAndSumSerde();

        // Your code goes here...!

        // 1. select the key you want to group the data with --> <runnerId>_<runSession>

        // 2. group the key

        // 3. window your group

        // 4. aggregate the data. Use the count and sum function

        // 5. map your aggregated keys to this format --> <runnerId>_<runSession>_<windowsStartTime>
    }

    @Override
    public Topology createTopology() {
        final Topology topology = super.createTopology();
        log.info("The topology is: \n {}", topology.describe());
        return topology;
    }

    @Override
    public String getUniqueAppId() {
        return String.format("windowing-app-%s", this.getOutputTopic());
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaConfig = super.createKafkaProperties();
        kafkaConfig.setProperty(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        return kafkaConfig;
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

}
