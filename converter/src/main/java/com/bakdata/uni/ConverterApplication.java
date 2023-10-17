package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaStreamsApplication;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

@Slf4j
public class ConverterApplication extends KafkaStreamsApplication {
    public static void main(final String[] args) {
        startApplication(new ConverterApplication(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder streamsBuilder) {
        final Serde<RunnersRawData> valueSerde = new KafkaJsonSchemaSerde<>();
        final Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl(),
                KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, RunnersRawData.class.getName());
        valueSerde.configure(config, false);

        // Your code goes here...!

    }

    @Override
    public Topology createTopology() {
        final Topology topology = super.createTopology();
        log.info("The topology is: \n {}", topology.describe());
        return topology;
    }

    @Override
    public String getUniqueAppId() {
        return String.format("converter-app-%s", this.getOutputTopic());
    }

    private static RunnersStatus toRunnersStatus(final RunnersRawData rawData) {
        return RunnersStatus.newBuilder()
                .setRunnerId(rawData.runnerId())
                .setSession(rawData.session().toLowerCase())
                .setRunTime(rawData.runTime())
                .setDistance((int) rawData.distance())
                .setHeartRate(rawData.heartRate())
                .setSpeed(rawData.speed())
                .build();
    }
}
