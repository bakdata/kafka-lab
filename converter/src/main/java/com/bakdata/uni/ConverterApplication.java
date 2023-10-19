package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaStreamsApplication;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;

@Slf4j
public class ConverterApplication extends KafkaStreamsApplication {
    public static void main(final String[] args) {
        startApplication(new ConverterApplication(), args);
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

    @Override
    public void buildTopology(final StreamsBuilder streamsBuilder) {
        // Configure value SerDe
        final Serde<RunnersRawData> valueSerde = new KafkaJsonSchemaSerde<>();
        final Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl(),
                KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, RunnersRawData.class.getName());
        valueSerde.configure(config, false);

        // Your code goes here...!

        // 1. Ready the data from the input topic

        // 2. Map the incoming stream of RunnersRawData to RunnersStatus using the function toRunnersStatus

        // 3. Bonus: Handle error using Kafka Error Handling

    }

    @Override
    public String getUniqueAppId() {
        return String.format("converter-app-%s", this.getOutputTopic());
    }
}
