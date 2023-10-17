package com.bakdata.kafkasummit;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.Wait.delay;

import com.bakdata.uni.RunnersDataProducer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class RunnersDataProducerIntegrationTest {
    private static final int TIMEOUT_SECONDS = 10;
    private static final String OUTPUT_TOPIC = "raw-game-data";
    private final EmbeddedKafkaCluster kafkaCluster = provisionWith(defaultClusterConfig());
    private RunnersDataProducer dataProducer;

    private static final String SCHEMA_REGISTRY_URL = "mock://test123";

    @InjectSoftAssertions
    private SoftAssertions softly;

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
        this.dataProducer = this.setupApp();
    }

    @AfterEach
    void teardown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldRunApp() throws InterruptedException {
        this.kafkaCluster.createTopic(TopicConfig.withName(OUTPUT_TOPIC).useDefaults());

        this.dataProducer.run();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final List<KeyValue<Void, RunnersRawData>> values = this.kafkaCluster.read(
            ReadKeyValues
                .from(OUTPUT_TOPIC, Void.class, RunnersRawData.class)
                .with(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class)
                .with(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, RunnersRawData.class.getName())
                .build()
        );

        this.softly.assertThat(values)
            .hasSize(4)
            .allSatisfy(keyValue -> this.softly.assertThat(keyValue.getKey()).isNull())
            .allSatisfy(keyValue -> this.softly.assertThat(keyValue.getValue().runnerId()).isEqualTo("123"))
            .satisfiesExactly(
                keyValue -> this.softly.assertThat(keyValue.getValue().runTime())
                    .isEqualTo(0),
                keyValue -> this.softly.assertThat(keyValue.getValue().runTime())
                    .isEqualTo(1),
                keyValue -> this.softly.assertThat(keyValue.getValue().runTime())
                    .isEqualTo(2),
                keyValue -> this.softly.assertThat(keyValue.getValue().runTime())
                    .isEqualTo(3)
            );
    }

    private RunnersDataProducer setupApp() {
        final RunnersDataProducer producerApp = new RunnersDataProducer("test-data.csv");
        producerApp.setBrokers(this.kafkaCluster.getBrokerList());
        producerApp.setSchemaRegistryUrl(SCHEMA_REGISTRY_URL);
        producerApp.setOutputTopic(OUTPUT_TOPIC);
        producerApp.setStreamsConfig(Map.of(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"));
        return producerApp;
    }
}
