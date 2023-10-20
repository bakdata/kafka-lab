package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaProducerApplication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.VoidSerializer;

@Slf4j
@Setter
@RequiredArgsConstructor
public class RunnersDataProducer extends KafkaProducerApplication {
    private final String fileName;
    private final ObjectMapper csvMapper = new CsvMapper().registerModule(new JavaTimeModule());

    public static void main(final String[] args) {
        startApplication(new RunnersDataProducer("data.csv"), args);
    }

    @Override
    protected void runApplication() {
        log.info("Starting runners producer...");
        // Your code goes here...!

        // 1. create a producer

        // 2. read the CSV records line by line to JSON

        // 3. send record to topic
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProperties.put(SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        return kafkaProperties;
    }
}
