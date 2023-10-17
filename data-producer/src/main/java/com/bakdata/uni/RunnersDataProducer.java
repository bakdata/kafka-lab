package com.bakdata.uni;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.KafkaProducerApplication;
import com.bakdata.kafkasummit.RunnersRawData;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Resources;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
        // Your code goes here!
        try (final KafkaProducer<Void, RunnersRawData> producer = this.createProducer()) {
            final URL url = Resources.getResource(this.fileName);
            final CsvSchema schema = CsvSchema.emptySchema().withHeader();
            try (final MappingIterator<RunnersRawData> mappingIterator =
                    this.csvMapper.readerFor(RunnersRawData.class)
                            .with(schema)
                            .readValues(url)) {
                while (mappingIterator.hasNext()) {
                    final RunnersRawData runnersRawData = mappingIterator.next();
                    final ProducerRecord<Void, RunnersRawData> record =
                            new ProducerRecord<>(this.getOutputTopic(), null, runnersRawData);
                    producer.send(record);
                }
            }
            producer.flush();
        } catch (final IOException exception) {
            log.error("Error occurred while reading the .csv file. \n {}", exception);
        }
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
