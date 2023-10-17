package com.bakdata.uni;

import static com.bakdata.kafka.ErrorCapturingValueMapper.captureErrors;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import com.bakdata.kafka.AvroDeadLetterConverter;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.ProcessedValue;
import com.bakdata.kafkasummit.RunnersRawData;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.VoidSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

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

        final KStream<Void, RunnersRawData> inputStream = streamsBuilder
            .stream(this.getInputTopics(), Consumed.with(new VoidSerde(), valueSerde));

        final KStream<Void, ProcessedValue<RunnersRawData, RunnersStatus>> mappedWithErrors =
            inputStream.mapValues(captureErrors(ConverterApplication::toRunnersStatus));

        mappedWithErrors.flatMapValues(ProcessedValue::getErrors)
            .processValues(AvroDeadLetterConverter.asProcessor("Could not map values to Avro"))
            .to(this.getErrorTopic());

        final KStream<Void, RunnersStatus> mapped = mappedWithErrors.flatMapValues(ProcessedValue::getValues);
        mapped.to(this.getOutputTopic());
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
