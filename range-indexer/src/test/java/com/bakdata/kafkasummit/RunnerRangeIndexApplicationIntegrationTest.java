package com.bakdata.kafkasummit;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

import com.bakdata.uni.RunnerRangeIndexerApplication;
import com.bakdata.uni.RunnersStatus;
import com.bakdata.uni.proto.RangeIndexerServiceGrpc;
import com.bakdata.uni.proto.RangeRequest;
import com.bakdata.uni.proto.RunnerAnalytics;
import com.bakdata.uni.proto.RunnerStatusReply;
import com.bakdata.uni.proto.Store;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class RunnerRangeIndexApplicationIntegrationTest {
    public static final String INPUT = "runners-status";
    private static final EmbeddedKafkaCluster kafkaCluster =
        provisionWith(EmbeddedKafkaClusterConfig.defaultClusterConfig());
    private static final String SCHEMA_REGISTRY_URL = "mock://test123";
    private final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:9090")
        .usePlaintext()
        .build();

    private final RangeIndexerServiceGrpc.RangeIndexerServiceBlockingStub stub =
        RangeIndexerServiceGrpc.newBlockingStub(this.channel);
    @InjectSoftAssertions
    private SoftAssertions softly;

    @BeforeAll
    static void setup() throws InterruptedException {
        kafkaCluster.start();

        final RunnerRangeIndexerApplication app = createExtractionApp();
        final Thread runThread = new Thread(app);
        runThread.start();

        produceDataToInputTopic();
        Thread.sleep(3000);
    }

    @AfterAll
    static void tearDown() {
        kafkaCluster.stop();
    }

    private static RunnerRangeIndexerApplication createExtractionApp() {
        final RunnerRangeIndexerApplication app = new RunnerRangeIndexerApplication();
        app.setInputTopics(List.of(INPUT));
        app.setBrokers(kafkaCluster.getBrokerList());
        app.setProductive(false);
        app.setSchemaRegistryUrl(SCHEMA_REGISTRY_URL);
        return app;
    }

    @Test
    void shouldGetOrderedRange() {
        final RangeRequest rangeRequest = RangeRequest.newBuilder()
            .setRunnerId("123")
            .setSessionId("xyz")
            .setStore(Store.ORDERED_RUN_TIME)
            .setFrom(0)
            .setTo(5)
            .build();
        final RunnerStatusReply runnerStatusRange = this.stub.getRunnerStatusRange(rangeRequest);
        final List<RunnerAnalytics> runnerStatusListList = runnerStatusRange.getRunnerStatusListList();
        this.softly.assertThat(runnerStatusListList)
            .hasSize(6);
    }

    @Test
    void shouldGetOrderedDistanceRange() {
        final RangeRequest rangeRequest = RangeRequest.newBuilder()
            .setRunnerId("123")
            .setSessionId("xyz")
            .setStore(Store.ORDERED_DISTANCE)
            .setFrom(0)
            .setTo(5)
            .build();
        final RunnerStatusReply runnerStatusRange = this.stub.getRunnerStatusRange(rangeRequest);
        this.softly.assertThat(runnerStatusRange.getRunnerStatusListList())
            .hasSize(3);
    }

    @Test
    void shouldGetUnorderedRange() {
        final RangeRequest rangeRequest = RangeRequest.newBuilder()
            .setRunnerId("123")
            .setSessionId("xyz")
            .setStore(Store.UNORDERED_RUN_TIME)
            .setFrom(0)
            .setTo(10)
            .build();
        final RunnerStatusReply runnerStatusRange = this.stub.getRunnerStatusRange(rangeRequest);
        this.softly.assertThat(runnerStatusRange.getRunnerStatusListList())
            .hasSize(3);
    }

    private static void produceDataToInputTopic() throws InterruptedException {
        final Collection<KeyValue<Void, RunnersStatus>> records = new ArrayList<>();
        for (int rt = 0; rt <= 200; rt++) {
            records.add(getKeyValue(rt));
        }
        final SendKeyValuesTransactional<Void, RunnersStatus> sendRequest = SendKeyValuesTransactional
            .inTransaction(INPUT, records)
            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
            .with(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL)
            .build();
        kafkaCluster.send(sendRequest);
    }

    private static KeyValue<Void, RunnersStatus> getKeyValue(final int gt) {
        final RunnersStatus status = new RunnersStatus(
            "123",
            "xyz",
            gt * 2,
            123,
            1.437000036,
            gt
        );
        return new KeyValue<>(null, status);
    }
}
