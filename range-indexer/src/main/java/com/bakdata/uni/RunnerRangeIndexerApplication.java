package com.bakdata.uni;

import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.uni.range.DefaultRangeKeyBuilder;
import com.bakdata.uni.range.RangeField;
import com.bakdata.uni.range.extractor.type.FieldType;
import com.bakdata.uni.topology.RangeTopology;
import com.bakdata.uni.topology.TopologyStrategy;
import com.bakdata.uni.topology.UnorderedRangeTopology;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import picocli.CommandLine;

@Slf4j
@Setter
public class RunnerRangeIndexerApplication extends KafkaStreamsApplication {
    @CommandLine.Option(names = "--grpc-host", description = "gRPC Host address")
    private String grpcHost = "localhost";

    @CommandLine.Option(names = "--grpc-port", description = "gRPC Port")
    private int grpcPort = 9090;

    public static void main(final String[] args) {
        startApplication(new RunnerRangeIndexerApplication(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder streamsBuilder) {
        final List<String> rangeKeys = List.of("runner_id", "session");
        final IndexInputStreamBuilder indexInputStreamBuilder =
            new IndexInputStreamBuilder(new DefaultRangeKeyBuilder());

        final KStream<Void, RunnersStatus> inputStream = streamsBuilder.stream(this.getInputTopics());
        final KStream<String, RunnersStatus> repartitionedStream =
            indexInputStreamBuilder.repartitionOnRangeKey(inputStream, rangeKeys);

        final TopologyStrategy rangeTopology = new RangeTopology<>(
            streamsBuilder,
            null,
            new RangeField(FieldType.INTEGER, "run_time", false),
            "ordered_run_time"
        );

        final TopologyStrategy unorderedRangeTopology = new UnorderedRangeTopology<>(
            streamsBuilder,
            null,
            new RangeField(FieldType.INTEGER, "run_time", false),
            "unordered_run_time"
        );

        final TopologyStrategy distanceRangeTopology = new RangeTopology<>(
            streamsBuilder,
            null,
            new RangeField(FieldType.INTEGER, "distance", false),
            "ordered_distance"
        );

        createTopology(
            repartitionedStream,
            rangeTopology,
            unorderedRangeTopology,
            distanceRangeTopology
        );
    }

    @Override
    public Topology createTopology() {
        final Topology topology = super.createTopology();
        log.info("The topology of the Kafka Stream app is:\n {}", topology.describe());
        return topology;
    }

    @Override
    protected void runStreamsApplication() {
        // start the gRPC service
        try {
            final GrpcServer grpcServer = GrpcServer.createGrpcServer(this.grpcPort, this.getStreams());
            grpcServer.start();
            log.info("gRPC server started on host {} and port {}", this.grpcHost, this.grpcPort);
        } catch (final IOException e) {
            log.error("Could not start gRPC server on host {} and port {}", this.grpcHost, this.grpcPort);
        }
        log.info("Starting the streams app");
        super.runStreamsApplication();
    }

    @Override
    public String getUniqueAppId() {
        return String.format("range-indexer-app-%s", this.getInputTopics().get(0));
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();

        final String endpoint = "%s:%s".formatted(this.grpcHost, this.grpcPort);
        kafkaProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
        kafkaProperties.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);

        return kafkaProperties;
    }

    private static <K, V> void createTopology(final KStream<K, V> stream, final TopologyStrategy... strategies) {
        for (final TopologyStrategy topologyStrategy : strategies) {
            topologyStrategy.create(stream);
        }
    }
}
