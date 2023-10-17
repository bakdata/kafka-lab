package com.bakdata.uni;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
@RequiredArgsConstructor
public class GrpcServer {
    private final Server server;

    /**
     * Create a RouteGuide server using serverBuilder as a base and features as data.
     */
    public static GrpcServer createGrpcServer(final int port, final KafkaStreams streams) {
        final Server server = ServerBuilder.forPort(port)
            .addService(new RunnersStatusGrpcHandler(streams))
            .build();
        return new GrpcServer(server);
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        this.server.start();
        log.info("Server started, listening on {}", this.server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            log.error("*** shutting down gRPC server since JVM is shutting down");
            try {
                GrpcServer.this.stop();
            } catch (final InterruptedException e) {
                e.printStackTrace(System.err);
            }
            log.error("*** server shut down");
        }));
    }

    /**
     * Stop serving requests and shutdown resources.
     */
    private void stop() throws InterruptedException {
        if (this.server != null) {
            this.server.shutdown().awaitTermination(30, SECONDS);
        }
    }
}
