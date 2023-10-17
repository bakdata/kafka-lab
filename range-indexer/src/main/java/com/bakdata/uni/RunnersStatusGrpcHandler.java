package com.bakdata.uni;

import com.bakdata.uni.proto.RangeIndexerServiceGrpc.RangeIndexerServiceImplBase;
import com.bakdata.uni.proto.RangeRequest;
import com.bakdata.uni.proto.RunnerAnalytics;
import com.bakdata.uni.proto.RunnerStatusReply;
import com.bakdata.uni.proto.Store;
import com.bakdata.uni.range.DefaultRangeKeyBuilder;
import com.bakdata.uni.range.indexer.RangeIndexer;
import com.bakdata.uni.range.indexer.ordered.ReadRangeIndexer;
import com.bakdata.uni.range.indexer.unordered.UnorderedReadRangeIndexer;
import com.bakdata.uni.range.padder.IntPadder;
import com.bakdata.uni.range.store.RangeKeyValueStore;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
@RequiredArgsConstructor
public class RunnersStatusGrpcHandler extends RangeIndexerServiceImplBase {
    private final KafkaStreams streams;

    @Override
    public void getRunnerStatusRange(final RangeRequest request,
        final StreamObserver<RunnerStatusReply> responseObserver) {
        final RangeIndexer<String, String> rangeIndexer = getRangeIndexer(request.getStore());
        final String storeName = request.getStore().name().toLowerCase();
        log.info("Getting range from store: {}", storeName);
        final RangeKeyValueStore<String, RunnersStatus> rangeStore =
            new RangeKeyValueStore<>(this.streams, rangeIndexer, storeName, new DefaultRangeKeyBuilder());
        final List<RunnersStatus> rangeValues =
            getRangeValues(request.getRunnerId(), request.getSessionId(), request.getFrom(),
                request.getTo(),
                rangeStore);
        log.debug("range values are: {}", rangeValues);
        final List<RunnerAnalytics> runnersStatuses = rangeValues.stream()
            .map(rangeValue -> RunnerAnalytics.newBuilder()
                .setRunTime(rangeValue.getRunTime())
                .setDistance(rangeValue.getDistance())
                .setHeartRate(rangeValue.getHeartRate())
                .setSpeed(rangeValue.getSpeed())
                .build())
            .toList();
        final RunnerStatusReply runnerStatusReply = RunnerStatusReply.newBuilder()
            .addAllRunnerStatusList(runnersStatuses)
            .build();
        responseObserver.onNext(runnerStatusReply);
        responseObserver.onCompleted();
    }

    private static RangeIndexer<String, String> getRangeIndexer(final Store store) {
        if (store == Store.UNORDERED_RUN_TIME) {
            return new UnorderedReadRangeIndexer<>(new IntPadder());
        }
        return new ReadRangeIndexer<>(new IntPadder());
    }

    private static List<RunnersStatus> getRangeValues(final String runnerId, final String session, final int from,
        final int to,
        final RangeKeyValueStore<? super String, RunnersStatus> rangeStore) {
        final String strFrom = String.valueOf(from);
        final String strTo = to == 0 ? String.valueOf(Integer.MAX_VALUE) : String.valueOf(to);
        log.info("RunnerId: {}", runnerId);
        log.info("From: {}", from);
        log.info("To: {}", to);

        return rangeStore.query(List.of(runnerId, session), strFrom, strTo);
    }
}
