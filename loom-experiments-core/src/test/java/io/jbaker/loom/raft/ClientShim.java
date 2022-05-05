/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import io.jbaker.loom.raft.api.AppendEntriesRequest;
import io.jbaker.loom.raft.api.AppendEntriesResponse;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.ApplyCommandResponse;
import io.jbaker.loom.raft.api.RaftService;
import io.jbaker.loom.raft.api.RaftServiceAsync;
import io.jbaker.loom.raft.api.RequestVoteRequest;
import io.jbaker.loom.raft.api.RequestVoteResponse;
import io.jbaker.loom.raft.api.ServerId;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class ClientShim implements RaftServiceAsync {
    private static final SafeIllegalStateException down = new SafeIllegalStateException("server is down");
    private static final SafeIllegalStateException timeout = new SafeIllegalStateException("timed out");
    private final ServerId serverId;
    private final Map<ServerId, ? extends RaftService> servers;

    private final Executor executor;
    private final Executor slowExecutor;
    private final ListeningScheduledExecutorService timeoutExecutor;

    private final Simulation simulation;

    private final Function<Instant, Mode> chooseMode;

    private ClientShim(
            ServerId serverId,
            Map<ServerId, ? extends RaftService> servers,
            Executor executor,
            Executor slowExecutor,
            ListeningScheduledExecutorService timeoutExecutor,
            Simulation simulation,
            Function<Instant, Mode> chooseMode) {
        this.serverId = serverId;
        this.servers = servers;
        this.executor = executor;
        this.slowExecutor = slowExecutor;
        this.timeoutExecutor = timeoutExecutor;
        this.simulation = simulation;
        this.chooseMode = chooseMode;
    }

    public static RaftServiceAsync createUnreliable(
            ServerId serverId, Map<ServerId, ? extends RaftService> servers, Simulation simulation) {
        long seed = simulation.random().nextLong();
        return new ClientShim(
                serverId,
                servers,
                simulation.newExecutor(new RealisticResponseTimeDistribution(0, 1)),
                simulation.newExecutor(new RealisticResponseTimeDistribution(0.95, 1)),
                MoreExecutors.listeningDecorator(
                        simulation.newScheduledExecutor(TimeDistribution.uniform(Duration.ZERO, Duration.ofMillis(1)))),
                simulation,
                now -> chooseMode(seed, now));
    }

    public static RaftServiceAsync createReliable(
            ServerId serverId, Map<ServerId, ? extends RaftService> servers, Simulation simulation) {
        return new ClientShim(
                serverId,
                servers,
                simulation.newExecutor(TimeDistribution.zero()),
                simulation.newExecutor(TimeDistribution.zero()),
                MoreExecutors.listeningDecorator(simulation.newScheduledExecutor(TimeDistribution.zero())),
                simulation,
                _now -> Mode.HEALTHY);
    }

    @Override
    public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return runOnServer(s -> s.appendEntries(request));
    }

    @Override
    public ListenableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return runOnServer(s -> s.requestVote(request));
    }

    @Override
    public ListenableFuture<ApplyCommandResponse> applyCommand(ApplyCommandRequest request) {
        return runOnServer(s -> s.applyCommand(request));
    }

    private <T> ListenableFuture<T> runOnServer(Function<RaftService, T> func) {
        return switch (chooseMode.apply(simulation.clock().instant())) {
            case HEALTHY -> Futures.submit(() -> func.apply(servers.get(serverId)), executor);
            case SLOW -> Futures.submit(() -> func.apply(servers.get(serverId)), slowExecutor);
            case TIMING_OUT -> timeoutExecutor.schedule(
                    () -> {
                        throw timeout;
                    },
                    10,
                    TimeUnit.SECONDS);
            case FAST_FAIL -> Futures.immediateFailedFuture(down);
        };
    }

    private static Mode chooseMode(long seed, Instant now) {
        Random random = new Random(now.truncatedTo(ChronoUnit.MINUTES).toEpochMilli() + seed);
        double sample = random.nextDouble();
        if (sample < 0.9) {
            return Mode.HEALTHY;
        } else if (sample < 0.98) {
            return Mode.SLOW;
        } else if (sample < 0.99) {
            return Mode.TIMING_OUT;
        } else {
            return Mode.FAST_FAIL;
        }
    }

    /**
     * I can't be bothered to figure out a continuous distribution, so this is a gross approximation
     */
    private static final class RealisticResponseTimeDistribution implements TimeDistribution {
        private static final Duration pMin = Duration.ofMillis(1);
        private static final Duration p25 = Duration.ofMillis(2);
        private static final Duration p50 = Duration.ofMillis(5);
        private static final Duration p75 = Duration.ofMillis(9);
        private static final Duration p95 = Duration.ofMillis(20);
        private static final Duration p99 = Duration.ofMillis(50);
        private static final Duration p999 = Duration.ofMillis(150);
        private static final Duration p9999 = Duration.ofSeconds(1);

        private static final Duration pMax = Duration.ofSeconds(10);

        private static final NavigableMap<Double, Duration> map = ImmutableSortedMap.<Double, Duration>naturalOrder()
                .put(0.0, pMin)
                .put(0.25, p25)
                .put(0.5, p50)
                .put(0.75, p75)
                .put(0.95, p95)
                .put(0.99, p99)
                .put(0.999, p999)
                .put(0.9999, p9999)
                .put(1.0, pMax)
                .build();

        private final double floor;
        private final double ceil;

        private RealisticResponseTimeDistribution(double floor, double ceil) {
            this.floor = floor;
            this.ceil = ceil;
        }

        private static Duration chooseLatency(double percentile) {
            Map.Entry<Double, Duration> lower = map.floorEntry(percentile);
            Map.Entry<Double, Duration> higher = map.ceilingEntry(percentile);
            double distance = higher.getKey() - lower.getKey();
            return Duration.ofNanos((long) (((percentile - lower.getKey())
                                    * lower.getValue().toNanos()
                            + (higher.getKey() - percentile) * higher.getValue().toNanos())
                    / distance));
        }

        @Override
        public Duration sample(Random random) {
            return chooseLatency(random.nextDouble(floor, ceil));
        }
    }

    private enum Mode {
        HEALTHY,
        SLOW,
        TIMING_OUT,
        FAST_FAIL,
    }
}
