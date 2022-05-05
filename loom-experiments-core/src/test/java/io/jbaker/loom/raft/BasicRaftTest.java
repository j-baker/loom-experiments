/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.conjure.java.lib.Bytes;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.Command;
import io.jbaker.loom.raft.api.LeadershipMode;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.RaftServiceAsync;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.config.ServerConfig;
import io.jbaker.loom.raft.resource.RaftResource;
import io.jbaker.loom.raft.runtime.LanguageRuntime;
import io.jbaker.loom.raft.store.PersistentState;
import io.jbaker.loom.raft.store.ServerState;
import io.jbaker.loom.raft.store.StateMachine;
import io.jbaker.loom.raft.store.StoreManager;
import io.jbaker.loom.raft.store.StoreManagerImpl;
import io.jbaker.loom.raft.util.BackgroundTask;
import io.jbaker.loom.raft.util.ServerIds;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class BasicRaftTest {

    private static List<InitializedServer> createServers(Simulation simulation) {
        List<Counter> counters = List.of(new Counter(), new Counter(), new Counter());
        List<InitializedServer> servers = create(
                simulation,
                3,
                counters.iterator()::next,
                new BackgroundTaskRunner(
                        simulation.newScheduledExecutor(TimeDistribution.uniform(Duration.ZERO, Duration.ofMillis(1)))),
                simulation.clock());
        return servers;
    }

    @Test
    void test() {
        Simulation simulation = Simulation.create(0);
        List<InitializedServer> servers = createServers(simulation);
        simulation.advanceTime(Duration.ofMillis(60000));
        long last = System.currentTimeMillis();
        int successCount = 0;
        for (int i = 0; i < 10_000_000; i++) {
            while (!servers.stream().anyMatch(server -> server.isLeader().isPresent())) {
                simulation.advanceTime(Duration.ofMillis(10));
            }
            if (i % 10000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println((now - last) + " " + i + " " + simulation.clock().instant());
                last = now;
            }
            ApplyCommandRequest request = ApplyCommandRequest.of(
                    Command.of(Bytes.from(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
            boolean roundSuccess;
            try {
                roundSuccess = simulation
                        .runUntilComplete(servers.stream()
                                .filter(server -> server.isLeader().isPresent())
                                .max(Comparator.comparingInt(
                                        server -> server.isLeader().get().get()))
                                .orElseThrow()
                                .client()
                                .applyCommand(request))
                        .getApplied();
            } catch (RuntimeException _e) {
                roundSuccess = false;
                simulation.advanceTime(Duration.ofMinutes(1));
            }
            successCount += roundSuccess ? 1 : 0;
        }
        simulation.advanceTime(Duration.ofMillis(100));
        assertThat(successCount).isEqualTo(100);
    }

    private static final class Counter implements StateMachine {
        private int count = 0;

        @Override
        public void apply(LogEntry _entry) {
            count++;
        }
    }

    private static final class BackgroundTaskRunner implements BackgroundTask.Executor {
        private static final SafeLogger log = SafeLoggerFactory.get(BackgroundTaskRunner.class);

        private final ScheduledExecutorService scheduledExecutorService;

        private BackgroundTaskRunner(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
        }

        @Override
        public void close() {
            scheduledExecutorService.shutdown();
        }

        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        public void register(BackgroundTask task) {
            scheduledExecutorService.schedule(() -> executeNow(task), 100, TimeUnit.MILLISECONDS);
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        private void executeNow(BackgroundTask task) {
            Duration duration = Duration.ofSeconds(1);
            try {
                duration = task.runOneIteration();
            } catch (Throwable t) {
                log.warn("background task threw", t);
            }
            scheduledExecutorService.schedule(() -> executeNow(task), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    record InitializedServer(ServerId id, RaftServiceAsync client, RaftResource server, StoreManager store) {
        @Override
        public String toString() {
            return store.call(ctx -> "id: %s, mode: %s, currentTerm: %s, votedFor: %s"
                    .formatted(
                            ctx.state().getPersistent().getMe(),
                            ctx.state().getMode(),
                            ctx.state().getPersistent().getCurrentTerm(),
                            ctx.state().getPersistent().getVotedFor()));
        }

        public Optional<TermId> isLeader() {
            return store.call(ctx -> {
                if (!ctx.state().getLeadershipMode().equals(LeadershipMode.LEADER)) {
                    return Optional.empty();
                }
                return Optional.of(ctx.state().getPersistent().getCurrentTerm());
            });
        }

        public boolean stateEquals(InitializedServer other) {
            return store.call(ctx -> other.store.call(ctx2 -> ctx.state().equals(ctx2.state())));
        }
    }

    private static final class SimulatedRuntime implements LanguageRuntime {
        private final Simulation simulation;
        private final ScheduledExecutorService scheduler;

        private SimulatedRuntime(Simulation simulation) {
            this.simulation = simulation;
            this.scheduler = simulation.newScheduledExecutor(
                    TimeDistribution.uniform(Duration.of(10, ChronoUnit.MICROS), Duration.ofMillis(2)));
        }

        @Override
        public Clock clock() {
            return simulation.clock();
        }

        @Override
        public void sleep(Duration duration) {
            CountDownLatch latch = new CountDownLatch(1);
            scheduler.schedule(latch::countDown, duration.toMillis(), TimeUnit.MILLISECONDS);
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public Lock newLock() {
            return new YieldingLock();
        }
    }

    private static final class YieldingLock extends ReentrantLock {
        @Override
        public void lock() {
            Thread.yield();
            super.lock();
        }

        @Override
        public void unlock() {
            super.unlock();
            Thread.yield();
        }
    }

    private static List<InitializedServer> create(
            Simulation simulation,
            int numServers,
            Supplier<StateMachine> stateMachineFactory,
            BackgroundTask.Executor backgroundTaskExecutor,
            Clock clock) {
        SimulatedRuntime runtime = new SimulatedRuntime(simulation);
        Map<ServerId, RaftResource> servers = new HashMap<>();
        Map<ServerId, RaftServiceAsync> clients = IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .collect(Collectors.toUnmodifiableMap(
                        Function.identity(), id -> ClientShim.createUnreliable(id, servers, simulation)));
        return IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .map(id -> {
                    ServerConfig config = new ServerConfig.Builder()
                            .electionTimeout(Duration.ofSeconds(1))
                            .me(id)
                            .build();
                    StoreManager storeManager = new StoreManagerImpl(
                            runtime,
                            ServerState.create(PersistentState.create(config.me()), stateMachineFactory.get()));

                    RaftResource resource = new RaftResource(config, storeManager, clock, clients);
                    servers.put(id, resource);
                    backgroundTaskExecutor.register(resource);
                    return new InitializedServer(
                            id, ClientShim.createReliable(id, servers, simulation), resource, storeManager);
                })
                .toList();
    }
}
