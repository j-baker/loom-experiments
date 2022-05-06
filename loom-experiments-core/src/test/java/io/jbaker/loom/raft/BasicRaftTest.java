/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import io.jbaker.loom.Hooks;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.Command;
import io.jbaker.loom.raft.api.LeadershipMode;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.RaftServiceAsync;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.config.ServerConfig;
import io.jbaker.loom.raft.resource.RaftResource;
import io.jbaker.loom.raft.runtime.LanguageRuntime;
import io.jbaker.loom.raft.simulation.DelayDistribution;
import io.jbaker.loom.raft.simulation.ReliableRpcSimulation;
import io.jbaker.loom.raft.simulation.Simulation;
import io.jbaker.loom.raft.store.PersistentState;
import io.jbaker.loom.raft.store.ServerState;
import io.jbaker.loom.raft.store.StateMachine;
import io.jbaker.loom.raft.store.StoreManager;
import io.jbaker.loom.raft.store.StoreManagerImpl;
import io.jbaker.loom.raft.util.BackgroundTask;
import io.jbaker.loom.raft.util.LogIndexes;
import io.jbaker.loom.raft.util.ServerIds;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import java.util.stream.Stream;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

public class BasicRaftTest {

    private static List<InitializedServer> createServers(CorrectnessValidator validator, Simulation simulation) {
        List<InitializedServer> servers = create(
                simulation,
                3,
                validator::newStateMachine,
                new BackgroundTaskRunner(simulation.newScheduledExecutor(
                        DelayDistribution.uniform(Duration.ZERO, Duration.ofMillis(1)))),
                simulation.clock(),
                validator);
        return servers;
    }

    @Test
    void test() throws InterruptedException {
        Simulation simulation = Simulation.create(0);
        CorrectnessValidator correctnessValidator = new CorrectnessValidator();
        List<InitializedServer> servers = createServers(correctnessValidator, simulation);
        simulation.advanceTime(Duration.ofMillis(60000));
        long last = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        int successCount = 0;
        int numRounds = 100_000;
        for (int i = 0; i < 100_000; i++) {
            while (!servers.stream()
                    .anyMatch(server -> server.isLeader().isPresent())) {
                simulation.advanceTime(Duration.ofMillis(10));
            }
            if (i % 10000 == 0) {
                correctnessValidator.validateLogMatchingProperty(
                        servers.stream().map(InitializedServer::store).toList());
                correctnessValidator.ensureNoBadnessFound();
                long now = System.currentTimeMillis();
                System.out.println((now - start) + " " + (now - last) + " " + i + " "
                        + simulation.clock().instant() + " " + simulation.getTaskCount());
                last = now;
            }
            ApplyCommandRequest request = ApplyCommandRequest.of(
                    Command.of(Bytes.from(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
            boolean roundSuccess;
            try {
                roundSuccess = simulation
                        .runUntilComplete(servers.stream()
                                .filter(server -> server.isLeader().isPresent())
                                .max(Comparator.comparingInt(server ->
                                        server.isLeader().get().get()))
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
        assertThat(successCount).isCloseTo(numRounds, Percentage.withPercentage(5));
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
                    DelayDistribution.uniform(Duration.of(10, ChronoUnit.MICROS), Duration.ofMillis(2)));
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
            Clock clock,
            Hooks hooks) {
        SimulatedRuntime runtime = new SimulatedRuntime(simulation);
        Map<ServerId, RaftResource> servers = new HashMap<>();
        Map<ServerId, RaftServiceAsync> clients = IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .collect(Collectors.toUnmodifiableMap(
                        Function.identity(), id -> ClientShim.create(simulation.rpc(), id, servers)));
        return IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .map(id -> {
                    ServerConfig config = new ServerConfig.Builder()
                            .electionTimeout(Duration.ofSeconds(5))
                            .me(id)
                            .build();
                    StoreManager storeManager = new StoreManagerImpl(
                            runtime,
                            ServerState.create(PersistentState.create(config.me()), stateMachineFactory.get()));

                    RaftResource resource = new RaftResource(
                            config,
                            storeManager,
                            clock,
                            clients,
                            MoreExecutors.listeningDecorator(simulation.newExecutor(
                                    DelayDistribution.uniform(Duration.ofNanos(1000), Duration.ofMillis(1)))),
                            hooks);
                    servers.put(id, resource);
                    backgroundTaskExecutor.register(resource);
                    return new InitializedServer(
                            id,
                            ClientShim.create(ReliableRpcSimulation.create(simulation), id, servers),
                            resource,
                            storeManager);
                })
                .toList();
    }

    private static final class CorrectnessValidator implements Hooks {
        private final Map<TermId, ServerId> leaders = new HashMap<>();
        private boolean incorrectnessDetected = false;
        private final List<LogEntry> canonicalStateMachineEntries = new ArrayList<>();

        public void ensureNoBadnessFound() {
            checkState(!incorrectnessDetected, "at some point, some form of incorrectness was found");
        }

        @Override
        public void onNewLeader(TermId termId, ServerId leader) {
            ServerId currentLeader = leaders.putIfAbsent(termId, leader);
            checkState(
                    currentLeader == null,
                    "two leaders were elected for the same",
                    SafeArg.of("term", termId),
                    SafeArg.of("currentLeader", currentLeader),
                    SafeArg.of("newLeader", leader));
        }

        public void validateLogMatchingProperty(List<StoreManager> storeManagers) {
            List<List<LogEntry>> allLogs = storeManagers.stream()
                    .map(store -> store.call(
                            ctx -> List.copyOf(ctx.state().getPersistent().getLog())))
                    .toList();
            nChoose2(allLogs).forEach(entry -> {
                List<LogEntry> left = entry.getKey();
                List<LogEntry> right = entry.getValue();

                for (int k = Math.min(left.size(), right.size()) - 1; k >= 0; k--) {
                    if (left.get(k).getTerm().equals(right.get(k).getTerm())) {
                        checkState(
                                left.subList(0, k + 1).equals(right.subList(0, k + 1)),
                                "log matching property was not maintained");
                        break;
                    }
                }
            });
        }

        private static <T> Stream<Entry<T, T>> nChoose2(List<T> list) {
            return IntStream.range(0, list.size()).boxed().flatMap(i -> IntStream.range(i + 1, list.size())
                    .mapToObj(j -> Map.entry(list.get(i), list.get(j))));
        }

        public StateMachine newStateMachine() {
            return new StateMachine() {
                private LogIndex currentIndex = LogIndexes.ZERO;

                @Override
                public void apply(LogIndex logIndex, LogEntry entry) {
                    checkState(
                            logIndex.equals(LogIndexes.inc(currentIndex)),
                            "out of order log entries passed to state machine");
                    if (canonicalStateMachineEntries.size() >= logIndex.get()) {
                        checkState(
                                canonicalStateMachineEntries
                                        .get(LogIndexes.listIndex(logIndex))
                                        .equals(entry),
                                "two state machines saw different entries for a given log index");
                    } else {
                        canonicalStateMachineEntries.add(entry);
                    }
                    currentIndex = logIndex;
                }
            };
        }

        private void checkState(boolean condition, String message, SafeArg<?>... args) {
            if (!condition) {
                incorrectnessDetected = true;
            }
            Preconditions.checkState(condition, message, args);
        }
    }
}
