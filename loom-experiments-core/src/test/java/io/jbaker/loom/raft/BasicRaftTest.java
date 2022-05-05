/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.MoreCollectors;
import com.palantir.conjure.java.lib.Bytes;
import io.jbaker.loom.raft.Raft.InitializedServer;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.Command;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.store.StateMachine;
import io.jbaker.loom.raft.util.BackgroundTask;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class BasicRaftTest {

    private static List<InitializedServer> createServers(Simulation simulation) {
        List<Counter> counters = List.of(new Counter(), new Counter(), new Counter());
        List<InitializedServer> servers = Raft.create(
                3,
                counters.iterator()::next,
                new BackgroundTaskRunner(simulation.newScheduledExecutor(
                        TimeDistribution.uniform(simulation.random(), Duration.ZERO, Duration.ofMillis(1)))),
                simulation.newExecutor(
                        TimeDistribution.uniform(simulation.random(), Duration.ofMillis(1), Duration.ofMillis(10))),
                simulation.clock());
        return servers;
    }

    @Test
    void test() {
        Simulation simulation = Simulation.create(0);
        List<InitializedServer> servers = createServers(simulation);
        simulation.advanceTime(Duration.ofMillis(60000));
        int successCount = 0;
        for (int i = 0; i < 100; i++) {
            ApplyCommandRequest request = ApplyCommandRequest.of(
                    Command.of(Bytes.from(Integer.toString(i).getBytes(StandardCharsets.UTF_8))));
            boolean roundSuccess = simulation
                    .runUntilComplete(servers.stream()
                            .filter(InitializedServer::isLeader)
                            .collect(MoreCollectors.onlyElement())
                            .client()
                            .applyCommand(request))
                    .getApplied();
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
                t.printStackTrace();
            }
            scheduledExecutorService.schedule(() -> executeNow(task), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
