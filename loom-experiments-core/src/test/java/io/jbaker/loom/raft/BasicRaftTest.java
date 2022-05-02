/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Futures;
import io.jbaker.loom.raft.Raft.ApplyCommandRequest;
import io.jbaker.loom.raft.Raft.InitializedServer;
import io.jbaker.loom.raft.Raft.LogEntry;
import io.jbaker.loom.raft.Raft.StateMachine;
import io.jbaker.loom.raft.util.BackgroundTask;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class BasicRaftTest {
    private final BackgroundTaskRunner backgroundTaskRunner = new BackgroundTaskRunner();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void afterEach() {
        executor.shutdown();
        backgroundTaskRunner.close();
    }

    @Test
    void test() throws InterruptedException {
        List<Counter> counters = List.of(new Counter(), new Counter(), new Counter());
        List<InitializedServer> servers = Raft.create(3, counters.iterator()::next, backgroundTaskRunner, executor);
        Thread.sleep(1000);
        int successCount = 0;
        for (int i = 0; i < 100; i++) {
            ApplyCommandRequest request =
                    new ApplyCommandRequest.Builder().data((byte) i).build();
            successCount += servers.stream()
                    .map(server -> Futures.getUnchecked(server.client().applyCommand(request)))
                    .mapToInt(response -> response.applied() ? 1 : 0)
                    .sum();
        }
        System.out.println("done!");
        Thread.sleep(1000);
        assertThat(successCount).isEqualTo(100);
    }

    private static final class Counter implements StateMachine {
        private int count = 0;

        @Override
        public void init() {}

        @Override
        public void apply(LogEntry _entry) {
            count++;
        }
    }

    private static final class BackgroundTaskRunner implements BackgroundTask.Executor {
        private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

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
