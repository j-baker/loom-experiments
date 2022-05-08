/*
 * (c) Copyright 2022 James Baker. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jbaker.loom.raft.simulation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.Futures;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

final class DefaultSimulation implements Simulation {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSimulation.class);
    private final Random random;

    private final PriorityQueue<QueuedTask> taskQueue = new PriorityQueue<>();

    private Instant now = Instant.EPOCH;

    private long taskId = 0;

    private final Clock clock = new SupplierClock(() -> now);

    DefaultSimulation(Random random) {
        this.random = random;
    }

    @Override
    public boolean runNextTask() {
        QueuedTask maybeTask = taskQueue.poll();
        if (maybeTask == null) {
            return false;
        }
        now = maybeTask.launchTime;
        try {
            maybeTask.task.run();
        } catch (RuntimeException e) {
            log.info("caught exception while running task, swallowing", e);
        }
        return !taskQueue.isEmpty();
    }

    @Override
    public <V> V runUntilComplete(Future<V> future) {
        while (!future.isDone() && runNextTask()) {
            // empty body
        }
        try {
            return Futures.getDone(future);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void advanceTime(Duration duration) {
        Instant endTime = now.plus(duration);
        while (!taskQueue.isEmpty() && taskQueue.peek().launchTime.isBefore(endTime)) {
            runNextTask();
        }
        now = endTime;
    }

    @Override
    public ExecutorService newExecutor(DelayDistribution delayDistribution) {
        return new SimulatedExecutor(clock, this::scheduleNewTaskInVThread, delayDistribution, random);
    }

    @Override
    public Random random() {
        return random;
    }

    private ThreadFactory newThreadFactory(String name, DelayDistribution delayDistribution) {
        Executor executor = task -> scheduleNewTask(task, delayDistribution.sample(random));
        return runnable ->
                HackVirtualThreads.virtualThreadBuilderFor(executor).name(name).unstarted(runnable);
    }

    @Override
    public long getTaskCount() {
        return taskId;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public ScheduledExecutorService newScheduledExecutor(DelayDistribution delayDistribution) {
        return new SimulatedExecutor(clock, this::scheduleNewTaskInVThread, delayDistribution, random);
    }

    private QueuedTask scheduleNewTaskInVThread(Runnable task, Duration delay) {
        AtomicReference<QueuedTask> ref = new AtomicReference<>();
        DelayDistribution distribution = new DelayDistribution() {
            private boolean isFirst = true;

            @Override
            public Duration sample(Random _random) {
                if (isFirst) {
                    isFirst = false;
                    return delay;
                }
                return Duration.ZERO;
            }
        };
        Executor executor = t -> ref.set(scheduleNewTask(t, distribution.sample(random)));
        HackVirtualThreads.virtualThreadBuilderFor(executor).start(task);
        return ref.get();
    }

    private QueuedTask scheduleNewTask(Runnable task, Duration delay) {
        QueuedTask queued = new QueuedTask(delay.isNegative() ? now : now.plus(delay), task, taskId++);
        taskQueue.add(queued);
        return queued;
    }

    private static final class SimulatedExecutor extends AbstractExecutorService implements ScheduledExecutorService {
        private final Clock clock;
        private final BiFunction<Runnable, Duration, QueuedTask> scheduleNewTask;
        private final DelayDistribution delayDistribution;
        private final Random random;

        private SimulatedExecutor(
                Clock clock,
                BiFunction<Runnable, Duration, QueuedTask> scheduleNewTask,
                DelayDistribution delayDistribution,
                Random random) {
            this.clock = clock;
            this.scheduleNewTask = scheduleNewTask;
            this.delayDistribution = delayDistribution;
            this.random = random;
        }

        private QueuedTask scheduleNewTask(Runnable runnable, Duration duration) {
            return scheduleNewTask.apply(runnable, duration);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable userTask, long delay, TimeUnit unit) {
            return schedule(executor -> Futures.submit(userTask, executor), delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> userTask, long delay, TimeUnit unit) {
            return schedule(executor -> Futures.submit(userTask, executor), delay, unit);
        }

        private <V> ScheduledFuture<V> schedule(Function<Executor, Future<V>> execute, long delay, TimeUnit unit) {
            Duration duration = delayDistribution.sample(random).plus(delay, unit.toChronoUnit());
            CapturingExecutor executor = new CapturingExecutor();
            Future<V> rawFuture = execute.apply(executor);
            QueuedTask task = scheduleNewTask(executor.retrieve(), duration);
            ScheduledFuture<V> result = new DelayedFuture<>(clock, rawFuture, task.launchTime);
            return result;
        }

        @Override
        public void execute(Runnable command) {
            scheduleNewTask(command, delayDistribution.sample(random));
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }
    }

    private static final class CapturingExecutor implements Executor {
        private Runnable runnable;

        @Override
        public void execute(Runnable command) {
            checkArgument(command != null, "input must not be null");
            checkState(runnable == null, "can only capture a single runnable");
            runnable = command;
        }

        public Runnable retrieve() {
            checkState(runnable != null, "have not captured a runnable yet");
            return runnable;
        }
    }

    private static final class DelayedFuture<V> extends ForwardingFuture<V> implements ScheduledFuture<V> {
        private final Clock clock;
        private final Future<? extends V> delegate;
        private final Instant triggerTime;

        private DelayedFuture(Clock clock, Future<? extends V> delegate, Instant triggerTime) {
            this.clock = clock;
            this.delegate = delegate;
            this.triggerTime = triggerTime;
        }

        @Override
        protected Future<? extends V> delegate() {
            return delegate;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Duration.between(clock.instant(), triggerTime).get(unit.toChronoUnit());
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.MICROSECONDS), other.getDelay(TimeUnit.MICROSECONDS));
        }
    }

    private static final class SupplierClock extends Clock {
        private final Supplier<Instant> supplier;

        private final ZoneId zone;

        private SupplierClock(Supplier<Instant> supplier) {
            this(supplier, ZoneId.of("Z"));
        }

        private SupplierClock(Supplier<Instant> supplier, ZoneId zone) {
            this.supplier = supplier;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId newZone) {
            return new SupplierClock(supplier, newZone);
        }

        @Override
        public Instant instant() {
            return supplier.get();
        }
    }

    private static final class QueuedTask implements Comparable<QueuedTask> {
        private final Instant launchTime;
        private final Runnable task;
        private final long tieBreak;

        private QueuedTask(Instant launchTime, Runnable task, long tieBreak) {
            this.launchTime = launchTime;
            this.task = task;
            this.tieBreak = tieBreak;
        }

        @Override
        public int compareTo(QueuedTask other) {
            int launchTimeCompared = launchTime.compareTo(other.launchTime);
            if (launchTimeCompared != 0) {
                return launchTimeCompared;
            }
            return Long.compare(tieBreak, other.tieBreak);
        }
    }
}
