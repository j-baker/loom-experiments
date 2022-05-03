/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static com.google.common.base.Preconditions.checkArgument;
import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.Futures;
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
    private final Random random;

    private final PriorityQueue<QueuedTask> taskQueue = new PriorityQueue<>();

    private Instant now = Instant.EPOCH;

    private final Clock clock = new SupplierClock(() -> now);

    DefaultSimulation(Random random) {
        this.random = random;
        // HackVirtualThreads.setup(new SimulatedExecutor(clock,
        //          this::scheduleNewTask, TimeDistribution.constant(Duration.ZERO)));
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
            e.printStackTrace();
        }
        return !taskQueue.isEmpty();
    }

    @Override
    public void runUntilIdle() {
        while (runNextTask()) {
            // empty body
        }
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
    public ExecutorService newExecutor(TimeDistribution delayDistribution) {
       // return Executors.newCachedThreadPool(newThreadFactory(delayDistribution));
        return new SimulatedExecutor(clock, this::scheduleNewTaskInVThread, delayDistribution);
    }

    @Override
    public Random random() {
        return random;
    }

    @Override
    public ThreadFactory newThreadFactory(TimeDistribution delayDistribution) {
        return newThreadFactory("thread", delayDistribution);
    }

    private ThreadFactory newThreadFactory(String name, TimeDistribution delayDistribution) {
        Executor executor = task -> scheduleNewTask(task, delayDistribution.sample());
        return runnable -> HackVirtualThreads.virtualThreadBuilderFor(executor).name(name).unstarted(runnable);
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public ScheduledExecutorService newScheduledExecutor(TimeDistribution timeDistribution) {
        //return Executors.newScheduledThreadPool(4, newThreadFactory(timeDistribution));
        return new SimulatedExecutor(clock, this::scheduleNewTaskInVThread, timeDistribution);
    }

    private QueuedTask scheduleNewTaskInVThread(Runnable task, Duration delay) {
        AtomicReference<QueuedTask> ref = new AtomicReference<>();
        TimeDistribution distribution = new TimeDistribution() {
            private boolean isFirst = true;
            @Override
            public Duration sample() {
                if (isFirst) {
                    isFirst = false;
                    return delay;
                }
                return Duration.ZERO;
            }
        };
        Executor executor = t -> ref.set(scheduleNewTask(t, distribution.sample()));
        HackVirtualThreads.virtualThreadBuilderFor(executor).start(task);
        return ref.get();
    }

    private QueuedTask scheduleNewTask(Runnable task, Duration delay) {
        QueuedTask queued = new QueuedTask(delay.isNegative() ? now : now.plus(delay), task, random.nextLong());
        taskQueue.add(queued);
        return queued;
    }

    private static final class SimulatedExecutor extends AbstractExecutorService implements ScheduledExecutorService {
        private final Clock clock;
        private final BiFunction<Runnable, Duration, QueuedTask> scheduleNewTask;
        private final TimeDistribution timeDistribution;

        private SimulatedExecutor(
                Clock clock,
                BiFunction<Runnable, Duration, QueuedTask> scheduleNewTask,
                TimeDistribution timeDistribution) {
            this.clock = clock;
            this.scheduleNewTask = scheduleNewTask;
            this.timeDistribution = timeDistribution;
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
            Duration duration =
                    timeDistribution.sample().plus(delay, unit.toChronoUnit());
            CapturingExecutor executor = new CapturingExecutor();
            Future<V> rawFuture = execute.apply(executor);
            QueuedTask task = scheduleNewTask(executor.retrieve(), duration);
            ScheduledFuture<V> result = new DelayedFuture<>(clock, rawFuture, task.launchTime);
            return result;
        }

        @Override
        public void execute(Runnable command) {
            scheduleNewTask(command, timeDistribution.sample());
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command,
                long initialDelay,
                long period,
                TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command,
                long initialDelay,
                long delay,
                TimeUnit unit) {
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
