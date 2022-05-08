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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

public final class RealisticRpcSimulation implements RpcSimulation {
    private static final double DUPLICATE_RPC_PERCENTAGE = 0.001;
    private static final RuntimeException down =
            new RuntimeException("server is down", new IOException("something bad happened"));
    private static final SafeIllegalStateException timeout = new SafeIllegalStateException("timed out");

    private static final DelayDistribution healthyDistribution = new RealisticResponseDelayDistribution(0, 1);
    private static final DelayDistribution slowDistribution = new RealisticResponseDelayDistribution(0.95, 1);
    private final ListeningScheduledExecutorService executor;
    private final Random random;
    private final long seed;
    private final Clock clock;

    private RealisticRpcSimulation(ListeningScheduledExecutorService executor, Random random, Clock clock) {
        this.executor = executor;
        this.random = random;
        this.clock = clock;
        seed = random.nextLong();
    }

    public static RpcSimulation create(Simulation simulation) {
        return new RealisticRpcSimulation(
                MoreExecutors.listeningDecorator(simulation.newScheduledExecutor(DelayDistribution.zero())),
                simulation.random(),
                simulation.clock());
    }

    @Override
    public <T> ListenableFuture<T> simulate(Supplier<T> supplier) {
        if (random.nextDouble() < DUPLICATE_RPC_PERCENTAGE) {
            executor.submit(() -> simulateSync(supplier));
        }
        return executor.submit(() -> simulateSync(supplier));
    }

    private <T> T simulateSync(Supplier<T> supplier) {
        return switch (chooseOutcome()) {
            case TIMEOUT -> timeout();
            case HEALTHY -> runOnActualServer(healthyDistribution, supplier);
            case SLOW -> runOnActualServer(slowDistribution, supplier);
            case FAIL -> fail(healthyDistribution);
        };
    }

    private <T> T fail(DelayDistribution distribution) {
        sleep(distribution.sample(random));
        throw down;
    }

    private <T> T timeout() {
        sleep(Duration.ofSeconds(10));
        throw timeout;
    }

    private <T> T runOnActualServer(DelayDistribution delayDistribution, Supplier<T> task) {
        Duration delay = delayDistribution.sample(random);
        double delayRatio = random.nextDouble();
        sleep(times(delay, delayRatio));
        try {
            return task.get();
        } finally {
            sleep(times(delay, 1 - delayRatio));
        }
    }

    private Outcome chooseOutcome() {
        Random rand = new Random(clock.instant().truncatedTo(ChronoUnit.MINUTES).toEpochMilli() + seed);
        double sample = rand.nextDouble();
        if (sample < 0.9) {
            return Outcome.HEALTHY;
        } else if (sample < 0.99999) {
            return Outcome.SLOW;
        } else if (sample < 0.999999) {
            return Outcome.TIMEOUT;
        } else {
            return Outcome.FAIL;
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void sleep(Duration duration) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.schedule(latch::countDown, duration);
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private enum Outcome {
        HEALTHY,
        SLOW,
        TIMEOUT,
        FAIL,
    }

    private static Duration times(Duration duration, double multiplicand) {
        return Duration.ofNanos((long) (duration.toNanos() * multiplicand));
    }

    /**
     * I can't be bothered to figure out a continuous distribution, so this is a gross approximation
     */
    private static final class RealisticResponseDelayDistribution implements DelayDistribution {
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

        private RealisticResponseDelayDistribution(double floor, double ceil) {
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
}
