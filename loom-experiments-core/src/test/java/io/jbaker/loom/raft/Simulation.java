/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public sealed interface Simulation permits DefaultSimulation {
    Clock clock();
    ScheduledExecutorService newScheduledExecutor(TimeDistribution timeDistribution);
    ExecutorService newExecutor(TimeDistribution delayDistribution);

    Random random();

    ThreadFactory newThreadFactory(TimeDistribution delayDistribution);

    void advanceTime(Duration duration);
    void runUntilIdle();

    <V> V runUntilComplete(Future<V> future);

    boolean runNextTask();

    static Simulation create(long seed) {
        return new DefaultSimulation(new Random(seed));
    }
}
