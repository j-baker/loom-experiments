/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.simulation;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public interface Simulation {
    long getTaskCount();

    Clock clock();

    ScheduledExecutorService newScheduledExecutor(DelayDistribution delayDistribution);

    ExecutorService newExecutor(DelayDistribution delayDistribution);

    Random random();

    void advanceTime(Duration duration);

    <V> V runUntilComplete(Future<V> future);

    boolean runNextTask();

    default RpcSimulation rpc() {
        return RealisticRpcSimulation.create(this);
    }

    static Simulation create(long seed) {
        return new DefaultSimulation(new Random(seed));
    }
}
