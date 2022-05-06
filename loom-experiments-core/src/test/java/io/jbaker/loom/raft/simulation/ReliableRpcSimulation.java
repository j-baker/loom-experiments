/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.simulation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.function.Supplier;

public final class ReliableRpcSimulation implements RpcSimulation {
    private final ListeningScheduledExecutorService executor;

    private ReliableRpcSimulation(ListeningScheduledExecutorService executor) {
        this.executor = executor;
    }

    public static RpcSimulation create(Simulation simulation) {
        return new ReliableRpcSimulation(MoreExecutors.listeningDecorator(
                simulation.newScheduledExecutor(DelayDistribution.constant(Duration.ofMillis(1)))));
    }

    @Override
    public <T> ListenableFuture<T> simulate(Supplier<T> supplier) {
        return executor.submit(supplier::get);
    }
}
