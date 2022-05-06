/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.simulation;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.function.Supplier;

public interface RpcSimulation {
    <T> ListenableFuture<T> simulate(Supplier<T> supplier);
}
