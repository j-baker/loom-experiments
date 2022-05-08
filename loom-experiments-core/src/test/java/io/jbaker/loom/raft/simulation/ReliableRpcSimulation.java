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
