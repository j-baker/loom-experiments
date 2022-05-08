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
