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

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

import io.jbaker.loom.raft.simulation.DelayDistribution;
import io.jbaker.loom.raft.simulation.Simulation;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class SimulationTest {
    private final Simulation simulation = Simulation.create(0);

    @Test
    void testExecutor() {
        Duration delay = Duration.ofMillis(10);
        ExecutorService executor = simulation.newExecutor(DelayDistribution.constant(delay));
        Instant now = simulation.clock().instant();
        Future<?> future = executor.submit(() -> {});
        simulation.runUntilComplete(future);
        assertThat(Duration.between(now, simulation.clock().instant())).isEqualTo(delay);
    }

    @Test
    void testScheduling() {
        Duration delay = Duration.ofMillis(10);
        ScheduledExecutorService executor = simulation.newScheduledExecutor(DelayDistribution.constant(Duration.ZERO));
        Instant now = simulation.clock().instant();
        Future<?> future = executor.schedule(() -> {}, delay.toMillis(), TimeUnit.MILLISECONDS);
        simulation.runUntilComplete(future);
        assertThat(Duration.between(now, simulation.clock().instant())).isEqualTo(delay);
    }
}
