/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static org.assertj.core.api.Assertions.assertThat;

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
        ExecutorService executor = simulation.newExecutor(TimeDistribution.constant(delay));
        Instant now = simulation.clock().instant();
        Future<?> future = executor.submit(() -> {});
        simulation.runUntilComplete(future);
        assertThat(Duration.between(now, simulation.clock().instant())).isEqualTo(delay);
    }

    @Test
    void testScheduling() {
        Duration delay = Duration.ofMillis(10);
        ScheduledExecutorService executor = simulation.newScheduledExecutor(TimeDistribution.constant(Duration.ZERO));
        Instant now = simulation.clock().instant();
        Future<?> future = executor.schedule(() -> {}, delay.toMillis(), TimeUnit.MILLISECONDS);
        simulation.runUntilComplete(future);
        assertThat(Duration.between(now, simulation.clock().instant())).isEqualTo(delay);

    }
}
