/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import java.time.Duration;
import java.util.Random;

@FunctionalInterface
interface TimeDistribution {
    Duration sample();

    static TimeDistribution add(TimeDistribution left, TimeDistribution right) {
        return () -> left.sample().plus(right.sample());
    }

    static TimeDistribution constant(Duration duration) {
        return () -> duration;
    }

    static TimeDistribution uniform(Random random, Duration from, Duration to) {
        long fromNanos = from.toNanos();
        long toNanos = to.toNanos();
        return () -> Duration.ofNanos(random.nextLong(fromNanos, toNanos));
    }
}
