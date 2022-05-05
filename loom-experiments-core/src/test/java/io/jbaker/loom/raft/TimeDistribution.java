/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import java.time.Duration;
import java.util.Random;

@FunctionalInterface
interface TimeDistribution {
    Duration sample(Random random);

    static TimeDistribution add(TimeDistribution left, TimeDistribution right) {
        return random -> left.sample(random).plus(right.sample(random));
    }

    static TimeDistribution zero() {
        return constant(Duration.ZERO);
    }

    static TimeDistribution constant(Duration duration) {
        return _random -> duration;
    }

    static TimeDistribution uniform(Duration from, Duration to) {
        long fromNanos = from.toNanos();
        long toNanos = to.toNanos();
        return random -> Duration.ofNanos(random.nextLong(fromNanos, toNanos));
    }
}
