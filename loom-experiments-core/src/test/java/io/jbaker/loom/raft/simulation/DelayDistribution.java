/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.simulation;

import java.time.Duration;
import java.util.Random;

@FunctionalInterface
public interface DelayDistribution {
    Duration sample(Random random);

    static DelayDistribution add(DelayDistribution left, DelayDistribution right) {
        return random -> left.sample(random).plus(right.sample(random));
    }

    static DelayDistribution zero() {
        return constant(Duration.ZERO);
    }

    static DelayDistribution constant(Duration duration) {
        return _random -> duration;
    }

    static DelayDistribution uniform(Duration from, Duration to) {
        long fromNanos = from.toNanos();
        long toNanos = to.toNanos();
        return random -> Duration.ofNanos(random.nextLong(fromNanos, toNanos));
    }
}
