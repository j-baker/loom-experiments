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
