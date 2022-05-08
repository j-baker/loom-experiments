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

package io.jbaker.loom.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import io.jbaker.loom.raft.simulation.HackVirtualThreads;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BrokenMemoizingSupplierTest {

    @Disabled("Expected to fail")
    @ParameterizedTest
    @MethodSource("range")
    void testAllCases(int seed) {
        AtomicInteger countCalls = new AtomicInteger();
        BrokenMemoizingSupplier<Integer> supplier =
                new BrokenMemoizingSupplier<>(new YieldingLock(), countCalls::incrementAndGet);
        Random random = new Random(seed);
        RandomizingExecutor executor = new RandomizingExecutor(random);
        newVirtualThread(executor, supplier::get);
        newVirtualThread(executor, supplier::get);
        executor.drain();
        assertThat(supplier.get()).isOne();
    }

    static Stream<Integer> range() {
        return IntStream.range(0, 10).boxed();
    }

    /**
     * This executor executes randomly for brevity. By tracking which tasks launch which other tasks, one could
     * generate all interleavings for small cases such as this.
     */
    private static final class RandomizingExecutor implements Executor {
        private final Random random;
        private final List<Runnable> queue = new ArrayList<>();

        private RandomizingExecutor(Random random) {
            this.random = random;
        }

        @Override
        public void execute(Runnable command) {
            queue.add(command);
        }

        public void drain() {
            while (!queue.isEmpty()) {
                Collections.shuffle(queue, random);
                Runnable task = queue.remove(queue.size() - 1);
                task.run();
            }
        }
    }

    // Thread.yield will pause the thread and cause it to become schedulable again.
    private static final class YieldingLock extends ReentrantLock {
        @Override
        public void lock() {
            Thread.yield();
            super.lock();
        }
    }

    private Thread newVirtualThread(Executor executor, Runnable runnable) {
        return HackVirtualThreads.virtualThreadBuilderFor(executor).start(runnable);
    }
}
