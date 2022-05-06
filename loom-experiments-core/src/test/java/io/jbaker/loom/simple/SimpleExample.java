/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.simple;

import static org.assertj.core.api.Assertions.assertThat;

import io.jbaker.loom.raft.simulation.HackVirtualThreads;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

public final class SimpleExample {
    @Test
    void testVirtualThread() {
        Queue<Runnable> executor = new ArrayDeque<>();
        Lock lock = new ReentrantLock();
        lock.lock();
        newVirtualThread(executor::add, lock::lock);
        assertThat(executor).hasSize(1).as("runnable for vthread has been submitted");
        executor.poll().run();
        assertThat(executor).hasSize(0).as("vthread has blocked, no longer runnable");
        lock.unlock();
        assertThat(executor).hasSize(1).as("due to unlock, the vthread is now schedulable again");
        executor.poll().run();
        assertThat(lock.tryLock()).isFalse().as("the virtual thread now holds the lock");
    }

    private Thread newVirtualThread(Executor executor, Runnable runnable) {
        return HackVirtualThreads.virtualThreadBuilderFor(executor).start(runnable);
    }
}
