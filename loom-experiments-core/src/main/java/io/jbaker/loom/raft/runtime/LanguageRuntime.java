/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public interface LanguageRuntime {
    Clock clock();

    void sleep(Duration duration);

    Lock newLock();

    final class ProductionLanguageRuntime implements LanguageRuntime {
        @Override
        public Clock clock() {
            return Clock.systemUTC();
        }

        @Override
        public void sleep(Duration duration) {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public Lock newLock() {
            return new ReentrantLock();
        }
    }
}
