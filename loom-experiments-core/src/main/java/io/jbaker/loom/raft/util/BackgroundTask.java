/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.util;

import java.io.Closeable;
import java.time.Duration;

public interface BackgroundTask {
    /**
     * Return the next time the task should be run.
     */
    Duration runOneIteration();

    interface Executor extends Closeable {
        @Override
        void close();

        void register(BackgroundTask task);
    }
}
