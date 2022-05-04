/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.util;

import static com.palantir.logsafe.Preconditions.checkArgument;

import io.jbaker.loom.raft.api.LogIndex;

public final class LogIndexes {
    private LogIndexes() {
    }

    public static final LogIndex ZERO = LogIndex.of(0);
    public static final LogIndex ONE = LogIndex.of(1);

    public static int listIndex(LogIndex index) {
        checkArgument(!index.equals(ZERO), "no list index for the sentinel entry");
        return index.get() - 1;
    }

    public static LogIndex inc(LogIndex index) {
        return LogIndex.of(index.get() + 1);
    }
}
