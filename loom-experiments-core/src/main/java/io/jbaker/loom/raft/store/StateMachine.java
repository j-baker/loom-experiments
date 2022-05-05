/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import io.jbaker.loom.raft.api.LogEntry;

public interface StateMachine {
    void apply(LogEntry entry);
}
