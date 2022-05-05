/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.util.LogIndexes;
import org.immutables.value.Value;

@Value.Modifiable
public interface VolatileState {
    static VolatileState create() {
        return ModifiableVolatileState.create();
    }

    @Value.Default
    default LogIndex getCommitIndex() {
        return LogIndexes.ZERO;
    }

    VolatileState setCommitIndex(LogIndex commitIndex);

    @Value.Default
    default LogIndex getLastApplied() {
        return LogIndexes.ZERO;
    }

    VolatileState setLastApplied(LogIndex lastApplied);
}
