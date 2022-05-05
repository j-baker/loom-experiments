/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.ServerId;
import java.util.Map;
import org.immutables.value.Value;

@Value.Modifiable
public interface LeaderVolatileState {
    static LeaderVolatileState create() {
        return ModifiableLeaderVolatileState.create();
    }

    Map<ServerId, LogIndex> getNextIndices();

    Map<ServerId, LogIndex> getMatchIndices();
}
