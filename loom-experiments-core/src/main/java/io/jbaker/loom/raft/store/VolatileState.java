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
