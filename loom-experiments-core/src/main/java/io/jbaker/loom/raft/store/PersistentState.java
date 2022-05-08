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

import io.jbaker.loom.raft.api.Command;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.LogEntryMetadata;
import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.util.LogIndexes;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Modifiable
public interface PersistentState {
    static PersistentState create(ServerId me) {
        return ModifiablePersistentState.create(me);
    }

    @Value.Parameter
    ServerId getMe();

    @Value.Default
    default TermId getCurrentTerm() {
        return TermId.of(0);
    }

    PersistentState setCurrentTerm(TermId currentTerm);

    Optional<ServerId> getVotedFor();

    PersistentState setVotedFor(Optional<ServerId> votedFor);

    List<LogEntry> getLog();

    default LogEntryMetadata lastLogEntry() {
        if (getLog().isEmpty()) {
            return LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO);
        }
        int index = getLog().size();
        TermId term = getLog().get(index - 1).getTerm();
        return LogEntryMetadata.of(term, LogIndex.of(index));
    }

    default LogEntryMetadata logMetadata(int logIndex) {
        return LogEntryMetadata.of(getLog().get(logIndex).getTerm(), LogIndex.of(logIndex + 1));
    }

    default LogEntryMetadata logMetadata(LogIndex logIndex) {
        if (logIndex.equals(LogIndexes.ZERO)) {
            return LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO);
        }
        return logMetadata(logIndex.get() - 1);
    }

    default LogEntryMetadata addEntryAsLeader(Command data) {
        LogEntry logEntry = LogEntry.of(getCurrentTerm(), data);
        getLog().add(logEntry);
        return LogEntryMetadata.of(getCurrentTerm(), LogIndex.of(getLog().size()));
    }
}
