/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
