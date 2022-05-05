/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import io.jbaker.loom.raft.api.LeadershipMode;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.util.LogIndexes;
import io.jbaker.loom.raft.util.TermIds;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Modifiable
public interface ServerState {
    static ServerState create(PersistentState persistentState, StateMachine stateMachine) {
        return ModifiableServerState.create(persistentState, stateMachine, VolatileState.create());
    }

    @Value.Parameter
    PersistentState getPersistent();

    @Value.Parameter
    StateMachine getStateMachine();

    @Value.Parameter
    VolatileState getVolatile();

    Optional<LeaderVolatileState> getLeaderVolatile();

    ServerState setLeaderVolatile(Optional<LeaderVolatileState> leaderVolatileState);

    @Value.Default
    default Instant getLastUpdated() {
        return Instant.MIN;
    }

    ServerState setLastUpdated(Instant lastUpdated);

    @Value.Default
    default LeadershipMode getLeadershipMode() {
        return LeadershipMode.FOLLOWER;
    }

    default LeadershipMode getMode() {
        return getLeadershipMode();
    }

    default void setMode(LeadershipMode newMode) {
        if (newMode.equals(getLeadershipMode())) {
            return;
        }

        if (newMode == LeadershipMode.LEADER) {
            setLeaderVolatile(Optional.of(LeaderVolatileState.create()));
        } else {
            setLeaderVolatile(Optional.empty());
        }
        ((ModifiableServerState) this).setLeadershipMode(newMode);
    }

    default void handleReceivedTerm(TermId term) {
        if (!TermIds.isGreaterThan(term, getPersistent().getCurrentTerm())) {
            return;
        }
        getPersistent().setCurrentTerm(term);
        setMode(LeadershipMode.FOLLOWER);
        getPersistent().setVotedFor(Optional.empty());
    }

    default void updateLeaderCommitIndex(Set<ServerId> otherServerIds) {
        int quorumSIze = ((otherServerIds.size() + 1) + 1) / 2;
        if (getMode() == LeadershipMode.LEADER) {
            List<LogIndex> matchIndices = Stream.concat(
                            Stream.of(getVolatile().getCommitIndex()),
                            otherServerIds.stream().map(id -> getLeaderVolatile()
                                    .get()
                                    .getMatchIndices()
                                    .getOrDefault(id, LogIndexes.ZERO)))
                    .sorted(Comparator.comparing(LogIndex::get))
                    .toList();
            int matchIndex = matchIndices.size() - quorumSIze;
            if (matchIndex < 0) {
                return;
            }
            LogIndex replicatedIndex = matchIndices.get(matchIndex);
            if (replicatedIndex.equals(LogIndexes.ZERO)) {
                return;
            }
            LogEntry logEntry = getPersistent().getLog().get(LogIndexes.listIndex(replicatedIndex));
            if (logEntry.getTerm().equals(getPersistent().getCurrentTerm())) {
                getVolatile().setCommitIndex(replicatedIndex);
            }
        }
    }

    default void keepStateMachineUpToDate() {
        while (getVolatile().getCommitIndex().get()
                > getVolatile().getLastApplied().get()) {
            getVolatile().setLastApplied(LogIndexes.inc(getVolatile().getLastApplied()));
            getStateMachine()
                    .apply(getPersistent()
                            .getLog()
                            .get(LogIndexes.listIndex(getVolatile().getLastApplied())));
        }
    }
}
