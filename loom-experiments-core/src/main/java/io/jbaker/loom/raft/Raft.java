/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import static com.palantir.logsafe.Preconditions.checkArgument;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import io.jbaker.loom.raft.api.AppendEntriesRequest;
import io.jbaker.loom.raft.api.AppendEntriesResponse;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.ApplyCommandResponse;
import io.jbaker.loom.raft.api.Command;
import io.jbaker.loom.raft.api.LeadershipMode;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.LogEntryMetadata;
import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.RequestVoteRequest;
import io.jbaker.loom.raft.api.RequestVoteResponse;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.util.BackgroundTask;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.immutables.value.Value;

public final class Raft {
    private Raft() {}

    private static final class LogIndexes {
        private LogIndexes() {}

        static final LogIndex ZERO = LogIndex.of(0);
        static final LogIndex ONE = LogIndex.of(1);

        public static boolean isGreaterThan(LogIndex left, LogIndex right) {
            return left.get() > right.get();
        }

        public static int listIndex(LogIndex index) {
            checkArgument(!index.equals(ZERO), "no list index for the sentinel entry");
            return index.get() - 1;
        }

        public static LogIndex inc(LogIndex index) {
            return LogIndex.of(index.get() + 1);
        }
    }

    private static final class ServerIds {
        private ServerIds() {}

        static ServerId of(int id) {
            return ServerId.of(new UUID(id, id));
        }
    }

    private static final class TermIds {
        private TermIds() {}

        public static boolean isGreaterThan(TermId left, TermId right) {
            return left.get() > right.get();
        }
    }

    private static final class PersistentState {
        private final ServerId me;
        private TermId currentTerm = TermId.of(0);
        private Optional<ServerId> votedFor = Optional.empty();
        private final List<LogEntry> log = new ArrayList<>();

        private PersistentState(ServerId me) {
            this.me = me;
        }

        public LogEntryMetadata lastLogEntry() {
            if (log.isEmpty()) {
                return LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO);
            }
            int index = log.size();
            TermId term = log.get(index - 1).getTerm();
            return LogEntryMetadata.of(term, LogIndex.of(index));
        }

        public LogEntryMetadata logMetadata(int logIndex) {
            return LogEntryMetadata.of(log.get(logIndex).getTerm(), LogIndex.of(logIndex + 1));
        }

        public LogEntryMetadata logMetadata(LogIndex logIndex) {
            if (logIndex.equals(LogIndexes.ZERO)) {
                return LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO);
            }
            return logMetadata(logIndex.get() - 1);
        }

        public LogEntryMetadata addEntryAsLeader(Command data) {
            LogEntry logEntry = LogEntry.of(currentTerm, data);
            log.add(logEntry);
            return LogEntryMetadata.of(currentTerm, LogIndex.of(log.size()));
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            PersistentState that = (PersistentState) other;
            return me.equals(that.me) && currentTerm.equals(that.currentTerm) && votedFor.equals(that.votedFor)
                    && log.equals(
                    that.log);
        }

        @Override
        public int hashCode() {
            return Objects.hash(me, currentTerm, votedFor, log);
        }
    }

    private static final class VolatileState {
        private LogIndex commitIndex = LogIndexes.ZERO;
        private LogIndex lastApplied = LogIndexes.ZERO;

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            VolatileState that = (VolatileState) other;
            return commitIndex.equals(that.commitIndex) && lastApplied.equals(that.lastApplied);
        }

        @Override
        public int hashCode() {
            return Objects.hash(commitIndex, lastApplied);
        }
    }

    private static final class LeaderVolatileState {
        private final Map<ServerId, LogIndex> nextIndices = new HashMap<>();
        private final Map<ServerId, LogIndex> matchIndices = new HashMap<>();

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            LeaderVolatileState that = (LeaderVolatileState) other;
            return nextIndices.equals(that.nextIndices) && matchIndices.equals(that.matchIndices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nextIndices, matchIndices);
        }
    }

    interface StateMachine {
        void init();

        void apply(LogEntry entry);
    }

    @Value.Immutable
    interface ServerConfig {
        ServerId me();

        Duration electionTimeout();

        class Builder extends ImmutableServerConfig.Builder {}
    }

    private static final class ServerState {
        @GuardedBy("this")
        private final PersistentState persistentState;

        @GuardedBy("this")
        private final VolatileState volatileState = new VolatileState();

        @GuardedBy("this")
        private Optional<LeaderVolatileState> leaderVolatileState = Optional.empty();

        @GuardedBy("this")
        private LeadershipMode mode = LeadershipMode.FOLLOWER;

        @GuardedBy("this")
        private Instant lastUpdated = Instant.MIN;

        // TODO(jbaker): Persistent state or volatile?
        private final StateMachine stateMachine;

        private ServerState(StateMachine stateMachine, PersistentState persistentState) {
            this.persistentState = persistentState;
            this.stateMachine = stateMachine;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            ServerState that = (ServerState) other;
            return persistentState.equals(that.persistentState) && volatileState.equals(that.volatileState)
                    && leaderVolatileState.equals(that.leaderVolatileState) && mode == that.mode
                    && lastUpdated.equals(that.lastUpdated);
        }

        @Override
        public int hashCode() {
            return Objects.hash(persistentState, volatileState, leaderVolatileState, mode, lastUpdated);
        }

        public synchronized LeadershipMode getLeadershipMode() {
            return mode;
        }

        @GuardedBy("this")
        private void setLeadershipMode(LeadershipMode newMode) {
            if (newMode == this.mode) {
                return;
            }

            if (newMode == LeadershipMode.LEADER) {
                leaderVolatileState = Optional.of(new LeaderVolatileState());
            } else {
                leaderVolatileState = Optional.empty();
            }
            this.mode = newMode;
        }

        @GuardedBy("this")
        private void handleReceivedTerm(TermId term) {
            if (!TermIds.isGreaterThan(term, persistentState.currentTerm)) {
                return;
            }
            persistentState.currentTerm = term;
            setLeadershipMode(LeadershipMode.FOLLOWER);
            persistentState.votedFor = Optional.empty();
        }

        @GuardedBy("this")
        private void updateLeaderCommitIndex(Set<ServerId> otherServerIds) {
            int quorumSIze = ((otherServerIds.size() + 1) + 1) / 2;
            if (mode == LeadershipMode.LEADER) {
                List<LogIndex> matchIndices = Stream.concat(
                                Stream.of(volatileState.commitIndex),
                                otherServerIds.stream().map(id -> leaderVolatileState
                                        .get()
                                        .matchIndices
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
                LogEntry logEntry = persistentState.log.get(LogIndexes.listIndex(replicatedIndex));
                if (logEntry.getTerm().equals(persistentState.currentTerm)) {
                    volatileState.commitIndex = replicatedIndex;
                }
            }
        }

        private synchronized void keepStateMachineUpToDate() {
            while (LogIndexes.isGreaterThan(volatileState.commitIndex, volatileState.lastApplied)) {
                volatileState.lastApplied = LogIndexes.inc(volatileState.lastApplied);
                stateMachine.apply(persistentState.log.get(LogIndexes.listIndex(volatileState.lastApplied)));
            }
        }
    }

    interface Server {
        AppendEntriesResponse appendEntries(AppendEntriesRequest request);

        RequestVoteResponse requestVote(RequestVoteRequest request);

        ApplyCommandResponse applyCommand(ApplyCommandRequest request);
    }

    interface Client {
        ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);

        ListenableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

        ListenableFuture<ApplyCommandResponse> applyCommand(ApplyCommandRequest request);
    }

    public record InitializedServer(ServerId id, Client client, Server server) {
        @Override
        public String toString() {
            ServerImpl serv = (ServerImpl)  server;
            synchronized (serv.state) {
                return "id: %s, mode: %s, currentTerm: %s, votedFor: %s".formatted(serv.me, serv.state.mode,
                        serv.state.persistentState.currentTerm, serv.state.persistentState.votedFor);
            }
        }

        public boolean isLeader() {
            ServerImpl serv = (ServerImpl)  server;
            synchronized (serv.state) {
                return serv.state.mode == LeadershipMode.LEADER;
            }
        }

        public boolean stateEquals(InitializedServer other) {
            ServerImpl serv1 = (ServerImpl)  server;
            ServerImpl serv2 = (ServerImpl) other.server;
            synchronized (serv1.state) {
                synchronized (serv2.state) {
                    return serv1.state.equals(serv2.state);
                }
            }
        }
    }

    public static List<InitializedServer> create(
            int numServers,
            Supplier<StateMachine> stateMachineFactory,
            BackgroundTask.Executor backgroundTaskExecutor,
            Executor clientExecutor,
            Clock clock) {
        Map<ServerId, ServerImpl> servers = new HashMap<>();
        Map<ServerId, Client> clients = IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .collect(Collectors.toUnmodifiableMap(
                        Function.identity(), id -> new ClientShim(id, servers, clientExecutor)));
        IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .forEach(id -> servers.put(
                        id,
                        new ServerImpl(
                                stateMachineFactory.get(),
                                new ServerConfig.Builder()
                                        .electionTimeout(Duration.ofMillis(100))
                                        .me(id)
                                        .build(),
                                clock,
                                clients)));
        IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .map(servers::get)
                .forEach(backgroundTaskExecutor::register);
        return IntStream.range(0, numServers)
                .mapToObj(ServerIds::of)
                .map(id -> new InitializedServer(id, clients.get(id), servers.get(id)))
                .toList();
    }

    private static final class ClientShim implements Client {
        private final ServerId serverId;
        private final Map<ServerId, ? extends Server> servers;

        private final Executor executor;

        private ClientShim(ServerId serverId, Map<ServerId, ? extends Server> servers, Executor executor) {
            this.serverId = serverId;
            this.servers = servers;
            this.executor = executor;
        }

        @Override
        public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            return runOnServer(s -> s.appendEntries(request));
        }

        @Override
        public ListenableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            return runOnServer(s -> s.requestVote(request));
        }

        @Override
        public ListenableFuture<ApplyCommandResponse> applyCommand(ApplyCommandRequest request) {
            return runOnServer(s -> s.applyCommand(request));
        }

        private <T> ListenableFuture<T> runOnServer(Function<Server, T> func) {
            return Futures.submit(() -> func.apply(servers.get(serverId)), executor);
        }
    }

    private static final class ServerImpl implements Server, BackgroundTask {
        private final ServerId me;
        private final ServerState state;
        private final ServerConfig serverConfig;
        private final Clock clock;

        private final Map<ServerId, Client> otherServers;

        private final int quorumSize;

        private ServerImpl(
                StateMachine stateMachine, ServerConfig serverConfig, Clock clock, Map<ServerId, Client> otherServers) {
            this.me = serverConfig.me();
            this.otherServers = Map.copyOf(Maps.filterKeys(otherServers, id -> !id.equals(serverConfig.me())));
            this.state = new ServerState(stateMachine, new PersistentState(serverConfig.me()));
            this.serverConfig = serverConfig;
            this.clock = clock;
            this.quorumSize = (this.otherServers.size() + 1 + 1) / 2;
        }

        @Override
        public Duration runOneIteration() {
            return progressLeadershipState(clock.instant());
        }

        private Duration progressLeadershipState(Instant now) {
            innerProgressLeadershipState(now);

            synchronized (state) {
                // possibly not right
                state.lastUpdated = now;

                return switch (state.mode.get()) {
                    case CANDIDATE, FOLLOWER -> serverConfig.electionTimeout();
                    case LEADER -> serverConfig.electionTimeout().dividedBy(2);
                    case UNKNOWN -> throw new SafeIllegalStateException("unreachable");
                };
            }
        }

        private boolean innerProgressLeadershipState(Instant now) {
            state.keepStateMachineUpToDate();

            synchronized (state) {
                if (state.mode == LeadershipMode.FOLLOWER
                        && (state.lastUpdated == null
                                || now.isAfter(state.lastUpdated.plus(serverConfig.electionTimeout())))) {
                    state.setLeadershipMode(LeadershipMode.CANDIDATE);
                }
            }

            if (state.getLeadershipMode() == LeadershipMode.CANDIDATE) {
                boolean wonElection = tryToWinElection();
                synchronized (state) {
                    if (wonElection && state.mode == LeadershipMode.CANDIDATE) {
                        state.setLeadershipMode(LeadershipMode.LEADER);
                    }
                }
            }

            if (state.getLeadershipMode() == LeadershipMode.LEADER) {
                sendNoOpUpdates();
                ensureFollowersUpToDate();
            }

            return false;
        }

        private void sendNoOpUpdates() {
            List<Supplier<ListenableFuture<AppendEntriesResponse>>> suppliers = new ArrayList<>();
            synchronized (state) {
                if (state.mode != LeadershipMode.LEADER) {
                    return;
                }
                otherServers.forEach((serverId, client) -> {
                    LogEntryMetadata logEntryMetadata = state.persistentState.logMetadata(
                            state.leaderVolatileState.get().matchIndices.getOrDefault(serverId, LogIndexes.ZERO));
                    suppliers.add(() -> client.appendEntries(AppendEntriesRequest.builder()
                            .leaderId(state.persistentState.me)
                            .term(state.persistentState.currentTerm)
                            .leaderCommit(state.volatileState.commitIndex)
                            .prevLogTerm(logEntryMetadata.getTerm())
                            .prevLogIndex(logEntryMetadata.getIndex())
                            .build()));
                });
            }
            // TODO(jbaker): refactor to use vthreads for this...
            for (ListenableFuture<AppendEntriesResponse> future :
                    suppliers.stream().map(Supplier::get).toList()) {
                try {
                    AppendEntriesResponse response = future.get();
                    synchronized (state) {
                        state.handleReceivedTerm(response.getTerm());
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    // TODO(jbaker): log
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        private void ensureFollowersUpToDate() {
            // TODO(jbaker): run this in vthreads
            otherServers.keySet().forEach(this::ensureFollowerUpToDate);
            synchronized (state) {
                state.updateLeaderCommitIndex(otherServers.keySet());
            }
        }

        private void ensureFollowerUpToDate(ServerId server) {
            while (true) {
                Optional<LeaderVolatileState> maybeLeaderState;
                LogEntryMetadata lastLogEntry;
                synchronized (state) {
                    maybeLeaderState = state.leaderVolatileState;
                    lastLogEntry = state.persistentState.lastLogEntry();
                }
                if (maybeLeaderState.isEmpty()) {
                    return;
                }
                LeaderVolatileState leaderState = maybeLeaderState.get();
                if (lastLogEntry.getIndex().equals(LogIndexes.ZERO)) {
                    return;
                }

                LogIndex nextIndex = leaderState.nextIndices.getOrDefault(server, LogIndexes.ONE);
                if (lastLogEntry.getIndex().get() >= nextIndex.get()) {
                    AppendEntriesRequest request;
                    synchronized (state) {
                        if (state.mode != LeadershipMode.LEADER) {
                            return;
                        }
                        List<LogEntry> logEntries = state.persistentState.log.subList(
                                LogIndexes.listIndex(nextIndex), state.persistentState.log.size());
                        LogEntryMetadata prevEntry = LogIndexes.listIndex(nextIndex) == 0
                                ? LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO)
                                : state.persistentState.logMetadata(LogIndexes.listIndex(nextIndex) - 1);
                        request = AppendEntriesRequest.builder()
                                .term(state.persistentState.currentTerm)
                                .leaderId(state.persistentState.me)
                                .leaderCommit(state.volatileState.commitIndex)
                                .prevLogIndex(prevEntry.getIndex())
                                .prevLogTerm(prevEntry.getTerm())
                                .entries(logEntries)
                                .build();
                    }
                    AppendEntriesResponse response =
                            Futures.getUnchecked(otherServers.get(server).appendEntries(request));
                    synchronized (state) {
                        // races?
                        if (response.getSuccess()) {
                            leaderState.matchIndices.put(server, lastLogEntry.getIndex());
                            leaderState.nextIndices.put(server, LogIndexes.inc(lastLogEntry.getIndex()));
                            return;
                        } else if (response.getTerm().equals(state.persistentState.currentTerm)) {
                            leaderState.nextIndices.remove(server);
                            // leaderState.nextIndices.put(server, nextIndex.dec());
                        } else {
                            state.handleReceivedTerm(response.getTerm());
                            return;
                        }
                    }
                } else {
                    return;
                }
            }
        }

        private boolean tryToWinElection() {
            TermId electionTerm;
            LogEntryMetadata lastLogEntry;
            synchronized (state) {
                if (state.mode != LeadershipMode.CANDIDATE) {
                    return false;
                }
                electionTerm = TermId.of(state.persistentState.currentTerm.get() + 1);
                state.persistentState.currentTerm = electionTerm;
                state.persistentState.votedFor = Optional.of(state.persistentState.me);
                lastLogEntry = state.persistentState.lastLogEntry();
            }
            RequestVoteRequest request = RequestVoteRequest.builder()
                    .candidateId(me)
                    .term(electionTerm)
                    .lastLogIndex(lastLogEntry.getIndex())
                    .lastLogTerm(lastLogEntry.getTerm())
                    .build();
            List<ListenableFuture<RequestVoteResponse>> voteResponses =
                    Futures.inCompletionOrder(otherServers.values().stream()
                            .map(client -> client.requestVote(request))
                            .toList());
            int votesCollected = 1;
            for (int i = 0; i < voteResponses.size() && votesCollected < quorumSize; i++) {
                try {
                    RequestVoteResponse response = voteResponses.get(i).get();
                    synchronized (state) {
                        // TODO(jbaker): handle what happened if term did change
                        state.handleReceivedTerm(response.getTerm());
                    }
                    if (response.getVoteGranted()) {
                        votesCollected++;
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    // TODO(jbaker): Log
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return votesCollected >= quorumSize;
        }

        @Override
        public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
            synchronized (state) {
                state.handleReceivedTerm(request.getTerm());
                return AppendEntriesResponse.builder()
                        .term(state.persistentState.currentTerm)
                        .success(internalAppendEntries(request))
                        .build();
            }
        }

        @GuardedBy("state")
        @SuppressWarnings("CyclomaticComplexity")
        private boolean internalAppendEntries(AppendEntriesRequest request) {
            if (request.getTerm().get() < state.persistentState.currentTerm.get()) {
                return false;
            }

            List<LogEntry> log = state.persistentState.log;

            int prevLogIndex = request.getPrevLogIndex().get();
            if (!request.getPrevLogIndex().equals(LogIndexes.ZERO)
                    && (log.size() <= prevLogIndex
                            || !log.get(prevLogIndex).getTerm().equals(request.getPrevLogTerm()))) {
                return false;
            }

            List<LogEntry> overlapping = log.subList(prevLogIndex, log.size());
            for (int i = 0; i < request.getEntries().size() && i < overlapping.size(); i++) {
                if (!request.getEntries().get(i).equals(overlapping.get(i))) {
                    overlapping.subList(i, overlapping.size()).clear();
                    break;
                }
            }

            log.addAll(request.getEntries()
                    .subList(request.getEntries().size() - (log.size() - prevLogIndex), request.getEntries().size()));

            if (request.getLeaderCommit().get() > state.volatileState.commitIndex.get()) {
                state.volatileState.commitIndex =
                        LogIndex.of(Math.min(request.getLeaderCommit().get(), log.size() - 1));
            }

            state.lastUpdated = clock.instant();
            return true;
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            synchronized (state) {
                state.handleReceivedTerm(request.getTerm());
                return RequestVoteResponse.builder()
                        .term(state.persistentState.currentTerm)
                        .voteGranted(requestVoteInternal(request))
                        .build();
            }
        }

        @GuardedBy("state")
        private boolean requestVoteInternal(RequestVoteRequest request) {
            if (request.getTerm().get() < state.persistentState.currentTerm.get()) {
                return false;
            }

            if (state.persistentState.votedFor.isPresent()
                    && !state.persistentState.votedFor.get().equals(request.getCandidateId())) {
                return false;
            }

            // TODO(jbaker): I'm not sure that this logic is quite correct...
            LogEntryMetadata ourLast = state.persistentState.lastLogEntry();
            if (request.getLastLogIndex().get() < ourLast.getIndex().get()
                    || (request.getLastLogIndex().get() == ourLast.getIndex().get()
                            && request.getLastLogTerm().get() < ourLast.getTerm().get())) {
                return false;
            }
            state.persistentState.votedFor = Optional.of(request.getCandidateId());
            return true;
        }

        @Override
        public ApplyCommandResponse applyCommand(ApplyCommandRequest request) {
            LogEntryMetadata entry;
            synchronized (state) {
                if (!state.mode.equals(LeadershipMode.LEADER)) {
                    return ApplyCommandResponse.of(false);
                }
                entry = state.persistentState.addEntryAsLeader(request.getData());
            }
            ensureFollowersUpToDate();
            synchronized (state) {
                if (state.volatileState.commitIndex.get() >= entry.getIndex().get()
                        && state.persistentState.logMetadata(entry.getIndex()).equals(entry)) {
                    state.keepStateMachineUpToDate();
                    return ApplyCommandResponse.of(true);
                } else {
                    return ApplyCommandResponse.of(false);
                }
            }
        }
    }
}
