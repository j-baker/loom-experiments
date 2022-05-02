/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.jbaker.loom.raft.util.BackgroundTask;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    record TermId(long term) implements ExtraComparable<TermId> {

        @Override
        public int compareTo(TermId termId) {
            return Long.compare(term, termId.term);
        }

        public TermId increment() {
            return new TermId(term + 1);
        }
    }

    interface ExtraComparable<T extends ExtraComparable<T>> extends Comparable<T> {
        default boolean greaterThan(T other) {
            return compareTo(other) > 0;
        }

        default boolean geq(T other) {
            return compareTo(other) >= 0;
        }

        default boolean lessThan(T other) {
            return compareTo(other) < 0;
        }
    }

    record LogIndex(int index) implements ExtraComparable<LogIndex> {
        static final LogIndex ZERO = new LogIndex(0);
        static final LogIndex ONE = new LogIndex(1);

        @Override
        public int compareTo(LogIndex logIndex) {
            return Integer.compare(index, logIndex.index);
        }

        public LogIndex inc() {
            return new LogIndex(index + 1);
        }

        public LogIndex dec() {
            return new LogIndex(index - 1);
        }

        public int listIndex() {
            return index - 1;
        }
    }

    record ServerId(UUID candidateId) {
        static ServerId id(int serverIndex) {
            return new ServerId(new UUID(serverIndex, serverIndex));
        }
    }

    record LogEntry(TermId term, byte[] data) {}

    private static final class PersistentState {
        private final ServerId me;
        private TermId currentTerm = new TermId(0);
        private Optional<ServerId> votedFor = Optional.empty();
        private final List<LogEntry> log = new ArrayList<>();

        private PersistentState(ServerId me) {
            this.me = me;
        }

        public LogEntryMetadata lastLogEntry() {
            if (log.isEmpty()) {
                return new LogEntryMetadata(LogIndex.ZERO, new TermId(0));
            }
            int index = log.size();
            TermId term = log.get(index - 1).term;
            return new LogEntryMetadata(new LogIndex(index), term);
        }

        public LogEntryMetadata logMetadata(int logIndex) {
            return new LogEntryMetadata(new LogIndex(logIndex + 1), log.get(logIndex).term);
        }

        public LogEntryMetadata logMetadata(LogIndex logIndex) {
            if (logIndex.equals(LogIndex.ZERO)) {
                return new LogEntryMetadata(LogIndex.ZERO, new TermId(0));
            }
            return logMetadata(logIndex.index - 1);
        }

        public LogEntryMetadata addEntryAsLeader(byte[] data) {
            LogEntry logEntry = new LogEntry(currentTerm, data);
            log.add(logEntry);
            return new LogEntryMetadata(new LogIndex(log.size()), currentTerm);
        }
    }

    record LogEntryMetadata(LogIndex logIndex, TermId termId) implements ExtraComparable<LogEntryMetadata> {
        @Override
        public int compareTo(LogEntryMetadata logEntryMetadata) {
            return Comparator.comparing(LogEntryMetadata::logIndex)
                    .thenComparing(LogEntryMetadata::termId)
                    .compare(this, logEntryMetadata);
        }
    }

    private static final class VolatileState {
        private LogIndex commitIndex = LogIndex.ZERO;
        private LogIndex lastApplied = LogIndex.ZERO;
    }

    private static final class LeaderVolatileState {
        private final Map<ServerId, LogIndex> nextIndices = new HashMap<>();
        private final Map<ServerId, LogIndex> matchIndices = new HashMap<>();
    }

    enum Mode {
        FOLLOWER,
        CANDIDATE,
        LEADER
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
        private Mode mode = Mode.FOLLOWER;

        @GuardedBy("this")
        private Instant lastUpdated;

        // TODO(jbaker): Persistent state or volatile?
        private final StateMachine stateMachine;

        private ServerState(StateMachine stateMachine, PersistentState persistentState) {
            this.persistentState = persistentState;
            this.stateMachine = stateMachine;
        }

        public synchronized Mode getMode() {
            return mode;
        }

        @GuardedBy("this")
        private void setMode(Mode mode) {
            if (mode == this.mode) {
                return;
            }

            if (mode == Mode.LEADER) {
                leaderVolatileState = Optional.of(new LeaderVolatileState());
            } else {
                leaderVolatileState = Optional.empty();
            }
            this.mode = mode;
        }

        @GuardedBy("this")
        private void handleReceivedTerm(TermId term) {
            if (!term.greaterThan(persistentState.currentTerm)) {
                return;
            }
            persistentState.currentTerm = term;
            setMode(Mode.FOLLOWER);
        }

        @GuardedBy("this")
        private void updateLeaderCommitIndex(Set<ServerId> otherServerIds) {
            int quorumSIze = ((otherServerIds.size() + 1) + 1) / 2;
            if (mode == Mode.LEADER) {
                List<LogIndex> matchIndices = Stream.concat(
                                Stream.of(volatileState.commitIndex),
                                otherServerIds.stream().map(id -> leaderVolatileState
                                        .get()
                                        .matchIndices
                                        .getOrDefault(id, LogIndex.ZERO)))
                        .sorted()
                        .toList();
                int matchIndex = matchIndices.size() - quorumSIze;
                if (matchIndex < 0) {
                    return;
                }
                LogIndex replicatedIndex = matchIndices.get(matchIndex);
                if (replicatedIndex.equals(LogIndex.ZERO)) {
                    return;
                }
                LogEntry logEntry = persistentState.log.get(replicatedIndex.listIndex());
                if (logEntry.term.equals(persistentState.currentTerm)) {
                    volatileState.commitIndex = replicatedIndex;
                }
            }
        }

        private synchronized void keepStateMachineUpToDate() {
            while (volatileState.commitIndex.greaterThan(volatileState.lastApplied)) {
                volatileState.lastApplied = volatileState.lastApplied.inc();
                stateMachine.apply(persistentState.log.get(volatileState.lastApplied.listIndex()));
            }
        }
    }

    @Value.Immutable
    interface AppendEntriesRequest {
        TermId term();

        ServerId leaderId();

        LogEntryMetadata prevLogEntry();

        List<LogEntry> entries();

        LogIndex leaderCommit();

        class Builder extends ImmutableAppendEntriesRequest.Builder {}
    }

    @Value.Immutable
    interface AppendEntriesResponse {
        TermId term();

        boolean success();

        class Builder extends ImmutableAppendEntriesResponse.Builder {}
    }

    @Value.Immutable
    interface RequestVoteRequest {
        TermId term();

        ServerId candidateId();

        LogEntryMetadata lastLogEntry();

        class Builder extends ImmutableRequestVoteRequest.Builder {}
    }

    @Value.Immutable
    interface RequestVoteResponse {
        TermId term();

        boolean voteGranted();

        class Builder extends ImmutableRequestVoteResponse.Builder {}
    }

    @Value.Immutable
    interface ApplyCommandRequest {
        byte[] data();

        class Builder extends ImmutableApplyCommandRequest.Builder {}
    }

    @Value.Immutable
    interface ApplyCommandResponse {

        /**
         * Returns true if command was applied to state machine. Returns false if was not leader.
         */
        boolean applied();

        static ApplyCommandResponse of(boolean value) {
            return new ApplyCommandResponse.Builder().applied(value).build();
        }

        class Builder extends ImmutableApplyCommandResponse.Builder {}
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

    public record InitializedServer(ServerId id, Client client, Server server) {}

    public static List<InitializedServer> create(
            int numServers,
            Supplier<StateMachine> stateMachineFactory,
            BackgroundTask.Executor backgroundTaskExecutor,
            Executor clientExecutor) {
        Map<ServerId, ServerImpl> servers = new HashMap<>();
        Map<ServerId, Client> clients = IntStream.range(0, numServers)
                .mapToObj(ServerId::id)
                .collect(Collectors.toUnmodifiableMap(
                        Function.identity(), id -> new ClientShim(id, servers, clientExecutor)));
        IntStream.range(0, numServers)
                .mapToObj(ServerId::id)
                .forEach(id -> servers.put(
                        id,
                        new ServerImpl(
                                stateMachineFactory.get(),
                                new ServerConfig.Builder()
                                        .electionTimeout(Duration.ofMillis(100))
                                        .me(id)
                                        .build(),
                                Clock.systemUTC(),
                                clients)));
        servers.values().forEach(backgroundTaskExecutor::register);
        return IntStream.range(0, numServers)
                .mapToObj(ServerId::id)
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
            this.quorumSize = (this.otherServers.size() + 1) / 2;
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

                return switch (state.mode) {
                    case CANDIDATE, FOLLOWER -> serverConfig.electionTimeout();
                    case LEADER -> serverConfig.electionTimeout().dividedBy(2);
                };
            }
        }

        private boolean innerProgressLeadershipState(Instant now) {
            state.keepStateMachineUpToDate();

            synchronized (state) {
                if (state.mode == Mode.FOLLOWER
                        && (state.lastUpdated == null
                                || now.isAfter(state.lastUpdated.plus(serverConfig.electionTimeout())))) {
                    state.setMode(Mode.CANDIDATE);
                }
            }

            if (state.getMode() == Mode.CANDIDATE) {
                boolean wonElection = tryToWinElection();
                synchronized (state) {
                    if (wonElection && state.mode == Mode.CANDIDATE) {
                        state.setMode(Mode.LEADER);
                    }
                }
            }

            if (state.getMode() == Mode.LEADER) {
                sendNoOpUpdates();
                ensureFollowersUpToDate();
            }

            return false;
        }

        private void sendNoOpUpdates() {
            List<Supplier<ListenableFuture<AppendEntriesResponse>>> suppliers = new ArrayList<>();
            synchronized (state) {
                if (state.mode != Mode.LEADER) {
                    return;
                }
                otherServers.forEach((serverId, client) -> new AppendEntriesRequest.Builder()
                        .leaderId(state.persistentState.me)
                        .term(state.persistentState.currentTerm)
                        .leaderCommit(state.volatileState.commitIndex)
                        .prevLogEntry(state.persistentState.logMetadata(
                                state.leaderVolatileState.get().matchIndices.getOrDefault(serverId, LogIndex.ZERO)))
                        .build());
            }
            // TODO(jbaker): refactor to use vthreads for this...
            for (ListenableFuture<AppendEntriesResponse> future :
                    suppliers.stream().map(Supplier::get).toList()) {
                try {
                    AppendEntriesResponse response = future.get();
                    synchronized (state) {
                        state.handleReceivedTerm(response.term());
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
                if (lastLogEntry.logIndex.equals(LogIndex.ZERO)) {
                    return;
                }

                LogIndex nextIndex = leaderState.nextIndices.getOrDefault(server, LogIndex.ONE);
                if (lastLogEntry.logIndex.geq(nextIndex)) {
                    AppendEntriesRequest request;
                    synchronized (state) {
                        if (state.mode != Mode.LEADER) {
                            return;
                        }
                        List<LogEntry> logEntries = state.persistentState.log.subList(
                                nextIndex.listIndex(), state.persistentState.log.size());
                        LogEntryMetadata prevEntry = nextIndex.listIndex() == 0
                                ? new LogEntryMetadata(LogIndex.ZERO, new TermId(0))
                                : state.persistentState.logMetadata(nextIndex.listIndex() - 1);
                        request = new AppendEntriesRequest.Builder()
                                .term(state.persistentState.currentTerm)
                                .leaderId(state.persistentState.me)
                                .leaderCommit(state.volatileState.commitIndex)
                                .prevLogEntry(prevEntry)
                                .entries(logEntries)
                                .build();
                    }
                    AppendEntriesResponse response =
                            Futures.getUnchecked(otherServers.get(server).appendEntries(request));
                    synchronized (state) {
                        // races?
                        if (response.success()) {
                            leaderState.matchIndices.put(server, lastLogEntry.logIndex);
                            leaderState.nextIndices.put(server, lastLogEntry.logIndex.inc());
                            return;
                        } else if (response.term().equals(state.persistentState.currentTerm)) {
                            leaderState.nextIndices.put(server, nextIndex.dec());
                        } else {
                            state.handleReceivedTerm(response.term());
                            return;
                        }
                    }
                }
            }
        }

        private boolean tryToWinElection() {
            TermId electionTerm;
            LogEntryMetadata lastLogEntry;
            synchronized (state) {
                if (state.mode != Mode.CANDIDATE) {
                    return false;
                }
                electionTerm = state.persistentState.currentTerm.increment();
                state.persistentState.currentTerm = electionTerm;
                state.persistentState.votedFor = Optional.of(state.persistentState.me);
                lastLogEntry = state.persistentState.lastLogEntry();
            }
            RequestVoteRequest request = new RequestVoteRequest.Builder()
                    .candidateId(me)
                    .term(electionTerm)
                    .lastLogEntry(lastLogEntry)
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
                        state.handleReceivedTerm(response.term());
                    }
                    if (response.voteGranted()) {
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
                return new AppendEntriesResponse.Builder()
                        .term(state.persistentState.currentTerm)
                        .success(internalAppendEntries(request))
                        .build();
            }
        }

        @GuardedBy("state")
        @SuppressWarnings("CyclomaticComplexity")
        private boolean internalAppendEntries(AppendEntriesRequest request) {
            if (request.term().lessThan(state.persistentState.currentTerm)) {
                return false;
            }

            List<LogEntry> log = state.persistentState.log;
            LogEntryMetadata prevLogEntry = request.prevLogEntry();

            int prevLogIndex = prevLogEntry.logIndex.listIndex() + 1;
            if (!prevLogEntry.logIndex.equals(LogIndex.ZERO)
                    && (log.size() <= prevLogIndex
                            || !log.get(prevLogIndex).term.equals(prevLogEntry.termId))) {
                return false;
            }

            List<LogEntry> overlapping = log.subList(prevLogIndex, log.size());
            for (int i = 0; i < request.entries().size() && i < overlapping.size(); i++) {
                if (!request.entries().get(i).equals(overlapping.get(i))) {
                    overlapping.subList(i, overlapping.size()).clear();
                    break;
                }
            }

            log.addAll(request.entries()
                    .subList(log.size() - prevLogIndex, request.entries().size()));

            if (request.leaderCommit().index > state.volatileState.commitIndex.index) {
                state.volatileState.commitIndex =
                        Ordering.natural().min(request.leaderCommit(), new LogIndex(log.size() - 1));
            }

            return true;
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            synchronized (state) {
                return new RequestVoteResponse.Builder()
                        .term(state.persistentState.currentTerm)
                        .voteGranted(requestVoteInternal(request))
                        .build();
            }
        }

        @GuardedBy("state")
        private boolean requestVoteInternal(RequestVoteRequest request) {
            state.handleReceivedTerm(request.term());

            if (request.term().lessThan(state.persistentState.currentTerm)) {
                return false;
            }

            if (state.persistentState.votedFor.isPresent()
                    && !state.persistentState.votedFor.get().equals(request.candidateId())) {
                return false;
            }

            // TODO(jbaker): I'm not sure that this logic is quite correct...
            LogEntryMetadata ourLast = state.persistentState.lastLogEntry();
            if (request.lastLogEntry().lessThan(ourLast)) {
                return false;
            }
            state.persistentState.votedFor = Optional.of(request.candidateId());
            return true;
        }

        @Override
        public ApplyCommandResponse applyCommand(ApplyCommandRequest request) {
            LogEntryMetadata entry;
            synchronized (state) {
                if (state.mode != Mode.LEADER) {
                    return ApplyCommandResponse.of(false);
                }
                entry = state.persistentState.addEntryAsLeader(request.data());
            }
            ensureFollowersUpToDate();
            synchronized (state) {
                if (state.volatileState.commitIndex.geq(entry.logIndex)
                        && state.persistentState.logMetadata(entry.logIndex).equals(entry)) {
                    state.keepStateMachineUpToDate();
                    return ApplyCommandResponse.of(true);
                } else {
                    return ApplyCommandResponse.of(false);
                }
            }
        }
    }
}
