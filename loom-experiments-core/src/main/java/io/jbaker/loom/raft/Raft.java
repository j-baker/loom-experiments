/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

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
import io.jbaker.loom.raft.util.LogIndexes;
import io.jbaker.loom.raft.util.ServerIds;
import io.jbaker.loom.raft.util.TermIds;
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

    @Value.Modifiable
    interface PersistentState {
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

    @Value.Modifiable
    interface VolatileState {
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


    @Value.Modifiable
    interface LeaderVolatileState {
        static LeaderVolatileState create() {
            return ModifiableLeaderVolatileState.create();
        }

        Map<ServerId, LogIndex> getNextIndices();
        Map<ServerId, LogIndex> getMatchIndices();
    }

    interface StateMachine {
        void apply(LogEntry entry);
    }

    @Value.Immutable
    interface ServerConfig {
        ServerId me();

        Duration electionTimeout();

        class Builder extends ImmutableServerConfig.Builder {}
    }

    @Value.Modifiable
    interface ServerState {
        static ServerState create(PersistentState persistentState, StateMachine stateMachine) {
            return ModifiableServerState.create(persistentState, stateMachine);
        }

        @Value.Parameter
        PersistentState getPersistent();

        @Value.Parameter
        StateMachine getStateMachine();

        @Value.Default
        default VolatileState getVolatile() {
            return VolatileState.create();
        }

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
            while (getVolatile().getCommitIndex().get() > getVolatile().getLastApplied().get()) {
                getVolatile().setLastApplied(LogIndexes.inc(getVolatile().getLastApplied()));
                getStateMachine().apply(getPersistent().getLog().get(LogIndexes.listIndex(getVolatile().getLastApplied())));
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
                return "id: %s, mode: %s, currentTerm: %s, votedFor: %s".formatted(serv.me, serv.state.getMode(),
                        serv.state.getPersistent().getCurrentTerm(), serv.state.getPersistent().getVotedFor());
            }
        }

        public boolean isLeader() {
            ServerImpl serv = (ServerImpl)  server;
            synchronized (serv.state) {
                return serv.state.getMode() == LeadershipMode.LEADER;
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
            this.state = ServerState.create(PersistentState.create(serverConfig.me()), stateMachine);
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
                state.setLastUpdated(now);

                return switch (state.getMode().get()) {
                    case CANDIDATE, FOLLOWER -> serverConfig.electionTimeout();
                    case LEADER -> serverConfig.electionTimeout().dividedBy(2);
                    case UNKNOWN -> throw new SafeIllegalStateException("unreachable");
                };
            }
        }

        private boolean innerProgressLeadershipState(Instant now) {
            state.keepStateMachineUpToDate();

            synchronized (state) {
                if (state.getMode() == LeadershipMode.FOLLOWER
                        && (state.getLastUpdated() == null
                                || now.isAfter(state.getLastUpdated().plus(serverConfig.electionTimeout())))) {
                    state.setMode(LeadershipMode.CANDIDATE);
                }
            }

            if (state.getLeadershipMode() == LeadershipMode.CANDIDATE) {
                boolean wonElection = tryToWinElection();
                synchronized (state) {
                    if (wonElection && state.getMode() == LeadershipMode.CANDIDATE) {
                        state.setMode(LeadershipMode.LEADER);
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
                if (state.getMode() != LeadershipMode.LEADER) {
                    return;
                }
                otherServers.forEach((serverId, client) -> {
                    LogEntryMetadata logEntryMetadata = state.getPersistent().logMetadata(
                            state.getLeaderVolatile().get().getMatchIndices().getOrDefault(serverId, LogIndexes.ZERO));
                    suppliers.add(() -> client.appendEntries(AppendEntriesRequest.builder()
                            .leaderId(state.getPersistent().getMe())
                            .term(state.getPersistent().getCurrentTerm())
                            .leaderCommit(state.getVolatile().getCommitIndex())
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
                    maybeLeaderState = state.getLeaderVolatile();
                    lastLogEntry = state.getPersistent().lastLogEntry();
                }
                if (maybeLeaderState.isEmpty()) {
                    return;
                }
                LeaderVolatileState leaderState = maybeLeaderState.get();
                if (lastLogEntry.getIndex().equals(LogIndexes.ZERO)) {
                    return;
                }

                LogIndex nextIndex = leaderState.getNextIndices().getOrDefault(server, LogIndexes.ONE);
                if (lastLogEntry.getIndex().get() >= nextIndex.get()) {
                    AppendEntriesRequest request;
                    synchronized (state) {
                        if (state.getMode() != LeadershipMode.LEADER) {
                            return;
                        }
                        List<LogEntry> logEntries = state.getPersistent().getLog().subList(
                                LogIndexes.listIndex(nextIndex), state.getPersistent().getLog().size());
                        LogEntryMetadata prevEntry = LogIndexes.listIndex(nextIndex) == 0
                                ? LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO)
                                : state.getPersistent().logMetadata(LogIndexes.listIndex(nextIndex) - 1);
                        request = AppendEntriesRequest.builder()
                                .term(state.getPersistent().getCurrentTerm())
                                .leaderId(state.getPersistent().getMe())
                                .leaderCommit(state.getVolatile().getCommitIndex())
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
                            leaderState.getMatchIndices().put(server, lastLogEntry.getIndex());
                            leaderState.getNextIndices().put(server, LogIndexes.inc(lastLogEntry.getIndex()));
                            return;
                        } else if (response.getTerm().equals(state.getPersistent().getCurrentTerm())) {
                            leaderState.getNextIndices().remove(server);
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
                if (state.getMode() != LeadershipMode.CANDIDATE) {
                    return false;
                }
                electionTerm = TermId.of(state.getPersistent().getCurrentTerm().get() + 1);
                state.getPersistent().setCurrentTerm(electionTerm);
                state.getPersistent().setVotedFor(Optional.of(state.getPersistent().getMe()));
                lastLogEntry = state.getPersistent().lastLogEntry();
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
                        .term(state.getPersistent().getCurrentTerm())
                        .success(internalAppendEntries(request))
                        .build();
            }
        }

        @GuardedBy("state")
        @SuppressWarnings("CyclomaticComplexity")
        private boolean internalAppendEntries(AppendEntriesRequest request) {
            if (request.getTerm().get() < state.getPersistent().getCurrentTerm().get()) {
                return false;
            }

            List<LogEntry> log = state.getPersistent().getLog();

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

            if (request.getLeaderCommit().get() > state.getVolatile().getCommitIndex().get()) {
                state.getVolatile().setCommitIndex(LogIndex.of(Math.min(request.getLeaderCommit().get(), log.size() - 1)));
            }

            state.setLastUpdated(clock.instant());
            return true;
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            synchronized (state) {
                state.handleReceivedTerm(request.getTerm());
                return RequestVoteResponse.builder()
                        .term(state.getPersistent().getCurrentTerm())
                        .voteGranted(requestVoteInternal(request))
                        .build();
            }
        }

        @GuardedBy("state")
        private boolean requestVoteInternal(RequestVoteRequest request) {
            if (request.getTerm().get() < state.getPersistent().getCurrentTerm().get()) {
                return false;
            }

            if (state.getPersistent().getVotedFor().isPresent()
                    && !state.getPersistent().getVotedFor().get().equals(request.getCandidateId())) {
                return false;
            }

            // TODO(jbaker): I'm not sure that this logic is quite correct...
            LogEntryMetadata ourLast = state.getPersistent().lastLogEntry();
            if (request.getLastLogIndex().get() < ourLast.getIndex().get()
                    || (request.getLastLogIndex().get() == ourLast.getIndex().get()
                            && request.getLastLogTerm().get() < ourLast.getTerm().get())) {
                return false;
            }
            state.getPersistent().setVotedFor(Optional.of(request.getCandidateId()));
            return true;
        }

        @Override
        public ApplyCommandResponse applyCommand(ApplyCommandRequest request) {
            LogEntryMetadata entry;
            synchronized (state) {
                if (!state.getMode().equals(LeadershipMode.LEADER)) {
                    return ApplyCommandResponse.of(false);
                }
                entry = state.getPersistent().addEntryAsLeader(request.getData());
            }
            ensureFollowersUpToDate();
            synchronized (state) {
                if (state.getVolatile().getCommitIndex().get() >= entry.getIndex().get()
                        && state.getPersistent().logMetadata(entry.getIndex()).equals(entry)) {
                    state.keepStateMachineUpToDate();
                    return ApplyCommandResponse.of(true);
                } else {
                    return ApplyCommandResponse.of(false);
                }
            }
        }
    }
}
