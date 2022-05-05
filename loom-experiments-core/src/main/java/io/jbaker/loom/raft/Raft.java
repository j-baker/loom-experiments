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
import io.jbaker.loom.raft.api.LeadershipMode;
import io.jbaker.loom.raft.api.LogEntry;
import io.jbaker.loom.raft.api.LogEntryMetadata;
import io.jbaker.loom.raft.api.LogIndex;
import io.jbaker.loom.raft.api.RequestVoteRequest;
import io.jbaker.loom.raft.api.RequestVoteResponse;
import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;
import io.jbaker.loom.raft.store.LeaderVolatileState;
import io.jbaker.loom.raft.store.PersistentState;
import io.jbaker.loom.raft.store.ServerState;
import io.jbaker.loom.raft.store.StateMachine;
import io.jbaker.loom.raft.store.StoreManager;
import io.jbaker.loom.raft.store.StoreManagerImpl;
import io.jbaker.loom.raft.util.BackgroundTask;
import io.jbaker.loom.raft.util.LogIndexes;
import io.jbaker.loom.raft.util.ServerIds;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;

public final class Raft {
    private Raft() {}

    @Value.Immutable
    interface ServerConfig {
        ServerId me();

        Duration electionTimeout();

        class Builder extends ImmutableServerConfig.Builder {}
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
            ServerImpl serv = (ServerImpl) server;
            return serv.store.call(ctx -> "id: %s, mode: %s, currentTerm: %s, votedFor: %s"
                    .formatted(
                            serv.me,
                            ctx.state().getMode(),
                            ctx.state().getPersistent().getCurrentTerm(),
                            ctx.state().getPersistent().getVotedFor()));
        }

        public boolean isLeader() {
            ServerImpl serv = (ServerImpl) server;
            return serv.store.call(ctx -> ctx.state().getLeadershipMode().equals(LeadershipMode.LEADER));
        }

        public boolean stateEquals(InitializedServer other) {
            ServerImpl serv1 = (ServerImpl) server;
            ServerImpl serv2 = (ServerImpl) other.server;
            return serv1.store.call(ctx -> serv2.store.call(ctx2 -> ctx.state().equals(ctx2.state())));
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
        private final StoreManager store;
        private final ServerConfig serverConfig;
        private final Clock clock;

        private final Map<ServerId, Client> otherServers;

        private final int quorumSize;

        private ServerImpl(
                StateMachine stateMachine, ServerConfig serverConfig, Clock clock, Map<ServerId, Client> otherServers) {
            this.me = serverConfig.me();
            this.otherServers = Map.copyOf(Maps.filterKeys(otherServers, id -> !id.equals(serverConfig.me())));
            this.store = new StoreManagerImpl(
                    new Runtime.ProductionRuntime(),
                    ServerState.create(PersistentState.create(serverConfig.me()), stateMachine));
            this.serverConfig = serverConfig;
            this.clock = clock;
            this.quorumSize = (this.otherServers.size() + 1 + 1) / 2;
        }

        @Override
        public Duration runOneIteration() {
            return progressLeadershipState(clock.instant());
        }

        private Duration progressLeadershipState(Instant now) {
            return store.call(ctx -> {
                innerProgressLeadershipState(ctx, now);
                // possibly not right
                ctx.state().setLastUpdated(now);

                return switch (ctx.state().getMode().get()) {
                    case CANDIDATE, FOLLOWER -> serverConfig.electionTimeout();
                    case LEADER -> serverConfig.electionTimeout().dividedBy(2);
                    case UNKNOWN -> throw new SafeIllegalStateException("unreachable");
                };
            });
        }

        private boolean innerProgressLeadershipState(StoreManager.Ctx ctx, Instant now) {
            ctx.state().keepStateMachineUpToDate();

            if (ctx.state().getMode() == LeadershipMode.FOLLOWER
                    && (ctx.state().getLastUpdated() == null
                            || now.isAfter(ctx.state().getLastUpdated().plus(serverConfig.electionTimeout())))) {
                ctx.state().setMode(LeadershipMode.CANDIDATE);
            }

            if (ctx.state().getLeadershipMode() == LeadershipMode.CANDIDATE) {
                boolean wonElection = tryToWinElection(ctx);
                if (wonElection && ctx.state().getMode() == LeadershipMode.CANDIDATE) {
                    ctx.state().setMode(LeadershipMode.LEADER);
                }
            }
            if (ctx.state().getLeadershipMode() == LeadershipMode.LEADER) {
                sendNoOpUpdates(ctx);
                ensureFollowersUpToDate(ctx);
            }

            return false;
        }

        private void sendNoOpUpdates(StoreManager.Ctx ctx) {
            List<Supplier<ListenableFuture<AppendEntriesResponse>>> suppliers = new ArrayList<>();
            if (ctx.state().getMode() != LeadershipMode.LEADER) {
                return;
            }
            otherServers.forEach((serverId, client) -> {
                LogEntryMetadata logEntryMetadata = ctx.state()
                        .getPersistent()
                        .logMetadata(ctx.state()
                                .getLeaderVolatile()
                                .get()
                                .getMatchIndices()
                                .getOrDefault(serverId, LogIndexes.ZERO));
                suppliers.add(() -> client.appendEntries(AppendEntriesRequest.builder()
                        .leaderId(ctx.state().getPersistent().getMe())
                        .term(ctx.state().getPersistent().getCurrentTerm())
                        .leaderCommit(ctx.state().getVolatile().getCommitIndex())
                        .prevLogTerm(logEntryMetadata.getTerm())
                        .prevLogIndex(logEntryMetadata.getIndex())
                        .build()));
            });
            ctx.runStateful(defer -> {
                // TODO(jbaker): refactor to use vthreads for this...
                for (ListenableFuture<AppendEntriesResponse> future :
                        suppliers.stream().map(Supplier::get).toList()) {
                    try {
                        AppendEntriesResponse response = future.get();
                        defer.callback(state -> state.handleReceivedTerm(response.getTerm()));
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                        // TODO(jbaker): log
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        private void ensureFollowersUpToDate(StoreManager.Ctx ctx) {
            // TODO(jbaker): run this in vthreads
            otherServers.keySet().forEach(id -> ensureFollowerUpToDate(ctx, id));
            ctx.state().updateLeaderCommitIndex(otherServers.keySet());
        }

        private void ensureFollowerUpToDate(StoreManager.Ctx ctx, ServerId server) {
            ServerState state = ctx.state();
            while (true) {
                Optional<LeaderVolatileState> maybeLeaderState = state.getLeaderVolatile();
                LogEntryMetadata lastLogEntry = state.getPersistent().lastLogEntry();

                if (maybeLeaderState.isEmpty()) {
                    return;
                }
                LeaderVolatileState leaderState = maybeLeaderState.get();
                if (lastLogEntry.getIndex().equals(LogIndexes.ZERO)) {
                    return;
                }

                LogIndex nextIndex = leaderState.getNextIndices().getOrDefault(server, LogIndexes.ONE);
                if (lastLogEntry.getIndex().get() < nextIndex.get()) {
                    return;
                }

                if (state.getMode() != LeadershipMode.LEADER) {
                    return;
                }
                List<LogEntry> logEntries = state.getPersistent()
                        .getLog()
                        .subList(
                                LogIndexes.listIndex(nextIndex),
                                state.getPersistent().getLog().size());
                LogEntryMetadata prevEntry = LogIndexes.listIndex(nextIndex) == 0
                        ? LogEntryMetadata.of(TermId.of(0), LogIndexes.ZERO)
                        : state.getPersistent().logMetadata(LogIndexes.listIndex(nextIndex) - 1);
                AppendEntriesRequest request = AppendEntriesRequest.builder()
                        .term(state.getPersistent().getCurrentTerm())
                        .leaderId(state.getPersistent().getMe())
                        .leaderCommit(state.getVolatile().getCommitIndex())
                        .prevLogIndex(prevEntry.getIndex())
                        .prevLogTerm(prevEntry.getTerm())
                        .entries(logEntries)
                        .build();

                AppendEntriesResponse response = ctx.callStateful(
                        _defer -> Futures.getUnchecked(otherServers.get(server).appendEntries(request)));
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
        }

        private boolean tryToWinElection(StoreManager.Ctx ctx) {
            TermId electionTerm =
                    TermId.of(ctx.state().getPersistent().getCurrentTerm().get() + 1);
            ctx.state().getPersistent().setCurrentTerm(electionTerm);
            ctx.state()
                    .getPersistent()
                    .setVotedFor(Optional.of(ctx.state().getPersistent().getMe()));
            LogEntryMetadata lastLogEntry = ctx.state().getPersistent().lastLogEntry();

            return ctx.callStateful(defer -> {
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
                        TermId responseTerm = response.getTerm();
                        defer.callback(state -> state.handleReceivedTerm(responseTerm));

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
            });
        }

        @Override
        public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
            return store.call(ctx -> {
                ctx.state().handleReceivedTerm(request.getTerm());
                return AppendEntriesResponse.builder()
                        .term(ctx.state().getPersistent().getCurrentTerm())
                        .success(appendEntries(ctx.state(), request))
                        .build();
            });
        }

        @GuardedBy("state")
        @SuppressWarnings("CyclomaticComplexity")
        private boolean appendEntries(ServerState state, AppendEntriesRequest request) {
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
                    .subList(
                            request.getEntries().size() - (log.size() - prevLogIndex),
                            request.getEntries().size()));

            if (request.getLeaderCommit().get()
                    > state.getVolatile().getCommitIndex().get()) {
                state.getVolatile()
                        .setCommitIndex(
                                LogIndex.of(Math.min(request.getLeaderCommit().get(), log.size() - 1)));
            }

            state.setLastUpdated(clock.instant());
            return true;
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            return store.call(ctx -> {
                ctx.state().handleReceivedTerm(request.getTerm());
                return RequestVoteResponse.builder()
                        .term(ctx.state().getPersistent().getCurrentTerm())
                        .voteGranted(requestVote(ctx.state(), request))
                        .build();
            });
        }

        private boolean requestVote(ServerState state, RequestVoteRequest request) {
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
                            && request.getLastLogTerm().get()
                                    < ourLast.getTerm().get())) {
                return false;
            }
            state.getPersistent().setVotedFor(Optional.of(request.getCandidateId()));
            return true;
        }

        @Override
        public ApplyCommandResponse applyCommand(ApplyCommandRequest request) {
            return store.call(ctx -> {
                if (!ctx.state().getMode().equals(LeadershipMode.LEADER)) {
                    return ApplyCommandResponse.of(false);
                }
                LogEntryMetadata entry = ctx.state().getPersistent().addEntryAsLeader(request.getData());
                ensureFollowersUpToDate(ctx);
                if (ctx.state().getVolatile().getCommitIndex().get()
                                >= entry.getIndex().get()
                        && ctx.state()
                                .getPersistent()
                                .logMetadata(entry.getIndex())
                                .equals(entry)) {
                    ctx.state().keepStateMachineUpToDate();
                    return ApplyCommandResponse.of(true);
                } else {
                    return ApplyCommandResponse.of(false);
                }
            });
        }
    }
}
