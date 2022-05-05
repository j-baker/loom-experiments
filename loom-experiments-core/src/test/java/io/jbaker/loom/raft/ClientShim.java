/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.jbaker.loom.raft.api.AppendEntriesRequest;
import io.jbaker.loom.raft.api.AppendEntriesResponse;
import io.jbaker.loom.raft.api.ApplyCommandRequest;
import io.jbaker.loom.raft.api.ApplyCommandResponse;
import io.jbaker.loom.raft.api.RaftService;
import io.jbaker.loom.raft.api.RaftServiceAsync;
import io.jbaker.loom.raft.api.RequestVoteRequest;
import io.jbaker.loom.raft.api.RequestVoteResponse;
import io.jbaker.loom.raft.api.ServerId;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

public final class ClientShim implements RaftServiceAsync {
    private final ServerId serverId;
    private final Map<ServerId, ? extends RaftService> servers;

    private final Executor executor;

    ClientShim(ServerId serverId, Map<ServerId, ? extends RaftService> servers, Executor executor) {
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

    private <T> ListenableFuture<T> runOnServer(Function<RaftService, T> func) {
        return Futures.submit(() -> func.apply(servers.get(serverId)), executor);
    }
}
