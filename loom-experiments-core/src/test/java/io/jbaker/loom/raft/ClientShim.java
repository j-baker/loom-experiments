/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

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
import io.jbaker.loom.raft.simulation.RealisticRpcSimulation;
import io.jbaker.loom.raft.simulation.ReliableRpcSimulation;
import io.jbaker.loom.raft.simulation.RpcSimulation;
import io.jbaker.loom.raft.simulation.Simulation;
import java.util.Map;
import java.util.function.Function;

public final class ClientShim implements RaftServiceAsync {
    private final ServerId serverId;
    private final Map<ServerId, ? extends RaftService> servers;
    private final RpcSimulation rpc;

    private ClientShim(ServerId serverId, Map<ServerId, ? extends RaftService> servers, RpcSimulation rpc) {
        this.serverId = serverId;
        this.servers = servers;
        this.rpc = rpc;
    }

    public static RaftServiceAsync create(
            RpcSimulation rpc, ServerId serverId, Map<ServerId, ? extends RaftService> servers) {
        return new ClientShim(serverId, servers, rpc);
    }

    public static RaftServiceAsync createUnreliable(
            ServerId serverId, Map<ServerId, ? extends RaftService> servers, Simulation simulation) {
        return new ClientShim(serverId, servers, RealisticRpcSimulation.create(simulation));
    }

    public static RaftServiceAsync createReliable(
            ServerId serverId, Map<ServerId, ? extends RaftService> servers, Simulation simulation) {
        return new ClientShim(serverId, servers, ReliableRpcSimulation.create(simulation));
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

    private <T> ListenableFuture<T> runOnServer(Function<RaftService, T> task) {
        return rpc.simulate(() -> task.apply(servers.get(serverId)));
    }
}
