/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import java.util.function.Consumer;
import java.util.function.Function;

public interface StoreManager {
    <R> R call(Function<Ctx, R> task);

    void run(Consumer<Ctx> task);

    interface Ctx {
        ServerState state();

        void runRemote(Consumer<Defer> task);

        <R> R callRemote(Function<Defer, R> task);
    }

    interface Defer {
        void callback(Consumer<ServerState> onCompletion);
    }
}
