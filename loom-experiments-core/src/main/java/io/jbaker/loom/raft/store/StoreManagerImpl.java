/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.store;

import static com.palantir.logsafe.Preconditions.checkState;

import com.google.common.base.Throwables;
import io.jbaker.loom.raft.Runtime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;
import jdk.incubator.concurrent.ScopeLocal;

public final class StoreManagerImpl implements StoreManager {
    private static final ScopeLocal<State> REENTRANCY_CHECKER = ScopeLocal.newInstance();
    private final Lock lock;
    private final ServerState serverState;

    public StoreManagerImpl(Runtime runtime, ServerState serverState) {
        this.lock = runtime.newLock();
        this.serverState = serverState;
    }

    private enum State {
        NOT_IN_TASK,
        IN_TASK,
        IN_STATEFUL_TASK,
    }

    private static State getState() {
        return REENTRANCY_CHECKER.orElse(State.NOT_IN_TASK);
    }

    @Override
    public <R> R call(Function<Ctx, R> task) {
        checkState(getState() == State.NOT_IN_TASK, "reentrancy of store usage is not allowed");
        try {
            return ScopeLocal.where(REENTRANCY_CHECKER, State.IN_TASK, () -> {
                lock.lock();
                try {
                    return task.apply(new CtxImpl());
                } finally {
                    lock.unlock();
                }
            });
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(Consumer<Ctx> task) {
        call(c -> {
            task.accept(c);
            return null;
        });
    }

    private final class CtxImpl implements Ctx {
        @Override
        public ServerState state() {
            return serverState;
        }

        @Override
        public void runStateful(Consumer<Defer> task) {
            callStateful(t -> {
                task.accept(t);
                return null;
            });
        }

        @Override
        public <R> R callStateful(Function<Defer, R> task) {
            checkState(getState() == State.IN_TASK, "cannot reentrantly call run/callStateful");
            lock.unlock();
            boolean threw = false;
            List<Consumer<ServerState>> callbacks = new ArrayList<>();
            try {
                return ScopeLocal.where(REENTRANCY_CHECKER, State.IN_STATEFUL_TASK, () -> task.apply(callbacks::add));
            } catch (Exception e) {
                threw = true;
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
            } finally {
                lock.lock();
                if (!threw) {
                    callbacks.forEach(callback -> callback.accept(state()));
                }
            }
        }
    }
}
