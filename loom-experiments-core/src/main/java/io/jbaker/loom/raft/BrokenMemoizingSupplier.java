/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

class BrokenMemoizingSupplier<T> implements Supplier<T> {
    private final Lock lock;
    private final Supplier<T> delegate;
    private volatile boolean initialized = false;
    private T value;

    BrokenMemoizingSupplier(Supplier<T> delegate) {
        this(new ReentrantLock(), delegate);
    }

    @VisibleForTesting
    BrokenMemoizingSupplier(Lock lock, Supplier<T> delegate) {
        this.lock = lock;
        this.delegate = delegate;
    }

    @Override
    public T get() {
        if (!initialized) {
            lock.lock();
            try {
                // This code is broken because initialized may have changed between the call to 'lock' and now.
                T result = delegate.get();
                value = result;
                initialized = true;
                return result;
            } finally {
                lock.unlock();
            }
        }
        return value;
    }
}
