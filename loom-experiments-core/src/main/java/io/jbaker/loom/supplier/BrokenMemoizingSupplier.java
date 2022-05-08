/*
 * (c) Copyright 2022 James Baker. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jbaker.loom.supplier;

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
