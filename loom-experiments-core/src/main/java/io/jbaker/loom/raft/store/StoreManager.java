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
