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

package io.jbaker.loom.raft.util;

import static com.palantir.logsafe.Preconditions.checkArgument;

import io.jbaker.loom.raft.api.LogIndex;

public final class LogIndexes {
    private LogIndexes() {}

    public static final LogIndex ZERO = LogIndex.of(0);
    public static final LogIndex ONE = LogIndex.of(1);

    public static int listIndex(LogIndex index) {
        checkArgument(!index.equals(ZERO), "no list index for the sentinel entry");
        return index.get() - 1;
    }

    public static LogIndex inc(LogIndex index) {
        return LogIndex.of(index.get() + 1);
    }
}