/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.util;

import io.jbaker.loom.raft.api.ServerId;
import java.util.UUID;

public final class ServerIds {
    private ServerIds() {}

    public static ServerId of(int id) {
        return ServerId.of(new UUID(id, id));
    }
}
