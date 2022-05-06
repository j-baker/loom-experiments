/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom;

import io.jbaker.loom.raft.api.ServerId;
import io.jbaker.loom.raft.api.TermId;

public interface Hooks {
    void onNewLeader(TermId termId, ServerId server);
}
