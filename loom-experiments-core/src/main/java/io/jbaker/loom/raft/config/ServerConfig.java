/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.config;

import io.jbaker.loom.raft.api.ServerId;
import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
public interface ServerConfig {
    ServerId me();

    Duration electionTimeout();

    class Builder extends ImmutableServerConfig.Builder {}
}
