/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.util;

import io.jbaker.loom.raft.api.TermId;

public final class TermIds {
    private TermIds() {}

    public static boolean isGreaterThan(TermId left, TermId right) {
        return left.get() > right.get();
    }
}
