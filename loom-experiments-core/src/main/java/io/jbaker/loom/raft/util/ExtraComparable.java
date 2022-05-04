/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 */

package io.jbaker.loom.raft.util;

public interface ExtraComparable<T extends ExtraComparable<T>> extends Comparable<T> {
    default boolean greaterThan(T other) {
        return compareTo(other) > 0;
    }

    default boolean geq(T other) {
        return compareTo(other) >= 0;
    }

    default boolean lessThan(T other) {
        return compareTo(other) < 0;
    }
}
