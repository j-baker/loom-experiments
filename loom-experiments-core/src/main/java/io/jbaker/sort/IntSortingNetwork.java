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

package io.jbaker.sort;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public final class IntSortingNetwork {
    public static final int MAX = 0;

    private IntSortingNetwork() {}

    public static void sort(int[] array, int from, int to) {
        switch (to - from) {
            case 0:
            case 1:
                return;
            case 2:
                sort2(array, from);
                return;
            case 3:
                sort3(array, from);
                return;
            case 4:
                sort4(array, from);
                return;
            default:
                throw new SafeIllegalStateException("above max for sorting network sort");
        }
    }

    private static void sort2(int[] array, int first) {
        compareAndExchange(array, first, first + 1);
    }

    private static void sort3(int[] array, int first) {
        compareAndExchange(array, first + 1, first + 2);
        compareAndExchange(array, first, first + 1);
        compareAndExchange(array, first + 1, first + 2);
    }

    private static final VectorSpecies<Integer> SPECIES_4 = IntVector.SPECIES_128;

    private static final Comparison COMPARISON_4_0 = createComparison(SPECIES_4, new Cmp(0, 1), new Cmp(2, 3));
    private static final VectorShuffle<Integer> COMPARISON_4_0_S = COMPARISON_4_0.shuffle;
    private static final VectorMask<Integer> COMPARISON_4_0_M = COMPARISON_4_0.mask;
    private static final Comparison COMPARISON_4_1 = createComparison(SPECIES_4, new Cmp(0, 2), new Cmp(1, 3));
    private static final VectorShuffle<Integer> COMPARISON_4_1_S = COMPARISON_4_1.shuffle;
    private static final VectorMask<Integer> COMPARISON_4_1_M = COMPARISON_4_1.mask;
    private static final Comparison COMPARISON_4_2 = createComparison(SPECIES_4, new Cmp(1, 2));
    private static final VectorShuffle<Integer> COMPARISON_4_2_S = COMPARISON_4_2.shuffle;
    private static final VectorMask<Integer> COMPARISON_4_2_M = COMPARISON_4_2.mask;

    private static void sort4(int[] array, int first) {
        IntVector v1 = IntVector.fromArray(SPECIES_4, array, first);
        IntVector v2 = compareAndExchange(v1, COMPARISON_4_0_S, COMPARISON_4_0_M);
        IntVector v3 = compareAndExchange(v2, COMPARISON_4_1_S, COMPARISON_4_1_M);
        IntVector v4 = compareAndExchange(v3, COMPARISON_4_2_S, COMPARISON_4_2_M);
        v4.intoArray(array, first);
    }

    private static void compareAndExchange(int[] array, int offset1, int offset2) {
        if (array[offset1] > array[offset2]) {
            int temp = array[offset1];
            array[offset1] = array[offset2];
            array[offset2] = temp;
        }
    }

    private static IntVector compareAndExchange(IntVector vector, Comparison comparison) {
        return compareAndExchange(vector, comparison.shuffle, comparison.mask);
    }

    private static IntVector compareAndExchange(IntVector vector, VectorShuffle<Integer> shuffle, VectorMask<Integer> mask) {
        IntVector vector1 = vector.rearrange(shuffle);
        return vector.min(vector1).blend(vector.max(vector1), mask);
    }

    private static Comparison createComparison(VectorSpecies<Integer> species, Cmp... wires) {
        int[] shuffle = new int[species.length()];
        boolean[] mask = new boolean[species.length()];

        for (int i = 0; i < species.length(); i++) {
            shuffle[i] = i;
            mask[i] = true;
        }

        for (Cmp comparison : wires) {
            shuffle[comparison.left] = comparison.right;
            shuffle[comparison.right] = comparison.left;
            mask[comparison.right] = false;
        }

        return new Comparison(VectorShuffle.fromValues(species, shuffle), VectorMask.fromValues(species, mask));
    }

    private record Cmp(int left, int right) {}

    private record Comparison(VectorShuffle<Integer> shuffle, VectorMask<Integer> mask) {}
}
