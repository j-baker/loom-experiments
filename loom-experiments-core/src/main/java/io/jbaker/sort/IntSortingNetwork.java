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

import java.util.Arrays;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public final class IntSortingNetwork {
    private IntSortingNetwork() {}

    private static final VectorSpecies<Integer> SPECIES_8 = IntVector.SPECIES_256;
    private static final Comparison COMPARISON_8_1 = createComparison(SPECIES_8, new Cmp(0, 7), new Cmp(1, 6), new Cmp(2, 5), new Cmp(3, 4));
    private static final VectorShuffle<Integer> COMPARISON_8_1_S = COMPARISON_8_1.shuffle;
    private static final VectorMask<Integer> COMPARISON_8_1_M = COMPARISON_8_1.mask;
    private static final Comparison COMPARISON_8_2 = createComparison(SPECIES_8, new Cmp(0, 3), new Cmp(1, 2), new Cmp(4, 7), new Cmp(5, 6));
    private static final VectorShuffle<Integer> COMPARISON_8_2_S = COMPARISON_8_2.shuffle;
    private static final VectorMask<Integer> COMPARISON_8_2_M = COMPARISON_8_2.mask;
    private static final Comparison COMPARISON_8_3 = createComparison(SPECIES_8, new Cmp(0, 1), new Cmp(2, 3), new Cmp(4, 5), new Cmp(6, 7));
    private static final VectorShuffle<Integer> COMPARISON_8_3_S = COMPARISON_8_3.shuffle;
    private static final VectorMask<Integer> COMPARISON_8_3_M = COMPARISON_8_3.mask;
    private static final Comparison COMPARISON_8_4 = createComparison(SPECIES_8, new Cmp(2, 4), new Cmp(3, 5));
    private static final VectorShuffle<Integer> COMPARISON_8_4_S = COMPARISON_8_4.shuffle;
    private static final VectorMask<Integer> COMPARISON_8_4_M = COMPARISON_8_4.mask;
    private static final Comparison COMPARISON_8_5 = createComparison(SPECIES_8, new Cmp(2, 3), new Cmp(4, 5));
    private static final VectorShuffle<Integer> COMPARISON_8_5_S = COMPARISON_8_5.shuffle;
    private static final VectorMask<Integer> COMPARISON_8_5_M = COMPARISON_8_5.mask;

    public static void sort8(int[] array, int from, int to) {
        if (from + SPECIES_8.length() > array.length) {
            Arrays.sort(array, from, to);
            return;
        }

        IntVector v0 = IntVector.fromArray(SPECIES_8, array, from);
        IntVector vector14 = v0.rearrange(COMPARISON_8_1_S);
        IntVector v1 = v0.min(vector14).blend(v0.max(vector14), COMPARISON_8_1_M);
        IntVector vector13 = v1.rearrange(COMPARISON_8_2_S);
        IntVector v2 = v1.min(vector13).blend(v1.max(vector13), COMPARISON_8_2_M);
        IntVector vector12 = v2.rearrange(COMPARISON_8_3_S);
        IntVector v3 = v2.min(vector12).blend(v2.max(vector12), COMPARISON_8_3_M);
        IntVector vector11 = v3.rearrange(COMPARISON_8_4_S);
        IntVector v4 = v3.min(vector11).blend(v3.max(vector11), COMPARISON_8_4_M);
        IntVector vector1 = v4.rearrange(COMPARISON_8_5_S);
        IntVector v5 = v4.min(vector1).blend(v4.max(vector1), COMPARISON_8_5_M);
        v5.intoArray(array, from);
    }

    private static final VectorSpecies<Integer> SPECIES_16 = IntVector.SPECIES_512;

    private static final Comparison COMPARISON_16_1 = createComparison(SPECIES_16,
            new Cmp(0, 15), new Cmp(1, 14), new Cmp(2, 13), new Cmp(3, 12), new Cmp(4, 11), new Cmp(5, 10), new Cmp(6, 9), new Cmp(7, 8));
    private static final VectorShuffle<Integer> COMPARISON_16_1_S = COMPARISON_16_1.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_1_M = COMPARISON_16_1.mask;
    private static final Comparison COMPARISON_16_2 = createComparison(SPECIES_16,
            new Cmp(0, 7), new Cmp(1, 6), new Cmp(2, 5), new Cmp(3, 4), new Cmp(8, 15), new Cmp(9, 14), new Cmp(10, 13), new Cmp(11, 12));
    private static final VectorShuffle<Integer> COMPARISON_16_2_S = COMPARISON_16_2.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_2_M = COMPARISON_16_2.mask;
    private static final Comparison COMPARISON_16_3 = createComparison(SPECIES_16,
            new Cmp(0, 3), new Cmp(1, 2), new Cmp(4, 7), new Cmp(5, 6), new Cmp(8, 11), new Cmp(9, 10), new Cmp(12, 15), new Cmp(13, 14));
    private static final VectorShuffle<Integer> COMPARISON_16_3_S = COMPARISON_16_3.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_3_M = COMPARISON_16_3.mask;
    private static final Comparison COMPARISON_16_4 = createComparison(SPECIES_16,
            new Cmp(0, 1), new Cmp(2, 8), new Cmp(10, 11), new Cmp(14, 15), new Cmp(3, 9), new Cmp(4, 5), new Cmp(6, 12), new Cmp(7, 13));
    private static final VectorShuffle<Integer> COMPARISON_16_4_S = COMPARISON_16_4.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_4_M = COMPARISON_16_4.mask;
    private static final Comparison COMPARISON_16_5 = createComparison(SPECIES_16,
            new Cmp(1, 4), new Cmp(5, 10), new Cmp(11, 14), new Cmp(2, 3), new Cmp(6, 9), new Cmp(12, 13), new Cmp(7, 8));
    private static final VectorShuffle<Integer> COMPARISON_16_5_S = COMPARISON_16_5.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_5_M = COMPARISON_16_5.mask;
    private static final Comparison COMPARISON_16_6 = createComparison(SPECIES_16,
            new Cmp(1, 2), new Cmp(3, 4), new Cmp(6, 7), new Cmp(8, 9), new Cmp(11, 12), new Cmp(13, 14));
    private static final VectorShuffle<Integer> COMPARISON_16_6_S = COMPARISON_16_6.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_6_M = COMPARISON_16_6.mask;
    private static final Comparison COMPARISON_16_7 = createComparison(SPECIES_16,
            new Cmp(2, 3), new Cmp(4, 5), new Cmp(10, 11), new Cmp(12, 13));
    private static final VectorShuffle<Integer> COMPARISON_16_7_S = COMPARISON_16_7.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_7_M = COMPARISON_16_7.mask;
    private static final Comparison COMPARISON_16_8 = createComparison(SPECIES_16,
            new Cmp(4, 6), new Cmp(5, 7), new Cmp(9, 11), new Cmp(10, 12));
    private static final VectorShuffle<Integer> COMPARISON_16_8_S = COMPARISON_16_8.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_8_M = COMPARISON_16_8.mask;
    private static final Comparison COMPARISON_16_9 = createComparison(SPECIES_16,
            new Cmp(3, 4), new Cmp(5, 6), new Cmp(7, 8), new Cmp(9, 10), new Cmp(11, 12));
    private static final VectorShuffle<Integer> COMPARISON_16_9_S = COMPARISON_16_9.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_9_M = COMPARISON_16_9.mask;
    private static final Comparison COMPARISON_16_10 = createComparison(SPECIES_16,
            new Cmp(6, 7), new Cmp(8, 9));
    private static final VectorShuffle<Integer> COMPARISON_16_10_S = COMPARISON_16_10.shuffle;
    private static final VectorMask<Integer> COMPARISON_16_10_M = COMPARISON_16_10.mask;

    public static void sort16(int[] array, int from, int to) {
        if (from + SPECIES_16.length() > array.length) {
            Arrays.sort(array, from, to);
            return;
        }
        IntVector v0 = IntVector.fromArray(SPECIES_16, array, from);
        IntVector v1 = compareAndExchange(v0, COMPARISON_16_1_S, COMPARISON_16_1_M);
        IntVector v2 = compareAndExchange(v1, COMPARISON_16_2_S, COMPARISON_16_2_M);
        IntVector v3 = compareAndExchange(v2, COMPARISON_16_3_S, COMPARISON_16_3_M);
        IntVector v4 = compareAndExchange(v3, COMPARISON_16_4_S, COMPARISON_16_4_M);
        IntVector v5 = compareAndExchange(v4, COMPARISON_16_5_S, COMPARISON_16_5_M);
        IntVector v6 = compareAndExchange(v5, COMPARISON_16_6_S, COMPARISON_16_6_M);
        IntVector v7 = compareAndExchange(v6, COMPARISON_16_7_S, COMPARISON_16_7_M);
        IntVector v8 = compareAndExchange(v7, COMPARISON_16_8_S, COMPARISON_16_8_M);
        IntVector v9 = compareAndExchange(v8, COMPARISON_16_9_S, COMPARISON_16_9_M);
        IntVector v10 = compareAndExchange(v9, COMPARISON_16_10_S, COMPARISON_16_10_M);
        v10.intoArray(array, from);
    }

    private static IntVector compareAndExchange(IntVector vector, Comparison comparison) {
        IntVector vector1 = vector.rearrange(comparison.shuffle);
        return vector.min(vector1).blend(vector.max(vector1), comparison.mask);
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
