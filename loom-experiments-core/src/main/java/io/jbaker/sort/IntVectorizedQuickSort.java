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

import java.util.stream.IntStream;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public class IntVectorizedQuickSort {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    private IntVectorizedQuickSort() {}

    public static void quicksort(int[] array, int from, int to) {
        int size = to - from;
        if (size <= 1) {
            return;
        }
        int pivot = partition(array, from, to);
        quicksort(array, from, pivot);
        quicksort(array, pivot + 1, to);
    }

    public static int partition(int[] array, int from, int to) {
        if (to - from == 1) {
            return from;
        }

        int pivot = array[to - 1];
        int crossover = doPartition(array, pivot, from, to - 1);
        int temp = array[crossover];
        array[to - 1] = temp;
        array[crossover] = pivot;
        return crossover;
    }

    @SuppressWarnings("CyclomaticComplexity")
    public static int doPartition(
            int[] array, int pivot, int from, int to) {
        int lowerPointer = from;
        int upperPointer = to;

        if (to - from == SPECIES.length()) {
            return doVectorSizePartition(array, pivot, from);
        }

        while (true) {
            int innerUpperPointer = upperPointer - SPECIES.length();
            if (lowerPointer + SPECIES.length() >= innerUpperPointer) { // >=?
                break;
            }

            IntVector lowerCandidates = IntVector.fromArray(SPECIES, array, lowerPointer);
            IntVector upperCandidates = IntVector.fromArray(SPECIES, array, innerUpperPointer);

            VectorMask<Integer> lowerMatching = lowerCandidates.lt(pivot);
            VectorMask<Integer> upperMatching = upperCandidates.lt(pivot);

            int lowerTrueCount = lowerMatching.trueCount();
            int upperTrueCount = upperMatching.trueCount();
            if (lowerTrueCount == SPECIES.length() || upperTrueCount == 0) {
                // array already properly pivoted on at least one side
                if (lowerTrueCount == SPECIES.length()) {
                    lowerPointer += SPECIES.length();
                }
                if (upperTrueCount == 0) {
                    upperPointer -= SPECIES.length();
                }
            } else if (lowerTrueCount == 0 || upperTrueCount == SPECIES.length()) {
                lowerCandidates.intoArray(array, innerUpperPointer);
                upperCandidates.intoArray(array, lowerPointer);
                if (lowerTrueCount == 0) {
                    upperPointer -= SPECIES.length();
                }
                if (upperTrueCount == SPECIES.length()) {
                    lowerPointer += SPECIES.length();
                }
            } else if (lowerTrueCount + upperTrueCount > SPECIES.length()) {
                // overflow on lower half
                int elementsFromUpper = SPECIES.length() - lowerTrueCount;

                IntVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                IntVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));

                VectorMask<Integer> mask = lowerOverflowMasks[lowerTrueCount];

                IntVector rotated = upperShuffled.rearrange(rotate(upperTrueCount));
                lowerShuffled.blend(rotated, mask).intoArray(array, lowerPointer);
                rotated.blend(lowerShuffled, mask).intoArray(array, innerUpperPointer);

                lowerPointer += SPECIES.length();
                upperPointer -= elementsFromUpper;
            } else {
                // overflow on upper half, symmetric to other overflow case
                IntVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                IntVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));

                VectorMask<Integer> mask = upperOverflowMasks[upperTrueCount];
                IntVector rotated = lowerShuffled.rearrange(rotate(lowerTrueCount));
                upperShuffled.blend(rotated, mask).intoArray(array, innerUpperPointer);
                rotated.blend(upperShuffled, mask).intoArray(array, lowerPointer);

                lowerPointer += upperTrueCount;
                upperPointer -= SPECIES.length();
            }
        }
        return doFallbackPartition(array, pivot, lowerPointer, upperPointer);
    }

    private static int doVectorSizePartition(int[] array, int pivot, int from) {
        IntVector vector = IntVector.fromArray(SPECIES, array, from);
        VectorMask<Integer> mask = vector.lt(pivot);
        vector.rearrange(compress(mask)).intoArray(array, from);
        return from + mask.trueCount();
    }

    private static int doFallbackPartition(int[] array, int pivot, int lowerPointerArg, int upperPointerArg) {
        int lowerPointer = lowerPointerArg;
        int upperPointer = upperPointerArg;
        while (lowerPointer < upperPointer) {
            boolean didWork = false;
            if (array[lowerPointer] < pivot) {
                lowerPointer++;
                didWork = true;
            }
            if (array[upperPointer - 1] >= pivot) {
                upperPointer--;
                didWork = true;
            }
            if (!didWork) {
                int tmp = array[lowerPointer];
                int index = --upperPointer;
                array[lowerPointer] = array[index];
                array[index] = tmp;
                lowerPointer++;
            }
        }
        return lowerPointer;
    }

    @SuppressWarnings("unchecked")
    private static final VectorMask<Integer>[] lowerOverflowMasks = IntStream.range(0, SPECIES.length())
            .mapToObj(lowerTrueCount -> {
                int elementsFromUpper = SPECIES.length() - lowerTrueCount;
                return VectorMask.fromLong(SPECIES, ((1L << elementsFromUpper) - 1 << lowerTrueCount));
            }).toArray(VectorMask[]::new);

    @SuppressWarnings("unchecked")
    private static final VectorMask<Integer>[] upperOverflowMasks = IntStream.range(0, SPECIES.length())
            .mapToObj(upperTrueCount -> VectorMask.fromLong(SPECIES, (1L << upperTrueCount) - 1))
            .toArray(VectorMask[]::new);

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Integer>[] compressions = IntStream.range(0, 1 << SPECIES.length())
            .mapToObj(index -> shuffle(index))
            .toArray(VectorShuffle[]::new);

    // takes a mask like 1, 0, 1, 0 and returns a shuffle like 0, 2, 1, 3
    private static VectorShuffle<Integer> compress(VectorMask<Integer> mask) {
        return compressions[(int) mask.toLong()];
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Integer>[] rotations = IntStream.range(0, SPECIES.length())
            .mapToObj(index -> VectorShuffle.iota(SPECIES, index, 1, true))
            .toArray(VectorShuffle[]::new);

    private static VectorShuffle<Integer> rotate(int by) {
        return rotations[by];
    }

    private static VectorShuffle<Integer> shuffle(int maskArg) {
        int mask = maskArg;
        int[] ret = new int[SPECIES.length()];
        int unmatchedIndex = ret.length - 1;
        int matchedIndex = 0;
        for (int i = 0; i < SPECIES.length(); i++) {
            if ((mask & 1) == 0) {
                ret[unmatchedIndex--] = i;
            } else {
                ret[matchedIndex++] = i;
            }
            mask >>= 1;
        }
        return VectorShuffle.fromValues(SPECIES, ret);
    }
}
