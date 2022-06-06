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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.stream.IntStream;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public class IntVectorizedQuickSort {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_256;

    private static final boolean USE_SORT_16 = IntVector.SPECIES_PREFERRED == IntVector.SPECIES_512;

    private IntVectorizedQuickSort() {}

    public static void quicksort(int[] array, int from, int to) {
        int size = to - from;
        if (size <= 1) {
            return;
        } else if (USE_SORT_16 && size <= 16) {
            IntSortingNetwork.sort16(array, from, to);
        } else if (size <= 8) {
            IntSortingNetwork.sort8(array, from, to);
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
        int crossover = doBetterPartition(array, pivot, from, to - 1);
        int temp = array[crossover];
        array[to - 1] = temp;
        array[crossover] = pivot;
        return crossover;
    }

    @VisibleForTesting
    static int doBetterPartition(int[] array, int pivot, int from, int to) {
        int size = to - from;

        int index = from;
        int upperBound = from + SPECIES.loopBound(size);
        int manualPivotStartingPoint = upperBound;

        if (index + SPECIES.length() >= upperBound) {
            return doFallbackPartition(array, pivot, from, to);
        }

        IntVector cachedVector;
        int cachedNumTrues;
        cachedVector = IntVector.fromArray(SPECIES, array, index);
        VectorMask<Integer> cachedMask = cachedVector.lt(pivot);
        cachedVector = cachedVector.rearrange(IntVectorizedQuickSort.compress(cachedMask));
        cachedNumTrues = cachedMask.trueCount();
        boolean initialize = false;
        while (index + SPECIES.length() < upperBound) {
            if (initialize) {
                initialize = false;
                cachedVector = IntVector.fromArray(SPECIES, array, index);
                cachedMask = cachedVector.lt(pivot);
                cachedVector = cachedVector.rearrange(IntVectorizedQuickSort.compress(cachedMask));
                cachedNumTrues = cachedMask.trueCount();
            }

            int index2 = index + SPECIES.length();

            IntVector vector2 = IntVector.fromArray(SPECIES, array, index2);
            VectorMask<Integer> mask2 = vector2.lt(pivot);
            int numTrues2 = mask2.trueCount();

            IntVector rearranged2 = vector2.rearrange(IntVectorizedQuickSort.compress(mask2));

            VectorMask<Integer> mask = IntVectorizedQuickSort.lowerOverflowMask(cachedNumTrues);
            IntVector rotated = rearranged2.rearrange(IntVectorizedQuickSort.rotateRight(cachedNumTrues));

            IntVector merged1 = cachedVector.blend(rotated, mask);
            IntVector merged2 = rotated.blend(cachedVector, mask);

            int totalTrues = cachedNumTrues + numTrues2;
            if (totalTrues == SPECIES.length()) {
                merged1.intoArray(array, index);
                upperBound -= SPECIES.length();
                IntVector newData = IntVector.fromArray(SPECIES, array, upperBound);
                newData.intoArray(array, index2);
                merged2.intoArray(array, upperBound);
                index += SPECIES.length();
                initialize = true;
            } else if (totalTrues < SPECIES.length()) {
                cachedVector = merged1;
                cachedNumTrues = totalTrues;
                upperBound -= SPECIES.length();
                IntVector newData = IntVector.fromArray(SPECIES, array, upperBound);
                newData.intoArray(array, index2);
                merged2.intoArray(array, upperBound);
            } else {
                cachedVector = merged2;
                cachedNumTrues = totalTrues - SPECIES.length();
                merged1.intoArray(array, index);
                index += SPECIES.length();
            }
        }

        // at this point, we have a setup that looks like the following (vector chunks)
        // LLLLLLLLLLLL??HHHHHHHH? where L is lower, H is higher, ? is unknown
        // The two ranges we care about are the chunk between our lower and higher segments. Because we demand 2 vectors
        // buffer space, we might have an unprocessed vectors in the middle, and one of these may only presently
        // be held in a vector.
        // The good news is that this operation is cheap.
        int difference = upperBound - index;
        if (difference == SPECIES.length()) {
            if (initialize) {
                cachedVector = IntVector.fromArray(SPECIES, array, index);
                cachedMask = cachedVector.lt(pivot);
                cachedVector = cachedVector.rearrange(IntVectorizedQuickSort.compress(cachedMask));
                cachedNumTrues = cachedMask.trueCount();
            }
            cachedVector.intoArray(array, index);
            index += cachedNumTrues;
        } else if (difference != 0) {
            throw new SafeIllegalStateException("unexpected");
        }

        for (int i = manualPivotStartingPoint; i < to; i++) {
            int temp = array[i];
            if (temp < pivot) {
                array[i] = array[index];
                array[index++] = temp;
            }
        }

        return index;
    }

    @VisibleForTesting
    static int doFallbackPartition(int[] array, int pivot, int lowerPointerArg, int upperPointerArg) {
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
    @VisibleForTesting
    static VectorMask<Integer> lowerOverflowMask(int index) {
        // lowerOverflowMasks[index];
        return VectorMask.fromArray(SPECIES, overflow, SPECIES.length() - index);
    }

    @SuppressWarnings("unchecked")
    private static final VectorMask<Integer>[] lowerOverflowMasks = IntStream.range(0, SPECIES.length() + 1)
            .map(lowerTrueCount -> lowerTrueCount % SPECIES.length())
            .mapToObj(lowerTrueCount -> {
                int elementsFromUpper = SPECIES.length() - lowerTrueCount;
                return VectorMask.fromLong(SPECIES, ((1L << elementsFromUpper) - 1 << lowerTrueCount));
            }).toArray(VectorMask[]::new);

    private static final boolean[] overflow = upperOverflowArray();

    private static boolean[] upperOverflowArray() {
        boolean[] result = new boolean[SPECIES.length() * 2];
        for (int i = SPECIES.length(); i < result.length; i++) {
            result[i] = true;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Integer>[] compressions = IntStream.range(0, 1 << SPECIES.length())
            .mapToObj(index -> shuffle(index))
            .toArray(VectorShuffle[]::new);

    @VisibleForTesting
    static VectorShuffle<Integer> compress(VectorMask<Integer> mask) {
        return compressions[(int) mask.toLong()];
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Integer>[] rotations = rotationsArray();

    private static VectorShuffle<Integer>[] rotationsArray() {
        VectorShuffle<Integer>[] r = IntStream.range(0, SPECIES.length() + 1)
                .mapToObj(index -> VectorShuffle.iota(SPECIES, index % SPECIES.length(), 1, true))
                .toArray(VectorShuffle[]::new);
        for (int i = 0; i < r.length / 2; i++) {
            VectorShuffle<Integer> tmp = r[i];
            r[i] = r[r.length - 1 - i];
            r[r.length - 1 - i] = tmp;
        }
        return r;
    }

    static VectorShuffle<Integer> rotateRight(int by) {
        return rotations[by];
    }

    private static VectorShuffle<Integer> shuffle(int maskArg) {
        int numTrues = VectorMask.fromLong(SPECIES, maskArg).trueCount();

        int mask = maskArg;
        int[] ret = new int[SPECIES.length()];
        int unmatchedIndex = numTrues;
        int matchedIndex = 0;
        for (int i = 0; i < SPECIES.length(); i++) {
            if ((mask & 1) == 0) {
                ret[unmatchedIndex++] = i;
            } else {
                ret[matchedIndex++] = i;
            }
            mask >>= 1;
        }
        return VectorShuffle.fromValues(SPECIES, ret);
    }
}
