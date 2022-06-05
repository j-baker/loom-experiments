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
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public class VectorizedQuickSort {
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final VectorShuffle<Double> REVERSE = VectorShuffle.iota(SPECIES, SPECIES.length() - 1, -1, false);

    private VectorizedQuickSort() {}

    public static void quicksort(double[] array, int from, int to) {
        if (to - from <= 1) {
            return;
//        } else if (to - from <= 1024) {
//            return;
        }
        int pivot = partition(array, from, to);
        quicksort(array, from, pivot);
        quicksort(array, pivot + 1, to);
    }

    public static int partition(double[] array, int from, int to) {
        if (to - from == 1) {
            return from;
        }

        double pivot = array[to - 1];
        int crossover = partition(array, pivot, from, to - 1);
        double temp = array[crossover];
        array[to - 1] = temp;
        array[crossover] = pivot;
        return crossover;
    }

    @SuppressWarnings("CyclomaticComplexity")
    public static int partition(
            double[] array, double pivot, int from, int to) {
        int lowerPointer = from;
        int upperPointer = to;

        while (true) {
            int innerUpperPointer = upperPointer - SPECIES.length();
            if (lowerPointer + SPECIES.length() >= innerUpperPointer) { // >=?
                break;
            }

            DoubleVector lowerCandidates = DoubleVector.fromArray(SPECIES, array, lowerPointer);
            DoubleVector upperCandidates = DoubleVector.fromArray(SPECIES, array, innerUpperPointer);

            VectorMask<Double> lowerMatching = lowerCandidates.lt(pivot);
            VectorMask<Double> upperMatching = upperCandidates.lt(pivot);

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

                DoubleVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                DoubleVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));


                VectorMask<Double> mask = VectorMask.fromLong(SPECIES, ((1L << elementsFromUpper) - 1) << lowerTrueCount);

                DoubleVector rotated = upperShuffled.rearrange(rotate(upperTrueCount));
                DoubleVector newLower = lowerShuffled.blend(rotated, mask);
                DoubleVector newUpper = rotated.blend(lowerShuffled, mask);
                newLower.intoArray(array, lowerPointer);
                newUpper.intoArray(array, innerUpperPointer);

                lowerPointer += SPECIES.length();
                upperPointer -= elementsFromUpper;
            } else {
                // overflow on upper half, symmetric to other overflow case
                DoubleVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                DoubleVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));

                VectorMask<Double> mask = VectorMask.fromLong(SPECIES, (1L << upperTrueCount) - 1);
                DoubleVector rotated = lowerShuffled.rearrange(rotate(lowerTrueCount));
                DoubleVector newUpper = upperShuffled.blend(rotated, mask);
                DoubleVector newLower = rotated.blend(upperShuffled, mask);

                newUpper.intoArray(array, innerUpperPointer);
                newLower.intoArray(array, lowerPointer);

                lowerPointer += upperTrueCount;
                upperPointer -= SPECIES.length();
            }
        }

        while (lowerPointer < upperPointer) {
            if (array[lowerPointer] < pivot) {
                lowerPointer++;
            } else if (array[upperPointer - 1] >= pivot) {
                upperPointer--;
            } else {
                double tmp = array[lowerPointer];
                int index = --upperPointer;
                array[lowerPointer] = array[index];
                array[index] = tmp;
                lowerPointer++;
            }
        }

        return lowerPointer;
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Double>[] compressions = IntStream.range(0, 1 << SPECIES.length())
            .mapToObj(index -> shuffle(index))
            .toArray(VectorShuffle[]::new);

    // takes a mask like 1, 0, 1, 0 and returns a shuffle like 0, 2, 1, 3
    private static VectorShuffle<Double> compress(VectorMask<Double> mask) {
        return compressions[(int) mask.toLong()];
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Double>[] rotations = IntStream.range(0, SPECIES.length())
            .mapToObj(index -> VectorShuffle.iota(SPECIES, index, 1, true))
            .toArray(VectorShuffle[]::new);

    private static VectorShuffle<Double> rotate(int by) {
        return rotations[by];
    }

    private static VectorShuffle<Double> shuffle(int maskArg) {
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
