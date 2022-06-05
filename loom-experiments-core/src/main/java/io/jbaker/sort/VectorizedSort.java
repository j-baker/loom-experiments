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
import java.util.stream.IntStream;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

public class VectorizedSort {
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    private static final VectorShuffle<Double> REVERSE = VectorShuffle.fromValues(SPECIES, 1, 0);

    private VectorizedSort() {}

    private static final Comparison C1 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 4, 5, 6, 7, 0, 1, 2, 3),
            VectorMask.fromLong(SPECIES, 0xF0));
    private static final Comparison C2 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 2, 3, 0, 1, 6, 7, 4, 5),
            VectorMask.fromLong(SPECIES, 0xCC));
    private static final Comparison C3 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 1, 0, 3, 2, 5, 4, 7, 6),
            VectorMask.fromLong(SPECIES, 0xAA));
    private static final Comparison C4 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 7, 6, 5, 4, 3, 2, 1, 0),
            VectorMask.fromLong(SPECIES, 0xF0));
    private static final Comparison C5 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 3, 2, 1, 0, 7, 6, 5, 4),
            VectorMask.fromLong(SPECIES, 0xCC));
    private static final Comparison C6 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 1, 0, 3, 2, 5, 4, 7, 6),
            VectorMask.fromLong(SPECIES, 0xAA));
    private static final Comparison C7 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 0, 1, 4, 5, 2, 3, 6, 7),
            VectorMask.fromLong(SPECIES, 0xB0));
    private static final Comparison C8 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 0, 2, 1, 4, 3, 6, 5, 7),
            VectorMask.fromLong(SPECIES, 0x55));
    private static final Comparison C9 = new Comparison(
            VectorShuffle.fromValues(SPECIES, 0, 1, 3, 2, 5, 4, 6, 7),
            VectorMask.fromLong(SPECIES, 0x28));

    @SuppressWarnings("CyclomaticComplexity")
    public static void partition(
            double[] array, double pivot, int from, int to) {
        int lowerPointer = 0;
        int upperPointer = to - from;

        while (true) {
            int innerUpperPointer = upperPointer - SPECIES.length();
            if (lowerPointer + SPECIES.length() >= upperPointer) { // >=?
                break;
            }

            DoubleVector lowerCandidates = DoubleVector.fromArray(SPECIES, array, lowerPointer);
            DoubleVector upperCandidates = DoubleVector.fromArray(SPECIES, array, innerUpperPointer);

            VectorMask<Double> lowerMatching = lowerCandidates.compare(VectorOperators.LT, pivot);
            VectorMask<Double> upperMatching = upperCandidates.compare(VectorOperators.LT, pivot);

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
            } else if (lowerTrueCount + upperTrueCount > SPECIES.length()) {
                // overflow on lower half
                int elementsFromUpper = SPECIES.length() - lowerTrueCount;

                DoubleVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                DoubleVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));

                DoubleVector newLower = lowerShuffled.unslice(lowerTrueCount, upperShuffled, 0);
                newLower.intoArray(array, lowerPointer);

                int mask = ((1 << upperTrueCount) - 1) << elementsFromUpper;

                DoubleVector reversed = upperShuffled.rearrange(REVERSE);
                DoubleVector withLowerElements = reversed.selectFrom(lowerShuffled, VectorMask.fromLong(SPECIES, mask));
                withLowerElements.intoArray(array, innerUpperPointer);

                lowerPointer += SPECIES.length();
                upperPointer -= elementsFromUpper;
            } else {
                // overflow on upper half, symmetric to other overflow case
                int elementsFromLower = SPECIES.length() - upperTrueCount;
                DoubleVector lowerShuffled = lowerCandidates.rearrange(compress(lowerMatching));
                DoubleVector upperShuffled = upperCandidates.rearrange(compress(upperMatching));

                DoubleVector newUpper = upperShuffled.unslice(SPECIES.length() - elementsFromLower, lowerShuffled, 1);
                newUpper.intoArray(array, innerUpperPointer);

                int mask = (1 << lowerTrueCount) - 1;

                DoubleVector reversed = lowerShuffled.rearrange(REVERSE);
                DoubleVector withUpperElements = reversed.selectFrom(upperCandidates, VectorMask.fromLong(SPECIES, mask));
                withUpperElements.intoArray(array, lowerPointer);

                lowerPointer += elementsFromLower;
                upperPointer -= SPECIES.length();
            }
        }

        while (lowerPointer < upperPointer - 1) {
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

        // fall back to manual pivot for remaining elements
    }

    @SuppressWarnings("unchecked")
    private static final VectorShuffle<Double>[] compressions = IntStream.range(0, 1 << SPECIES.length())
            .mapToObj(index -> shuffle(index))
            .toArray(VectorShuffle[]::new);

    // takes a mask like 1, 0, 1, 0 and returns a shuffle like 0, 2, 1, 3
    private static VectorShuffle<Double> compress(VectorMask<Double> mask) {
        return compressions[(int) mask.toLong()];
    }

    private static VectorShuffle<Double> shuffle(int maskArg) {
        int mask = maskArg;
        int[] ret = new int[SPECIES.length()];
        int unmatchedIndex = ret.length - 1;
        int matchedIndex = 0;
        for (int i = 0; i < SPECIES.length(); i++) {
            if ((mask & 1) == 0) {
                ret[i] = unmatchedIndex--;
            } else {
                ret[i] = matchedIndex++;
            }
            mask >>= 1;
        }
        return VectorShuffle.fromValues(SPECIES, ret);
    }

    public static void sort(double[] array) {
        for (int i = 0; i < array.length; i += 8) {
            sort(DoubleVector.fromArray(SPECIES, array, i)).intoArray(array, i);
        }
        for (int i = 16; i <= array.length; i <<= 1) {
            for (int j = 0; j < array.length; j += i) {
                merge(array, j, i);
            }
        }
    }

    private static void merge(double[] array, int origin, int length) {
        if (length == 8) {
            merge(DoubleVector.fromArray(SPECIES, array, origin)).intoArray(array, origin);
        } else {
            int half = length / 2;
            for (int i = 0; i < half; i += SPECIES.length()) {
                DoubleVector left = DoubleVector.fromArray(SPECIES, array, origin + i);
                DoubleVector right = DoubleVector.fromArray(SPECIES, array,  origin + length - i - 8).rearrange(REVERSE);
                left.min(right).intoArray(array, origin + i);
                left.max(right).rearrange(REVERSE).intoArray(array, origin + length - i - 8);
            }
            int newLength = length / 2;
            bitonicSort(array, origin, newLength);
            bitonicSort(array, origin + newLength, newLength);
        }
    }

    private static void bitonicSort(double[] array, int origin, int length) {
        if (length == 8) {
            DoubleVector vec = DoubleVector.fromArray(SPECIES, array, origin);
            bitonicSort(vec).intoArray(array, origin);
        } else {
            halfCleaner(array, origin, length);
            int newLength = length / 2;
            bitonicSort(array, origin, newLength);
            bitonicSort(array, origin + newLength, newLength);
        }
    }

    // only works for powers of 2
    private static void halfCleaner(double[] array, int origin, int length) {

        int half = length / 2;

        for (int i = origin; i < origin + half; i += SPECIES.length()) {
            DoubleVector left = DoubleVector.fromArray(SPECIES, array, i);
            DoubleVector right = DoubleVector.fromArray(SPECIES, array, i + half);
            left.min(right).intoArray(array, i);
            left.max(right).intoArray(array, i + half);
        }
    }

    @VisibleForTesting
    static DoubleVector merge(DoubleVector input) {
        DoubleVector data = input;
        data = compareAndExchange(data, C4);
        data = compareAndExchange(data, C2);
        data = compareAndExchange(data, C3);
        return data;
    }

    @VisibleForTesting
    static DoubleVector bitonicSort(DoubleVector input) {
        DoubleVector data = input;
        data = compareAndExchange(data, C1);
        data = compareAndExchange(data, C2);
        data = compareAndExchange(data, C3);
        return data;
    }

    @VisibleForTesting
    static DoubleVector sort(DoubleVector input) {
        DoubleVector data = input;
        data = compareAndExchange(data, C4);
        data = compareAndExchange(data, C5);
        data = compareAndExchange(data, C6);
        data = compareAndExchange(data, C7);
        data = compareAndExchange(data, C8);
        data = compareAndExchange(data, C9);
        return data;
    }

    @VisibleForTesting
    static DoubleVector vector(double v0, double v1, double v2, double v3, double v4, double v5, double v6, double v7) {
        return DoubleVector.fromArray(SPECIES, new double[] {v0, v1, v2, v3, v4, v5, v6, v7}, 0);
    }

    private static DoubleVector compareAndExchange(DoubleVector vector, Comparison comparison) {
        DoubleVector vector1 = vector.rearrange(comparison.shuffle);
        DoubleVector vMin = vector.min(vector1);
        DoubleVector vMax = vector.max(vector1);
        return vMin.blend(vMax, comparison.mask);
    }

    private record Comparison(VectorShuffle<Double> shuffle, VectorMask<Double> mask) {}
}
