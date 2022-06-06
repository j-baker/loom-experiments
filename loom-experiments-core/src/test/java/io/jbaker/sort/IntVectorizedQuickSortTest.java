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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class IntVectorizedQuickSortTest {

    @Test
    void testNewPivot() throws InterruptedException {
        int[] pivotArray = new int[] { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        IntVectorizedQuickSort.doBetterPartition(pivotArray, 8, 0, pivotArray.length);
    }

    @Test
    void testPivot() {
        int runCount = 10;
        List<int[]> toSort = IntStream.range(0, runCount)
                .mapToObj(_x -> randomDoubles(1 << 25))
                .toList();
        IntVectorizedQuickSort.quicksort(randomDoubles(1 << 25), 0, 1 << 25);
        Instant now = Instant.now();
        toSort.forEach(array -> IntVectorizedQuickSort.quicksort(array, 0, array.length));
        System.out.println(Duration.between(now, Instant.now()).dividedBy(runCount));
    }

    @Test
    void testPivotOld() {
        int runCount = 10;
        List<int[]> toSort = IntStream.range(0, runCount)
                .mapToObj(_x -> randomDoubles(1 << 25))
                .toList();
        IntVectorizedQuickSort.quicksort(randomDoubles(1 << 25), 0, 1 << 25);
        Instant now = Instant.now();
        toSort.forEach(Arrays::sort);
        System.out.println(Duration.between(now, Instant.now()).dividedBy(runCount));
    }

    @Test
    void testCorrectness() {
        for (int i = 1000000; i <= 1000000; i++) {
            int count = i;
            int[] expected = randomDoubles(count);
            int[] actual = randomDoubles(count);
            Arrays.sort(expected);
            IntVectorizedQuickSort.quicksort(actual, 0, actual.length);
            assertThat(actual).as(Integer.toString(i)).isEqualTo(expected);
        }
    }

    private static int[] randomDoubles(int count) {
        return new Random(0).ints(count).toArray();
    }
}
