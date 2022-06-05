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

public class VectorizedQuickSortTest {

    @Test
    void testPivot() {
        int runCount = 10;
        List<double[]> toSort = IntStream.range(0, runCount)
                .mapToObj(_x -> randomDoubles(1 << 24))
                .toList();
        VectorizedQuickSort.quicksort(randomDoubles(1 << 24), 0, 1 << 24);
        Instant now = Instant.now();
        toSort.forEach(array -> VectorizedQuickSort.quicksort(array, 0, array.length));
        System.out.println(Duration.between(now, Instant.now()).dividedBy(runCount));
    }

    @Test
    void testCorrectness() {
        for (int i = 0; i < 1; i++) {
            int count = 100000000;
            double[] expected = randomDoubles(count);
            double[] actual = randomDoubles(count);
            Arrays.sort(expected);
            VectorizedQuickSort.quicksort(actual, 0, actual.length);
            assertThat(actual).as(Integer.toString(i)).isEqualTo(expected);
        }
    }

    @Test
    void testOldStyle() {
        int runCount = 5;
        List<double[]> toSort = IntStream.range(0, runCount)
                .mapToObj(_x -> randomDoubles(1 << 24))
                .toList();
        Instant now = Instant.now();
        toSort.forEach(array -> Arrays.sort(array));
        System.out.println(Duration.between(now, Instant.now()).dividedBy(runCount));
    }

    private static double[] randomDoubles(int count) {
        return new Random(0).doubles(count).toArray();
    }
}
