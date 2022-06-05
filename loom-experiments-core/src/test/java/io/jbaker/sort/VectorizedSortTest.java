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
import java.util.Random;
import org.junit.jupiter.api.Test;

public class VectorizedSortTest {

    @Test
    void testPivot() {
        double[] data = new double[] {-2.0, 1.0, -1.0, 2.0};
        VectorizedSort.partition(data, 0.0, 0, 4);
        assertThat(data).containsExactly(1.0, 2.0, -1.0, 2.0);
    }

    @Test
    void testFullSort() {
        double[] first = randomDoubles(24);
        double[] second = randomDoubles(24);
        Instant t1 = Instant.now();
        VectorizedSort.sort(first);
        Instant t2 = Instant.now();
        Arrays.sort(second);
        Instant t3 = Instant.now();
        System.out.printf("new: %s, old: %s%n", Duration.between(t1, t2), Duration.between(t2, t3));
        //assertThat(first).isEqualTo(second);
    }

    private static double[] randomDoubles(int power) {
        Random rand = new Random(0);
        double[] ret = new double[1 << power];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = rand.nextDouble();
        }
        return ret;
    }

    @Test
    void testVectorSort() {
        assertThat(VectorizedSort.sort(VectorizedSort.vector(7.0, 6.0, 2.0, 4.0, 3.0, 5.0, 1.0, 0.0)))
                .isEqualTo(VectorizedSort.vector(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0));
    }
}
