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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"})//, "-XX:+UnlockDiagnosticVMOptions", "-XX:CompileCommand=print,*.doPartition", "-XX:PrintAssemblyOptions=intel"})
@OutputTimeUnit(TimeUnit.SECONDS)
public class SortBenchmark {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    private final int[] master = new int[1_000_000];
    private int[] toSort = new int[1_000_000];

    @Setup(Level.Trial)
    public final void setupMaster() {
        Random random = new Random(0);
        for (int i = 0; i < master.length; i++) {
            master[i] = random.nextInt();
        }
    }

    @Setup(Level.Invocation)
    public final void setup() {
        toSort = master.clone();
    }

    //@Benchmark
    @OperationsPerInvocation(1_000_000)
    public final void sort8Network() {
        int bound = SPECIES.loopBound(1_000_000);
        for (int i = 0; i < bound; i += SPECIES.length()) {
            IntSortingNetwork.sort8(toSort, i, i + 8);
        }
    }

    //@Benchmark
    @OperationsPerInvocation(1_000_000)
    public final void sort8Jdk() {
        int bound = SPECIES.loopBound(1_000_000);
        for (int i = 0; i < bound; i += SPECIES.length()) {
            Arrays.sort(toSort, i, i + 8);
        }
    }

    //@Benchmark
    @OperationsPerInvocation(1_000_000)
    public final void benchmarkRearrange(Blackhole blackhole) {
        IntVectorizedQuickSort.doBetterPartition(toSort, 0, 0, toSort.length);
    }

    @Benchmark
    @OperationsPerInvocation(1_000_000)
    public final int[] simpleSort() {
        IntVectorizedQuickSort.quicksort(toSort, 0, toSort.length);
        return toSort;
    }

    //@Benchmark
    @OperationsPerInvocation(1_000_000)
    public final int[] partition() {
        IntVectorizedQuickSort.partition(toSort, 0, toSort.length);
        return toSort;
    }

    //@Benchmark
    @OperationsPerInvocation(1_000_000)
    public final int[] jdkSort() {
        Arrays.sort(toSort);
        return toSort;
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println(IntVector.SPECIES_PREFERRED.toString());
        Options opt = new OptionsBuilder()
                .include(".*" + SortBenchmark.class.getSimpleName() + ".*")
            //    .addProfiler(GCProfiler.class)
//                .addProfiler(LinuxPerfProfiler.class)
           //     .addProfiler(LinuxPerfNormProfiler.class)
 //              .addProfiler(JavaFlightRecorderProfiler.class)
    //            .addProfiler(LinuxPerfAsmProfiler.class)
                .build();
        new Runner(opt).run();
    }
}
