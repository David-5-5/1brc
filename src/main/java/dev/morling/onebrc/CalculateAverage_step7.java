/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.lang.Double.parseDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_step7 {
    private static final String FILE = "./measurements.txt";

    static final File file = new File("measurements.txt");
    static final long length = file.length();
    static final int chunkCount = Runtime.getRuntime().availableProcessors();
    final long[] chunkStartOffsets = new long[chunkCount];


    public static void main(String[] args) throws Exception {
        var start = System.currentTimeMillis();
        Step1.calculate();
        System.err.format("Took %,d ms\n", System.currentTimeMillis() - start);
    }

    static class Step1 {
        private static void calculate() throws Exception {
            @SuppressWarnings("resource")
            var allStats = new BufferedReader(new FileReader(FILE))
                    .lines()
                    .parallel()
                    .collect(
                            groupingBy(line -> line.substring(0, line.indexOf(';')),
                                    summarizingDouble(line -> parseDouble(line.substring(line.indexOf(';') + 1)))));
            var result = allStats.entrySet().stream().collect(Collectors.toMap(
                    Entry::getKey,
                    e -> {
                        var stats = e.getValue();
                        return String.format("%.1f/%.1f/%.1f",
                                stats.getMin(), stats.getAverage(), stats.getMax());
                    },
                    (l, r) -> r,
                    TreeMap::new));
            System.out.println(result);
        }
    }

    static class Step2 {
        

        private static void calculate() throws Exception {
            final var results = new StationStats[chunkCount][];

            try (var file = new RandomAccessFile(new File(FILE), "r")) {
                MemorySegment mappedFile = file.getChannel().map(
                        MapMode.READ_ONLY, 0, length, Arena.global());
            }
        }
    }


    static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        StationStats(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }    

}