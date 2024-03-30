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

import static java.util.stream.Collectors.groupingBy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;

/**
 * Copy from baseline 22.697s for 0.1Bs
 * 1. stream(...).parallel().
 * 6.561s
 */
public class CalculateAverage_luming {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void solution1() throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m ->
        // Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) /
        // 10.0)));
        // System.out.println(measurements1);

        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                // Add parallel method
                .parallel()
                .map(l -> new Measurement(l.split(";")))
                .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }

    static int precessors = Runtime.getRuntime().availableProcessors();
    static ExecutorService executorService = Executors.newFixedThreadPool(precessors);

    // ----------------------------------------------------------------------------------------------------------------------
    public static void solution2() throws IOException {

        Map<String, MeasurementAggregator> collector = new TreeMap<>();

        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            final AtomicLong start = new AtomicLong(0);
            int allocated = 0;
            long chunkSize = channel.size() / precessors;

            while (allocated < precessors) {
                Future future = executorService.submit(() -> {
                    try {
                        read(channel.map(FileChannel.MapMode.READ_ONLY, start.longValue(), chunkSize));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    start.addAndGet(chunkSize);
                });
            }

        }
    }

    private static void read(MappedByteBuffer buffer) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        System.out.println(new String(data, StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws IOException {
        solution2();
    }
}
