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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
                // The are no obliviously improvements between groupingBy and groupingByConcurrent
                .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }

    // -------------------------------------------------------------------------------------------------
    // ---------------------------- AS FOLLOWING IS SOLUTION 2 ---------------------------
    // -------------------------------------------------------------------------------------------------
    static byte line_separator = '\n';
    static ExecutorService executorService = Executors.newFixedThreadPool(8);
    static int threads = 72;

    public static void solution2() throws IOException, InterruptedException, ExecutionException {
        Map<String, MeasurementAggregator> result = new TreeMap<>();
        Map<Integer, PartialResult> partials = new TreeMap<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long chunkSize = channel.size() / threads;
            int leave = (int) channel.size() % threads;
            // System.out.println("channel.size() = " + channel.size());
            // System.out.println("precessors = " + precessors);

            if (chunkSize < 1024) {
                threads = 1;
                chunkSize = channel.size();
                leave = 0;
            }
            while (chunk++ < threads) {
                final Future<PartialResult> future = executorService
                        .submit(new PartialCallable(channel, chunk - 1, chunkSize, leave));

                partials.put(future.get().chunk, future.get());

            }
            executorService.shutdown();

            String prefix = "";
            for (int i = 0; i < threads; i++) {
                // System.out.prinln("fist line")
                if (i == 0) {
                    result = partials.get(i).partial;
                    prefix = partials.get(i).lastLine;
                }
                else {
                    for (String key : partials.get(i).partial.keySet()) {
                        if (result.containsKey(key)) {
                            MeasurementAggregator agg1 = result.get(key);
                            MeasurementAggregator agg2 = partials.get(i).partial.get(key);
                            agg1.min = Math.min(agg1.min, agg2.min);
                            agg1.max = Math.max(agg1.max, agg2.max);
                            agg1.sum += agg2.sum;
                            agg1.count += agg2.count;
                        }
                        else {
                            result.put(key, partials.get(i).partial.get(key));
                        }
                    }

                    handle(result, prefix + partials.get(i).firstLine);

                    prefix = partials.get(i).lastLine;
                }
            }

            Map<String, ResultRow> measurements = new TreeMap<>();
            // result = partials.get(1).partial;
            for (String key : result.keySet()) {
                ResultRow row = new ResultRow(result.get(key).min, (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count, result.get(key).max);
                measurements.put(key, row);
            }
            System.out.println(measurements);
        }
    }

    public static class PartialCallable implements Callable<PartialResult> {
        FileChannel channel;
        int chunk;
        long chunkSize;
        int leave;

        PartialCallable(FileChannel channel, int chunk, long chunkSize, int leave) {
            this.channel = channel;
            this.chunk = chunk;
            this.chunkSize = chunkSize;
            this.leave = leave;
        }

        @Override
        public PartialResult call() throws Exception {
            long size = chunkSize + (chunk + 1 == threads ? leave : 0);
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk * chunkSize, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            PartialResult result = new PartialResult(chunk, buffer);
            result.groupingBy();
            return result;
        }
    }

    private static void handle(Map<String, MeasurementAggregator> aggs, String line) {

        String[] value = line.split(";");
        if (value.length < 2) {
            // empty line
            // System.err.println("error, can't find two elements of '" + new String(line, StandardCharsets.UTF_8) + "'");
            return;
        }
        if (aggs.containsKey(value[0])) {
            MeasurementAggregator agg = aggs.get(value[0]);
            agg.count++;
            agg.min = Math.min(agg.min, Double.valueOf(value[1]));
            agg.max = Math.max(agg.max, Double.valueOf(value[1]));
            agg.sum += Double.valueOf(value[1]);
        }
        else {
            MeasurementAggregator agg = new MeasurementAggregator();
            agg.count = 1;
            agg.max = agg.min = agg.sum = Double.valueOf(value[1]);
            aggs.put(value[0], agg);
        }
    }

    static BiFunction<MeasurementAggregator, MeasurementAggregator, MeasurementAggregator> biFunction = (agg1, agg2) -> {
        var res = new MeasurementAggregator();
        res.min = Math.min(agg1.min, agg2.min);
        res.max = Math.max(agg1.max, agg2.max);
        res.sum = agg1.sum + agg2.sum;
        res.count = agg1.count + agg2.count;
        return res;
    };

    public static class PartialResult {
        int chunk;
        Map<String, MeasurementAggregator> partial = new TreeMap<>();
        ByteBuffer buffer;
        String firstLine = "", lastLine = "";

        PartialResult(int chunk, ByteBuffer buffer) {
            this.chunk = chunk;
            this.buffer = buffer;

        }

        // used for solution 2
        public void groupingBy() {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            int begin = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] == line_separator) {
                    byte[] line = new byte[i - begin];
                    System.arraycopy(data, begin, line, 0, line.length);
                    // System.out.println(new String(line, StandardCharsets.UTF_8));
                    if (begin == 0 && chunk != 0) {
                        firstLine = new String(line, StandardCharsets.UTF_8);
                    }
                    else
                        handle(partial, new String(line, StandardCharsets.UTF_8));
                    // set begin for next line
                    begin = i + 1;
                }

            }
            // Last
            byte[] line = new byte[data.length - begin];
            System.arraycopy(data, begin, line, 0, line.length);
            if (chunk + 1 != threads) {
                lastLine = new String(line, StandardCharsets.UTF_8);
            }
            else
                handle(partial, new String(line, StandardCharsets.UTF_8));
        }

        // used for sulution 3
        public void streams() {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            int begin = 0, end = data.length - 1;
            if (chunk != 0) {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == line_separator) {
                        byte[] line = new byte[i];
                        System.arraycopy(data, begin, line, 0, line.length);
                        firstLine = new String(line, StandardCharsets.UTF_8);
                        begin = i + 1;
                        break;
                    }
                }
            }
            if (chunk + 1 != threads) {
                for (int i = data.length - 1; i >= 0; i--) {
                    if (data[i] == line_separator) {
                        end = i;
                        byte[] line = new byte[data.length - end - 1];
                        if (end + 1 < data.length) {
                            System.arraycopy(data, end + 1, line, 0, line.length);
                            lastLine = new String(line, StandardCharsets.UTF_8);
                        }
                        break;
                    }
                }
            }
            byte[] content = new byte[end - begin];
            System.arraycopy(data, begin, content, 0, content.length);

            Collector<Measurement, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
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
                    });

            partial = new BufferedReader(new StringReader(new String(content, StandardCharsets.UTF_8))).lines()
                    .parallel()
                    .map(l -> new Measurement(l.split(";")))
                    // The are no obliviously improvements between groupingBy and groupingByConcurrent
                    .collect(Collectors.groupingBy(m -> m.station(), collector));

        }

    }

    // -------------------------------------------------------------------------------------------------
    // ---------------------------- AS FOLLOWING IS SOLUTION 3 ---------------------------
    // -------------------------------------------------------------------------------------------------
    public static void solution3() throws IOException, InterruptedException, ExecutionException {
        Map<String, MeasurementAggregator> result = new TreeMap<>();
        Map<Integer, PartialResult> partials = new TreeMap<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long chunkSize = channel.size() / threads;
            int leave = (int) channel.size() % threads;
            // System.out.println("channel.size() = " + channel.size());
            // System.out.println("precessors = " + precessors);

            if (chunkSize < 1024) {
                threads = 1;
                chunkSize = channel.size();
                leave = 0;
            }
            while (chunk++ < threads) {
                final Future<PartialResult> future = executorService
                        .submit(new StreamCallable(channel, chunk - 1, chunkSize, leave));

                partials.put(future.get().chunk, future.get());

            }
            executorService.shutdown();

            String prefix = "";
            for (int i = 0; i < threads; i++) {
                // System.out.prinln("fist line")
                if (i == 0) {
                    result = partials.get(i).partial;
                    prefix = partials.get(i).lastLine;
                }
                else {
                    for (String key : partials.get(i).partial.keySet()) {
                        result.merge(key, partials.get(i).partial.get(key),
                                biFunction);
                    }

                    handle(result, prefix + partials.get(i).firstLine);

                    prefix = partials.get(i).lastLine;
                }
            }

            Map<String, ResultRow> measurements = new TreeMap<>();
            // result = partials.get(1).partial;
            for (String key : result.keySet()) {
                ResultRow row = new ResultRow(result.get(key).min, (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count, result.get(key).max);
                measurements.put(key, row);
            }
            System.out.println(measurements);
        }
    }

    public static class StreamCallable implements Callable<PartialResult> {
        FileChannel channel;
        int chunk;
        long chunkSize;
        int leave;

        StreamCallable(FileChannel channel, int chunk, long chunkSize, int leave) {
            this.channel = channel;
            this.chunk = chunk;
            this.chunkSize = chunkSize;
            this.leave = leave;
        }

        @Override
        public PartialResult call() throws Exception {
            long size = chunkSize + (chunk + 1 == threads ? leave : 0);
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk * chunkSize, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            PartialResult result = new PartialResult(chunk, buffer);
            result.streams();
            return result;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // Long begin = System.currentTimeMillis();
        solution3();
        // System.out.println("Execute : " + (System.currentTimeMillis() - begin));

    }
}
