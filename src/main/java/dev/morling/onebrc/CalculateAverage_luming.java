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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import sun.misc.Unsafe;

/**
 * Copy from baseline 22.697s for 0.1Bs
 * 2 solution 2
 */
public class CalculateAverage_luming {

    private static int NAME_LENGTH_LIMIT_BYTES = 128;

    private static final String FILE = "./measurements.txt";

    // ------------------------------------------------------------------------
    // The Bean of input and output
    // ------------------------------------------------------------------------
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

        MeasurementAggregator() {
        }

        MeasurementAggregator(double val) {
            this.min = this.max = this.sum = val;
            this.count += 1;
        }

    }

    // ------------------------------------------------------------------------
    // The Functions used by Collector
    // ------------------------------------------------------------------------
    private static BinaryOperator<MeasurementAggregator> combiner = (agg1, agg2) -> {
        var res = new MeasurementAggregator();
        res.min = Math.min(agg1.min, agg2.min);
        res.max = Math.max(agg1.max, agg2.max);
        res.sum = agg1.sum + agg2.sum;
        res.count = agg1.count + agg2.count;
        return res;
    };

    private static BiConsumer<MeasurementAggregator, Measurement> accumulator = (a, m) -> {
        a.min = Math.min(a.min, m.value);
        a.max = Math.max(a.max, m.value);
        a.sum += m.value;
        a.count++;
    };

    private static Function<MeasurementAggregator, ResultRow> finisher = agg -> {
        return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
    };

    // -----------------------------------------------------------------------------
    // Base line
    // solution 1 modifey from baseline only stream(...).parallel().
    // 6.561s
    // -----------------------------------------------------------------------------
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
                MeasurementAggregator::new, accumulator, combiner, finisher);

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                // Add parallel method
                .parallel()
                .map(l -> new Measurement(l.split(";")))
                // The are no obliviously improvements between groupingBy and
                // groupingByConcurrent
                .collect(Collectors.groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }

    // ------------------------------------------------------------------------------------------------
    // Sharable static class and method
    // ------------------------------------------------------------------------------------------------
    final static int BUFFER_SIZE = 108;
    final static int BLOCK_SIZE = 128 * 1024 * 1024;
    final static byte LINE_SEPARATOR = '\n';
    final static int PROCESSORS = Runtime.getRuntime().availableProcessors();

    static ExecutorService executorService = Executors.newFixedThreadPool(PROCESSORS);
    static int threads = PROCESSORS * 9;

    public static class SolutionCallable implements Callable<PartialResult> {
        FileChannel channel;
        int chunk;
        long chunkSize;
        int leave;
        Consumer<PartialResult> action;

        SolutionCallable(FileChannel channel, int chunk, long chunkSize, int leave, Consumer<PartialResult> action) {
            this.channel = channel;
            this.chunk = chunk;
            this.chunkSize = chunkSize;
            this.leave = leave;
            this.action = action;
        }

        @Override
        public PartialResult call() throws Exception {
            long size = chunkSize + (chunk + 1 == threads ? leave : 0);
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, chunk * chunkSize, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            PartialResult result = new PartialResult(chunk, buffer);
            action.accept(result);
            return result;
        }
    }

    public static class PartialResult {
        int chunk;
        Map<String, MeasurementAggregator> partial = new HashMap<>(1024 * 16);
        ByteBuffer buffer;
        String firstLine = "", lastLine = "";

        PartialResult(int chunk, ByteBuffer buffer) {
            this.chunk = chunk;
            this.buffer = buffer;
        }

    }

    // -------------------------------------------------------------------------------------------------
    // ---------------------------- AS FOLLOWING IS SOLUTION 2
    // ---------------------------
    // Solution 2 :
    // Using Threadpool with Runtime.getRuntime().availableProcessors()
    // Each thread read line and merge
    // 22.295
    // -------------------------------------------------------------------------------------------------
    public static void solution2() throws IOException, InterruptedException, ExecutionException {
        Map<String, MeasurementAggregator> result = new TreeMap<>();
        Map<Integer, PartialResult> partials = new TreeMap<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long chunkSize = channel.size() / threads;
            int leave = (int) channel.size() % threads;

            if (chunkSize < 1024) {
                threads = 1;
                chunkSize = channel.size();
                leave = 0;
            }
            while (chunk++ < threads) {
                final Future<PartialResult> future = executorService
                        .submit(new SolutionCallable(channel, chunk - 1, chunkSize, leave, r -> {
                            groupingBy(r);
                        }));

                partials.put(future.get().chunk, future.get());

            }
            executorService.shutdown();

            String prefix = "";
            for (int i = 0; i < threads; i++) {
                // System.out.prinln("fist line")
                if (i == 0) {
                    result = partials.get(i).partial;
                    prefix = partials.get(i).lastLine;
                } else {
                    for (String key : partials.get(i).partial.keySet()) {
                        result.merge(key, partials.get(i).partial.get(key),
                                combiner);
                    }

                    String[] line = (prefix + partials.get(i).firstLine).split(";");
                    if (line.length == 2)
                        result.merge(line[0], new MeasurementAggregator(Double.parseDouble(line[1])), combiner);

                    prefix = partials.get(i).lastLine;
                }
            }

            Map<String, ResultRow> measurements = new TreeMap<>();
            // result = partials.get(1).partial;
            for (String key : result.keySet()) {
                ResultRow row = new ResultRow(result.get(key).min,
                        (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count, result.get(key).max);
                measurements.put(key, row);
            }
            System.out.println(measurements);
        }
    }

    // used for solution 2
    public static void groupingBy(PartialResult result) {
        byte[] data = new byte[result.buffer.remaining()];
        result.buffer.get(data);
        int begin = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == LINE_SEPARATOR) {
                byte[] line = new byte[i - begin];
                System.arraycopy(data, begin, line, 0, line.length);
                // System.out.println(new String(line, StandardCharsets.UTF_8));
                if (begin == 0 && result.chunk != 0) {
                    result.firstLine = new String(line, StandardCharsets.UTF_8);
                } else {
                    String[] part = new String(line, StandardCharsets.UTF_8).split(";");
                    if (part.length == 2)
                        result.partial.merge(part[0], new MeasurementAggregator(Double.parseDouble(part[1])), combiner);
                }
                // set begin for next line
                begin = i + 1;
            }

        }
        // Last
        byte[] line = new byte[data.length - begin];
        System.arraycopy(data, begin, line, 0, line.length);
        if (result.chunk + 1 != threads) {
            result.lastLine = new String(line, StandardCharsets.UTF_8);
        } else {
            String[] part = new String(line, StandardCharsets.UTF_8).split(";");
            if (part.length == 2)
                result.partial.merge(part[0], new MeasurementAggregator(Double.parseDouble(part[1])), combiner);
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Solution 3 :
    // Using Threadpool with Runtime.getRuntime().availableProcessors()
    // Each thread readline to stream and execute groupby
    // 9.698 s
    // -------------------------------------------------------------------------------------------------
    public static void solution3() throws IOException, InterruptedException, ExecutionException {
        Map<String, MeasurementAggregator> result = new TreeMap<>();
        Map<Integer, PartialResult> partials = new TreeMap<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long chunkSize = channel.size() / threads;
            int leave = (int) channel.size() % threads;

            if (chunkSize < 1024) {
                threads = 1;
                chunkSize = channel.size();
                leave = 0;
            }
            while (chunk++ < threads) {
                final Future<PartialResult> future = executorService
                        .submit(new SolutionCallable(channel, chunk - 1, chunkSize, leave, r -> {
                            streams(r);
                        }));

                partials.put(future.get().chunk, future.get());

            }
            executorService.shutdown();

            String prefix = "";
            for (int i = 0; i < threads; i++) {
                // System.out.prinln("fist line")
                if (i == 0) {
                    result = partials.get(i).partial;
                    prefix = partials.get(i).lastLine;
                } else {
                    for (String key : partials.get(i).partial.keySet()) {
                        result.merge(key, partials.get(i).partial.get(key),
                                combiner);
                    }

                    String[] line = (prefix + partials.get(i).firstLine).split(";");
                    if (line.length == 2)
                        result.merge(line[0], new MeasurementAggregator(Double.parseDouble(line[1])), combiner);

                    prefix = partials.get(i).lastLine;
                }
            }

            Map<String, ResultRow> measurements = new TreeMap<>();
            for (String key : result.keySet()) {
                ResultRow row = new ResultRow(result.get(key).min,
                        (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count, result.get(key).max);
                measurements.put(key, row);
            }
            System.out.println(measurements);
        }
    }

    // used for sulution 3
    public static void streams(PartialResult result) {
        byte[] data = new byte[result.buffer.remaining()];
        result.buffer.get(data);
        int begin = 0, end = data.length - 1;
        if (result.chunk != 0) {
            for (int i = 0; i < data.length; i++) {
                if (data[i] == LINE_SEPARATOR) {
                    byte[] line = new byte[i];
                    System.arraycopy(data, begin, line, 0, line.length);
                    result.firstLine = new String(line, StandardCharsets.UTF_8);
                    begin = i + 1;
                    break;
                }
            }
        }
        if (result.chunk + 1 != threads) {
            for (int i = data.length - 1; i >= 0; i--) {
                if (data[i] == LINE_SEPARATOR) {
                    end = i;
                    byte[] line = new byte[data.length - end - 1];
                    if (end + 1 < data.length) {
                        System.arraycopy(data, end + 1, line, 0, line.length);
                        result.lastLine = new String(line, StandardCharsets.UTF_8);
                    }
                    break;
                }
            }
        }
        byte[] content = new byte[end - begin];
        System.arraycopy(data, begin, content, 0, content.length);

        Collector<Measurement, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
                MeasurementAggregator::new, accumulator, combiner);

        result.partial = new BufferedReader(new StringReader(new String(content, StandardCharsets.UTF_8)))
                .lines().parallel()
                .map(l -> new Measurement(l.split(";")))
                // The are no obliviously improvements between split and indexOf
                // .map(l -> {
                // int pos = l.indexOf(';');
                // return new Measurement(new String[]{ l.substring(0, pos), l.substring(pos +
                // 1) });
                // })
                // The are no obliviously improvements between groupingBy and
                // groupingByConcurrent
                .collect(Collectors.groupingBy(m -> m.station(), collector));

    }

    // -------------------------------------------------------------------------------------------------
    // ---------------------------- AS FOLLOWING IS SOLUTION 4
    // ---------------------------
    // ---------------------------- Modified from my solution 2 to ags313
    // 1) Change specified threads to specified BLOCK_SIZE for each thread
    // Result -- 23.516 s
    //
    // 2) Add static method valueOf for class Key, then it can change key
    // from String to Key instance
    // Result -- Verify Fail
    //
    // 3) Change read line to read byte
    // -------------------------------------------------------------------------------------------------
    private static class Key implements Comparable<Key> {
        private final byte[] value = new byte[NAME_LENGTH_LIMIT_BYTES];
        private int hashCode;
        private int length = 0;

        public static Key valueOf(String v) {
            Key key = new Key();
            byte[] bs = v.getBytes();
            for (int i = 0; i < bs.length; i++) {
                key.accept(bs[i]);
            }
            return key;
        }

        // https://stackoverflow.com/questions/20952739/how-would-you-convert-a-string-to-a-64-bit-integer
        public void accept(byte b) {
            value[length] = b;
            length += 1;
            hashCode = hashCode * 10191 + b;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that)
                return true;
            if (that == null)
                return false;
            Key key = (Key) that; // not checking class, nothing else uses this
            if (hashCode != key.hashCode)
                return false;
            if (length != key.length)
                return false;
            for (int i = 0; i < length; i++) {
                if (UNSAFE.getByte(value, i) != UNSAFE.getByte(key.value, i)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return new String(value, 0, length);
        }

        void reset() {
            length = 0;
            hashCode = 0;
        }

        @Override
        public int compareTo(Key other) {
            return toString().compareTo(other.toString());
        }
    }

    public static class Solution4Result {
        int chunk;
        Map<Key, MeasurementAggregator> partial = new HashMap<>(1024 * 16);
        ByteBuffer buffer;
        String firstLine = "", lastLine = "";

        Solution4Result(int chunk, ByteBuffer buffer) {
            this.chunk = chunk;
            this.buffer = buffer;
        }

    }

    public static class Solution4Callable implements Callable<Solution4Result> {
        FileChannel channel;
        int chunk;
        long begin;
        long size;
        Consumer<Solution4Result> action;

        Solution4Callable(FileChannel channel, int chunk, long begin, long size, Consumer<Solution4Result> action) {
            this.channel = channel;
            this.chunk = chunk;
            this.begin = begin;
            this.size = size;
            this.action = action;
        }

        @Override
        public Solution4Result call() throws Exception {

            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            Solution4Result result = new Solution4Result(chunk, buffer);
            action.accept(result);
            return result;
        }
    }

    public static void solution4() throws IOException, InterruptedException, ExecutionException {
        Map<Key, MeasurementAggregator> result = new TreeMap<>();
        Map<Integer, Solution4Result> partials = new TreeMap<>();
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long begin = 0;
            // System.out.println("channel.size = " + channel.size());
            while (begin < channel.size()) {
                long size = Math.min(BLOCK_SIZE, channel.size() - begin);

                if (size == BLOCK_SIZE) {
                    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, size,
                            Math.min(BUFFER_SIZE, channel.size() - begin - size));
                    while (buffer.hasRemaining()) {
                        byte cur = buffer.get();
                        size++;
                        if (cur == LINE_SEPARATOR) {
                            break;
                        }
                    }
                }
                // System.out.println("execute thread from " + begin + " and size = " + size);
                final Future<Solution4Result> future = executorService
                        .submit(new Solution4Callable(channel, chunk++, begin, size, r -> {
                            handle4(r);
                        }));
                // Next block
                begin += size;
                partials.put(future.get().chunk, future.get());

            }
            executorService.shutdown();

            for (int i = 0; i < chunk; i++) {
                if (i == 0) {
                    result = partials.get(i).partial;
                } else {
                    for (Key key : partials.get(i).partial.keySet()) {
                        result.merge(key, partials.get(i).partial.get(key),
                                combiner);
                    }
                }
            }

            Map<String, ResultRow> measurements = new TreeMap<>();
            for (Key key : result.keySet()) {
                ResultRow row = new ResultRow(result.get(key).min,
                        (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count, result.get(key).max);
                measurements.put(key.toString(), row);
            }
            System.out.println(measurements);
        }

    }

    public static void handle4(Solution4Result result) {
        byte[] data = new byte[result.buffer.remaining()];
        result.buffer.get(data);
        int begin = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == LINE_SEPARATOR) {
                byte[] line = new byte[i - begin];
                System.arraycopy(data, begin, line, 0, line.length);
                // System.out.println(new String(line, StandardCharsets.UTF_8));
                if (begin == 0 && result.chunk != 0) {
                    result.firstLine = new String(line, StandardCharsets.UTF_8);
                } else {
                    String[] part = new String(line, StandardCharsets.UTF_8).split(";");
                    if (part.length == 2)
                        result.partial.merge(Key.valueOf(part[0]),
                                new MeasurementAggregator(Double.parseDouble(part[1])), combiner);
                }
                // set begin for next line
                begin = i + 1;
            }

        }
        // Last
        byte[] line = new byte[data.length - begin];
        System.arraycopy(data, begin, line, 0, line.length);
        if (result.chunk + 1 != threads) {
            result.lastLine = new String(line, StandardCharsets.UTF_8);
        } else {
            String[] part = new String(line, StandardCharsets.UTF_8).split(";");
            if (part.length == 2)
                result.partial.merge(Key.valueOf(part[0]), new MeasurementAggregator(Double.parseDouble(part[1])),
                        combiner);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // Long begin = System.currentTimeMillis();
        solution4();
        // System.out.println("Execute : " + (System.currentTimeMillis() - begin));

    }

    private static final Unsafe UNSAFE = unsafe();

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
