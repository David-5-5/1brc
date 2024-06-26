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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
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
        @SuppressWarnings("unchecked")
        final Future<PartialResult>[] futures = new Future[threads];
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
            while (chunk < threads) {
                futures[chunk] = executorService
                        .submit(new SolutionCallable(channel, chunk, chunkSize, leave, r -> {
                            groupingBy(r);
                        }));

                chunk++;

            }
            for (int i = 0; i < threads; i++)
                partials.put(i, futures[i].get());

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
                }
                else {
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
        }
        else {
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
        @SuppressWarnings("unchecked")
        final Future<PartialResult>[] futures = new Future[threads];
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
            while (chunk < threads) {
                futures[chunk] = executorService
                        .submit(new SolutionCallable(channel, chunk, chunkSize, leave, r -> {
                            streams(r);
                        }));

                chunk++;

            }

            for (int i = 0; i < threads; i++)
                partials.put(futures[i].get().chunk, futures[i].get());

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
    // 2) Modify function.get() asynchronized, actural implement concurrent.
    // Result -- 7 s
    //
    // 3) Add static method valueOf for class Key, then it can change key
    // from String to Key instance
    // Replace of The UNSAFE class
    // Result -- 8.429
    //
    // 4) Change read line to read byte
    // Result -- 5.429
    //
    // 5) Use Stats replace of MeasurementAggregator
    // Result -- 2.388
    // based on step 5, direct Use The UNSAFE class The elapse is 81.334 s
    //
    // 6) Step 6
    // new Key() when key is first occur
    // Result -- 1.605 Use The UNSAFE class
    // Result -- 1.584 Without The UNSAFE class
    //
    // Sum, ON MY OPINION
    // 1, read byte is faster than readline with String
    // 2, Create minimal objects as possible.
    // 3, Add with long/int type, At the end, transfer to Double
    // -------------------------------------------------------------------------------------------------
    private static class Key implements Comparable<Key> {
        private final byte[] value = new byte[NAME_LENGTH_LIMIT_BYTES];
        private int hashCode;
        private int length = 0;

        // https://stackoverflow.com/questions/20952739/how-would-you-convert-a-string-to-a-64-bit-integer
        public void accept(byte b) {
            value[length] = b;
            length += 1;
            hashCode = hashCode * 10191 + b;
        }

        // @Override
        // public boolean equals(Object that) {
        // if (this == that)
        // return true;
        // if (that == null)
        // return false;
        // Key key = (Key) that; // not checking class, nothing else uses this
        // if (hashCode == key.hashCode && length == key.length)
        // return true;

        // return false;
        // }

        /**
         * The original equals
         * 
         * @param that
         * @return
         */
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

            // As following solution is a good choice.
            // for (int i = 0; i < length; i++) {
            // // Don't use unsafe
            // if (value[i] != key.value[i]) {
            // return false;
            // }
            // }

            // Use UNSAFE is alternative solution
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

    static class Stats {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long total;
        private int count;

        Stats() {
        }

        @Override
        public String toString() {
            return BigDecimal.valueOf(min / (10.0)).setScale(1, RoundingMode.HALF_UP) + "/"
                    + BigDecimal.valueOf(total / (10.0 * count)).setScale(1, RoundingMode.HALF_UP) + '/'
                    + BigDecimal.valueOf(max / (10.0)).setScale(1, RoundingMode.HALF_UP);
        }

        void merge(Stats that) {
            max = Math.max(this.max, that.max);
            min = Math.min(this.min, that.min);
            total += that.total;
            count += that.count;
        }
    }

    public static class Solution4Result {

        Map<Key, Stats> aggs = new HashMap<>(1024 * 16);
        ByteBuffer buffer;

        Solution4Result(ByteBuffer buffer) {
            this.buffer = buffer;
        }

    }

    public static class Solution4Callable implements Callable<Solution4Result> {
        FileChannel channel;
        long begin;
        long size;
        Consumer<Solution4Result> action;

        Solution4Callable(FileChannel channel, long begin, long size, Consumer<Solution4Result> action) {
            this.channel = channel;
            this.begin = begin;
            this.size = size;
            this.action = action;
        }

        @Override
        public Solution4Result call() throws Exception {

            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            Solution4Result result = new Solution4Result(buffer);
            action.accept(result);
            return result;
        }
    }

    public static void solution4() throws IOException, InterruptedException, ExecutionException {
        Map<Key, Stats> result = new TreeMap<>();
        @SuppressWarnings("unchecked")
        final Future<Solution4Result>[] futures = new Future[128];

        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long begin = 0;
            // System.out.println("channel.size = " + channel.size());
            while (begin < channel.size()) {
                long size = Math.min(BLOCK_SIZE, channel.size() - begin);

                if (size == BLOCK_SIZE) {
                    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin + size,
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
                futures[chunk] = executorService
                        .submit(new Solution4Callable(channel, begin, size, r -> {
                            handle4(r);
                        }));
                // Next block
                begin += size;

                chunk++;

            }

            for (int i = 0; i < chunk; i++) {
                Solution4Result part = futures[i].get();

                part.aggs.forEach((key, value) -> result.computeIfAbsent(key, __ -> new Stats()).merge(value));
            }

            executorService.shutdown();

            // Map<String, ResultRow> measurements = new TreeMap<>();
            // for (Key key : result.keySet()) {
            // ResultRow row = new ResultRow(result.get(key).min,
            // (Math.round(result.get(key).sum * 10.0) / 10.0) / result.get(key).count,
            // result.get(key).max);
            // measurements.put(key.toString(), row);
            // }
            System.out.println(new TreeMap<>(result));
        }

    }

    /**
     * 
     * @param result
     */
    public static void handle4(Solution4Result result) {
        Key key = new Key();
        while (result.buffer.remaining() > 1) {
            key.reset();
            boolean negative = false;

            var value = 0;

            // Acquire the key
            while (result.buffer.remaining() > 0) {
                byte b = result.buffer.get();
                if (b == ';')
                    break;
                key.accept(b);
            }
            // Acquire the value
            loop2: while (result.buffer.remaining() > 0) {
                byte b = result.buffer.get();
                switch (b) {
                    case '-':
                        negative = true;
                        break;
                    case '.':
                        value = (value * 10) + (result.buffer.get() - '0'); // single decimal
                        break;
                    case '\n':
                        break loop2;
                    default:
                        value = (value * 10) + (b - '0');
                }
            }
            if (negative)
                value = -value;

            // Merge the current
            Stats stats = result.aggs.computeIfAbsent(key, c -> new Stats());
            if (stats.count == 0)
                key = new Key();

            stats.count++;
            stats.total += value;
            if (value < stats.min) {
                stats.min = value;
            }
            if (value > stats.max) {
                stats.max = value;
            }
        }
    }

    /**
     * AS FOLLOWING IS SOLUTION 5 Modified from my solution4 to tonivade
     * 
     * 
     * 1) Do not replace {@link ExecutorService} to {@link StructuredTaskScope}
     * 2) Non-image mode:
     * 1.238 vs 1.191 (original)
     * Image Mode:
     * 1.024 vs 0.781 (original)
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void solution5() throws IOException, InterruptedException, ExecutionException {
        Map<String, Stat5> result = new TreeMap<>();
        @SuppressWarnings("unchecked")
        final Future<Solution5Result>[] futures = new Future[128];

        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            int chunk = 0;
            long begin = 0;
            // System.out.println("channel.size = " + channel.size());
            while (begin < channel.size()) {
                long size = Math.min(BLOCK_SIZE, channel.size() - begin);

                if (size == BLOCK_SIZE) {
                    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin + size,
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
                futures[chunk] = executorService
                        .submit(new Solution5Callable(channel, begin, size, r -> {
                            handle5(r);
                        }));
                // Next block
                begin += size;

                chunk++;

            }

            for (int i = 0; i < chunk; i++) {
                Solution5Result part = futures[i].get();
                part.merge(result);
            }

            executorService.shutdown();

            System.out.println(new TreeMap<>(result));
        }
    }

    static class Stat5 {
        private byte[] name;
        private final int hash;

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long total;
        private int count;

        Stat5(byte[] source, int length, int hash) {
            name = new byte[length];
            System.arraycopy(source, 0, name, 0, length);
            this.hash = hash;
        }

        boolean sameName(int length, int hash) {
            return name.length == length && this.hash == hash;
        }

        void add(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            total += value;
            count++;
        }

        String getName() {
            return new String(name, StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return BigDecimal.valueOf(min / (10.0)).setScale(1, RoundingMode.HALF_UP) + "/"
                    + BigDecimal.valueOf(total / (10.0 * count)).setScale(1, RoundingMode.HALF_UP) + '/'
                    + BigDecimal.valueOf(max / (10.0)).setScale(1, RoundingMode.HALF_UP);
        }

        Stat5 merge(Stat5 that) {
            max = Math.max(this.max, that.max);
            min = Math.min(this.min, that.min);
            total += that.total;
            count += that.count;
            return this;
        }
    }

    public static class Solution5Result {

        private static final int NUMBER_OF_BUCKETS = 1000;
        private static final int BUCKET_SIZE = 50;

        final Stat5[][] buckets = new Stat5[NUMBER_OF_BUCKETS][BUCKET_SIZE];

        Stat5 find(byte[] name, int length, int hash) {
            var bucket = buckets[Math.abs(hash % NUMBER_OF_BUCKETS)];
            for (int i = 0; i < BUCKET_SIZE; i++) {
                if (bucket[i] == null) {
                    bucket[i] = new Stat5(name, length, hash);
                    return bucket[i];
                }
                else if (bucket[i].sameName(length, hash)) {
                    return bucket[i];
                }
            }
            throw new IllegalStateException("no more space left");
        }

        ByteBuffer buffer;

        Solution5Result(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        void merge(Map<String, Stat5> result) {
            for (Stat5[] bucket : buckets) {
                for (Stat5 station : bucket) {
                    if (station != null) {
                        result.merge(station.getName(), station, Stat5::merge);
                    }
                }
            }
        }

    }

    public static class Solution5Callable implements Callable<Solution5Result> {
        FileChannel channel;
        long begin;
        long size;
        Consumer<Solution5Result> action;

        Solution5Callable(FileChannel channel, long begin, long size, Consumer<Solution5Result> action) {
            this.channel = channel;
            this.begin = begin;
            this.size = size;
            this.action = action;
        }

        @Override
        public Solution5Result call() throws Exception {

            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin, size);

            // System.out.println(new String(data, StandardCharsets.UTF_8));
            Solution5Result result = new Solution5Result(buffer);
            action.accept(result);
            return result;
        }
    }

    /**
     * 
     * @param result
     */
    public static void handle5(Solution5Result result) {
        byte[] name = new byte[128];
        while (result.buffer.remaining() > 1) {
            boolean negative = false;
            int nlen = 0;
            int hash = 1;

            var value = 0;

            // Acquire the key
            while (result.buffer.remaining() > 0) {
                byte b = result.buffer.get();
                if (b == ';')
                    break;
                name[nlen++] = b;
                hash = 31 * hash + b;
            }
            // Acquire the value
            loop2: while (result.buffer.remaining() > 0) {
                byte b = result.buffer.get();
                switch (b) {
                    case '-':
                        negative = true;
                        break;
                    case '.':
                        value = (value * 10) + (result.buffer.get() - '0'); // single decimal
                        break;
                    case '\n':
                        break loop2;
                    default:
                        value = (value * 10) + (b - '0');
                }
            }
            if (negative)
                value = -value;

            // Merge the current
            result.find(name, nlen, hash).add(value);

        }
    }

    /**
     * AS FOLLOWING IS SOLUTION 6 Modified from my solution5
     * 
     * 
     * 1) Replace {@link ExecutorService} to {@link StructuredTaskScope}
     * 2) Non-image mode:
     * 1.182 vs 1.191 (original)
     * Image Mode:
     * 1.046 vs 0.781 (original)
     * 
     * !!!!!!!!!!! HOW TO IMPROVED WITH IMAGE !!!!!!!!!!!!!!!
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void solution6() throws IOException, InterruptedException, ExecutionException {
        Map<String, Stat5> result = new TreeMap<>();

        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            long begin = 0;
            // System.out.println("channel.size = " + channel.size());
            try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                var tasks = new ArrayList<Subtask<Solution5Result>>();
                while (begin < channel.size()) {
                    long size = Math.min(BLOCK_SIZE, channel.size() - begin);

                    if (size == BLOCK_SIZE) {
                        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin + size,
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
                    tasks.add(scope.fork(new Solution5Callable(channel, begin, size, r -> {
                        handle5(r);
                    })));
                    // Next block

                    begin += size;
                }
                scope.join();
                scope.throwIfFailed();
                for (var subtask : tasks) {
                    subtask.get().merge(result);
                }

            }

            executorService.shutdown();

            System.out.println(new TreeMap<>(result));
        }
    }

    /**
     * AS FOLLOWING IS SOLUTION 7 Modified from my solution5 to study from isolgpus
     * 
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void solution7() throws IOException, InterruptedException, ExecutionException {
        Map<String, Stat7> result = new TreeMap<>();
        List<Future<Stat7[]>> futures = new ArrayList<>();

        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            FileChannel channel = file.getChannel();

            long begin = 0;
            // System.out.println("channel.size = " + channel.size());
            while (begin < channel.size()) {

                long size = Math.min(BLOCK_SIZE, channel.size() - begin);

                // if (size == BLOCK_SIZE) {
                // ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, begin + size,
                // Math.min(BUFFER_SIZE, channel.size() - begin - size));
                // while (buffer.hasRemaining()) {
                // byte cur = buffer.get();
                // size++;
                // if (cur == LINE_SEPARATOR) {
                // break;
                // }
                // }
                // }
                long block = begin;
                // long blockSize = size;
                // System.out.println("execute thread from " + begin + " and size = " + size);
                // futures.add(executorService.submit(() -> handle7(channel, block, blockSize, channel.size())));
                futures.add(executorService.submit(() -> handle7(channel, block, size, channel.size())));
                // Next block
                begin += size;

            }
            List<Stat7[]> Stats = new ArrayList<>();
            for (Future<Stat7[]> stat : futures) {
                Stats.add(stat.get());
            }
            executorService.shutdown();
            Map<String, Stat7> collectors = mergeMeasurements(Stats);
            List<MeasurementResult> results = collectors.values().stream().map(MeasurementResult::from).toList();

            System.out.println(
                    "{" + results.stream().map(MeasurementResult::toString).collect(Collectors.joining(", ")) + "}");
        }
    }

    private static class MeasurementResult {
        private final String name;
        private final double mean;
        private final BigDecimal max;
        private final BigDecimal min;

        public MeasurementResult(String name, double mean, BigDecimal max, BigDecimal min) {

            this.name = name;
            this.mean = mean;
            this.max = max;
            this.min = min;
        }

        @Override
        public String toString() {
            return name + "=" + min + "/" + mean + "/" + max;
        }

        public static MeasurementResult from(Stat7 mc) {
            double mean = Math.round((double) mc.total / (double) mc.count) / 10d;
            BigDecimal max = BigDecimal.valueOf(mc.max).divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP);
            BigDecimal min = BigDecimal.valueOf(mc.min).divide(BigDecimal.TEN, 1, RoundingMode.HALF_UP);
            return new MeasurementResult(new String(mc.name), mean, max, min);
        }
    }

    private static Map<String, Stat7> mergeMeasurements(List<Stat7[]> resultsFromAllChunk) {
        // Map<String, MeasurementCollector> mergedResults = new TreeMap<>();
        Map<String, Stat7> mergedResults = new TreeMap<>(Comparator.naturalOrder());

        for (int i = 0; i < HISTOGRAMS_LENGTH; i++) {
            for (Stat7[] resultFromSpecificChunk : resultsFromAllChunk) {
                Stat7 measurementCollectorFromChunk = resultFromSpecificChunk[i];
                while (measurementCollectorFromChunk != null) {
                    Stat7 currentMergedResult = mergedResults.get(new String(measurementCollectorFromChunk.name));
                    if (currentMergedResult == null) {
                        currentMergedResult = new Stat7(measurementCollectorFromChunk.name,
                                measurementCollectorFromChunk.nameSum);
                        mergedResults.put(new String(currentMergedResult.name), currentMergedResult);
                    }
                    currentMergedResult.merge(measurementCollectorFromChunk);
                    measurementCollectorFromChunk = measurementCollectorFromChunk.link;
                }
            }
        }

        return mergedResults;
    }

    public static final int HISTOGRAMS_LENGTH = 1024 * 32;
    public static final int HISTOGRAMS_MASK = HISTOGRAMS_LENGTH - 1;
    public static final byte SEPERATOR = 59;
    public static final byte OFFSET = 48;
    public static final byte NEGATIVE = 45;
    public static final byte DECIMAL_POINT = 46;
    // public static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE - 100; // bit of
    // wiggle room
    public static final byte NEW_LINE = 10;

    /**
     * 
     * @param result
     * @throws IOException
     */
    public static Stat7[] handle7(FileChannel channel, long begin, long size, long maxLen) throws IOException {
        long seekStart = Math.max(begin - 1, 0);
        long length = Math.min(size + 200, maxLen - seekStart);

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, seekStart, length);
        // MappedByteBuffer r = channel.map(FileChannel.MapMode.READ_ONLY, seekStart, length);
        boolean isNegative;
        byte[] valueBuffer = new byte[3];
        int i = 0;
        // seek to the start of the next message
        if (begin != 0) {
            while (buffer.get() != NEW_LINE) {
                i++;
            }
            i++;
        }
        Stat7[] stats = new Stat7[HISTOGRAMS_LENGTH];
        try {
            while (i <= size) {
                int nameSum = 0;
                int hashResult = 0;
                int nameStart;
                byte aChar;
                nameStart = i;
                int nameBufferIndex = 0;
                int valueIndex = 0;

                // optimistically assume that the name is at least 4 bytes
                int firstInt = buffer.getInt();
                nameBufferIndex = 4;
                nameSum = firstInt;
                hashResult = 31 * firstInt;

                while ((aChar = buffer.get()) != SEPERATOR) {
                    nameSum += aChar;
                    // hash as we go, stolen after a discussion with palmr
                    hashResult = 31 * hashResult + aChar;
                    nameBufferIndex++;

                    // oh no we read too much, do it the byte for byte way instead
                    if (aChar == NEW_LINE) {
                        buffer.position(i);
                        nameBufferIndex = 0;
                        nameSum = 0;
                        hashResult = 0;
                    }
                }

                i += nameBufferIndex + 1;

                isNegative = (aChar = buffer.get()) == NEGATIVE;
                valueIndex = readNumber(isNegative, valueBuffer, valueIndex, aChar, buffer);

                int decimalValue = buffer.getShort() >> 8;

                int value = resolveValue(valueIndex, valueBuffer, decimalValue, isNegative);

                Stat7 stat = resolveStat7(stats, hashResult, nameStart,
                        nameBufferIndex, nameSum, buffer);

                stat.feed(value);
                i += valueIndex + (isNegative ? 4 : 3);
            }
        }
        catch (BufferUnderflowException e) {
            // if (i != maxLen - seekStart) {
            // e.printStackTrace();
            // throw new RuntimeException(e);
            // }
        }
        return stats;
    }

    private static int readNumber(boolean isNegative, byte[] valueBuffer, int valueIndex, byte aChar,
                                  MappedByteBuffer r) {
        if (!isNegative) {
            valueBuffer[valueIndex++] = aChar;
        }

        // maybe one or two more
        while ((aChar = r.get()) != DECIMAL_POINT) {
            valueBuffer[valueIndex++] = aChar;
        }
        return valueIndex;
    }

    private static int resolveValue(int valueIndex, byte[] valueBuffer, int decimalValue, boolean isNegative) {
        int value;
        if (valueIndex == 1) {
            value = ((valueBuffer[0] - OFFSET) * 10) + (decimalValue - OFFSET);
        }
        else {// it's 2 digits
            value = ((valueBuffer[0] - OFFSET) * 100) + ((valueBuffer[1] - OFFSET) * 10) + (decimalValue - OFFSET);
        }

        if (isNegative) {
            value = Math.negateExact(value);
        }
        return value;
    }

    static class Stat7 {
        private byte[] name;

        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long total;
        private int count;

        // introduce from isolgpus
        private int nameSum;
        public Stat7 link;

        // remove compare to Stat5
        // private final int hash;

        Stat7(byte[] name, int nameSum) {
            this.name = name;
            this.nameSum = nameSum;
        }

        void feed(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            total += value;
            count++;
        }

        Stat7 merge(Stat7 that) {
            max = Math.max(this.max, that.max);
            min = Math.min(this.min, that.min);
            total += that.total;
            count += that.count;
            return this;
        }
    }

    private static boolean nameEquals(byte[] existingName, int existingNameSum, int incomingNameSum,
                                      int nameBufferIndex) {

        if (existingName.length != nameBufferIndex) {
            return false;
        }

        return incomingNameSum == existingNameSum;
    }

    private static Stat7 resolveStat7(Stat7[] stats, int hash, int nameStart,
                                      int nameBufferLength, int nameSum, MappedByteBuffer r) {
        Stat7 stat = stats[hash & HISTOGRAMS_MASK];
        if (stat == null) {
            byte[] nameBuffer = new byte[nameBufferLength];
            r.get(nameStart, nameBuffer, 0, nameBufferLength);

            stat = new Stat7(nameBuffer, nameSum);
            stats[hash & HISTOGRAMS_MASK] = stat;
        }
        else {
            // collision unhappy path, try to avoid
            while (!nameEquals(stat.name, stat.nameSum, nameSum, nameBufferLength)) {
                if (stat.link == null) {
                    byte[] nameBuffer = new byte[nameBufferLength];
                    r.get(nameStart, nameBuffer, 0, nameBufferLength);
                    stat.link = new Stat7(nameBuffer, nameSum);
                    stat = stat.link;
                    break;
                }
                else {
                    stat = stat.link;
                }
            }

        }
        return stat;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        // Long begin = System.currentTimeMillis();
        solution7();
        // System.out.println("Execute : " + (System.currentTimeMillis() - begin));

    }

    private static final Unsafe UNSAFE = unsafe();

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
