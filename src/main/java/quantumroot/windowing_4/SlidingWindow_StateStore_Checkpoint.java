package quantumroot.windowing_4;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.CheckpointConfig;


import java.time.*;
import java.time.Duration;
import java.time.format.DateTimeFormatter;

// CORRECT IMPORTS FOR RocksDB
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

/**
 * Use data generator DataGenerator_3 to generate the data for this program
 */
public class SlidingWindow_StateStore_Checkpoint {

    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("output")) {
            throw new Exception("No output path provided. Use --output <outputPath>");
        }

        if (!params.has("checkpoint")) {
            throw new Exception("No output path provided. Use --checkpoint <file:///tmp/flink-checkpoints-sliding>");
        }

        // ******** CHECKPOINTING + Hashmap statestore DB ********

        // Enable checkpointing every 10 seconds with EXACTLY_ONCE semantics
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        /**
         *  Enable checkpointing every 10 seconds
         *  Guarantees EXACTLY_ONCE processing (no data loss, no duplicates)
         *  If the job crashes, Flink restarts from the last checkpoint
         * Data processed between checkpoint and crash is replayed from source
         * No data is lost or duplicated
          */
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // Checkpoint configuration
        /**
         * Prevents checkpoint overhead from slowing down the job
         * Without this: checkpoint might start before previous one finishes → cascading delays
         * With this: checkpoints don't overlap
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        /**
         * What it does:
         * If a checkpoint takes longer than 60 seconds (60000 ms), abort it
         *
         * Why it matters:
         * Prevents slow checkpoints from blocking the job indefinitely
         * If a checkpoint hangs, the job continues without that checkpoint
         * Fail-fast approach: don't wait forever, move on
         */
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        /**
         * What it does:
         * Allows up to 3 consecutive checkpoint failures before aborting the entire job
         *
         * Why it matters:
         * Checkpoints can fail due to temporary issues (disk full, network hiccup)
         * If 1 fails, we try the next one
         * If 3 in a row fail → indicates a real problem → kill the job
         */
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        /**
         * What it does:
         * When you manually stop the job, keep the checkpoint files on disk
         *
         * Why it matters:
         *
         * Allows job to resume from where it left off
         * Without this: checkpoint deleted when job stops → must restart from beginning
         * With this: you can pause and resume jobs seamlessly
         */
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Set state backend
        /**
         * When to Use RocksDB?
         * ✅ Use RocksDB if:
         * Large state: > 100GB of aggregations/joins
         * Many keys: Millions of unique keys (sessions, customer IDs)
         * Long windows: Processing large historical batches
         * Cost matters: Memory is expensive; disk is cheap
         *
         * ✅ Use HashMapStateBackend if:
         * Small state: < 10GB
         * Simple aggregations: Basic sums, counts
         * Low latency critical: Need fastest possible
         * Short windows: Real-time windows (seconds/minutes)
         */
        env.setStateBackend(new HashMapStateBackend());

        // Set checkpoint storage path
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(params.get("checkpoint")));

        /**
         * T=0s:   Job starts, windows opening
         *         State: Store-1=0, Store-2=0, Store-3=0
         *
         * T=4s:   Data arrives
         *         State: Store-1=150, Store-2=200, Store-3=180
         *
         * T=10s:  ✓ CHECKPOINT #1 saved to disk
         *         Snapshot: Store-1=1000, Store-2=2500, Store-3=1800
         *
         * T=15s:  More data processed
         *         State: Store-1=1150, Store-2=2700, Store-3=1950
         *
         * T=20s:  ✓ CHECKPOINT #2 saved to disk
         *
         * T=22s:  💥 JOB CRASHES!
         *
         * T=23s:  Flink restarts from CHECKPOINT #2
         *         Restores: Store-1=1150, Store-2=2700, Store-3=1950
         *         Resumes from T=20s (no data loss!)
         */

        // *****************************************



        WatermarkStrategy<Tuple3<Long, String, Integer>> ws =
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.f0);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple3<Long, String, Integer>> parsed =
                data.map((MapFunction<String, Tuple3<Long, String, Integer>>) line -> {
                            String[] parts = line.split(",");
                            return Tuple3.of(
                                    System.currentTimeMillis(),  // Use current time as timestamp
                                    parts[1],                     // sensor/store name
                                    Integer.parseInt(parts[2])    // temperature/value
                            );
                        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT))
                        .assignTimestampsAndWatermarks(ws);

        DataStream<String> windowedOutput = parsed
                .keyBy(t -> t.f1)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new SumAggregate(), new WindowResult());

        windowedOutput.print();

        windowedOutput.addSink(
                StreamingFileSink.forRowFormat(
                                new Path(params.get("output")),
                                new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build()
        );

        env.execute("Total Sales - Sliding Window with RocksDB Checkpoint");
    }

    public static class SumAggregate
            implements AggregateFunction<Tuple3<Long, String, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<Long, String, Integer> value, Integer acc) {
            return acc + value.f2;
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class WindowResult
            extends ProcessWindowFunction<Integer, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Integer> values, Collector<String> out) {
            Integer sum = values.iterator().next();
            long ws = context.window().getStart();
            long we = context.window().getEnd();
            out.collect(
                    String.format("Store: %s | Total Sales: %d | Window: [%s → %s]",
                            key, sum,
                            Instant.ofEpochMilli(ws).atZone(ZoneId.systemDefault()).format(fmt),
                            Instant.ofEpochMilli(we).atZone(ZoneId.systemDefault()).format(fmt))
            );
        }
    }
}
