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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.time.Duration;

public class SlidingWindow_SumSales_3 {

    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("output")) {
            throw new Exception("No output path provided. Use --output <outputPath>");
        }

        // Watermarks
        WatermarkStrategy<Tuple3<Long, String, Integer>> ws =
                WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.f0);

        // Input: timestamp, storeId, saleAmount
        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple3<Long, String, Integer>> parsed =
                data.map((MapFunction<String, Tuple3<Long, String, Integer>>) line -> {
                    String[] parts = line.split(",");
                    return Tuple3.of(
                            Long.parseLong(parts[0]),   // timestamp
                            parts[1],                   // store ID
                            Integer.parseInt(parts[2])  // sale amount
                    );
                }) .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT))
                        .assignTimestampsAndWatermarks(ws);

        // ******** WINDOW SUM ********
        DataStream<String> windowedOutput = parsed
                .keyBy(t -> t.f1)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new SumAggregate(), new WindowResult());

        // Print output
        windowedOutput.print();

        // Write to file
        windowedOutput.addSink(
                StreamingFileSink.forRowFormat(
                                new Path(params.get("output")),
                                new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build()
        );

        env.execute("Total Sales - Sliding Window");
    }

    // --------------- AGGREGATOR (SUM FUNCTION) -------------------
    public static class SumAggregate
            implements AggregateFunction<Tuple3<Long, String, Integer>, Integer, Integer> {

        // This is initial state before the window starts.
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        // Every new event → Flink calls add
        @Override
        public Integer add(Tuple3<Long, String, Integer> value, Integer acc) {
            return acc + value.f2;   // sum of sales
        }

        // Flink calls getResult when the window is closed.
        @Override
        public Integer getResult(Integer acc) {
            return acc; // final sum
        }

        /**
         * How to combine accumulators of the two windows being merged.
         *
         * merge(acc1, acc2) is used only for MERGEABLE WINDOWS, like:
         * Session Windows
         * Dynamic gap windows
         * (internally for certain scalable window operators)
         *
         * It tells Flink how to combine two partial accumulators when windows are merged together.
         * @param a
         * @param b
         * @return
         */
        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    // --------------- PROCESS WINDOW FUNCTION -------------------
    public static class WindowResult
            extends ProcessWindowFunction<Integer, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Integer> values,
                            Collector<String> out) {

            Integer sum = values.iterator().next();

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            String startHuman = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowStart),
                    ZoneId.systemDefault()
            ).format(fmt);

            String endHuman = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(windowEnd),
                    ZoneId.systemDefault()
            ).format(fmt);

            out.collect(String.format(
                    "Store: %s | Total Sales: %d | Window: [%s → %s]",
                    key, sum, startHuman, endHuman
            ));
        }

    }
}
