package quantumroot.windowing_4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindows_SalesSum_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("output")) {
            throw new Exception("No output path provided. Use --output <outputPath>");
        }

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // Parse input line to Tuple3<Month, Category, Sales>
        DataStream<Tuple3<String, String, Integer>> mapped = data.map(new Splitter());

        // Sum sales grouped by month and category in tumbling windows of 5 seconds
        DataStream<Tuple3<String, String, Integer>> aggregated = mapped
                .keyBy(t -> t.f0 + "_" + t.f1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceSales());

        aggregated.addSink(StreamingFileSink
                .forRowFormat(new Path(params.get("output")),
                        new SimpleStringEncoder< Tuple3 <String,String,Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        env.execute("Sum of Sales by Month and Category");
    }

    // Mapper splits line into tuple (month, category, sales)
    public static class Splitter implements MapFunction<String, Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> map(String value) throws Exception {
            // Format: 01-01-2023,June,Category3,5
            String[] parts = value.split(",");
            String month = parts[1];
            String category = parts[2];
            int sales = Integer.parseInt(parts[3]);
            return Tuple3.of(month, category, sales);
        }
    }

    // Reducer sums the sales (f2) for same month_category_key
    public static class ReduceSales implements ReduceFunction<Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> v1,
                                                      Tuple3<String, String, Integer> v2) throws Exception {
            return Tuple3.of(v1.f0, v1.f1, v1.f2 + v2.f2);
        }
    }
}