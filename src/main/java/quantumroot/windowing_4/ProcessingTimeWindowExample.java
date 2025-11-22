package quantumroot.windowing_4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ProcessingTimeWindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.fromElements(
                "apple", "banana", "cat", "dog", "elephant", "fish", "goat"
        );

        // Tumbling processing time window of 5 seconds
        input.map((MapFunction<String, String>) value -> {
                    System.out.println("Received: " + value + " at processing time.");
                    return value;
                }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));
//                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction<String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
//                    @Override
//                    public void process(Context context, Iterable<String> elements, Collector<String> out) {
//                        int count = 0;
//                        for (String elem : elements) count++;
//                        System.out.println("Processing Time Window Computed Count: " + count);
//                        out.collect("Count: " + count);
//                    }
//                }
//                )
//                .print();

        env.execute("Processing Time Window Example");
    }
}

