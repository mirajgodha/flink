/**
 * This module demonstrates a Flink streaming application that performs windowed aggregation
 * on a stream of random events and writes the results to files at regular intervals.
 * The application showcases:
 * - Custom event generation
 * - Keyed windowed aggregation
 * - Time-based windowing_4 (tumbling windows)
 * - File sink with rolling policy
 */
package quantumroot.dataStream_3;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * Main class that sets up and executes a Flink streaming job.
 * The job generates random events, aggregates them in 10-second windows by key,
 * and writes the results to rolling files.
 */
public class StreamAggregationToFile_3 {

    /**
     * Represents a simple event with a key and a value.
     * The key is used for grouping/aggregation, and the value is the numeric data to be aggregated.
     */
    public static class Event {
        public String key;
        public int value;

        public Event() {}
        public Event(String key, int value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "," + value;
        }
    }

    /**
     * Custom source function that generates random events with keys A, B, or C
     * and random integer values between 0-99.
     * Emits one event every 200ms until cancelled.
     */
    public static class EventSource implements SourceFunction<Event> {
        private volatile boolean running = true;
        private final String[] keys = {"A", "B", "C"};
        private final Random rand = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // Continuously generate random events until cancelled
            while (running) {
                String key = keys[rand.nextInt(keys.length)];
                int value = rand.nextInt(100);
                ctx.collect(new Event(key, value));
                Thread.sleep(200);  // emit every 200 ms
            }
        }

        @Override
        public void cancel() {
            // Signal the source to stop emitting events
            running = false;
        }
    }

    /**
     * Main method that configures and executes the Flink streaming job.
     * The job flow is: Source → KeyBy → Window → Aggregate → Sink
     * 
     * @param args Command line arguments (not used)
     * @throws Exception if job execution fails
     */
    public static void main(String[] args) throws Exception {

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // trigger checkpoints so sink commits periodically

        // Create a source stream of random events
        DataStream<Event> events = env.addSource(new EventSource())
                .name("Event Source");

        // Process the stream with keyed windowed aggregation
        SingleOutputStreamOperator<String> aggregated = events
                // keyby - Group all records with the same key together — and let
                // each group be processed independently and in parallel
                .keyBy(event -> event.key)
                // Use tumbling windows of 10 seconds based on processing time
                // This creates non-overlapping windows that trigger every 10 seconds
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // Apply window function to aggregate events within each window
                .apply(new WindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Event> input, Collector<String> out) {
                        // Calculate sum and count of values for this key in the current window
                        int sum = 0, count = 0;
                        for (Event e : input) {
                            sum += e.value;
                            count++;
                        }
                        // Convert window start to system local time (IST)
                        LocalDateTime localStart = LocalDateTime.ofInstant(
                                java.time.Instant.ofEpochMilli(window.getStart()),
                                ZoneId.systemDefault());

                        String formattedTime = localStart.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                        out.collect(String.format(
                                "WindowStart=%s, Key=%s, Count=%d, Sum=%d",
                                formattedTime, key, count, sum));
                    }
                });

        // Configure the file sink with a rolling policy
        // - Rolls over every 10 seconds (withRolloverInterval)
        // - Rolls over if no new data arrives for 5 seconds (withInactivityInterval)
        // - Rolls over if file size exceeds 128MB (withMaxPartSize)
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy.builder()
                                .withRolloverInterval(10_000L)   // 10 s rollover
                                .withInactivityInterval(5_000L)
                                .withMaxPartSize(1024 * 1024 * 128L)
                                .build())
                .build();

        // Add the sink to the data stream
        aggregated.addSink(sink)
                .name("File Sink");

        // Execute the Flink job with a descriptive name
        env.execute("Streaming Aggregation → File Every 10s");
    }
}
