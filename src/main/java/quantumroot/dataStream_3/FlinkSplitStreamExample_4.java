package quantumroot.dataStream_3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// Demonstrates Flink's side output feature to split a single data stream
// into multiple output streams based on specific conditions.
// This example splits a stream of numbers into even and odd numbers.
public class FlinkSplitStreamExample_4 {

    // Main method to set up and execute the Flink job
    public static void main(String[] args) throws Exception {
        // Initialize Flink's streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a data stream from a fixed set of numbers (1 to 10)
        // In a real application, this could be from a source like Kafka, Kinesis, etc.
        DataStream<Integer> numbers = env.fromElements(1,2,3,4,5,6,7,8,9,10);

        // Define side output tags for even and odd numbers
        // These tags are used to identify and collect the side outputs
        final OutputTag<Integer> evenTag = new OutputTag<Integer>("even"){};  // Tag for even numbers
        final OutputTag<Integer> oddTag = new OutputTag<Integer>("odd"){};    // Tag for odd numbers

        // Process the input stream and split it into side outputs based on even/odd condition
        // SingleOutputStreamOperator is used to collect the main output (though we don't use it here)
        SingleOutputStreamOperator<Integer> processedStream = numbers.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                // Route each number to the appropriate side output based on whether it's even or odd
                if (value % 2 == 0) {
                    // Send even numbers to the 'even' side output
                    ctx.output(evenTag, value);
                } else {
                    // Send odd numbers to the 'odd' side output
                    ctx.output(oddTag, value);
                }
                // Note: We're not using the main output collector (out) in this example
            }
        });

        // Extract the side output streams using their respective tags
        // These are now separate streams that can be processed independently
        DataStream<Integer> evenStream = processedStream.getSideOutput(evenTag);
        DataStream<Integer> oddStream = processedStream.getSideOutput(oddTag);

        // Print the results to standard output
        // In a real application, these could be written to different sinks or processed further
        evenStream.print("Even Numbers");  // Print even numbers with a prefix
        oddStream.print("Odd Numbers");    // Print odd numbers with a prefix

        // Execute the Flink job with a descriptive name
        env.execute("Flink Split Stream Example");
    }
}
