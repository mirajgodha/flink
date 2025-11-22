package quantumroot.dataStream_3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Demonstrates Flink's iterative stream processing with a feedback loop
// This example shows how to create a loop that processes data multiple times
// until a certain condition is met (in this case, until x >= 10)
public class FlinkIterateExample_5 {
    // Main method to set up and execute the Flink job
    public static void main(String[] args) throws Exception {
        // Initialize Flink's streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create initial stream with one element (0) to start the iteration
        // This is the starting point of our iterative process
        DataStream<Integer> initialStream = env.fromElements(0);

        // Create an iterative stream from the initial stream
        // This allows us to create a feedback loop in the data flow
        IterativeStream<Integer> iteration = initialStream.iterate();

        // Define the transformation that will be applied in each iteration
        // In this case, we simply increment the value by 1 each time
        DataStream<Integer> feedback = iteration.map(x -> {
            // Increment the current value by 1
            int incremented = x + 1;
            // Print the current and new value for demonstration
            System.out.println("Processing value: " + x + " -> incremented to: " + incremented);
            return incremented;
        });

        // Split the stream into two parts:
        // 1. Values that meet the termination condition (x >= 10) - these exit the loop
        // 2. Values that need further processing (x < 10) - these go back for another iteration
        DataStream<Integer> main = feedback.filter(x -> x >= 10); // Output stream for final results
        DataStream<Integer> feedbackStream = feedback.filter(x -> x < 10); // Feedback for next iteration

        // Close the iteration by connecting the feedback stream back to the iteration head
        // This creates the feedback loop where values < 10 are processed again
        iteration.closeWith(feedbackStream);

        // Print the final results (values that reached the termination condition)
        // These are the values that made it out of the iteration loop
        main.print("Final Results (x >= 10)");

        // Execute the Flink job with a descriptive name
        env.execute("Flink Iterate Operator");
    }
}
