package quantumroot.dataStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkIterateExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create initial stream with one element for looping
        DataStream<Integer> initialStream = env.fromElements(0);

        // Create an iterative stream
        IterativeStream<Integer> iteration = initialStream.iterate();

        // Define feedback: add 1 to each element
        DataStream<Integer> feedback = iteration.map(x -> {
            int incremented = x + 1;
            System.out.println("Processing value: " + x + " -> incremented to: " + incremented);
            return incremented;
        });

        // Define where to stop the loop (e.g., x >= 10)
        DataStream<Integer> main = feedback.filter(x -> x >= 10); // create output stream for the final
        DataStream<Integer> feedbackStream = feedback.filter(x -> x < 10); // feedback for next iteration

        // Feed the feedback to the loop
        iteration.closeWith(feedbackStream);

        // Output the final results
        main.print("Final Results (x >= 10)");
        env.execute("Flink Iterate Operator");
    }
}

