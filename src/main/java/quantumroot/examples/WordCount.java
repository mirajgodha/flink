// Package declaration for the WordCount examples
package quantumroot.examples;

// Import necessary Flink classes
import org.apache.flink.api.common.functions.MapFunction;  // For mapping operations
import org.apache.flink.api.java.DataSet;  // Main abstraction for data in batch processing
import org.apache.flink.api.java.ExecutionEnvironment;  // Entry point for Flink batch execution
import org.apache.flink.api.java.tuple.Tuple2;  // A tuple with 2 fields (word, count)
import org.apache.flink.api.java.utils.ParameterTool;  // For handling command line parameters
import org.apache.flink.core.fs.FileSystem;  // For file system operations


/**
 * A simple WordCount program that counts the occurrences of words in a text file.
 * This demonstrates the basic structure of a Flink batch processing job.
 */
public class WordCount {
    
    /**
     * Main method - entry point of the Flink program
     * @param args Command line arguments (expects --input <inputPath> --output <outputPath>)
     * @throws Exception if there's an error during job execution
     */
    public static void main(String[] args) throws Exception {
        // Set up the execution environment for Flink
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        // Parse command line parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        
        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("input")) {
            throw new Exception("No input path provided. Provide input as --input <inputPath>");
        }

        // Read the input text file into a DataSet of Strings (each string is a line)
        DataSet<String> text = env.readTextFile(params.get("input"));
        
        // Transform the DataSet of lines into a DataSet of (word, 1) tuples
        DataSet<Tuple2<String, Integer>> tokenized = text.map(new Tokenizer());
        
        // Group by the first field (word) and sum the second field (count)
        // groupBy(0) groups by the first field of the tuple
        // sum(1) sums the second field (index 1) of the tuple
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        
        // If output path is provided, write the result to the specified location
        if (params.has("output")) {
            // Write the result as CSV with newline as record delimiter and space as field delimiter
            counts.writeAsCsv(
                params.get("output"),  // output path
                "\n",                 // record delimiter (newline)
                " ",                  // field delimiter (space)
                FileSystem.WriteMode.OVERWRITE  // overwrite if file exists
            );
            
            // Execute the Flink job with a name for the web interface
            env.execute("WordCount Example");
        }else {
            System.out.println("No output path provided");
            throw new Exception("No output path provided. Provide output as --output <outputPath>");
        }
    }

    /**
     * Tokenizer class that implements MapFunction to convert each line of text into (word, 1) tuples.
     * This is a user-defined function that processes each input record.
     */
    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        
        /**
         * Maps a line of text to a (word, 1) tuple.
         * @param value A line of text from the input file
         * @return A tuple containing (word, 1) for each word in the line
         */
        @Override
        public Tuple2<String, Integer> map(String value) {
            // For each word in the line, create a tuple (word, 1)
            // This is the "map" phase of MapReduce
            return new Tuple2<>(value.trim(), 1);
        }
    }
}
