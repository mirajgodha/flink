package quantumroot.joins_2;

// Import necessary Flink classes

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

/**
 * This program demonstrates an Inner Join operation in Apache Flink.
 * It joins_2 two datasets: one containing person information (id, name) and
 * another containing location information (id, state).
 * The join is performed on the common 'id' field.
 */
@SuppressWarnings("serial")
public class JoinHints_5 {
    
    /**
     * Main method - entry point of the Flink program
     * @param args Command line arguments (--input1 <path> --input2 <path> --output <path>)
     * @throws Exception if there's an error during execution
     */
    public static void main(String[] args) throws Exception {
        // Initialize Flink's batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        // Parse command line parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        // Make parameters available in Flink's web interface
        env.getConfig().setGlobalJobParameters(params);
        
        // Validate input parameters
        if (!params.has("input1")) {
            throw new Exception("No input path provided. Provide input as --input1 <inputPath>");
        }
        if (!params.has("input2")) {
            throw new Exception("No input path provided. Provide input as --input2 <inputPath>");
        }
        if (!params.has("output")) {
            throw new Exception("No output path provided. Provide output as --output <outputPath>");
        }

        /**
         * Read and parse the first input file (person data)
         * Expected format: id,name (e.g., "1,John")
         * Output: DataSet of (Integer id, String name)
         */
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) {
                        // Split each line by comma
                        String[] words = value.split(",");
                        // Return a tuple of (id, name)
                        return new Tuple2<Integer, String>(
                            Integer.parseInt(words[0]),  // Convert id to Integer
                            words[1]                     // Name as String
                        );
                    }
                });

        /**
         * Read and parse the second input file (location data)
         * Expected format: id,state (e.g., "1,DC")
         * Output: DataSet of (Integer id, String state)
         */
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) {
                        // Split each line by comma
                        String[] words = value.split(",");
                        // Return a tuple of (id, state)
                        return new Tuple2<Integer, String>(
                            Integer.parseInt(words[0]),  // Convert id to Integer
                            words[1]                     // State as String
                        );
                    }
                });

        /**
         * Perform an inner join between personSet and locationSet
         * Join condition: person.id == location.id
         * Output: DataSet of (Integer id, String name, String state)
         */
        DataSet<Tuple3<Integer, String, String>> joined = personSet
                // Join the two datasets
                //Total 6 hints are defined..
                //1. BROADCAST_HASH_FIRST - broadcast the first dataset which is small
                //2. BROADCAST_HASH_SECOND - broadcast the second dataset which is small
                //3. REPARTITION_HASH_FIRST - repartition the first dataset with hash, both are large inputs but first one is small than second
                //4. REPARTITION_HASH_SECOND - repartition the second dataset with hash, both are large inputs but second one is small than first
                //5. REPARTITION_SORT_MERGE - repartition both datasets with sort merge
                //6. OPTIMIZER_CHOOSES - let optimizer choose the best hint
                .join(locationSet, JoinHint.BROADCAST_HASH_FIRST)
                // Specify join keys (field 0 from first dataset = id)
                .where(0)
                // Must match field 0 from second dataset (id)
                .equalTo(0)
                // Define how to combine the matching records
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, 
                                     Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(
                            Tuple2<Integer, String> person,  // Record from personSet
                            Tuple2<Integer, String> location // Matching record from locationSet
                    ) {
                        // Create a new tuple with (id, name, state)
                        return new Tuple3<Integer, String, String>(
                            person.f0,    // id from person
                            person.f1,    // name from person
                            location.f1   // state from location
                        );
                    }
                });

        // Write the result to the specified output path as CSV
        // Format: each line contains "id name state" separated by spaces
        joined.writeAsCsv(
            params.get("output"),  // output directory
            "\n",                 // row delimiter (newline)
            " ",                  // field delimiter (space)
            FileSystem.WriteMode.OVERWRITE  // overwrite if output exists
        );

        // Execute the Flink job with a descriptive name
        env.execute("Inner Join Example");
    }
}
