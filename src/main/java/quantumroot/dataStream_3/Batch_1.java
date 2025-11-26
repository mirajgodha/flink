package quantumroot.dataStream_3;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;

public class Batch_1 {
    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a bounded collection source (acts like a batch input)
        DataStream<String> batchData = env.fromCollection(Arrays.asList("alpha", "beta", "gamma"));

        // Transform: map + keyBy + sum
        //One counter tracking total length of all even-length words
        //One counter tracking total length of all odd-length words
        DataStream<Tuple2<Integer, Integer>> keyedCounts = batchData
                //Example: "apple" → length 5 → (1, 5)
                //"ball" → length 4 → (0, 4)
                .map(word -> Tuple2.of(word.length() % 2, word.length()))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                //key 0 → even length words
                //key 1 → odd length words
                .keyBy(value -> value.f0)
                //Aggregates the second field (f1 = length) for each key.
                .sum(1);


        // Output to log folder
        keyedCounts.print();

        // Trigger execution
        env.execute("Unified Batch via dataStream_3 API");
    }
}
