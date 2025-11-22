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
        DataStream<Tuple2<Integer, Integer>> keyedCounts = batchData
                .map(word -> Tuple2.of(word.length() % 2, word.length()))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);


        // Output to log folder
        keyedCounts.print();

        // Trigger execution
        env.execute("Unified Batch via dataStream_3 API");
    }
}
