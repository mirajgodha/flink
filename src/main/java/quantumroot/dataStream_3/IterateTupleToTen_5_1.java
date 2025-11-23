package quantumroot.dataStream_3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateTupleToTen_5_1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("output")) {
            throw new Exception("No output path provided. Provide output as --output <output path iterate.txt>");
        }

        // Produce a stream of tuples (number, iterations it needed to reach 10)
        // (0,0) (1,0) (2,0) (3,0) (4,0) (5,0)
        DataStream < Tuple2 < Long, Integer >> data =
                env.fromSequence(0, 5).map(new MapFunction < Long, Tuple2 < Long, Integer >> () {
                    public Tuple2 < Long, Integer > map(Long value) {
                        return new Tuple2 < Long, Integer > (value, 0);
                    }
                });

        // prepare stream for iteration
        // ( 0,0   1,0  2,0  3,0   4,0  5,0 )
        // max iteration time = 5000ms , wait for 5 seconds to get the next tuple or stop if no tuple is received
        IterativeStream < Tuple2 < Long, Integer >> iteration = data.iterate(5000);

        // define iteration
        //   plusone  (number, iterations it needed to reach 10)  1,1   2,1  3,1   4,1   5,1   6,1
        DataStream < Tuple2 < Long, Integer >> plusOne =
                iteration.map(new MapFunction < Tuple2 < Long, Integer > , Tuple2 < Long, Integer >> () {
                    public Tuple2 < Long, Integer > map(Tuple2 < Long, Integer > value) {
                        if (value.f0 == 10)
                            return value;
                        else
                            return new Tuple2 < Long, Integer > (value.f0 + 1, value.f1 + 1);
                    }
                });

        // part of stream to be used in next iteration (number not equal to 10)
        DataStream < Tuple2 < Long, Integer >> notEqualtoten =
                plusOne.filter(new FilterFunction < Tuple2 < Long, Integer >> () {
                    public boolean filter(Tuple2 < Long, Integer > value) {
                        if (value.f0 == 10)
                            return false;
                        else
                            return true;
                    }
                });

        // feed data back to next iteration
        iteration.closeWith(notEqualtoten);

        // data not feedback to iteration
        DataStream < Tuple2 < Long, Integer >> equaltoten =
                plusOne.filter(new FilterFunction < Tuple2 < Long, Integer >> () {
                    public boolean filter(Tuple2 < Long, Integer > value) {
                        if (value.f0 == 10) return true;
                        else return false;
                    }
                });

        equaltoten.writeAsText(params.get("output") + "_equaltoten");

        env.execute("Iteration Demo");
    }
}
