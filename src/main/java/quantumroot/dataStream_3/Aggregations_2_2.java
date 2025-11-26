package quantumroot.dataStream_3;

//package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregations_2_2 {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parse command line parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("input")) {
            throw new Exception("No input path provided. Provide input as --input <inputPath to avg1 file>");
        }

        if (!params.has("output")) {
            throw new Exception("No input path provided. Provide output as --output <output path agg>");
        }


        DataStream < String > data = env.readTextFile(params.get("input"));

        // month, category,product, profit,
        DataStream < Tuple4 < String, String, String, Integer >> mapped = data.map(new Splitter());
        // tuple  [June,Category5,Bat,12]
        //       [June,Category4,Perfume,10]
        mapped.keyBy(t -> t.f0).sum(3).writeAsText(params.get("output") + "_sum");

        /**
         * Returns only the minimum value of that field
         *
         * BUT keeps the other fields from the first record where the min was seen, not from the actual smallest record later
         *
         * It's a partial update, not the full row
         *
         * 📌 It updates only the min field, not the entire tuple.
         */
        mapped.keyBy(t -> t.f0).min(3).writeAsText(params.get("output") + "_min");

        /**
         * Returns the entire record (tuple) that contains the minimum value
         *
         * If a later record has the same min value, you can choose:
         *
         * minBy(field, true) → take first
         *
         * minBy(field, false) → take last
         *
         * 📌 It returns the full tuple from the event with the smallest field value.
         */
        mapped.keyBy(t -> t.f0).minBy(3).writeAsText(params.get("output") + "_minBy");

        mapped.keyBy(t -> t.f0).max(3).writeAsText(params.get("output") +"_max");

        mapped.keyBy(t -> t.f0).maxBy(3).writeAsText(params.get("output") + "maxBy");
        // execute program
        env.execute("Aggregation");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Splits the input string into a tuple of (month, category, product, profit)
     */
    public static class Splitter implements MapFunction < String, Tuple4 < String, String, String, Integer >> {
        public Tuple4 < String,
                String,
                String,
                Integer > map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple4 < String, String, String, Integer > (words[1], words[2], words[3], Integer.parseInt(words[4]));
        } //    June    Category5      Bat               12
    }
}