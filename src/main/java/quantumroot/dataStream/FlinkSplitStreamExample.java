package quantumroot.dataStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FlinkSplitStreamExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> numbers = env.fromElements(1,2,3,4,5,6,7,8,9,10);

        // Define side output tags
        final OutputTag<Integer> evenTag = new OutputTag<Integer>("even"){};
        final OutputTag<Integer> oddTag = new OutputTag<Integer>("odd"){};

        // Use SingleOutputStreamOperator to store the result of process()
        SingleOutputStreamOperator<Integer> processedStream = numbers.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                if (value % 2 == 0) {
                    ctx.output(evenTag, value);
                } else {
                    ctx.output(oddTag, value);
                }
            }
        });

        // Get side outputs
        DataStream<Integer> evenStream = processedStream.getSideOutput(evenTag);
        DataStream<Integer> oddStream = processedStream.getSideOutput(oddTag);

        // Print the results in local.out logs file, can see on flink web ui
        evenStream.print("Even Numbers");
        oddStream.print("Odd Numbers");
        env.execute("Side Output Example");
    }
}
