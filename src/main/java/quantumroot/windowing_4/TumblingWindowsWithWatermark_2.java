package quantumroot.windowing_4;
import java.io.OutputStream;
import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.text.SimpleDateFormat;
import java.util.Date;
//TumblingEvent
// Use DataGenerator_2 to generate the data for this program
public class TumblingWindowsWithWatermark_2 {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);

    if (!params.has("output")) {
      throw new Exception("No output path provided. Provide output as --output <output path profit_per_month.txt>");
    }

      /**
       *
       * Flink assumes event-time never goes backward
       * Incoming events have timestamps that are strictly increasing
       * Zero out-of-orderness allowed
       *Watermarks = “latest seen timestamp”
       * If an older event arrives → it is considered late.
       *
       * withTimestampAssigner(...) - We tell Flink which field contains the event timestamp.
       * (event, timestamp) -> event.f0
       * For each incoming tuple (ts, value) - Use ts (event.f0) as the event-time timestamp
       */
    WatermarkStrategy < Tuple2 < Long, String >> ws =
      WatermarkStrategy
      . < Tuple2 < Long, String >> forMonotonousTimestamps()
      .withTimestampAssigner((event, timestamp) -> event.f0);

    DataStream < String > data = env.socketTextStream("localhost", 9090);

    // Socket Text Stream, Split the input by comma
    DataStream < Tuple2 < Long, String >> sum = data.map(new MapFunction < String, Tuple2 < Long, String >> () {
        public Tuple2 < Long, String > map(String s) {
          String[] words = s.split(",");
          return new Tuple2 < Long, String > (Long.parseLong(words[0]), words[1]);
        }
      })
      .assignTimestampsAndWatermarks(ws)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce(new ReduceFunction < Tuple2 < Long, String >> () {
        public Tuple2 < Long, String > reduce(Tuple2 < Long, String > t1, Tuple2 < Long, String > t2) {
          int num1 = Integer.parseInt(t1.f1);
          int num2 = Integer.parseInt(t2.f1);
          int sum = num1 + num2;
          Timestamp t = new Timestamp(System.currentTimeMillis());
          return new Tuple2 < Long, String > (t.getTime(), "" + sum);
        }
      });
    sum.addSink(StreamingFileSink
      .forRowFormat(new Path(params.get("output")),
        //Converting to human readable time before printing
        (Tuple2<Long, String> value, OutputStream out) -> {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          String formattedTime = sdf.format(new Date(value.f0));
          String output = String.format("%s,%s\n", formattedTime, value.f1);
          out.write(output.getBytes("UTF-8"));
        })
      .withRollingPolicy(DefaultRollingPolicy.builder().build())
      .build());

    // execute program
    env.execute("Window");
  }
}
