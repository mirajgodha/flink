package quantumroot.tableAPI_6;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;


public class TableAPI {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Parse command line parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("input")) {
            throw new Exception("No input path provided. Provide input as --input <inputPath to data/transactions.csv file>");
        }

        // Create table from CSV file
        tEnv.executeSql(
                "CREATE TABLE transactions ("
                        + "  txn_id STRING, "
                        + "  amount INT, "
                        + "  currency STRING, "
                        + "  source STRING, "
                        + "  destination STRING, "
                        + "  ts BIGINT "
                        + ") WITH ("
                        + "  'connector' = 'filesystem', "
                        + "  'path' = '" + params.get("input") + "', "
                        + "  'format' = 'csv'"
                        + ")"
        );
        // Select only high-value transactions > 50000
        Table highValue =
                tEnv.from("transactions")
                        .filter($("amount").isGreater(50000))
                        .select($("txn_id"), $("amount"), $("currency"));

        // Group by currency and find average txn amount
        Table avgPerCurrency =
                tEnv.from("transactions")
                        .groupBy($("currency"))
                        .select(
                                $("currency"),
                                $("amount").avg().as("avg_amount")
                        );

        // Print both results
        /**
         * Output you will see
         * High Value Transactions:
         *  +---------+--------+----------+
         *  | txn_id  | amount | currency |
         *  +---------+--------+----------+
         *  | TXN002  | 78000  | INR      |
         *  | TXN004  | 91000  | EUR      |
         *  +---------+--------+----------+
         *  */
         highValue.execute().print();

         /**
         * Avg Amount by Currency:
         *  +----------+------------+
         *  | currency | avg_amount |
         *  +----------+------------+
         *  | USD      | 21100      |
         *  | INR      | 78000      |
         *  | EUR      | 91000      |
         *  +----------+------------+
         */
        avgPerCurrency.execute().print();
    }
}

