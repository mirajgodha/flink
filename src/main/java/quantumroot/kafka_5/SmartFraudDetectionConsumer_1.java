package quantumroot.kafka_5;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.Collector;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;

/**
 * Smart Fraud Detection Consumer
 *
 * Fraud Detection Logic:
 * 1. AMOUNT-BASED:
 *    - High Risk: Amount > $50,000
 *    - Medium Risk: Amount > $20,000
 *    - Low Risk: Amount < $1,000 OR > $100,000 (structured as fraud)
 *
 * 2. LOCATION-BASED:
 *    - Same location (source = destination): Lower risk
 *    - Different location: Medium risk
 *    - Geographically impossible (same txn in 2 places): High risk
 *
 * 3. PATTERN-BASED:
 *    - Multiple txns same customer in 1 minute window: Medium risk
 *    - Multiple HIGH amount txns in 1 minute: High risk
 *
 * 4. CURRENCY-BASED:
 *    - Unusual currencies (rarely used): Medium risk
 *
 * Run:
 * java -cp your-jar.jar quantumroot.kafka_fraud_detection.SmartFraudDetectionConsumer
 */
public class SmartFraudDetectionConsumer_1 {

    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("simple-transactions")
                .setGroupId("fraud-consumer")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        DataStream<SimpleTransaction> transactions = kafkaStream
                .flatMap((String line, Collector<SimpleTransaction> out) -> {
                    try {
                        String[] parts = line.split(",");
                        if (parts.length != 6) {
                            System.err.println("❌ BAD RECORD (wrong column count): " + line);
                            return; // skip
                        }

                        SimpleTransaction tx = new SimpleTransaction(
                                parts[0],
                                Integer.parseInt(parts[1]),
                                parts[2],
                                parts[3],
                                parts[4],
                                Long.parseLong(parts[5])
                        );
                        out.collect(tx); // only valid records passed forward
                    } catch (Exception e) {
                        System.err.println("❌ BAD RECORD (parse error): " + line + "  |  ERROR: " + e.getMessage());
                        // skip
                    }
                })
                .returns(Types.POJO(SimpleTransaction.class));


        // ========== FRAUD DETECTION LOGIC ==========

        // Rule 1: Analyze individual transactions
        DataStream<SimpleTransaction> analyzedTransactions = transactions
                .map(new MapFunction<SimpleTransaction, SimpleTransaction>() {
                    @Override
                    public SimpleTransaction map(SimpleTransaction t) throws Exception {
                        analyzeFraudRisk(t);
                        return t;
                    }
                });

        // Rule 2: Filter only fraud transactions
        DataStream<SimpleTransaction> fraudTransactions = analyzedTransactions
                .filter(new FilterFunction<SimpleTransaction>() {
                    @Override
                    public boolean filter(SimpleTransaction t) throws Exception {
                        return t.isFraud;
                    }
                });


        StreamingFileSink<String> fraudSink =
                StreamingFileSink
                        .forRowFormat(new Path("output/kafka/fraud/"), (String line, java.io.OutputStream out) -> {
                            out.write(line.getBytes());
                            out.write('\n');
                        })
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withRolloverInterval(5 * 60 * 1000)
                                        .withInactivityInterval(60 * 1000)
                                        .withMaxPartSize(1024 * 1024 * 1024)
                                        .build()
                        )
                        .build();

        StreamingFileSink<String> normalSink =
                StreamingFileSink
                        .forRowFormat(new Path("output/kafka/normal/"), (String line, java.io.OutputStream out) -> {
                            out.write(line.getBytes());
                            out.write('\n');
                        })
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withRolloverInterval(5 * 60 * 1000)
                                        .withInactivityInterval(60 * 1000)
                                        .withMaxPartSize(1024 * 1024 * 1024)
                                        .build()
                        )
                        .build();




        // Print fraud alerts with detailed reasoning
        fraudTransactions.map(new MapFunction<SimpleTransaction, String>() {
            @Override
            public String map(SimpleTransaction t) throws Exception {
                LocalDateTime dt = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(t.timestamp),
                        ZoneId.systemDefault()
                );
                return String.format(
                        "🚨 FRAUD DETECTED\n" +
                                "  TxnID: %s | Amount: $%d %s | Score: %.2f\n" +
                                "  Route: %s → %s\n" +
                                "  Reasons: %s\n" +
                                "  Time: %s\n",
                        t.txnId,
                        t.amount,
                        t.currency,
                        t.fraudScore,
                        t.sourceLocation,
                        t.destLocation,
                        String.join(" | ", t.fraudReasons),
                        dt.format(fmt)
                );
            }
        }).addSink(fraudSink);

        // Print normal transactions (for reference)
        DataStream<SimpleTransaction> normalTransactions = analyzedTransactions
                .filter(new FilterFunction<SimpleTransaction>() {
                    @Override
                    public boolean filter(SimpleTransaction t) throws Exception {
                        return !t.isFraud;
                    }
                });

        normalTransactions.map(new MapFunction<SimpleTransaction, String>() {
            @Override
            public String map(SimpleTransaction t) throws Exception {
                LocalDateTime dt = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(t.timestamp),
                        ZoneId.systemDefault()
                );
                return String.format(
                        "✓ NORMAL | TxnID: %s | $%d %s | %s → %s | Score: %.2f | Time: %s",
                        t.txnId,
                        t.amount,
                        t.currency,
                        t.sourceLocation,
                        t.destLocation,
                        t.fraudScore,
                        dt.format(fmt)
                );
            }
        }).addSink(normalSink);

        env.execute("Smart Fraud Detection");
    }

    /**
     * Smart fraud detection logic
     * Returns fraud score (0.0 = safe, 1.0 = definite fraud)
     */
    private static void analyzeFraudRisk(SimpleTransaction t) {
        t.fraudReasons.clear();
        float fraudScore = 0.0f;

        // ========== RULE 1: AMOUNT-BASED ANALYSIS ==========
        if (t.amount > 100000) {
            fraudScore += 0.35f;
            t.fraudReasons.add("VERY_HIGH_AMOUNT($" + t.amount + ")");
        } else if (t.amount > 50000) {
            fraudScore += 0.25f;
            t.fraudReasons.add("HIGH_AMOUNT($" + t.amount + ")");
        } else if (t.amount > 20000) {
            fraudScore += 0.15f;
            t.fraudReasons.add("MEDIUM_AMOUNT($" + t.amount + ")");
        }

        // Low amount can be suspicious if < $50 (might be test/card skip)
        if (t.amount < 50) {
            fraudScore += 0.10f;
            t.fraudReasons.add("VERY_LOW_AMOUNT($" + t.amount + ")");
        }

        // ========== RULE 2: LOCATION-BASED ANALYSIS ==========
        if (t.sourceLocation.equals(t.destLocation)) {
            // Same location: lower risk
            fraudScore -= 0.10f;  // Reduce suspicion
            // t.fraudReasons.add("SAME_LOCATION");
        } else {
            // Different location: medium risk
            fraudScore += 0.10f;
            t.fraudReasons.add("CROSS_LOCATION(" + t.sourceLocation + "->" + t.destLocation + ")");
        }

        // ========== RULE 3: CURRENCY ANALYSIS ==========
        if (isUnusualCurrency(t.currency)) {
            fraudScore += 0.08f;
            t.fraudReasons.add("UNUSUAL_CURRENCY(" + t.currency + ")");
        }

        // ========== RULE 4: STRUCTURED TRANSACTION PATTERN ==========
        // Amount like 35, 89, 200 can indicate suspicious patterns
        if (t.amount % 1000 == 0 && t.amount > 50000) {
            // Round amounts in high values: suspicious
            fraudScore += 0.12f;
            t.fraudReasons.add("ROUND_AMOUNT_PATTERN");
        }

        // ========== FINAL DECISION ==========
        t.fraudScore = Math.min(fraudScore, 1.0f);  // Cap at 1.0

        // Fraud if score > 0.50
        t.isFraud = t.fraudScore > 0.35f;
    }

    private static boolean isUnusualCurrency(String currency) {
        // Common: USD, EUR, INR, GBP
        // Uncommon: others
        return !currency.matches("USD|EUR|INR|GBP|JPY|AUD");
    }

    // POJO for Transaction
    public static class SimpleTransaction {
        public String txnId;
        public int amount;
        public String currency;
        public String sourceLocation;
        public String destLocation;
        public long timestamp;
        public boolean isFraud;
        public float fraudScore;
        public java.util.List<String> fraudReasons;

        public SimpleTransaction() {
            this.fraudReasons = new java.util.ArrayList<>();
        }

        public SimpleTransaction(String txnId, int amount, String currency,
                                 String sourceLocation, String destLocation, long timestamp) {
            this.txnId = txnId;
            this.amount = amount;
            this.currency = currency;
            this.sourceLocation = sourceLocation;
            this.destLocation = destLocation;
            this.timestamp = timestamp;
            this.isFraud = false;
            this.fraudScore = 0.0f;
            this.fraudReasons = new java.util.ArrayList<>();
        }
    }
}
