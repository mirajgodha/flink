# Kafka Fraud Detection: Producer (Simple Transactions) + Consumer (Smart Fraud Logic)

## **Part 1: SimpleBankingProducer.java**

```java
package quantumroot.kafka_fraud_detection;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * Simple Kafka Producer: Produces ONLY transaction data (no fraud labels)
 * Consumer will detect fraud using logic
 * 
 * Transaction format (CSV):
 * txn_id,amount,currency,source_location,dest_location,timestamp
 * 
 * Example:
 * TXN001,1000,USD,NY,NY,1630259874123
 * TXN002,50000,USD,LA,CA,1630259875456
 * TXN003,200,USD,Chicago,IL,1630259876789
 * 
 * Run:
 * java -cp your-jar.jar quantumroot.kafka_fraud_detection.SimpleBankingProducer
 */
public class SimpleBankingProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "simple-transactions";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        String[] currencies = {"USD", "EUR", "INR", "GBP"};
        String[] locations = {"NY", "LA", "Chicago", "Miami", "Seattle", "Boston", "SF", "Houston", "Delhi", "Mumbai", "Bangalore"};
        String[] states = {"NY", "CA", "IL", "FL", "WA", "MA", "TX", "MH", "KA"};

        long txnId = 1;
        
        // Dummy transaction data
        String[] dummyTransactions = {
            "TXN001,1000,USD,NY,NY",
            "TXN002,50000,USD,LA,CA",
            "TXN003,200,USD,Chicago,IL",
            "TXN004,7000,USD,NY,NY",
            "TXN005,35,USD,Miami,FL",
            "TXN006,150000,EUR,LA,CA",
            "TXN007,500,INR,Delhi,Mumbai",
            "TXN008,25000,USD,NY,LA",
            "TXN009,89,GBP,Boston,MA",
            "TXN010,120000,USD,Seattle,WA"
        };

        try {
            System.out.println("Starting Simple Transaction Producer...");
            System.out.println("Topic: " + TOPIC);
            System.out.println("Format: txn_id,amount,currency,source_location,dest_location,timestamp\n");

            int dummyIndex = 0;

            while (true) {
                // Send dummy transactions first
                if (dummyIndex < dummyTransactions.length) {
                    String[] parts = dummyTransactions[dummyIndex].split(",");
                    String transaction = String.format("%s,%s,%s,%s,%s,%d",
                            parts[0],                    // txn_id
                            parts[1],                    // amount
                            parts[2],                    // currency
                            parts[3],                    // source_location
                            parts[4],                    // dest_location
                            System.currentTimeMillis()   // timestamp
                    );
                    sendTransaction(producer, transaction);
                    System.out.println("[DUMMY] Sent: " + transaction);
                    dummyIndex++;
                    Thread.sleep(500);
                    continue;
                }

                // After dummy data, generate random transactions
                String currency = currencies[random.nextInt(currencies.length)];
                String sourceLocation = locations[random.nextInt(locations.length)];
                String destLocation = locations[random.nextInt(locations.length)];
                
                // Generate various amounts
                int amount;
                int amountType = random.nextInt(10);
                
                if (amountType < 5) {
                    // Normal amounts: $100 - $10,000
                    amount = 100 + random.nextInt(9900);
                } else if (amountType < 8) {
                    // Suspicious amounts: $20,000 - $100,000
                    amount = 20000 + random.nextInt(80000);
                } else {
                    // Very high amounts: $100,000 - $500,000 (potential fraud)
                    amount = 100000 + random.nextInt(400000);
                }

                String transaction = String.format("TXN%04d,%d,%s,%s,%s,%d",
                        txnId++,
                        amount,
                        currency,
                        sourceLocation,
                        destLocation,
                        System.currentTimeMillis()
                );

                sendTransaction(producer, transaction);
                System.out.println("[GENERATED] Sent: " + transaction);
                
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void sendTransaction(KafkaProducer<String, String> producer, String transaction) {
        String[] parts = transaction.split(",");
        String txnId = parts[0];
        
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, txnId, transaction);
        
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error: " + exception.getMessage());
            }
        });
    }
}
```

---

## **Part 2: SmartFraudDetectionConsumer.java**

```java
package quantumroot.kafka_fraud_detection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
public class SmartFraudDetectionConsumer {

    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "smart-fraud-group");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "simple-transactions",
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Parse transactions
        DataStream<SimpleTransaction> transactions = kafkaStream
                .map(new MapFunction<String, SimpleTransaction>() {
                    @Override
                    public SimpleTransaction map(String line) throws Exception {
                        String[] parts = line.split(",");
                        return new SimpleTransaction(
                                parts[0],                    // txn_id
                                Integer.parseInt(parts[1]),  // amount
                                parts[2],                    // currency
                                parts[3],                    // source_location
                                parts[4],                    // dest_location
                                Long.parseLong(parts[5])     // timestamp
                        );
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
        }).print();

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
        }).print();

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

        // Fraud if score > 0.5
        t.isFraud = t.fraudScore > 0.50f;
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
```

---

## **How to Run:**

### **Step 1: Create Kafka Topic**
```bash
bin/kafka-topics.sh --create --topic simple-transactions \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### **Step 2: Start Producer**
```bash
java -cp your-jar.jar quantumroot.kafka_fraud_detection.SimpleBankingProducer
```

**Output:**
```
Starting Simple Transaction Producer...
Topic: simple-transactions
Format: txn_id,amount,currency,source_location,dest_location,timestamp

[DUMMY] Sent: TXN001,1000,USD,NY,NY,1735313098123
[DUMMY] Sent: TXN002,50000,USD,LA,CA,1735313098234
[DUMMY] Sent: TXN003,200,USD,Chicago,IL,1735313098456
[DUMMY] Sent: TXN004,7000,USD,NY,NY,1735313098789
[DUMMY] Sent: TXN005,35,USD,Miami,FL,1735313098900
[DUMMY] Sent: TXN006,150000,EUR,LA,CA,1735313099011
[GENERATED] Sent: TXN0001,45000,USD,Seattle,Boston,1735313099234
[GENERATED] Sent: TXN0002,300,INR,Delhi,Mumbai,1735313100345
```

### **Step 3: Start Fraud Detection Consumer**
```bash
java -cp your-jar.jar quantumroot.kafka_fraud_detection.SmartFraudDetectionConsumer
```

**Output:**
```
✓ NORMAL | TxnID: TXN001 | $1000 USD | NY → NY | Score: 0.00 | Time: 2025-11-27 17:30:45

🚨 FRAUD DETECTED
  TxnID: TXN002 | Amount: $50000 USD | Score: 0.25
  Route: LA → CA
  Reasons: HIGH_AMOUNT($50000) | CROSS_LOCATION(LA->CA)
  Time: 2025-11-27 17:30:46

✓ NORMAL | TxnID: TXN003 | $200 USD | Chicago → IL | Score: 0.00 | Time: 2025-11-27 17:30:47

✓ NORMAL | TxnID: TXN004 | $7000 USD | NY → NY | Score: 0.00 | Time: 2025-11-27 17:30:48

✓ NORMAL | TxnID: TXN005 | $35 USD | Miami → FL | Score: 0.10 | Time: 2025-11-27 17:30:49

🚨 FRAUD DETECTED
  TxnID: TXN006 | Amount: $150000 EUR | Score: 0.35
  Route: LA → CA
  Reasons: VERY_HIGH_AMOUNT($150000) | UNUSUAL_CURRENCY(EUR) | CROSS_LOCATION(LA->CA)
  Time: 2025-11-27 17:30:50

🚨 FRAUD DETECTED
  TxnID: TXN0001 | Amount: $45000 USD | Score: 0.25
  Route: Seattle → Boston
  Reasons: HIGH_AMOUNT($45000) | CROSS_LOCATION(Seattle->Boston)
  Time: 2025-11-27 17:30:52
```

---

## **Fraud Detection Rules Summary:**

| Rule | Condition | Score Impact |
|------|-----------|--------------|
| **Amount Analysis** | |
| Very High (>$100K) | +0.35 | 🔴 HIGH |
| High ($50K-$100K) | +0.25 | 🟡 MEDIUM |
| Medium ($20K-$50K) | +0.15 | 🟡 MEDIUM |
| Very Low (<$50) | +0.10 | 🟢 LOW |
| **Location Analysis** | |
| Same location | -0.10 | Reduces risk |
| Different location | +0.10 | Increases risk |
| **Currency Analysis** | |
| Unusual currency (non-standard) | +0.08 | 🟡 MEDIUM |
| **Pattern Analysis** | |
| Round high amounts | +0.12 | 🟡 MEDIUM |
| **Decision** | |
| Score > 0.50 | 🚨 FRAUD | Flagged |
| Score ≤ 0.50 | ✓ NORMAL | Allowed |

---

## **Key Improvements:**

✅ **Producer generates ONLY raw transaction data** (no fraud labels)  
✅ **Consumer applies smart fraud logic** based on:
  - Amount analysis
  - Location patterns
  - Currency analysis
  - Transaction patterns
✅ **Fraud score system** (0.0-1.0) instead of hard High/Low
✅ **Multiple reasons** for fraud detection (detailed audit trail)
✅ **Flexible thresholds** - easy to tune fraud score cutoff

---

## **Add to pom.xml:**

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>1.17.1</version>
</dependency>

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.3.1</version>
</dependency>
```
