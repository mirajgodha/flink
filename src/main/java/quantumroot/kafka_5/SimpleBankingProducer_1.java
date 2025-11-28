package quantumroot.kafka_5;

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
public class SimpleBankingProducer_1 {

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
                "TXN001,1000,USD,NY,NY,1764255944523",
                "TXN002,50000,USD,LA,CA,1764255944523",
                "TXN003,200,USD,Chicago,IL,1764255945545",
                "TXN004,7000,USD,NY,NY,1764255945545",
                "TXN005,35,USD,Miami,FL,1764255946546",
                "TXN006,150000,EUR,LA,CA,1764255946546",
                "TXN007,500,INR,Delhi,Mumbai,1764255948554",
                "TXN008,25000,USD,NY,LA,1764255945545",
                "TXN009,89,GBP,Boston,MA,1764255948554",
                "TXN010,120000,USD,Seattle,WA,1764255948554"
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
                            parts,                    // txn_id
                            parts,                    // amount
                            parts,                    // currency
                            parts,                    // source_location
                            parts,                    // dest_location
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
