package quantumroot.dataStream_3;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
public class DataGeneratorExercise {
    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        String[] cabIds = {"CAB-001", "CAB-002", "CAB-003", "CAB-004"};
        String[] numberPlates = {"MH12AB1234", "MH14CD5678", "DL01EF9999", "KA51GH4321"};
        String[] cabTypes = {"Sedan", "SUV", "Mini", "Prime"};
        String[] driverNames = {"Amit Sharma", "Rohit Verma", "Sanjay Patil", "Kiran Rao"};
        String[] locations = {"Pune Station", "Hinjewadi", "Kothrud", "Baner", "MG Road"};
        String[] destinations = {"Airport", "Kharadi", "Viman Nagar", "Hadapsar", "Wakad"};

        Random random = new Random();

        try (ServerSocket server = new ServerSocket(9090)) {
            System.out.println("Listening on port 9090...");
            Socket socket = server.accept();
            System.out.println("Connected to " + socket.getRemoteSocketAddress());

            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                while (true) {
                    long timestamp = Instant.now().toEpochMilli();

                    int idx = random.nextInt(cabIds.length);
                    String cabId = cabIds[idx];
                    String plate = numberPlates[idx];
                    String cabType = cabTypes[random.nextInt(cabTypes.length)];
                    String driverName = driverNames[idx];

                    boolean ongoingTrip = random.nextBoolean();
                    String pickup = locations[random.nextInt(locations.length)];
                    String destination = destinations[random.nextInt(destinations.length)];

                    int passengerCount = ongoingTrip ? 1 + random.nextInt(4) : 0;

                    // cab_id, cab_number_plate, cab_type, cab_driver_name, ongoing_trip/not,
                    // pickup_location, destination, passenger_count
                    String line = String.format(
                            "%s,%s,%s,%s,%s,%s,%s,%d",
                            cabId,
                            plate,
                            cabType,
                            driverName,
                            ongoingTrip ? "ONGOING" : "IDLE",
                            pickup,
                            destination,
                            passengerCount
                    );

                    System.out.println("Sent: " + line + " @ " + LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(timestamp),
                            ZoneId.systemDefault()
                    ).format(fmt));

                    out.println(line);

                    Thread.sleep(4000); // emit every 4 seconds
                }
            } finally {
                socket.close();
                System.out.println("Connection closed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

