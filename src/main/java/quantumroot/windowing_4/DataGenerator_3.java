package quantumroot.windowing_4;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.Random;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DataGenerator_3 {

    private static final DateTimeFormatter fmt =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static void main(String[] args) {
        String[] store = {"Store-1", "Store-2", "Store-3", "Store-4"};
        Random random = new Random();

        try (ServerSocket server = new ServerSocket(9090)) {
            System.out.println("Listening on port 9090...");
            Socket socket = server.accept();
            System.out.println("Connected to " + socket.getRemoteSocketAddress());

            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                while (true) {
                    long timestamp = Instant.now().toEpochMilli();
                    String store_local = store[random.nextInt(store.length)];
                    int sales = 15 + random.nextInt(15);  // temp between 15 and 29

                    String line = String.format("%d,%s,%d", timestamp, store_local, sales);
                    System.out.println("Sent: " + line + ", " + LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(timestamp),
                            ZoneId.systemDefault()
                    ).format(fmt));
                    out.println(line);

                    Thread.sleep(4000);  // emit every 5 second
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
