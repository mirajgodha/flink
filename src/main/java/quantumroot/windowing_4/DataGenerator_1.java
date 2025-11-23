package quantumroot.windowing_4;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class DataGenerator_1 {
    public static void main(String[] args) {
        String[] months = {"June", "July", "August"};
        String[] categories = {"Clothes", "Glasses", "Shoes"};

        Random random = new Random();

        try (ServerSocket server = new ServerSocket(9090)) {
            System.out.println("Listening on port 9090...");
            Socket socket = server.accept();
            System.out.println("Got connection from " + socket.getRemoteSocketAddress());

            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                while (true) {
                    String month = months[random.nextInt(months.length)];
                    String category = categories[random.nextInt(categories.length)];
                    int sales = 1 + random.nextInt(10);

                    String line = String.format("01-01-2023,%s,%s,%d", month, category, sales);

                    out.println(line);
                    System.out.println("Sent: " + line);

                    Thread.sleep(2500);
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
