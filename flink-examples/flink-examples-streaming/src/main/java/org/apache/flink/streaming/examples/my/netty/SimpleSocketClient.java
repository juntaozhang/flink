package org.apache.flink.streaming.examples.my.netty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SimpleSocketClient {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 19998;

        try {
            Socket socket = new Socket(host, port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("Hello, Server!");

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Server response: " + line);
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
