package ru.vez.kvstore;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

/** A simple key-value store client that connects to a KV Store server. It allows users to send GET, SET, and DEL commands interactively.
 * Features:
 * - Connect to the key-value server via TCP socket.
 * - Read user input from the terminal (System.in).
 * - Send commands (e.g. SET foo bar) to the server.
 * - Print the server’s responses (OK, value, nil, ERR).
 * 
 * Improvement ideas:
 * Add try-catch for graceful disconnect handling.
 * Auto-reconnect on failure.
 * Server closes connection - Detect EOF and exit gracefully.
 * Exit on command like QUIT.
 * Wrap it in a CLI app with command history (e.g. via JLine).
 */
public class KeyValueClient {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost", 6379);      // This port matches the one used in KeyValueServer.
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));     // receives responses from the server.
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));   // sends commands to the server.
        Scanner scanner = new Scanner(System.in);   // reads terminal input from the user.

        System.out.println("Connected to KV Store. Type commands:");

        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            writer.write(line + "\n");
            writer.flush();

            String response = reader.readLine();
            System.out.println(response);
        }
    }
}

