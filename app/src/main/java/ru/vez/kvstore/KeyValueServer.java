package ru.vez.kvstore;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/** This is the IPC key-value store server that receives client requests (via TCP socket), parses commands, and delegates key-value operations (GET, SET, and DEL) to StoreEngine.
 * It uses a StoreEngine for data storage and handles client connections in separate threads (Each client runs in its own thread).
 * Pros:
 * - Easy to implement.
 * - Works well for a few concurrent users.
 * Cons:
 * - Doesn’t scale to 1000s of clients without switching to NIO Selector or thread pools.
 * 
 * Improvement ideas:
 * - Add socket timeouts.
 * - Handle malformed input (e.g., bad command formats).
 * - Gracefully close sockets on error or disconnect.
 * - Add max buffer sizes to avoid memory overuse.
 * - Use a ThreadPoolExecutor for better concurrency handling.
 * - Support RESP-style binary protocol like Redis for efficiency.
 */
public class KeyValueServer {
    private static final int PORT = 6379;   // Standard Redis-like port (can be changed).
    private final StoreEngine engine;       // The storage engine used for actual data ops.

    public KeyValueServer(StoreEngine engine) {
        this.engine = engine;
    }

    /** Creates a listening TCP socket. Enters an infinite loop waiting for new client connections.
     * - Accepts a single client.
     * - Launches a new thread per client to handle commands concurrently.
     * - Can later be replaced with thread pool or NIO selector for scaling.
     */
    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("KV Server started on port " + PORT);

            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();
            }
        }
    }

    /** This is where the core command-handling happens. Read input and process line-by-line (\n-terminated).
     * Example: `SET foo bar\n`
     * 
     * @param socket
     */
    private void handleClient(Socket socket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.strip().split("\\s+", 4); // Supports commands with up to 4 parts (e.g., `SET key value ttl`).
                String command = parts[0].toUpperCase();
                String response;

                // Delegates to the correct method of StoreEngine.
                switch (command) {
                    case "GET" -> {
                        if (parts.length != 2) {
                            response = "ERR wrong number of arguments for GET\n";
                        } else {
                            String val = engine.get(parts[1]);
                            response = (val != null ? val : "nil") + "\n";
                        }
                    }
                    case "SET" -> {
                        if (parts.length < 3 || parts.length > 4) {
                            response = "ERR wrong number of arguments for SET\n";
                        } else {
                            String key = parts[1];
                            String value = parts[2];
                            long ttlMillis = 0;
                            if (parts.length == 4) {
                                try {
                                    ttlMillis = Long.parseLong(parts[3]);
                                } catch (NumberFormatException e) {
                                    response = "ERR invalid TTL value\n";
                                    writer.write(response);
                                    writer.flush();
                                    continue;
                                }
                            }
                            engine.set(key, value, ttlMillis);
                            response = "OK\n";
                        }
                    }
                    case "DEL" -> {
                        if (parts.length != 2) {
                            response = "ERR wrong number of arguments for DEL\n";
                        } else {
                            boolean removed = engine.del(parts[1]);
                            response = removed ? "OK\n" : "nil\n";
                        }
                    }
                    default -> response = "ERR unknown command\n";
                }
                writer.write(response);
                writer.flush();
            }

        } catch (IOException e) {
            System.err.println("Client connection error: " + e.getMessage());
        }
    }
}