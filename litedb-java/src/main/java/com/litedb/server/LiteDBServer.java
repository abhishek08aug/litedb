package com.litedb.server;

import com.litedb.lsm.LSMEngine;
import com.litedb.query.QueryParser;
import com.litedb.query.QueryResult;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LiteDBServer — TCP server that exposes the LSM engine over a text protocol.
 *
 * CONCEPT:
 *   A database server accepts client connections, reads commands, executes
 *   them against the storage engine, and sends back responses.
 *
 *   Architecture:
 *     - Main thread: accepts new TCP connections
 *     - Thread pool: one thread per active client connection
 *     - Each client gets its own QueryParser (stateless, shares the engine)
 *
 *   Wire protocol (text-based, like Redis):
 *     Client sends: "SET name Alice\n"
 *     Server replies: "OK\n"
 *
 *     Client sends: "GET name\n"
 *     Server replies: "VALUE Alice\n"
 *
 *   This is exactly how Redis, Memcached, and early MongoDB work.
 */
public class LiteDBServer {

    private final int         port;
    private final LSMEngine   engine;
    private final ExecutorService threadPool;
    private       ServerSocket serverSocket;
    private volatile boolean  running = false;

    private final AtomicLong connectionsAccepted = new AtomicLong(0);
    private final AtomicLong commandsExecuted    = new AtomicLong(0);

    public LiteDBServer(int port, LSMEngine engine) {
        this.port       = port;
        this.engine     = engine;
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "litedb-client-handler");
            t.setDaemon(true);
            return t;
        });
    }

    // ------------------------------------------------------------------ //
    //  Lifecycle                                                          //
    // ------------------------------------------------------------------ //

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;
        System.out.println("[Server] LiteDB listening on port " + port);

        while (running) {
            try {
                Socket client = serverSocket.accept();
                connectionsAccepted.incrementAndGet();
                threadPool.submit(() -> handleClient(client));
            } catch (SocketException e) {
                if (running) System.out.println("[Server] Accept error: " + e.getMessage());
            }
        }
    }

    public void stop() throws IOException {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) serverSocket.close();
        threadPool.shutdown();
        System.out.println("[Server] Stopped. Connections=" + connectionsAccepted.get()
                + " Commands=" + commandsExecuted.get());
    }

    // ------------------------------------------------------------------ //
    //  Client handler                                                     //
    // ------------------------------------------------------------------ //

    private void handleClient(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        System.out.println("[Server] Client connected: " + remote);

        QueryParser parser = new QueryParser(engine);

        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), "UTF-8"));
            PrintWriter writer = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true);

            // Send banner
            writer.print("+LiteDB 1.0 ready\n");
            writer.flush();

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                QueryResult result = parser.execute(line);
                commandsExecuted.incrementAndGet();

                writer.print(result.toWire());
                writer.flush();

                if (result.status == QueryResult.Status.QUIT) break;
            }
        } catch (IOException e) {
            System.out.println("[Server] Client " + remote + " disconnected: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }

        System.out.println("[Server] Client disconnected: " + remote);
    }

    // ======================================================================= //
    //  DEMO — starts server, runs a self-test client, then shuts down         //
    // ======================================================================= //

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("litedb_server_demo_");
        LSMEngine engine = new LSMEngine(tmpDir.toString());

        int port = 7379;
        LiteDBServer server = new LiteDBServer(port, engine);

        // Start server in background thread
        Thread serverThread = new Thread(() -> {
            try { server.start(); } catch (IOException e) {
                if (server.running) System.out.println("[Server] Error: " + e.getMessage());
            }
        }, "litedb-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // Give server time to bind
        Thread.sleep(200);

        System.out.println("============================================================");
        System.out.println("LITEDB SERVER DEMO — self-test client");
        System.out.println("============================================================\n");

        // Run a client
        try (Socket socket = new Socket("127.0.0.1", port);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), "UTF-8"));
             PrintWriter writer = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true)) {

            // Read banner
            System.out.println("  Banner: " + reader.readLine());

            String[][] commands = {
                {"PING",                  null},
                {"SET name Alice",        null},
                {"SET age 30",            null},
                {"GET name",              null},
                {"GET missing",           null},
                {"SCAN a z",              null},
                {"DELETE age",            null},
                {"GET age",               null},
                {"STATS",                 null},
                {"QUIT",                  null},
            };

            for (String[] cmd : commands) {
                writer.print(cmd[0] + "\n");
                writer.flush();

                // Read response (may be multi-line for SCAN)
                StringBuilder response = new StringBuilder();
                String line;
                if (cmd[0].startsWith("SCAN")) {
                    // Read until SCAN_END
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                        if (line.startsWith("SCAN_END")) break;
                        response.append(" | ");
                    }
                } else {
                    line = reader.readLine();
                    if (line != null) response.append(line);
                }
                System.out.printf("  > %-25s → %s%n", cmd[0], response);
            }
        }

        Thread.sleep(100);
        server.stop();
        engine.close();
        deleteDir(tmpDir.toFile());

        System.out.println("\n[Done] Server demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Each client gets its own thread (thread-per-connection model)");
        System.out.println("  2. The engine is shared — thread safety is in LSMEngine/MemTable");
        System.out.println("  3. Text protocol is easy to test with: nc localhost 7379");
        System.out.println("  4. Production databases use async I/O (Netty, io_uring) for scale");
    }

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) { for (File f : dir.listFiles()) deleteDir(f); }
        dir.delete();
    }
}