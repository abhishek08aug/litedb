package com.litedb.server;

import com.litedb.lsm.LSMEngine;
import com.litedb.query.QueryParser;
import com.litedb.query.QueryResult;

import java.io.*;
import java.net.*;
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
 *
 *   To connect, use {@link com.litedb.client.LiteDBClient} (or any TCP client
 *   such as {@code nc localhost 7379}).
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
    //  Entry point — brings up a persistent server and blocks until stopped   //
    // ======================================================================= //
    //
    //  Usage:
    //    java -cp target/classes com.litedb.server.LiteDBServer [--port N] [--data-dir DIR]
    //
    //  Defaults: --port 7379, --data-dir ./litedb-data
    //  Data persists across restarts (the LSM engine replays the WAL and loads
    //  existing SSTables on startup). Stop with Ctrl-C.
    //
    public static void main(String[] args) throws Exception {
        int    port    = 7379;
        String dataDir = "./litedb-data";

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--port":     port    = Integer.parseInt(args[i + 1]); break;
                case "--data-dir": dataDir = args[i + 1];                    break;
                default: /* ignore */                                        break;
            }
        }

        LSMEngine    engine = new LSMEngine(dataDir);
        LiteDBServer server = new LiteDBServer(port, engine);

        // Graceful shutdown on Ctrl-C: stop accepting, flush, close the engine.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.stop(); engine.close(); } catch (Exception ignored) {}
        }, "litedb-shutdown"));

        System.out.println("[LiteDB] data dir: " + dataDir);
        System.out.println("[LiteDB] connect with:  java -cp target/classes com.litedb.client.LiteDBClient"
                + (port == 7379 ? "" : " --port " + port));
        System.out.println("[LiteDB]          or:  nc localhost " + port);
        System.out.println("[LiteDB] press Ctrl-C to stop.");

        server.start();   // blocks in the accept loop until stop()
    }
}
