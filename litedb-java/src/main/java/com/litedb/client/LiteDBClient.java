package com.litedb.client;

import java.io.*;
import java.net.Socket;

/**
 * LiteDBClient — a text-protocol client for {@link com.litedb.server.LiteDBServer}.
 *
 * Two modes:
 *   - interactive (default): a REPL — type commands, see responses
 *   - --demo: runs a fixed command sequence (handy as a smoke test)
 *
 * Wire protocol (text, like Redis): each command is a single line; the server
 * replies with one line, except SCAN which streams rows until "SCAN_END n".
 *
 * Usage:
 *   java -cp target/classes com.litedb.client.LiteDBClient [--host H] [--port N] [--demo]
 *   Defaults: --host 127.0.0.1, --port 7379
 *
 *   Supported commands: PING, SET k v, GET k, DELETE k, SCAN start end, STATS, HELP, QUIT
 */
public class LiteDBClient {

    private final String host;
    private final int    port;

    public LiteDBClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        String  host = "127.0.0.1";
        int     port = 7379;
        boolean demo = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host": host = args[++i];                    break;
                case "--port": port = Integer.parseInt(args[++i]);  break;
                case "--demo": demo = true;                         break;
                default: /* ignore */                               break;
            }
        }

        new LiteDBClient(host, port).run(demo);
    }

    private void run(boolean demo) throws IOException {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), "UTF-8"));
             PrintWriter out = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true)) {

            System.out.println(in.readLine());   // server banner

            if (demo) {
                runDemo(in, out);
            } else {
                runInteractive(in, out);
            }
        }
    }

    /** REPL: read a line from stdin, send it, print the response. */
    private void runInteractive(BufferedReader serverIn, PrintWriter serverOut) throws IOException {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("litedb> ");
        String line;
        while ((line = stdin.readLine()) != null) {
            String cmd = line.trim();
            if (cmd.isEmpty()) { System.out.print("litedb> "); continue; }

            serverOut.print(cmd + "\n");
            serverOut.flush();
            System.out.println(readResponse(serverIn, cmd));

            if (cmd.equalsIgnoreCase("QUIT") || cmd.equalsIgnoreCase("EXIT")) break;
            System.out.print("litedb> ");
        }
    }

    /** Fixed sequence — a smoke test / showcase of the protocol. */
    private void runDemo(BufferedReader serverIn, PrintWriter serverOut) throws IOException {
        String[] commands = {
            "PING", "SET name Alice", "SET age 30", "GET name", "GET missing",
            "SCAN a z", "DELETE age", "GET age", "STATS", "QUIT",
        };
        for (String cmd : commands) {
            serverOut.print(cmd + "\n");
            serverOut.flush();
            System.out.printf("  > %-22s -> %s%n", cmd, readResponse(serverIn, cmd));
        }
    }

    /** A single reply, except SCAN which streams rows until "SCAN_END". */
    private String readResponse(BufferedReader serverIn, String cmd) throws IOException {
        if (cmd.toUpperCase().startsWith("SCAN")) {
            StringBuilder sb = new StringBuilder();
            String l;
            while ((l = serverIn.readLine()) != null) {
                sb.append(l);
                if (l.startsWith("SCAN_END")) break;
                sb.append(" | ");
            }
            return sb.toString();
        }
        return serverIn.readLine();
    }
}
