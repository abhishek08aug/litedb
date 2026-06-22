package com.litedb.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Rpc — length-framed JSON-over-TCP request/response, the transport for every cluster interaction
 * (Raft vote/append, client routing, 2PC). A message is a 4-byte big-endian length prefix followed
 * by a UTF-8 JSON body:
 *
 *   request:  {"method": <str>, "payload": <obj>}
 *   response: {"ok": true, "result": <obj>} | {"ok": false, "error": <str>}
 *
 * {@link Server} is threaded (one thread per connection). {@link Client} keeps one persistent
 * connection per target and reconnects once on failure, so a peer that dies and restarts heals
 * transparently.
 */
public final class Rpc {

    /** A handler maps a request payload to a result object. */
    public interface Handler {
        Object handle(Map<String, Object> payload) throws Exception;
    }

    static void sendMsg(DataOutputStream out, Object obj) throws IOException {
        byte[] data = Json.encode(obj).getBytes("UTF-8");
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> recvMsg(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        return (Map<String, Object>) Json.parse(new String(data, "UTF-8"));
    }

    // ---- server -----------------------------------------------------------

    public static final class Server {
        private final int port;
        private final Map<String, Handler> handlers;
        private volatile boolean running;
        private ServerSocket socket;

        public Server(int port, Map<String, Handler> handlers) {
            this.port = port;
            this.handlers = handlers;
        }

        public void start() throws IOException {
            socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", port), 128);
            running = true;
            Thread t = new Thread(this::acceptLoop, "rpc-accept-" + port);
            t.setDaemon(true);
            t.start();
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket conn = socket.accept();
                    Thread t = new Thread(() -> serve(conn), "rpc-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    break;
                }
            }
        }

        private void serve(Socket conn) {
            try {
                DataInputStream in = new DataInputStream(conn.getInputStream());
                DataOutputStream out = new DataOutputStream(conn.getOutputStream());
                while (running) {
                    Map<String, Object> req = recvMsg(in);
                    if (!running) break;
                    String method = (String) req.get("method");
                    Handler h = method == null ? null : handlers.get(method);
                    Map<String, Object> resp = new LinkedHashMap<>();
                    if (h == null) {
                        resp.put("ok", false);
                        resp.put("error", "unknown method: " + method);
                    } else {
                        try {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> payload = (Map<String, Object>) req.get("payload");
                            if (payload == null) payload = new LinkedHashMap<>();
                            resp.put("ok", true);
                            resp.put("result", h.handle(payload));
                        } catch (Exception ex) {
                            resp.put("ok", false);
                            resp.put("error", ex.getClass().getSimpleName() + ": " + ex.getMessage());
                        }
                    }
                    sendMsg(out, resp);
                }
            } catch (IOException e) {
                // connection closed
            } finally {
                try { conn.close(); } catch (IOException ignored) {}
            }
        }

        public void stop() {
            running = false;
            if (socket != null) {
                try { socket.close(); } catch (IOException ignored) {}
            }
        }
    }

    // ---- client -----------------------------------------------------------

    public static final class Client {
        private final int timeoutMs;
        private final Map<String, Socket> conns = new ConcurrentHashMap<>();
        private final Map<String, Object> locks = new ConcurrentHashMap<>();

        public Client(int timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        private Object lockFor(String addr) {
            return locks.computeIfAbsent(addr, k -> new Object());
        }

        /** Send one request; never throws — failures come back as {"ok": false, "error": ...}. */
        public Map<String, Object> call(String host, int port, String method,
                                        Map<String, Object> payload) {
            String addr = host + ":" + port;
            synchronized (lockFor(addr)) {
                String lastErr = "unknown";
                for (int attempt = 0; attempt < 2; attempt++) {
                    Socket sock = conns.get(addr);
                    try {
                        if (sock == null) {
                            sock = new Socket();
                            sock.connect(new InetSocketAddress(host, port), timeoutMs);
                            sock.setSoTimeout(timeoutMs);
                            conns.put(addr, sock);
                        }
                        DataOutputStream out = new DataOutputStream(sock.getOutputStream());
                        DataInputStream in = new DataInputStream(sock.getInputStream());
                        Map<String, Object> msg = new LinkedHashMap<>();
                        msg.put("method", method);
                        msg.put("payload", payload);
                        sendMsg(out, msg);
                        return recvMsg(in);
                    } catch (IOException e) {
                        lastErr = e.getClass().getSimpleName() + ": " + e.getMessage();
                        if (sock != null) {
                            try { sock.close(); } catch (IOException ignored) {}
                        }
                        conns.remove(addr);
                    }
                }
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("ok", false);
                err.put("error", "rpc to " + addr + " failed: " + lastErr);
                return err;
            }
        }

        public void close() {
            for (Socket s : conns.values()) {
                try { s.close(); } catch (IOException ignored) {}
            }
            conns.clear();
        }
    }

    private Rpc() {}
}
