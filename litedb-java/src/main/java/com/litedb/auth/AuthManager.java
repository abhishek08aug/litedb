package com.litedb.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AuthManager — Authentication, authorization, and connection pooling.
 *
 * CONCEPT:
 *   Every production database needs:
 *     1. Authentication — verify who you are (username + password)
 *     2. Authorization  — verify what you can do (RBAC: roles & permissions)
 *     3. Connection pooling — reuse expensive TCP connections
 *
 *   Password storage:
 *     NEVER store plaintext passwords. Store: SHA-256(salt + password).
 *     Salt prevents rainbow table attacks (precomputed hash lookups).
 *     Production: use bcrypt/scrypt/Argon2 (deliberately slow to brute-force).
 *
 *   RBAC (Role-Based Access Control):
 *     Users are assigned roles (ADMIN, READ_WRITE, READ_ONLY).
 *     Roles have permissions (SELECT, INSERT, UPDATE, DELETE, DDL).
 *     This is how PostgreSQL, MySQL, and Oracle implement access control.
 *
 *   Connection pooling:
 *     Opening a TCP connection + TLS handshake + auth = ~10ms.
 *     With 1000 req/s, that's 10s of overhead per second — unacceptable.
 *     Pool: maintain N pre-authenticated connections, reuse them.
 *     HikariCP (Java), pgBouncer (PostgreSQL), ProxySQL (MySQL) do this.
 */
public class AuthManager {

    // ------------------------------------------------------------------ //
    //  Roles and permissions                                              //
    // ------------------------------------------------------------------ //

    public enum Permission { SELECT, INSERT, UPDATE, DELETE, DDL, ADMIN }

    public enum Role {
        ADMIN     (Permission.values()),
        READ_WRITE(Permission.SELECT, Permission.INSERT, Permission.UPDATE, Permission.DELETE),
        READ_ONLY (Permission.SELECT);

        private final Set<Permission> permissions;

        Role(Permission... perms) {
            this.permissions = EnumSet.copyOf(Arrays.asList(perms));
        }

        public boolean can(Permission p) { return permissions.contains(p); }
    }

    // ------------------------------------------------------------------ //
    //  User record                                                        //
    // ------------------------------------------------------------------ //

    static class User {
        final String username;
        final String passwordHash; // SHA-256(salt + password)
        final String salt;
        final Role   role;
        volatile boolean locked = false;

        User(String username, String passwordHash, String salt, Role role) {
            this.username     = username;
            this.passwordHash = passwordHash;
            this.salt         = salt;
            this.role         = role;
        }
    }

    // ------------------------------------------------------------------ //
    //  Session token                                                      //
    // ------------------------------------------------------------------ //

    public static class Session {
        public final String token;
        public final String username;
        public final Role   role;
        public final long   expiresAt;

        Session(String token, String username, Role role, long ttlMs) {
            this.token     = token;
            this.username  = username;
            this.role      = role;
            this.expiresAt = System.currentTimeMillis() + ttlMs;
        }

        public boolean isExpired() { return System.currentTimeMillis() > expiresAt; }

        @Override public String toString() {
            return "Session{user=" + username + ", role=" + role
                 + ", expires=" + (expiresAt - System.currentTimeMillis()) + "ms}";
        }
    }

    // ------------------------------------------------------------------ //
    //  Connection pool                                                    //
    // ------------------------------------------------------------------ //

    public static class ConnectionPool {
        private final String            name;
        private final int               maxSize;
        private final BlockingQueue<Connection> available;
        private final AtomicInteger     totalCreated = new AtomicInteger(0);
        private final AtomicInteger     waitCount    = new AtomicInteger(0);

        public static class Connection {
            public final int    id;
            public final String owner;
            volatile boolean    inUse = false;

            Connection(int id, String owner) { this.id = id; this.owner = owner; }

            @Override public String toString() { return "Conn#" + id + "(" + owner + ")"; }
        }

        public ConnectionPool(String name, int maxSize) {
            this.name      = name;
            this.maxSize   = maxSize;
            this.available = new LinkedBlockingQueue<>(maxSize);
            // Pre-create connections
            for (int i = 0; i < maxSize; i++) {
                available.offer(new Connection(totalCreated.incrementAndGet(), "pool"));
            }
            System.out.println("[Pool] " + name + " initialized with " + maxSize + " connections");
        }

        /** Borrow a connection (blocks up to timeoutMs). */
        public Connection acquire(String user, long timeoutMs) throws InterruptedException {
            waitCount.incrementAndGet();
            Connection conn = available.poll(timeoutMs, TimeUnit.MILLISECONDS);
            waitCount.decrementAndGet();
            if (conn == null) throw new RuntimeException("Connection pool exhausted (timeout)");
            conn.inUse = true;
            conn.owner.equals(user); // track owner (simplified)
            return conn;
        }

        /** Return a connection to the pool. */
        public void release(Connection conn) {
            conn.inUse = false;
            available.offer(conn);
        }

        public int available() { return available.size(); }
        public int waiting()   { return waitCount.get(); }

        @Override public String toString() {
            return name + "[available=" + available() + "/" + maxSize + "]";
        }
    }

    // ------------------------------------------------------------------ //
    //  AuthManager state                                                  //
    // ------------------------------------------------------------------ //

    private final Map<String, User>    users    = new ConcurrentHashMap<>();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final SecureRandom         rng      = new SecureRandom();
    private final long                 sessionTtlMs;

    public AuthManager(long sessionTtlMs) {
        this.sessionTtlMs = sessionTtlMs;
    }

    // ------------------------------------------------------------------ //
    //  User management                                                    //
    // ------------------------------------------------------------------ //

    public void createUser(String username, String password, Role role) {
        String salt = randomHex(16);
        String hash = sha256(salt + password);
        users.put(username, new User(username, hash, salt, role));
        System.out.println("[Auth] Created user '" + username + "' role=" + role);
    }

    public void lockUser(String username) {
        User u = users.get(username);
        if (u != null) { u.locked = true; System.out.println("[Auth] Locked user '" + username + "'"); }
    }

    // ------------------------------------------------------------------ //
    //  Authentication                                                     //
    // ------------------------------------------------------------------ //

    public Session login(String username, String password) {
        User u = users.get(username);
        if (u == null)   throw new AuthException("Unknown user: " + username);
        if (u.locked)    throw new AuthException("Account locked: " + username);
        String hash = sha256(u.salt + password);
        if (!hash.equals(u.passwordHash)) throw new AuthException("Invalid password");

        String token = randomHex(32);
        Session session = new Session(token, username, u.role, sessionTtlMs);
        sessions.put(token, session);
        System.out.println("[Auth] Login OK: " + session);
        return session;
    }

    public void logout(String token) {
        sessions.remove(token);
        System.out.println("[Auth] Logged out token=" + token.substring(0, 8) + "...");
    }

    // ------------------------------------------------------------------ //
    //  Authorization                                                      //
    // ------------------------------------------------------------------ //

    public Session authorize(String token, Permission required) {
        Session s = sessions.get(token);
        if (s == null)      throw new AuthException("Invalid or expired token");
        if (s.isExpired())  { sessions.remove(token); throw new AuthException("Session expired"); }
        if (!s.role.can(required))
            throw new AuthException("Permission denied: " + s.username
                    + " (role=" + s.role + ") cannot " + required);
        return s;
    }

    public boolean canDo(String token, Permission p) {
        try { authorize(token, p); return true; }
        catch (AuthException e) { return false; }
    }

    // ------------------------------------------------------------------ //
    //  Helpers                                                            //
    // ------------------------------------------------------------------ //

    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) { throw new RuntimeException(e); }
    }

    private String randomHex(int bytes) {
        byte[] buf = new byte[bytes];
        rng.nextBytes(buf);
        StringBuilder sb = new StringBuilder();
        for (byte b : buf) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static class AuthException extends RuntimeException {
        public AuthException(String msg) { super(msg); }
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws InterruptedException {
        System.out.println("============================================================");
        System.out.println("AUTH MANAGER + CONNECTION POOL DEMO");
        System.out.println("============================================================\n");

        AuthManager auth = new AuthManager(60_000); // 60s session TTL

        // Create users
        System.out.println("[Step 1] Create users");
        auth.createUser("admin",    "secret123",  Role.ADMIN);
        auth.createUser("analyst",  "readpass",   Role.READ_ONLY);
        auth.createUser("appuser",  "apppass",    Role.READ_WRITE);

        // Login
        System.out.println("\n[Step 2] Login");
        Session adminSession   = auth.login("admin",   "secret123");
        Session analystSession = auth.login("analyst", "readpass");

        // Authorization checks
        System.out.println("\n[Step 3] Authorization checks");
        for (Permission p : Permission.values()) {
            System.out.printf("  admin   can %-8s: %b%n", p, auth.canDo(adminSession.token, p));
        }
        System.out.println();
        for (Permission p : Permission.values()) {
            System.out.printf("  analyst can %-8s: %b%n", p, auth.canDo(analystSession.token, p));
        }

        // Wrong password
        System.out.println("\n[Step 4] Wrong password");
        try { auth.login("admin", "wrongpass"); }
        catch (AuthManager.AuthException e) { System.out.println("  Expected error: " + e.getMessage()); }

        // Lock account
        System.out.println("\n[Step 5] Lock account");
        auth.lockUser("analyst");
        try { auth.login("analyst", "readpass"); }
        catch (AuthManager.AuthException e) { System.out.println("  Expected error: " + e.getMessage()); }

        // Connection pool
        System.out.println("\n[Step 6] Connection pool");
        ConnectionPool pool = new ConnectionPool("litedb-pool", 5);
        System.out.println("  Pool state: " + pool);

        ConnectionPool.Connection c1 = pool.acquire("admin", 1000);
        ConnectionPool.Connection c2 = pool.acquire("appuser", 1000);
        System.out.println("  Acquired: " + c1 + ", " + c2);
        System.out.println("  Pool state: " + pool);

        pool.release(c1);
        pool.release(c2);
        System.out.println("  After release: " + pool);

        System.out.println("\n[Done] Auth + pool demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Passwords stored as SHA-256(salt+password) — never plaintext");
        System.out.println("  2. Salt prevents rainbow table attacks");
        System.out.println("  3. RBAC: roles define what operations a user can perform");
        System.out.println("  4. Session tokens avoid re-authenticating every request");
        System.out.println("  5. Connection pools amortize expensive connection setup cost");
    }
}