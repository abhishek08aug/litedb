# Authentication, RBAC, Connection Pooling & Rate Limiting

A database must answer three operational questions on every request: *who* is connecting (authentication), *what* are they allowed to do (authorization), and *how are connection resources and abuse controlled* (pooling and rate limiting). LiteDB implements all four concerns in a single security/operations layer.

These mechanisms correspond to production systems such as PostgreSQL roles, Django's password hashing, PgBouncer, and Nginx rate limiting.

---

## Authentication

Authentication verifies identity. Storing passwords in plaintext is never acceptable; a leak would expose every credential directly. LiteDB stores only a **salted, iterated hash**.

### PBKDF2 with a per-user salt

```
stored = PBKDF2(password, salt, iterations)
```

- **Salt** — a random value unique to each user, stored alongside the hash. It ensures two users with the same password produce different hashes, defeating precomputed (rainbow-table) attacks.
- **Iterations** — the hash is applied many times. General-purpose hashes (MD5, SHA-256) are designed to be *fast*, which helps an attacker brute-force. PBKDF2 is deliberately *slow*, raising the cost of each guess.

Verification re-hashes the supplied password with the stored salt and compares:

```python
salt, expected = users[username].salt, users[username].password_hash
ok = (pbkdf2(password, salt, iterations) == expected)
```

### Account lockout

Repeated failed logins increment a per-user counter; after a threshold the account is locked, blunting online brute-force attempts. A successful login resets the counter.

### Audit log

Every authentication event — login success, bad password, lockout — is appended to an audit log with the username, outcome, and reason. Operational security depends on being able to answer "who tried to access what, and when."

---

## Authorization (RBAC)

Authentication establishes *who*; authorization decides *what they may do*. LiteDB uses **Role-Based Access Control**: permissions are granted to roles, and users are assigned a role, rather than granting permissions to users directly. This keeps access policy small and auditable.

### Permissions and roles

Permissions map to operations (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, and DDL such as `CREATE`/`DROP`, plus `ADMIN`). Roles are fixed bundles of permissions:

| Role | Permissions |
|------|-------------|
| `SUPERUSER` | all permissions |
| `READ_WRITE` | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| `READ_ONLY` | `SELECT` |
| `NO_ACCESS` | none |

Every operation is checked against the caller's role before execution:

```python
auth.create_user("alice", "s3cret!", role=Role.READ_WRITE)
user = auth.authenticate("alice", "s3cret!")   # raises on bad creds / lockout
auth.check_permission(user, Permission.INSERT)  # raises if the role lacks it
```

This is the **principle of least privilege**: a reporting user gets `READ_ONLY`, an application service gets `READ_WRITE`, and `SUPERUSER` is reserved for administration. A locked account is denied regardless of role.

---

## Connection Pooling

Opening a connection is expensive: a TCP handshake, authentication, and per-connection server state. Under load, creating and tearing down a connection per request dominates cost. A **connection pool** keeps a bounded set of established connections and hands them out for reuse.

```
acquire() → take an idle connection (or create one, up to max)
   ... use it ...
release() → return it to the pool (do not close)
```

The pool is **bounded** — it never exceeds a maximum, which protects the server from connection exhaustion. A maintenance pass retires connections that have been idle too long or have exceeded their maximum lifetime, so the pool self-heals stale connections.

```
┌──────────────── Connection Pool (max = N) ─────────────────┐
│  [in-use] [in-use] [idle] [idle] ...                        │
│     ▲                  │                                     │
│  acquire()          release()                                │
│  maintenance: drop idle-too-long / past-max-lifetime conns   │
└──────────────────────────────────────────────────────────────┘
```

LiteDB exposes the pool through a context manager so a borrowed connection is always returned, even on error.

---

## Rate Limiting

Pooling bounds *concurrent* resource use; rate limiting bounds the *request rate* of a single client, protecting the server from accidental or malicious floods. LiteDB uses a **token bucket**:

- The bucket holds up to `capacity` tokens and refills at a fixed `rate` (tokens/second).
- Each request consumes one or more tokens; if the bucket is empty, the request is rejected (or must wait).

```
allow(cost=1):
    refill bucket based on elapsed time (capped at capacity)
    if tokens >= cost: tokens -= cost; return True
    else: return False
```

The bucket's `capacity` permits short **bursts** (a backlog of tokens), while the `rate` caps the **sustained** throughput. This is the same algorithm used by Nginx `limit_req` and AWS API Gateway.

---

## Putting It Together

A request passes through all four layers in order:

```
client connects
   │
   ▼
authenticate (PBKDF2 verify; reject on bad creds / lockout)   ← who?
   │
   ▼
acquire a pooled connection                                   ← resource control
   │
   ▼
rate-limit check (token bucket)                               ← abuse control
   │
   ▼
authorize the command against the role (RBAC)                 ← what?
   │
   ▼
execute → release connection → audit
```

---

## Real-World Systems

| Mechanism | Where it appears in production |
|-----------|-------------------------------|
| PBKDF2 password hashing | Django, PostgreSQL `scram-sha-256`, the bcrypt/scrypt family |
| Role-Based Access Control | PostgreSQL roles + `GRANT`, MySQL privileges, cloud IAM |
| Connection pooling | PgBouncer, ProxySQL, HikariCP, application-side pools |
| Token-bucket rate limiting | Nginx `limit_req`, AWS API Gateway, Redis-based limiters |

---

## Review Questions

1. Why is a deliberately *slow* hash (PBKDF2) preferred over a fast one (SHA-256) for passwords?
2. What attack does a per-user salt defeat that iteration count alone does not?
3. Why grant permissions to roles rather than directly to users?
4. What does a bounded connection pool protect the server from?
5. How do a token bucket's `capacity` and `rate` parameters control bursts versus sustained load differently?

---

**Implemented in:** `litedb-python/auth_pool.py`  
**Java:** `com.litedb.auth.AuthManager`

**Next:** [Module 12 — Metrics & Observability](../12-observability/metrics-and-observability.md) covers how to measure and trace what the database is doing.
