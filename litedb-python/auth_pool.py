"""
auth_pool.py — Authentication, Authorization & Connection Pooling

CONCEPT:
  Production databases need three security/resource layers:

  1. AUTHENTICATION — "Who are you?"
     Verify identity via username + password (hashed with bcrypt/PBKDF2).
     We use PBKDF2-HMAC-SHA256 (Python stdlib, no extra deps).

  2. AUTHORIZATION — "What can you do?"
     Role-Based Access Control (RBAC):
       - Roles: SUPERUSER, READ_WRITE, READ_ONLY, NO_ACCESS
       - Permissions: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ADMIN
       - Users are assigned roles; roles grant permissions

  3. CONNECTION POOLING — "Efficient resource management"
     Opening a TCP connection + TLS handshake + auth is expensive (~10ms).
     A connection pool keeps N connections open and reuses them.

     Pool states:
       IDLE     — connection is open, waiting to be used
       IN_USE   — connection is checked out by a client
       CLOSED   — connection is dead, needs replacement

     Pool algorithms:
       - Min/max pool size
       - Acquire timeout (fail fast if pool exhausted)
       - Max lifetime (recycle old connections)
       - Health check (ping before returning from pool)

  4. RATE LIMITING — "Prevent abuse"
     Token bucket algorithm:
       - Bucket holds up to `capacity` tokens
       - Tokens refill at `rate` tokens/second
       - Each request consumes 1 token
       - If bucket empty → reject request (429 Too Many Requests)

  Real databases:
    PostgreSQL: pg_hba.conf for auth, connection pool via PgBouncer
    MySQL:      mysql.user table, ProxySQL for pooling
    MongoDB:    SCRAM-SHA-256 auth, built-in connection pool
    Redis:      requirepass, redis-py connection pool
"""

import hashlib
import hmac
import os
import time
import threading
import queue
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable, Any


# ======================================================================= #
#  Authentication                                                          #
# ======================================================================= #

class AuthError(Exception):
    pass


def _hash_password(password: str, salt: bytes = None) -> tuple[bytes, bytes]:
    """
    Hash a password using PBKDF2-HMAC-SHA256.
    Returns (salt, hash).

    PBKDF2 parameters:
      - 260,000 iterations (OWASP 2023 recommendation)
      - 32-byte output
      - SHA-256 hash function
    """
    if salt is None:
        salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260_000, dklen=32)
    return salt, dk


def _verify_password(password: str, salt: bytes, stored_hash: bytes) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    _, computed = _hash_password(password, salt)
    return hmac.compare_digest(computed, stored_hash)


# ======================================================================= #
#  Authorization (RBAC)                                                    #
# ======================================================================= #

class Permission(Enum):
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    CREATE = auto()
    DROP   = auto()
    ADMIN  = auto()


class Role(Enum):
    SUPERUSER  = "SUPERUSER"
    READ_WRITE = "READ_WRITE"
    READ_ONLY  = "READ_ONLY"
    NO_ACCESS  = "NO_ACCESS"


ROLE_PERMISSIONS: dict[Role, set[Permission]] = {
    Role.SUPERUSER:  set(Permission),  # all permissions
    Role.READ_WRITE: {Permission.SELECT, Permission.INSERT,
                      Permission.UPDATE, Permission.DELETE},
    Role.READ_ONLY:  {Permission.SELECT},
    Role.NO_ACCESS:  set(),
}


@dataclass
class User:
    username: str
    salt: bytes
    password_hash: bytes
    role: Role
    created_at: float = field(default_factory=time.time)
    last_login: Optional[float] = None
    login_attempts: int = 0
    locked: bool = False

    def has_permission(self, perm: Permission) -> bool:
        if self.locked:
            return False
        return perm in ROLE_PERMISSIONS[self.role]

    def __repr__(self):
        return f"User({self.username!r}, role={self.role.value}, locked={self.locked})"


class AuthManager:
    """
    Manages user accounts, password hashing, and login.

    Features:
      - PBKDF2-HMAC-SHA256 password hashing
      - Account lockout after N failed attempts
      - Role-based access control
      - Audit log of all auth events
    """

    MAX_LOGIN_ATTEMPTS = 5
    LOCKOUT_DURATION   = 300  # seconds

    def __init__(self):
        self._users: dict[str, User] = {}
        self._lock = threading.RLock()
        self._audit_log: list[dict] = []

    def create_user(self, username: str, password: str, role: Role = Role.READ_ONLY) -> User:
        """Create a new user with hashed password."""
        with self._lock:
            if username in self._users:
                raise AuthError(f"User {username!r} already exists")
            if len(password) < 8:
                raise AuthError("Password must be at least 8 characters")
            salt, pw_hash = _hash_password(password)
            user = User(username=username, salt=salt, password_hash=pw_hash, role=role)
            self._users[username] = user
            self._audit("CREATE_USER", username, success=True)
            return user

    def authenticate(self, username: str, password: str) -> User:
        """
        Authenticate a user. Returns User on success, raises AuthError on failure.
        Implements account lockout after MAX_LOGIN_ATTEMPTS failures.
        """
        with self._lock:
            user = self._users.get(username)
            if user is None:
                self._audit("LOGIN_FAILED", username, success=False, reason="user_not_found")
                # Constant-time dummy check to prevent user enumeration
                _verify_password(password, os.urandom(16), os.urandom(32))
                raise AuthError("Invalid username or password")

            if user.locked:
                self._audit("LOGIN_BLOCKED", username, success=False, reason="account_locked")
                raise AuthError(f"Account locked. Contact administrator.")

            if not _verify_password(password, user.salt, user.password_hash):
                user.login_attempts += 1
                if user.login_attempts >= self.MAX_LOGIN_ATTEMPTS:
                    user.locked = True
                    self._audit("ACCOUNT_LOCKED", username, success=False,
                                reason=f"too_many_failures ({user.login_attempts})")
                else:
                    self._audit("LOGIN_FAILED", username, success=False,
                                reason=f"bad_password (attempt {user.login_attempts})")
                raise AuthError("Invalid username or password")

            # Success
            user.login_attempts = 0
            user.last_login = time.time()
            self._audit("LOGIN_OK", username, success=True)
            return user

    def change_password(self, username: str, old_password: str, new_password: str):
        """Change a user's password (requires old password)."""
        user = self.authenticate(username, old_password)
        with self._lock:
            if len(new_password) < 8:
                raise AuthError("Password must be at least 8 characters")
            salt, pw_hash = _hash_password(new_password)
            user.salt = salt
            user.password_hash = pw_hash
            self._audit("PASSWORD_CHANGED", username, success=True)

    def set_role(self, admin_user: User, target_username: str, new_role: Role):
        """Change a user's role (requires ADMIN permission)."""
        if not admin_user.has_permission(Permission.ADMIN):
            raise AuthError(f"User {admin_user.username!r} lacks ADMIN permission")
        with self._lock:
            if target_username not in self._users:
                raise AuthError(f"User {target_username!r} not found")
            self._users[target_username].role = new_role
            self._audit("ROLE_CHANGED", target_username, success=True,
                        extra={"new_role": new_role.value, "by": admin_user.username})

    def unlock_user(self, admin_user: User, target_username: str):
        """Unlock a locked account (requires ADMIN permission)."""
        if not admin_user.has_permission(Permission.ADMIN):
            raise AuthError("ADMIN permission required")
        with self._lock:
            user = self._users.get(target_username)
            if user:
                user.locked = False
                user.login_attempts = 0
                self._audit("ACCOUNT_UNLOCKED", target_username, success=True)

    def check_permission(self, user: User, perm: Permission, resource: str = ""):
        """Raise AuthError if user lacks the required permission."""
        if not user.has_permission(perm):
            self._audit("PERMISSION_DENIED", user.username, success=False,
                        extra={"permission": perm.name, "resource": resource})
            raise AuthError(
                f"User {user.username!r} lacks {perm.name} permission on {resource!r}"
            )
        self._audit("PERMISSION_GRANTED", user.username, success=True,
                    extra={"permission": perm.name, "resource": resource})

    def _audit(self, event: str, username: str, success: bool, reason: str = "", extra: dict = None):
        self._audit_log.append({
            "timestamp": time.time(),
            "event": event,
            "username": username,
            "success": success,
            "reason": reason,
            **(extra or {}),
        })

    def get_audit_log(self, limit: int = 20) -> list[dict]:
        with self._lock:
            return self._audit_log[-limit:]

    def list_users(self) -> list[dict]:
        with self._lock:
            return [
                {"username": u.username, "role": u.role.value,
                 "locked": u.locked, "last_login": u.last_login}
                for u in self._users.values()
            ]


# ======================================================================= #
#  Connection Pool                                                         #
# ======================================================================= #

class ConnectionState(Enum):
    IDLE    = "IDLE"
    IN_USE  = "IN_USE"
    CLOSED  = "CLOSED"


@dataclass
class PooledConnection:
    """Represents one connection in the pool."""
    conn_id: int
    state: ConnectionState = ConnectionState.IDLE
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    use_count: int = 0
    _owner: Optional[str] = None  # who checked it out

    def is_expired(self, max_lifetime: float) -> bool:
        return (time.time() - self.created_at) > max_lifetime

    def is_idle_too_long(self, max_idle: float) -> bool:
        return (time.time() - self.last_used) > max_idle

    def __repr__(self):
        return (f"Conn(id={self.conn_id}, state={self.state.value}, "
                f"uses={self.use_count}, age={time.time()-self.created_at:.1f}s)")


class ConnectionPool:
    """
    A database connection pool.

    Algorithm:
      - Maintain a pool of min_size to max_size connections
      - acquire(): get an idle connection (or create one up to max_size)
      - release(): return connection to pool
      - Connections exceeding max_lifetime are recycled
      - Background thread closes excess idle connections

    This simulates what PgBouncer, HikariCP, and SQLAlchemy pool do.
    """

    def __init__(
        self,
        min_size: int = 2,
        max_size: int = 10,
        acquire_timeout: float = 5.0,
        max_lifetime: float = 3600.0,  # 1 hour
        max_idle_time: float = 600.0,  # 10 minutes
        connect_fn: Optional[Callable[[], Any]] = None,
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.acquire_timeout = acquire_timeout
        self.max_lifetime = max_lifetime
        self.max_idle_time = max_idle_time
        self._connect_fn = connect_fn or (lambda: None)

        self._connections: list[PooledConnection] = []
        self._lock = threading.RLock()
        self._available = threading.Semaphore(max_size)
        self._next_id = 1

        # Stats
        self.stats_acquired = 0
        self.stats_released = 0
        self.stats_created = 0
        self.stats_destroyed = 0
        self.stats_timeouts = 0

        # Pre-warm with min_size connections
        for _ in range(min_size):
            self._create_connection()

        # Background maintenance thread
        threading.Thread(target=self._maintenance_loop, daemon=True).start()

    def _create_connection(self) -> PooledConnection:
        """Create a new connection and add to pool."""
        conn = PooledConnection(conn_id=self._next_id)
        self._next_id += 1
        self._connect_fn()  # simulate actual connection
        self._connections.append(conn)
        self.stats_created += 1
        return conn

    def acquire(self, owner: str = "unknown") -> PooledConnection:
        """
        Get a connection from the pool.
        Blocks up to acquire_timeout seconds.
        Raises TimeoutError if no connection available.
        """
        deadline = time.time() + self.acquire_timeout
        while time.time() < deadline:
            with self._lock:
                # Find an idle connection
                for conn in self._connections:
                    if conn.state == ConnectionState.IDLE:
                        if conn.is_expired(self.max_lifetime):
                            # Recycle expired connection
                            conn.state = ConnectionState.CLOSED
                            self._connections.remove(conn)
                            self.stats_destroyed += 1
                            new_conn = self._create_connection()
                            new_conn.state = ConnectionState.IN_USE
                            new_conn._owner = owner
                            new_conn.use_count += 1
                            new_conn.last_used = time.time()
                            self.stats_acquired += 1
                            return new_conn
                        conn.state = ConnectionState.IN_USE
                        conn._owner = owner
                        conn.use_count += 1
                        conn.last_used = time.time()
                        self.stats_acquired += 1
                        return conn

                # No idle connection — create one if under max_size
                if len(self._connections) < self.max_size:
                    conn = self._create_connection()
                    conn.state = ConnectionState.IN_USE
                    conn._owner = owner
                    conn.use_count += 1
                    conn.last_used = time.time()
                    self.stats_acquired += 1
                    return conn

            # Pool exhausted — wait a bit and retry
            time.sleep(0.01)

        self.stats_timeouts += 1
        raise TimeoutError(
            f"Could not acquire connection within {self.acquire_timeout}s "
            f"(pool size: {len(self._connections)}/{self.max_size})"
        )

    def release(self, conn: PooledConnection):
        """Return a connection to the pool."""
        with self._lock:
            if conn.state == ConnectionState.IN_USE:
                conn.state = ConnectionState.IDLE
                conn._owner = None
                conn.last_used = time.time()
                self.stats_released += 1

    def close_all(self):
        """Close all connections (called on shutdown)."""
        with self._lock:
            for conn in self._connections:
                conn.state = ConnectionState.CLOSED
                self.stats_destroyed += 1
            self._connections.clear()

    def _maintenance_loop(self):
        """Background thread: close excess idle connections."""
        while True:
            time.sleep(30)
            with self._lock:
                idle_conns = [c for c in self._connections
                              if c.state == ConnectionState.IDLE]
                # Keep at least min_size connections
                excess = len(idle_conns) - self.min_size
                for conn in idle_conns[:excess]:
                    if conn.is_idle_too_long(self.max_idle_time):
                        conn.state = ConnectionState.CLOSED
                        self._connections.remove(conn)
                        self.stats_destroyed += 1

    def pool_stats(self) -> dict:
        with self._lock:
            idle = sum(1 for c in self._connections if c.state == ConnectionState.IDLE)
            in_use = sum(1 for c in self._connections if c.state == ConnectionState.IN_USE)
            return {
                "total": len(self._connections),
                "idle": idle,
                "in_use": in_use,
                "min_size": self.min_size,
                "max_size": self.max_size,
                "acquired": self.stats_acquired,
                "released": self.stats_released,
                "created": self.stats_created,
                "destroyed": self.stats_destroyed,
                "timeouts": self.stats_timeouts,
            }

    def __repr__(self):
        s = self.pool_stats()
        return f"ConnectionPool(idle={s['idle']}, in_use={s['in_use']}, max={s['max_size']})"


class PooledConnectionContext:
    """Context manager for safe connection acquire/release."""

    def __init__(self, pool: ConnectionPool, owner: str = ""):
        self._pool = pool
        self._owner = owner
        self._conn: Optional[PooledConnection] = None

    def __enter__(self) -> PooledConnection:
        self._conn = self._pool.acquire(self._owner)
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._pool.release(self._conn)
        return False


# ======================================================================= #
#  Rate Limiter (Token Bucket)                                             #
# ======================================================================= #

class RateLimiter:
    """
    Token bucket rate limiter.

    Algorithm:
      - Bucket holds up to `capacity` tokens
      - Tokens refill at `rate` tokens/second
      - Each request consumes `cost` tokens (default 1)
      - If bucket has enough tokens → allow, else → deny

    Used by: AWS API Gateway, Nginx, Redis (INCR + EXPIRE pattern)
    """

    def __init__(self, rate: float, capacity: float):
        """
        rate:     tokens added per second
        capacity: maximum tokens in bucket
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity  # start full
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self.stats_allowed = 0
        self.stats_denied = 0

    def _refill(self):
        """Add tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.rate
        self._tokens = min(self.capacity, self._tokens + new_tokens)
        self._last_refill = now

    def allow(self, cost: float = 1.0) -> bool:
        """
        Try to consume `cost` tokens.
        Returns True if allowed, False if rate limited.
        """
        with self._lock:
            self._refill()
            if self._tokens >= cost:
                self._tokens -= cost
                self.stats_allowed += 1
                return True
            self.stats_denied += 1
            return False

    def tokens_available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def stats(self) -> dict:
        return {
            "rate": self.rate,
            "capacity": self.capacity,
            "tokens_available": round(self.tokens_available(), 2),
            "allowed": self.stats_allowed,
            "denied": self.stats_denied,
        }


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("AUTHENTICATION, AUTHORIZATION & CONNECTION POOL DEMO")
    print("=" * 60)

    # ------------------------------------------------------------------ #
    # Part 1: Authentication                                              #
    # ------------------------------------------------------------------ #
    print("\n[Part 1] Authentication — PBKDF2 password hashing")
    auth = AuthManager()

    # Create users
    admin = auth.create_user("admin", "Admin@1234!", Role.SUPERUSER)
    alice = auth.create_user("alice", "Alice@5678!", Role.READ_WRITE)
    bob   = auth.create_user("bob",   "Bob@9012!",  Role.READ_ONLY)
    print(f"  Created users: {[u['username'] for u in auth.list_users()]}")

    # Successful login
    print("\n  Login attempts:")
    try:
        user = auth.authenticate("alice", "Alice@5678!")
        print(f"  ✓ alice logged in: {user}")
    except AuthError as e:
        print(f"  ✗ {e}")

    # Wrong password
    try:
        auth.authenticate("alice", "wrongpassword")
    except AuthError as e:
        print(f"  ✗ alice wrong password: {e}")

    # Account lockout
    print("\n  Account lockout after 5 failed attempts:")
    for i in range(5):
        try:
            auth.authenticate("bob", "wrongpassword")
        except AuthError as e:
            print(f"    Attempt {i+1}: {e}")

    # ------------------------------------------------------------------ #
    # Part 2: Authorization (RBAC)                                        #
    # ------------------------------------------------------------------ #
    print("\n[Part 2] Authorization — Role-Based Access Control")

    alice_user = auth.authenticate("alice", "Alice@5678!")
    admin_user = auth.authenticate("admin", "Admin@1234!")

    print(f"\n  alice role: {alice_user.role.value}")
    print(f"  alice permissions: {[p.name for p in ROLE_PERMISSIONS[alice_user.role]]}")

    # Check permissions
    for perm in [Permission.SELECT, Permission.INSERT, Permission.DROP, Permission.ADMIN]:
        allowed = alice_user.has_permission(perm)
        print(f"  alice can {perm.name}? {'✓' if allowed else '✗'}")

    # Admin changes alice's role
    print(f"\n  Admin demotes alice to READ_ONLY...")
    auth.set_role(admin_user, "alice", Role.READ_ONLY)
    alice_user = auth.authenticate("alice", "Alice@5678!")
    print(f"  alice new role: {alice_user.role.value}")
    print(f"  alice can INSERT? {'✓' if alice_user.has_permission(Permission.INSERT) else '✗'}")

    # Permission check with error
    try:
        auth.check_permission(alice_user, Permission.INSERT, "users_table")
    except AuthError as e:
        print(f"  ✗ Permission denied: {e}")

    # ------------------------------------------------------------------ #
    # Part 3: Connection Pool                                             #
    # ------------------------------------------------------------------ #
    print("\n[Part 3] Connection Pool")

    connect_count = [0]
    def mock_connect():
        connect_count[0] += 1

    pool = ConnectionPool(min_size=2, max_size=5, connect_fn=mock_connect)
    print(f"  Pool initialized: {pool}")
    print(f"  Connections created on init: {connect_count[0]}")

    # Acquire and release connections
    print("\n  Acquiring 4 connections...")
    conns = []
    for i in range(4):
        conn = pool.acquire(owner=f"worker-{i}")
        conns.append(conn)
        print(f"    Acquired: {conn}")

    print(f"  Pool stats: {pool.pool_stats()}")

    # Release 2
    print("\n  Releasing 2 connections...")
    pool.release(conns[0])
    pool.release(conns[1])
    print(f"  Pool stats: {pool.pool_stats()}")

    # Context manager
    print("\n  Using context manager:")
    with PooledConnectionContext(pool, owner="ctx-worker") as conn:
        print(f"    Got connection: {conn}")
    print(f"    After context: {pool.pool_stats()}")

    # Release remaining held connections before exhaustion test
    pool.release(conns[2])
    pool.release(conns[3])

    # Pool exhaustion
    print("\n  Testing pool exhaustion (max_size=5)...")
    all_conns = [pool.acquire(f"exhaust-{i}") for i in range(5)]
    try:
        # Pool is now full (5/5 in use) — next acquire must timeout
        overflow_pool = ConnectionPool(min_size=1, max_size=2,
                                       acquire_timeout=0.1, connect_fn=mock_connect)
        c1 = overflow_pool.acquire("t1")
        c2 = overflow_pool.acquire("t2")
        overflow_pool.acquire("t3")  # should timeout
    except TimeoutError as e:
        print(f"  ✗ Pool exhausted: {e}")

    for c in all_conns:
        pool.release(c)

    # Concurrent access
    print("\n  Concurrent pool access (10 threads, max_size=5)...")
    results = []
    def use_pool(tid):
        try:
            with PooledConnectionContext(pool, f"thread-{tid}") as conn:
                time.sleep(0.02)  # simulate query
                results.append(f"Thread {tid}: used conn {conn.conn_id}")
        except TimeoutError:
            results.append(f"Thread {tid}: TIMEOUT")

    threads = [threading.Thread(target=use_pool, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    for r in sorted(results): print(f"    {r}")
    print(f"  Final pool stats: {pool.pool_stats()}")

    # ------------------------------------------------------------------ #
    # Part 4: Rate Limiter                                                #
    # ------------------------------------------------------------------ #
    print("\n[Part 4] Rate Limiter — Token Bucket")
    limiter = RateLimiter(rate=10.0, capacity=10.0)  # 10 req/sec, burst of 10

    print(f"  Initial tokens: {limiter.tokens_available():.1f}")

    # Burst: 10 requests immediately (should all pass)
    allowed = sum(1 for _ in range(10) if limiter.allow())
    denied  = sum(1 for _ in range(5)  if not limiter.allow())
    print(f"  Burst 10 requests: {allowed} allowed")
    print(f"  Next 5 requests (bucket empty): {denied} denied")

    # Wait for refill
    time.sleep(0.5)
    print(f"  After 0.5s wait, tokens: {limiter.tokens_available():.1f}")
    allowed2 = sum(1 for _ in range(5) if limiter.allow())
    print(f"  5 more requests: {allowed2} allowed")
    print(f"  Rate limiter stats: {limiter.stats()}")

    # ------------------------------------------------------------------ #
    # Part 5: Audit log                                                   #
    # ------------------------------------------------------------------ #
    print("\n[Part 5] Audit log (last 10 events)")
    for entry in auth.get_audit_log(10):
        ts = time.strftime("%H:%M:%S", time.localtime(entry["timestamp"]))
        status = "✓" if entry["success"] else "✗"
        print(f"  {ts} {status} {entry['event']:20s} user={entry['username']}")

    print("\n[Done] Auth & connection pool demo complete.")
    print("\nKey insights:")
    print("  1. PBKDF2 with 260K iterations makes brute-force impractical")
    print("  2. Constant-time comparison prevents timing attacks")
    print("  3. Account lockout prevents brute-force login attacks")
    print("  4. RBAC: roles grant permissions, users get roles")
    print("  5. Connection pool: reuse expensive connections (saves ~10ms/query)")
    print("  6. Token bucket: allows bursts while enforcing average rate")
    print("  7. Audit log: every auth event recorded for compliance/forensics")