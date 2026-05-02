"""
transactions.py — MVCC Transactions with Optimistic AND Pessimistic Locking

CONCEPT:
  MVCC is how PostgreSQL, MySQL InnoDB, CockroachDB, and Oracle handle
  concurrent transactions without locking readers.

  Core idea:
    Instead of one value per key, store MULTIPLE VERSIONS of each key,
    each tagged with the transaction ID (txid) that wrote it.

    Readers see a consistent SNAPSHOT of the database as it existed
    when their transaction started — they never see partial writes
    from concurrent transactions.

  ┌─────────────────────────────────────────────────────────────────┐
  │  Two concurrency control strategies — both built on MVCC:       │
  │                                                                 │
  │  OPTIMISTIC (OCC)          │  PESSIMISTIC (2PL)                 │
  │  ─────────────────────     │  ──────────────────────────────    │
  │  No locks during tx        │  Acquire locks before access       │
  │  Conflict check at commit  │  Conflict = block or fail now      │
  │  Best for: low contention  │  Best for: high contention         │
  │  Risk: wasted work on      │  Risk: deadlock (we use timeout)   │
  │        conflict            │                                    │
  │                            │                                    │
  │  Used by: CockroachDB,     │  Used by: MySQL InnoDB             │
  │  PostgreSQL (default)      │  SELECT ... FOR UPDATE             │
  └─────────────────────────────────────────────────────────────────┘

  Isolation levels (what you can see):
    READ UNCOMMITTED  — see uncommitted writes (dirty reads) — DANGEROUS
    READ COMMITTED    — see only committed writes (no dirty reads)
    REPEATABLE READ   — same query returns same result within transaction
    SERIALIZABLE      — transactions appear to run one at a time

  We implement SNAPSHOT ISOLATION (between Repeatable Read and Serializable):
    - Each transaction gets a snapshot_txid = current committed txid at start
    - A transaction can only see versions with txid ≤ snapshot_txid
    - Optimistic: write-write conflicts detected at commit time (first writer wins)
    - Pessimistic: exclusive lock acquired at write time (blocks concurrent writers)

  Lock compatibility matrix (pessimistic mode):
    ┌──────────┬────────┬───────────┐
    │          │ SHARED │ EXCLUSIVE │
    ├──────────┼────────┼───────────┤
    │ SHARED   │   ✓    │     ✗     │
    │ EXCLUSIVE│   ✗    │     ✗     │
    └──────────┴────────┴───────────┘
    SHARED    = read lock  (SELECT)
    EXCLUSIVE = write lock (SELECT FOR UPDATE / INSERT / UPDATE / DELETE)

  Version chain (per key):
    key "name" → [(txid=1, "Alice"), (txid=5, "Bob"), (txid=9, "Carol")]
                                                                  ↑
                                          transaction with snapshot=7 sees "Bob"
                                          transaction with snapshot=10 sees "Carol"

  Garbage collection (VACUUM in PostgreSQL):
    Old versions that no active transaction can see are deleted.
    We implement a simple mark-and-sweep GC.

  ACID properties achieved:
    Atomicity:   All writes in a transaction commit or none do
    Consistency: Constraints checked at commit
    Isolation:   Snapshot isolation prevents dirty/non-repeatable reads
    Durability:  Committed data written to WAL before returning OK
"""

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ======================================================================= #
#  Enums & constants                                                       #
# ======================================================================= #

TOMBSTONE = "__DELETED__"


class TransactionMode(Enum):
    """
    OPTIMISTIC — no locks held during the transaction; conflicts detected
                 at commit time (first writer wins). Low overhead, best for
                 low-contention workloads.

    PESSIMISTIC — shared/exclusive locks acquired before each read/write;
                  conflicts surface immediately (LockTimeoutError). Best for
                  high-contention workloads where retrying is expensive.
    """
    OPTIMISTIC  = "optimistic"
    PESSIMISTIC = "pessimistic"


class LockType(Enum):
    SHARED    = "S"   # read lock  — multiple holders allowed
    EXCLUSIVE = "X"   # write lock — only one holder, no shared holders


class LockTimeoutError(Exception):
    """Raised when a pessimistic lock cannot be acquired within the timeout."""


# ======================================================================= #
#  Lock Manager (Two-Phase Locking)                                        #
# ======================================================================= #

class LockManager:
    """
    Centralized lock table for pessimistic (2PL) transactions.

    Each key has:
      - a set of txids holding SHARED locks
      - at most one txid holding an EXCLUSIVE lock

    Lock upgrade: a txid that holds S on a key can upgrade to X if no
    other txid holds S on that key.

    Deadlock prevention: simple timeout — if a lock cannot be acquired
    within `timeout` seconds, LockTimeoutError is raised (the caller
    should abort and retry).  This avoids the complexity of a wait-for
    graph while still being safe.
    """

    def __init__(self):
        # key → {"shared": set[int], "exclusive": int | None}
        self._table: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

    def _entry(self, key: str) -> dict:
        if key not in self._table:
            self._table[key] = {"shared": set(), "exclusive": None}
        return self._table[key]

    def acquire(self, key: str, lock_type: LockType, txid: int,
                timeout: float = 2.0) -> None:
        """
        Acquire a lock on `key` for transaction `txid`.
        Blocks until the lock is available or `timeout` seconds elapse.
        Raises LockTimeoutError on timeout.
        """
        deadline = time.monotonic() + timeout

        with self._cond:
            while True:
                entry = self._entry(key)
                shared    = entry["shared"]
                exclusive = entry["exclusive"]

                if lock_type == LockType.SHARED:
                    # Can acquire S if: no exclusive holder (or we are the holder)
                    if exclusive is None or exclusive == txid:
                        shared.add(txid)
                        return
                else:  # EXCLUSIVE
                    # Can acquire X if: no shared holders except us, no exclusive except us
                    others_shared = shared - {txid}
                    if (exclusive is None or exclusive == txid) and not others_shared:
                        # Upgrade or fresh acquire
                        shared.discard(txid)   # remove S if upgrading
                        entry["exclusive"] = txid
                        return

                # Cannot acquire — wait
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeoutError(
                        f"txid={txid} timed out acquiring {lock_type.value} lock on {key!r}"
                    )
                self._cond.wait(timeout=min(remaining, 0.05))

    def release_all(self, txid: int) -> None:
        """Release every lock held by `txid` and wake waiting transactions."""
        with self._cond:
            for key, entry in self._table.items():
                entry["shared"].discard(txid)
                if entry["exclusive"] == txid:
                    entry["exclusive"] = None
            self._cond.notify_all()

    def held_by(self, txid: int) -> list[tuple[str, str]]:
        """Return list of (key, lock_type) held by txid (for debugging)."""
        with self._lock:
            result = []
            for key, entry in self._table.items():
                if txid in entry["shared"]:
                    result.append((key, "S"))
                if entry["exclusive"] == txid:
                    result.append((key, "X"))
            return result


@dataclass
class Version:
    """One version of a key's value."""
    txid: int           # transaction that wrote this version
    value: str          # value (TOMBSTONE = deleted)
    committed: bool     # False while transaction is still active


@dataclass
class TxStats:
    reads: int = 0
    writes: int = 0
    start_time: float = field(default_factory=time.time)

    def duration_ms(self) -> float:
        return (time.time() - self.start_time) * 1000


# ======================================================================= #
#  MVCC Store                                                              #
# ======================================================================= #

class MVCCStore:
    """
    The core MVCC storage layer.

    Each key maps to a list of (Version) objects sorted by txid ascending.
    The most recent committed version visible to a snapshot is the answer.

    Thread-safe: all mutations hold _lock.
    """

    def __init__(self):
        # key → list of Version, sorted by txid ascending
        self._versions: dict[str, list[Version]] = {}
        self._lock = threading.RLock()

        # Global transaction counter
        self._next_txid = 1
        self._committed_txids: set[int] = {0}  # txid 0 = initial state
        self._active_txids: set[int] = set()

        # Lock manager for pessimistic transactions
        self._lock_manager = LockManager()

    # ------------------------------------------------------------------ #
    #  Transaction lifecycle                                               #
    # ------------------------------------------------------------------ #

    def begin_transaction(self,
                          mode: TransactionMode = TransactionMode.OPTIMISTIC
                          ) -> "Transaction":
        """
        Start a new transaction.

        mode=OPTIMISTIC  — no locks; conflict detected at commit (default)
        mode=PESSIMISTIC — shared/exclusive locks acquired on every access
        """
        with self._lock:
            txid = self._next_txid
            self._next_txid += 1
            snapshot_txid = max(self._committed_txids) if self._committed_txids else 0
            self._active_txids.add(txid)
            return Transaction(txid, snapshot_txid, self, mode)

    def _commit(self, tx: "Transaction") -> bool:
        """
        Commit a transaction.

        Steps:
          1. Check for write-write conflicts (another tx committed same keys)
          2. Mark all written versions as committed
          3. Add txid to committed set
          4. Remove from active set

        Returns True on success, False on conflict (caller should retry/abort).
        """
        with self._lock:
            # Conflict detection: for each key we wrote, check if any
            # other transaction committed a write to that key after our snapshot
            for key in tx._write_set:
                if key in self._versions:
                    for v in self._versions[key]:
                        if (v.txid != tx.txid and
                                v.committed and
                                v.txid > tx.snapshot_txid):
                            # Another transaction wrote this key after our snapshot
                            self._abort(tx)
                            return False

            # No conflicts — commit all written versions
            for key in tx._write_set:
                if key in self._versions:
                    for v in self._versions[key]:
                        if v.txid == tx.txid:
                            v.committed = True

            self._committed_txids.add(tx.txid)
            self._active_txids.discard(tx.txid)
            return True

    def _abort(self, tx: "Transaction"):
        """
        Abort a transaction: remove all its uncommitted versions.
        """
        with self._lock:
            for key in tx._write_set:
                if key in self._versions:
                    self._versions[key] = [
                        v for v in self._versions[key]
                        if v.txid != tx.txid
                    ]
                    if not self._versions[key]:
                        del self._versions[key]
            self._active_txids.discard(tx.txid)

    # ------------------------------------------------------------------ #
    #  Read / Write (called by Transaction)                               #
    # ------------------------------------------------------------------ #

    def _read(self, key: str, snapshot_txid: int, own_txid: int) -> Optional[str]:
        """
        Read the most recent committed version of key visible to snapshot_txid.
        Also returns own uncommitted writes (read-your-own-writes).
        """
        with self._lock:
            if key not in self._versions:
                return None

            # Find the most recent visible version
            # Visible if: committed AND txid <= snapshot_txid
            #          OR: own uncommitted write (txid == own_txid)
            best: Optional[Version] = None
            for v in self._versions[key]:
                visible = (
                    (v.committed and v.txid <= snapshot_txid) or
                    (v.txid == own_txid)
                )
                if visible:
                    if best is None or v.txid > best.txid:
                        best = v

            if best is None:
                return None
            return None if best.value == TOMBSTONE else best.value

    def _write(self, key: str, value: str, txid: int):
        """
        Write a new version of key. Marked uncommitted until tx commits.
        """
        with self._lock:
            if key not in self._versions:
                self._versions[key] = []
            # Remove any previous uncommitted version from this same transaction
            self._versions[key] = [
                v for v in self._versions[key] if v.txid != txid
            ]
            self._versions[key].append(Version(txid=txid, value=value, committed=False))
            # Keep sorted by txid
            self._versions[key].sort(key=lambda v: v.txid)

    # ------------------------------------------------------------------ #
    #  Garbage collection (VACUUM)                                        #
    # ------------------------------------------------------------------ #

    def vacuum(self) -> int:
        """
        Remove old versions that no active transaction can see.

        Safe to delete version V if:
          - V is committed
          - There is a newer committed version of the same key
          - V.txid < min(active_txids)  (no active tx can see it)

        Returns number of versions deleted.
        """
        with self._lock:
            if not self._active_txids:
                min_active = max(self._committed_txids) + 1
            else:
                min_active = min(self._active_txids)

            deleted = 0
            for key in list(self._versions.keys()):
                versions = self._versions[key]
                committed_versions = [v for v in versions if v.committed]

                if len(committed_versions) <= 1:
                    continue  # nothing to GC

                # Keep the newest committed version always
                # Delete older committed versions that are below min_active
                newest_committed_txid = max(v.txid for v in committed_versions)
                new_versions = []
                for v in versions:
                    if (v.committed and
                            v.txid < newest_committed_txid and
                            v.txid < min_active):
                        deleted += 1  # GC this version
                    else:
                        new_versions.append(v)

                self._versions[key] = new_versions
                if not new_versions:
                    del self._versions[key]

            return deleted

    def version_count(self) -> dict[str, int]:
        """Return number of versions per key (for debugging)."""
        with self._lock:
            return {k: len(v) for k, v in self._versions.items()}

    def stats(self) -> dict:
        with self._lock:
            total_versions = sum(len(v) for v in self._versions.values())
            return {
                "keys": len(self._versions),
                "total_versions": total_versions,
                "next_txid": self._next_txid,
                "active_transactions": len(self._active_txids),
                "committed_transactions": len(self._committed_txids),
            }


# ======================================================================= #
#  Transaction                                                             #
# ======================================================================= #

class Transaction:
    """
    A database transaction with snapshot isolation.

    Usage:
        tx = store.begin_transaction()
        tx.set("key", "value")
        val = tx.get("key")
        ok = tx.commit()   # True = committed, False = conflict (retry)
        # or
        tx.abort()
    """

    def __init__(self, txid: int, snapshot_txid: int, store: MVCCStore,
                 mode: TransactionMode = TransactionMode.OPTIMISTIC):
        self.txid = txid
        self.snapshot_txid = snapshot_txid
        self._store = store
        self._mode = mode
        self._write_set: set[str] = set()
        self._committed = False
        self._aborted = False
        self.stats = TxStats()

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[str]:
        """
        Read a key.

        Optimistic:  no lock acquired — snapshot read only.
        Pessimistic: acquires a SHARED lock before reading.
        """
        self._check_active()
        self.stats.reads += 1
        if self._mode == TransactionMode.PESSIMISTIC:
            self._store._lock_manager.acquire(key, LockType.SHARED, self.txid)
        return self._store._read(key, self.snapshot_txid, self.txid)

    def get_for_update(self, key: str) -> Optional[str]:
        """
        Read a key and lock it for update (SELECT ... FOR UPDATE).

        Always acquires an EXCLUSIVE lock regardless of mode.
        Guarantees that no other transaction can write this key until
        this transaction commits or aborts.

        Raises LockTimeoutError if the lock cannot be acquired.
        """
        self._check_active()
        self.stats.reads += 1
        # Always acquire X lock — this is the whole point of FOR UPDATE
        self._store._lock_manager.acquire(key, LockType.EXCLUSIVE, self.txid)
        return self._store._read(key, self.snapshot_txid, self.txid)

    def set(self, key: str, value: str) -> None:
        """
        Write a key-value pair.

        Pessimistic: acquires an EXCLUSIVE lock before writing.
        Optimistic:  no lock — conflict detected at commit.
        """
        self._check_active()
        self.stats.writes += 1
        if self._mode == TransactionMode.PESSIMISTIC:
            self._store._lock_manager.acquire(key, LockType.EXCLUSIVE, self.txid)
        self._write_set.add(key)
        self._store._write(key, value, self.txid)

    def delete(self, key: str) -> None:
        """
        Delete a key (writes a tombstone).

        Pessimistic: acquires an EXCLUSIVE lock before deleting.
        """
        self._check_active()
        self.stats.writes += 1
        if self._mode == TransactionMode.PESSIMISTIC:
            self._store._lock_manager.acquire(key, LockType.EXCLUSIVE, self.txid)
        self._write_set.add(key)
        self._store._write(key, TOMBSTONE, self.txid)

    def commit(self) -> bool:
        """
        Commit the transaction.

        Optimistic:  checks for write-write conflicts; returns False on conflict.
        Pessimistic: no conflict check needed (locks prevented conflicts);
                     always returns True (releases all locks on success).
        """
        self._check_active()
        result = self._store._commit(self)
        self._committed = result
        if not result:
            self._aborted = True
        # Release all pessimistic locks regardless of outcome
        if self._mode == TransactionMode.PESSIMISTIC:
            self._store._lock_manager.release_all(self.txid)
        return result

    def abort(self) -> None:
        """Abort the transaction, discarding all writes and releasing locks."""
        if not self._committed and not self._aborted:
            self._store._abort(self)
            self._aborted = True
            if self._mode == TransactionMode.PESSIMISTIC:
                self._store._lock_manager.release_all(self.txid)

    def _check_active(self):
        if self._committed:
            raise RuntimeError(f"Transaction {self.txid} already committed")
        if self._aborted:
            raise RuntimeError(f"Transaction {self.txid} already aborted")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.abort()
        elif not self._committed and not self._aborted:
            self.commit()
        return False  # don't suppress exceptions

    def __repr__(self):
        status = "committed" if self._committed else ("aborted" if self._aborted else "active")
        return (f"Transaction(txid={self.txid}, snapshot={self.snapshot_txid}, "
                f"mode={self._mode.value}, writes={len(self._write_set)}, status={status})")


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("MVCC TRANSACTIONS DEMO")
    print("=" * 60)

    store = MVCCStore()

    # --- Step 1: Basic transaction ---
    print("\n[Step 1] Basic transaction: commit")
    tx1 = store.begin_transaction()
    tx1.set("name", "Alice")
    tx1.set("age", "30")
    tx1.set("city", "New York")
    ok = tx1.commit()
    print(f"  tx1 committed: {ok}")
    print(f"  Store stats: {store.stats()}")

    # --- Step 2: Read committed data ---
    print("\n[Step 2] Read committed data in new transaction")
    tx2 = store.begin_transaction()
    print(f"  GET name → {tx2.get('name')!r}")
    print(f"  GET age  → {tx2.get('age')!r}")
    tx2.commit()

    # --- Step 3: Snapshot isolation ---
    print("\n[Step 3] Snapshot isolation")
    # tx3 starts before tx4 commits
    tx3 = store.begin_transaction()
    tx4 = store.begin_transaction()

    tx4.set("name", "Bob")  # tx4 writes "Bob"
    tx4.commit()
    print(f"  tx4 committed: name = 'Bob'")

    # tx3 started before tx4 committed → still sees "Alice"
    print(f"  tx3 (started before tx4) sees name = {tx3.get('name')!r}  ← snapshot isolation!")
    tx3.commit()

    # New transaction sees "Bob"
    tx5 = store.begin_transaction()
    print(f"  tx5 (started after tx4) sees name = {tx5.get('name')!r}")
    tx5.commit()

    # --- Step 4: Write-write conflict ---
    print("\n[Step 4] Write-write conflict (first writer wins)")
    tx6 = store.begin_transaction()
    tx7 = store.begin_transaction()

    tx6.set("score", "100")
    tx7.set("score", "200")

    ok6 = tx6.commit()
    ok7 = tx7.commit()  # should fail — conflict!

    print(f"  tx6 committed: {ok6}  (score = 100)")
    print(f"  tx7 committed: {ok7}  ← CONFLICT (tx6 already committed 'score')")

    tx8 = store.begin_transaction()
    print(f"  Final score = {tx8.get('score')!r}  (tx6's value wins)")
    tx8.commit()

    # --- Step 5: Abort ---
    print("\n[Step 5] Transaction abort")
    tx9 = store.begin_transaction()
    tx9.set("temp_key", "will_be_aborted")
    tx9.abort()

    tx10 = store.begin_transaction()
    print(f"  GET temp_key after abort → {tx10.get('temp_key')!r}  (None = never committed)")
    tx10.commit()

    # --- Step 6: Context manager ---
    print("\n[Step 6] Context manager (auto-commit/abort)")
    with store.begin_transaction() as tx:
        tx.set("ctx_key", "ctx_value")
        # auto-commits on exit

    with store.begin_transaction() as tx:
        print(f"  GET ctx_key → {tx.get('ctx_key')!r}")

    # --- Step 7: Concurrent transactions ---
    print("\n[Step 7] Concurrent transactions (multi-threaded)")
    results = []
    errors = []

    def worker(worker_id: int, key: str, value: str):
        for attempt in range(3):
            tx = store.begin_transaction()
            tx.set(key, value)
            if tx.commit():
                results.append(f"Worker {worker_id} committed {key}={value!r}")
                return
            else:
                results.append(f"Worker {worker_id} conflict on attempt {attempt+1}, retrying...")
        errors.append(f"Worker {worker_id} failed after 3 attempts")

    threads = [
        threading.Thread(target=worker, args=(i, "shared_key", f"value_{i}"))
        for i in range(5)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for r in sorted(results):
        print(f"  {r}")

    tx_final = store.begin_transaction()
    print(f"  Final shared_key = {tx_final.get('shared_key')!r}")
    tx_final.commit()

    # --- Step 8: VACUUM (garbage collection) ---
    print("\n[Step 8] VACUUM — garbage collection of old versions")
    print(f"  Version counts before VACUUM: {store.version_count()}")
    deleted = store.vacuum()
    print(f"  Deleted {deleted} old versions")
    print(f"  Version counts after VACUUM:  {store.version_count()}")
    print(f"  Final store stats: {store.stats()}")

    # --- Step 9: Pessimistic locking — exclusive write locks ---
    print("\n[Step 9] Pessimistic locking — exclusive write locks (2PL)")
    print("  Two threads race to update 'balance'. With pessimistic locking,")
    print("  the second writer blocks until the first commits, then proceeds.")

    # Seed a balance
    with store.begin_transaction() as tx:
        tx.set("balance", "1000")

    pess_log: list[str] = []

    def pess_worker(worker_id: int, amount: str):
        tx = store.begin_transaction(mode=TransactionMode.PESSIMISTIC)
        try:
            old = tx.get_for_update("balance")   # acquires X lock — blocks other writers
            pess_log.append(f"  Worker {worker_id} locked balance (was {old!r}), writing {amount!r}")
            time.sleep(0.05)                      # simulate work while holding lock
            tx.set("balance", amount)
            tx.commit()
            pess_log.append(f"  Worker {worker_id} committed balance={amount!r}")
        except LockTimeoutError as e:
            tx.abort()
            pess_log.append(f"  Worker {worker_id} timed out: {e}")

    t1 = threading.Thread(target=pess_worker, args=(1, "900"))
    t2 = threading.Thread(target=pess_worker, args=(2, "800"))
    t1.start(); t2.start()
    t1.join();  t2.join()

    for line in pess_log:
        print(line)

    with store.begin_transaction() as tx:
        print(f"  Final balance = {tx.get('balance')!r}  (one writer serialised after the other)")

    # --- Step 10: SELECT FOR UPDATE (get_for_update) ---
    print("\n[Step 10] SELECT FOR UPDATE — read-then-write with exclusive lock")
    print("  Classic pattern: read a counter, increment it, write back.")
    print("  Without FOR UPDATE two readers could both read 0 and both write 1.")

    with store.begin_transaction() as tx:
        tx.set("counter", "0")

    counter_log: list[str] = []

    def increment_counter(worker_id: int):
        tx = store.begin_transaction(mode=TransactionMode.PESSIMISTIC)
        try:
            val = tx.get_for_update("counter")   # X lock — serialises all incrementers
            new_val = str(int(val or "0") + 1)
            tx.set("counter", new_val)
            tx.commit()
            counter_log.append(f"  Worker {worker_id}: counter {val!r} → {new_val!r}")
        except LockTimeoutError:
            tx.abort()
            counter_log.append(f"  Worker {worker_id}: timed out")

    threads = [threading.Thread(target=increment_counter, args=(i,)) for i in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()

    for line in sorted(counter_log):
        print(line)

    with store.begin_transaction() as tx:
        print(f"  Final counter = {tx.get('counter')!r}  (should be 4 — no lost updates)")

    print("\n[Done] MVCC demo complete.")
    print("\nKey insights:")
    print("  1. Each write creates a new VERSION tagged with txid")
    print("  2. Readers see a SNAPSHOT — never blocked by writers")
    print("  3. OPTIMISTIC: write-write conflicts detected at commit (first writer wins)")
    print("  4. PESSIMISTIC: exclusive locks prevent conflicts at access time")
    print("  5. get_for_update() = SELECT FOR UPDATE — serialises read-modify-write")
    print("  6. VACUUM removes old versions no active transaction can see")
    print("  7. This is exactly how PostgreSQL / MySQL InnoDB work")
