"""
replication.py — Async WAL-Based Replication

CONCEPT:
  Replication streams WAL entries from a primary node to one or more
  replica nodes. This is how MySQL binlog replication, PostgreSQL
  streaming replication, and Cassandra hinted handoff work.

  Architecture:
    Primary:
      - Writes go to WAL + MemTable as normal
      - A ReplicationPublisher tails the WAL and sends new entries
        to all connected replicas over TCP

    Replica:
      - A ReplicationSubscriber connects to the primary
      - Receives WAL entries and applies them to its own LSM engine
      - Maintains a "replication offset" (last applied sequence number)
      - On reconnect, sends its offset so primary can replay missed entries

  Consistency model: ASYNC replication
    - Primary does NOT wait for replica to acknowledge before returning OK
    - This means replica may lag behind primary (eventual consistency)
    - If primary crashes before replica receives an entry → data loss possible
    - For stronger guarantees: SYNC replication (wait for N replicas to ack)

  This is the same trade-off as:
    - MySQL: async vs semi-sync vs sync replication
    - Cassandra: ONE vs QUORUM vs ALL consistency levels
    - Kafka: acks=0 vs acks=1 vs acks=all

  Wire protocol (primary → replica):
    Each WAL entry is sent as a JSON line:
      {"seq": 42, "op": "SET", "key": "name", "val": "Alice"}\\n

    Replica sends its current offset on connect:
      OFFSET 41\\n

    Primary responds with all entries after offset 41.
"""

import sys
import os
import json
import socket
import threading
import time
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402

from wal import WALEntry  # type: ignore


# ======================================================================= #
#  PRIMARY SIDE: ReplicationPublisher                                      #
# ======================================================================= #

class ReplicationPublisher:
    """
    Runs on the primary. Accepts replica connections and streams WAL entries.

    When a replica connects:
      1. Replica sends: OFFSET <last_applied_seq>
      2. Publisher replays all WAL entries with seq > last_applied_seq
      3. Publisher then streams new entries as they arrive (tail mode)

    Thread model:
      - One thread per replica connection
      - WAL entries are pushed via a queue to each replica thread
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._replicas: list["ReplicaConnection"] = []
        self._lock = threading.Lock()
        self._running = False
        self._server_thread: threading.Thread | None = None

        # In-memory WAL buffer for replay (in production: read from WAL file)
        self._wal_buffer: list[WALEntry] = []
        self._wal_lock = threading.Lock()

    def start(self):
        """Start the replication server in a background thread."""
        self._running = True
        self._server_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._server_thread.start()
        print(f"[Replication] Publisher listening on {self.host}:{self.port}")

    def publish(self, entry: WALEntry):
        """
        Called by the primary after every WAL append.
        Buffers the entry and fans it out to all connected replicas.
        """
        with self._wal_lock:
            self._wal_buffer.append(entry)

        with self._lock:
            dead = []
            for replica in self._replicas:
                try:
                    replica.enqueue(entry)
                except Exception:
                    dead.append(replica)
            for r in dead:
                self._replicas.remove(r)

    def _accept_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(16)
        sock.settimeout(1.0)

        while self._running:
            try:
                conn, addr = sock.accept()
                print(f"[Replication] Replica connected from {addr}")
                rc = ReplicaConnection(conn, addr, self._wal_buffer, self._wal_lock)
                with self._lock:
                    self._replicas.append(rc)
                rc.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    print(f"[Replication] Accept error: {e}")

        sock.close()

    def stop(self):
        self._running = False


class ReplicaConnection(threading.Thread):
    """
    Manages the connection to one replica.
    Handles initial catch-up (replay from offset) then streams new entries.
    """

    def __init__(self, conn: socket.socket, addr: tuple,
                 wal_buffer: list[WALEntry], wal_lock: threading.Lock):
        super().__init__(daemon=True)
        self.conn = conn
        self.addr = addr
        self._wal_buffer = wal_buffer
        self._wal_lock = wal_lock
        self._queue: list[WALEntry] = []
        self._queue_lock = threading.Lock()
        self._queue_event = threading.Event()

    def enqueue(self, entry: WALEntry):
        """Called by publisher to push a new entry to this replica."""
        with self._queue_lock:
            self._queue.append(entry)
        self._queue_event.set()

    def run(self):
        try:
            # Step 1: Read replica's current offset
            f = self.conn.makefile("r")
            first_line = f.readline().strip()

            last_seq = -1
            if first_line.startswith("OFFSET "):
                try:
                    last_seq = int(first_line.split(" ", 1)[1])
                except ValueError:
                    pass

            print(f"[Replication] Replica {self.addr} at offset {last_seq}")

            # Step 2: Replay missed entries
            with self._wal_lock:
                missed = [e for e in self._wal_buffer if e.sequence > last_seq]

            for entry in missed:
                self._send_entry(entry)

            print(f"[Replication] Replayed {len(missed)} missed entries to {self.addr}")

            # Step 3: Stream new entries as they arrive
            while True:
                self._queue_event.wait(timeout=5.0)
                self._queue_event.clear()

                with self._queue_lock:
                    pending = list(self._queue)
                    self._queue.clear()

                for entry in pending:
                    self._send_entry(entry)

        except (BrokenPipeError, ConnectionResetError):
            print(f"[Replication] Replica {self.addr} disconnected")
        except Exception as e:
            print(f"[Replication] Replica {self.addr} error: {e}")
        finally:
            self.conn.close()

    def _send_entry(self, entry: WALEntry):
        line = json.dumps(entry.to_dict()) + "\n"
        self.conn.sendall(line.encode("utf-8"))


# ======================================================================= #
#  REPLICA SIDE: ReplicationSubscriber                                     #
# ======================================================================= #

class ReplicationSubscriber:
    """
    Runs on the replica. Connects to the primary and applies WAL entries.

    On connect: sends current offset so primary can replay missed entries.
    Then continuously receives and applies new entries.
    """

    def __init__(self, primary_host: str, primary_port: int,
                 apply_fn: Callable[[WALEntry], None],
                 initial_offset: int = -1):
        """
        apply_fn: called for each received WAL entry to apply it to the replica engine
        initial_offset: last sequence number already applied (-1 = start from beginning)
        """
        self.primary_host = primary_host
        self.primary_port = primary_port
        self._apply_fn = apply_fn
        self._offset = initial_offset
        self._running = False
        self._thread: threading.Thread | None = None
        self._entries_applied = 0

    def start(self):
        """Start replication in a background thread."""
        self._running = True
        self._thread = threading.Thread(target=self._replication_loop, daemon=True)
        self._thread.start()
        print(f"[Replica] Connecting to primary {self.primary_host}:{self.primary_port}")

    def _replication_loop(self):
        """Connect to primary, send offset, receive and apply entries."""
        while self._running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.primary_host, self.primary_port))

                # Send our current offset
                offset_msg = f"OFFSET {self._offset}\n"
                sock.sendall(offset_msg.encode("utf-8"))
                print(f"[Replica] Sent offset {self._offset} to primary")

                # Receive and apply entries
                f = sock.makefile("r")
                while self._running:
                    line = f.readline()
                    if not line:
                        break  # primary disconnected

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        d = json.loads(line)
                        entry = WALEntry.from_dict(d)
                        self._apply_fn(entry)
                        self._offset = entry.sequence
                        self._entries_applied += 1
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"[Replica] Bad entry: {e}")

                sock.close()

            except ConnectionRefusedError:
                print(f"[Replica] Primary not available, retrying in 2s...")
                time.sleep(2)
            except Exception as e:
                print(f"[Replica] Error: {e}, retrying in 2s...")
                time.sleep(2)

    def stop(self):
        self._running = False

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def entries_applied(self) -> int:
        return self._entries_applied


# ======================================================================= #
#  DEMO — simulate primary + replica in the same process                  #
# ======================================================================= #

if __name__ == "__main__":
    import tempfile, shutil

    print("=" * 60)
    print("REPLICATION DEMO")
    print("=" * 60)

    # --- Setup: two in-memory stores ---
    primary_store: dict[str, str] = {}
    replica_store: dict[str, str] = {}

    # --- Primary: start replication publisher ---
    publisher = ReplicationPublisher("127.0.0.1", 17379)
    publisher.start()
    time.sleep(0.1)

    # --- Replica: apply function ---
    def apply_to_replica(entry: WALEntry):
        if entry.operation == "SET":
            replica_store[entry.key] = entry.value or ""
            print(f"  [Replica] Applied SET {entry.key!r} = {entry.value!r} (seq={entry.sequence})")
        elif entry.operation == "DELETE":
            replica_store.pop(entry.key, None)
            print(f"  [Replica] Applied DELETE {entry.key!r} (seq={entry.sequence})")

    # --- Replica: start subscriber ---
    subscriber = ReplicationSubscriber("127.0.0.1", 17379, apply_to_replica, initial_offset=-1)
    subscriber.start()
    time.sleep(0.2)

    # --- Step 1: Write to primary ---
    print("\n[Step 1] Writing to primary...")
    seq = 0
    for key, value in [("name", "Alice"), ("age", "30"), ("city", "New York")]:
        primary_store[key] = value
        entry = WALEntry(seq, "SET", key, value)
        seq += 1
        publisher.publish(entry)
        print(f"  [Primary] SET {key!r} = {value!r}")

    time.sleep(0.3)  # let replication catch up

    # --- Step 2: Compare primary vs replica ---
    print("\n[Step 2] Comparing primary vs replica state...")
    print(f"  Primary store: {primary_store}")
    print(f"  Replica store: {replica_store}")
    match = primary_store == replica_store
    print(f"  Stores match: {match} ✓" if match else f"  Stores match: {match} ✗ (replication lag)")

    # --- Step 3: Delete on primary ---
    print("\n[Step 3] Deleting 'age' on primary...")
    del primary_store["age"]
    entry = WALEntry(seq, "DELETE", "age")
    seq += 1
    publisher.publish(entry)
    time.sleep(0.2)

    print(f"  Primary store: {primary_store}")
    print(f"  Replica store: {replica_store}")

    # --- Step 4: Show replication lag concept ---
    print("\n[Step 4] Replication lag demonstration...")
    print("  Writing 5 entries rapidly to primary (replica may lag)...")
    for i in range(5):
        key = f"rapid_{i}"
        primary_store[key] = f"val_{i}"
        entry = WALEntry(seq, "SET", key, f"val_{i}")
        seq += 1
        publisher.publish(entry)

    print(f"  Primary has {len(primary_store)} keys immediately")
    time.sleep(0.1)
    print(f"  Replica has {len(replica_store)} keys after 100ms (async replication)")
    time.sleep(0.3)
    print(f"  Replica has {len(replica_store)} keys after 400ms total")
    print(f"  Entries applied by replica: {subscriber.entries_applied}")

    subscriber.stop()
    publisher.stop()

    print("\n[Done] Replication demo complete.")
    print("\nKey insights:")
    print("  1. Primary writes to WAL, then publishes to replicas asynchronously")
    print("  2. Replica sends its offset on connect → primary replays missed entries")
    print("  3. Async replication = eventual consistency (replica may lag)")
    print("  4. Sync replication = stronger consistency but higher write latency")
    print("  5. This is the same model as MySQL binlog / PostgreSQL WAL streaming")