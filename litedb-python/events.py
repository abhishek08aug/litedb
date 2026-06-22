"""
events.py — a per-node, human-readable event log.

Each node owns one of these and threads an `emit` into its Raft groups, shard replicas, and request
handlers. Every meaningful action records a sentence that explains *what* happened and *why* — an
election timing out, recognizing a leader, routing a write by consistent hashing, replicating an
entry, applying a committed entry, running 2PC. The dashboard polls these incrementally (by sequence
number) and shows one live feed per instance, so you can watch the distributed reasoning unfold.
"""

import threading
import time


class EventLog:
    def __init__(self, capacity: int = 600):
        self._lock = threading.Lock()
        self._events: list[dict] = []
        self._seq = 0
        self._cap = capacity

    def emit(self, category: str, message: str) -> None:
        with self._lock:
            self._seq += 1
            self._events.append({
                "seq": self._seq,
                "t": time.time(),
                "cat": category,
                "msg": message,
            })
            if len(self._events) > self._cap:
                self._events = self._events[-self._cap:]

    def since(self, after_seq: int) -> dict:
        with self._lock:
            return {
                "events": [e for e in self._events if e["seq"] > after_seq],
                "next": self._seq,
            }
