"""
hlc.py — Hybrid Logical Clock.

A monotonic, globally-comparable timestamp. It is anchored to the wall clock (nanoseconds, via the
shared monotonic-ish system clock) so timestamps are meaningful and ordered across processes, but it
never goes backwards even if the clock does, and it bumps by one tick to break exact ties. On one
machine all processes read the same nanosecond clock, so a `begin()` and a later write are strictly
ordered — which is what snapshot isolation needs. `update()` merges a timestamp observed on an
incoming RPC so a node's clock stays ahead of any causally-earlier event it learns about; across
real machines that propagation is what bounds clock drift.

Nanoseconds (~1.8e18) fit comfortably in 63 bits, so a timestamp slots straight into the MVCC
version-key encoding.
"""

import threading
import time


class HLC:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = 0

    def now(self) -> int:
        """A fresh timestamp strictly greater than any previously returned by this clock."""
        with self._lock:
            wall = time.time_ns()
            self._last = wall if wall > self._last else self._last + 1
            return self._last

    def update(self, remote_ts: int) -> int:
        """Merge a timestamp observed from elsewhere; return a fresh one that dominates it."""
        with self._lock:
            wall = time.time_ns()
            self._last = max(self._last + 1, remote_ts + 1, wall)
            return self._last
