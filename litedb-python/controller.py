"""
controller.py — a thin CLIENT of the Placement Driver Raft group.

The control plane is no longer a single in-process orchestrator. The authority now lives in a
replicated **PD Raft group** co-located on `PD_NODES` (see `pd.py`): membership decisions are
committed to its log and the PD leader runs reconcile + failure detection. This class is just the
client the dashboard (and smokes) use:

  * add_node / remove_node — submit a decision to the PD leader (retrying to find it), which appends
    it to the PD Raft log; the PD leader then reconciles the data cluster.
  * active / placement — read the authoritative state back from the PD (kept fresh by a poll).
  * the failure detector now runs inside the PD leader, so `start_failure_detector()` is a no-op kept
    only for call-site compatibility.

So a controller (dashboard) process can come and go freely — the durable decisions and the healing
live in the fault-tolerant PD group, not here.
"""

import threading
import time
from typing import Callable, Optional

from cluster_config import INITIAL_NODES, NODES, PD_NODES, compute_placement
from rpc import RPCClient


class Controller:
    def __init__(self, active: Optional[list[str]] = None,
                 on_event: Optional[Callable[[str], None]] = None):
        self.rpc = RPCClient(timeout=3.0)
        self._emit = on_event or (lambda m: None)
        self._active = list(active if active is not None else INITIAL_NODES)
        self.placement: dict[str, list[str]] = compute_placement(self._active)
        self._running = True
        threading.Thread(target=self._refresh_loop, daemon=True).start()

    # ---- read-through of the PD's authoritative state ---------------------

    @property
    def active(self) -> list[str]:
        return self._active

    def _pd_status(self) -> Optional[dict]:
        for n in PD_NODES:
            host, port = NODES[n]
            r = self.rpc.call(host, port, "pd_status", {}, timeout=1.5)
            if r.get("ok") and isinstance(r.get("result"), dict) and r["result"].get("ok"):
                return r["result"]
        return None

    def _refresh_loop(self) -> None:
        while self._running:
            st = self._pd_status()
            if st:
                self._active = st["active"]
                self.placement = st["placement"]
            time.sleep(1.0)

    def stop(self) -> None:
        self._running = False

    # ---- submit decisions to the PD leader --------------------------------

    def _propose(self, decision: dict, retries: int = 60) -> bool:
        for _ in range(retries):
            st = self._pd_status()
            target = (st["leader"] if st and st.get("leader") else None) or PD_NODES[0]
            host, port = NODES[target]
            r = self.rpc.call(host, port, "pd_propose", {"decision": decision}, timeout=3.0)
            if r.get("ok") and isinstance(r.get("result"), dict) and r["result"].get("ok"):
                return True
            time.sleep(0.2)
        return False

    def add_node(self, new_node: str) -> None:
        self._emit(f"ADD node {new_node}: proposing add_node to the PD Raft group")
        ok = self._propose({"op": "add_node", "node": new_node})
        tail = "committed — PD is reconciling placement" if ok else "FAILED (no PD leader)"
        self._emit(f"ADD node {new_node}: {tail}")

    def remove_node(self, node: str, dead: bool = False) -> None:
        self._emit(f"REMOVE node {node} (dead={dead}): proposing remove_node to the PD Raft group")
        ok = self._propose({"op": "remove_node", "node": node, "dead": dead})
        tail = "committed — PD is re-replicating to restore RF" if ok else "FAILED (no PD leader)"
        self._emit(f"REMOVE node {node}: {tail}")

    # ---- kept for call-site compatibility ---------------------------------

    def start_failure_detector(self, interval: float = 2.0, reap_after: float = 5.0) -> None:
        """No-op: the failure detector now runs inside the PD leader (see pd.py)."""

    def broadcast_placement(self) -> None:
        """No-op: the PD owns placement and reconciles it; nothing to broadcast from here."""
