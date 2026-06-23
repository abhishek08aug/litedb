"""
autoheal_smoke.py — proves the gossip failure detector AUTO-restores replication factor.

Scenario (4 nodes, RF 3, so reaping one genuinely re-replicates):
  1. Start 4 nodes; write 12 keys.
  2. Start the controller's failure detector and then **kill a node — with no manual controller call**.
  3. Gossip marks it dead; the controller's reconcile loop sees the majority verdict and, past the
     grace window, auto-fires remove_node(dead=True) → the dead node's shards re-replicate onto the
     survivors → RF back to 3 live replicas everywhere, data intact.

This closes the loop from "gossip detected a death" to "redundancy healed" with zero human action.
Run:  python autoheal_smoke.py
"""

import os

# 4 nodes so that reaping one actually moves a replica onto a survivor (RF 3 over 4 → 3 over 3).
os.environ.setdefault("LITEDB_CLUSTER_NODES", "4")

import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
from collections import Counter  # noqa: E402

import _loader  # noqa: F401,E402
from cluster_client import ClusterClient  # noqa: E402
from cluster_config import DATA_ROOT, INITIAL_NODES, SHARDS  # noqa: E402
from controller import Controller  # noqa: E402


def spawn(nid: str):
    return subprocess.Popen([sys.executable, "node.py", nid],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_until(fn, timeout=40.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(0.25)
    raise AssertionError("timeout: " + what)


def hosts_of(client):
    out = {}
    for st in client.status():
        if st.get("alive"):
            out[st["node"]] = {sh["group"] for sh in st.get("shards", [])}
    return out


def ready_leaders(client):
    m = {}
    for st in client.status():
        for sh in st.get("shards", []):
            if sh["role"] == "leader" and sh.get("ready"):
                m[sh["group"]] = sh["node"]
    return m


def main():
    n_shards = len(SHARDS)
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    procs = {n: spawn(n) for n in INITIAL_NODES}
    ctrl = Controller()
    try:
        client = ClusterClient()
        wait_until(lambda: len(ready_leaders(client)) == n_shards,
                   what=f"{n_shards} leaders on initial {len(INITIAL_NODES)} nodes")
        ctrl.broadcast_placement()
        print(f"started with {INITIAL_NODES} (RF 3)")

        for i in range(12):
            assert client.put(f"key{i}", f"val{i}").get("ok"), f"put key{i}"
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12))
        print("  wrote + read 12 keys")

        # The PD leader runs the failure detector autonomously — nothing to start. Kill a non-PD data
        # node (a clean data-node death; killing a PD member is covered by pd_failover_smoke).
        victim = INITIAL_NODES[-1]
        print(f"\nkilling {victim} — NO manual controller call; the PD failure detector must notice")
        procs[victim].kill()
        procs[victim].wait()

        wait_until(lambda: victim not in ctrl.active, what="controller starts reaping the dead node")
        print(f"  controller detected {victim} dead and is re-replicating (active now "
              f"{sorted(ctrl.active)})")

        # poll the OBSERVABLE replication state — remove_node frees `active` first and re-replicates
        # after, and Raft catch-up takes a moment, so wait for RF to actually be restored.
        def fully_healed() -> bool:
            h = hosts_of(client)
            if victim in h:
                return False
            c = Counter(s for ns in h.values() for s in ns)
            return len(c) == n_shards and all(c[s] == 3 for s in c)

        wait_until(fully_healed, what="RF re-replicated back to 3 on every shard")
        hosts = hosts_of(client)
        assert victim not in hosts, f"{victim} should host nothing: {hosts.get(victim)}"
        cnt = Counter(s for ns in hosts.values() for s in ns)
        assert all(cnt[s] == 3 for s in cnt), f"RF should be 3 on survivors: {dict(cnt)}"
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12)), "data intact after auto-heal"
        print(f"  RF restored to 3 across survivors {sorted(hosts)}; all 12 keys intact")

        print("\nAUTO-HEAL OK: a node died with zero manual action; gossip flagged it DEAD and the "
              "controller's failure detector re-replicated to restore RF, data preserved.")
    finally:
        ctrl.stop()
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
