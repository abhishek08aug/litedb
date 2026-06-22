"""
rebalance_smoke.py — proves dynamic membership: add a node (shards + data rebalance onto it) and
remove a node (its shards re-replicate to restore RF), all online while data stays readable.
"""
import shutil
import subprocess
import sys
import time
from collections import Counter

from cluster_client import ClusterClient
from cluster_config import DATA_ROOT, INITIAL_NODES, make_partitioner
from controller import Controller


def spawn(nid):
    return subprocess.Popen([sys.executable, "node.py", nid],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_until(fn, timeout=20.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(0.25)
    raise AssertionError("timeout: " + what)


def hosts_of(client):
    """node -> set of shards it currently hosts (from live status)."""
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
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    part = make_partitioner()
    procs = {n: spawn(n) for n in INITIAL_NODES}
    try:
        client = ClusterClient()
        ctrl = Controller()
        wait_until(lambda: len(ready_leaders(client)) == 6, what="6 leaders on initial 3 nodes")
        ctrl.broadcast_placement()
        print(f"started with {INITIAL_NODES}; each shard on all 3 (RF 3)")

        print("\nwriting 12 keys...")
        for i in range(12):
            assert client.put(f"key{i}", f"val{i}").get("ok"), f"put key{i}"
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12))
        print("  all 12 readable")

        # ---- ADD node-4 ----
        print("\nADD node-4 (spawn it, then rebalance)...")
        procs["node-4"] = spawn("node-4")
        time.sleep(1.5)  # let it come up
        ctrl.add_node("node-4")
        wait_until(lambda: len(ready_leaders(client)) == 6, what="all shards have a ready leader after add")

        hosts = hosts_of(client)
        n4_shards = hosts.get("node-4", set())
        print(f"  node-4 now hosts {sorted(n4_shards)}")
        assert n4_shards, "node-4 should host some shards after rebalancing (data moved onto it)"
        # every shard is still on exactly RF=3 nodes
        cnt = Counter()
        for ns in hosts.values():
            for s in ns:
                cnt[s] += 1
        assert all(cnt[s] == 3 for s in cnt), f"every shard should be on 3 nodes: {dict(cnt)}"
        print(f"  placement spread across 4 nodes: { {n: len(s) for n, s in sorted(hosts.items())} }")

        # data that landed on a shard now hosted by node-4 must be present + readable
        moved_key = next((f"key{i}" for i in range(12) if part.shard_for(f"key{i}") in n4_shards), None)
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12)), "data must survive rebalancing"
        if moved_key:
            print(f"  e.g. {moved_key} (shard {part.shard_for(moved_key)}) is on node-4 and still reads "
                  f"= {client.get(moved_key)}")
        # write a new key after add — cluster still fully functional
        assert client.put("after-add", "ok").get("ok") and client.get("after-add") == "ok"
        print("  reads/writes fine after add ✓")

        # ---- REMOVE node-4 ----
        print("\nREMOVE node-4 (re-replicate its shards back to restore RF on the other 3)...")
        ctrl.remove_node("node-4")
        wait_until(lambda: len(ready_leaders(client)) == 6, what="ready leaders after remove")
        time.sleep(0.5)
        hosts = hosts_of(client)
        assert not hosts.get("node-4"), f"node-4 should host nothing after removal: {hosts.get('node-4')}"
        cnt = Counter(s for ns in hosts.values() for s in ns)
        assert all(cnt[s] == 3 for s in cnt), f"RF should be restored to 3 everywhere: {dict(cnt)}"
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12)), "data intact after removal"
        print("  node-4 drained; placement back on 3 nodes; all data intact ✓")

        print("\nREBALANCE OK: added a node (shards+data moved onto it) and removed it "
              "(shards re-replicated), online, data preserved throughout.")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
