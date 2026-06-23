"""
pd_failover_smoke.py — proves the control plane survives its OWN leader's death.

The placement driver is now a Raft group (`pd.py`). This kills the **PD leader** (which is also a
data node) and asserts:
  1. a NEW PD leader is elected among the surviving PD replicas (the durable decision log carries over);
  2. that new PD leader, with no human action, detects the dead node and re-replicates its shards to
     restore RF — i.e. it finishes the healing the old leader would have done.

So a control-plane leader crash is no longer a SPOF: decisions live in the PD Raft log and any
surviving replica can take over and continue. Run:  python pd_failover_smoke.py
"""

import os

os.environ.setdefault("LITEDB_CLUSTER_NODES", "4")  # 4 nodes: 3 PD + 1 extra, so survivors hold RF 3

import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402
from collections import Counter  # noqa: E402

import _loader  # noqa: F401,E402
from cluster_client import ClusterClient  # noqa: E402
from cluster_config import DATA_ROOT, INITIAL_NODES, NODES, PD_NODES, SHARDS  # noqa: E402
from rpc import RPCClient  # noqa: E402


def spawn(nid: str):
    return subprocess.Popen([sys.executable, "node.py", nid],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_until(fn, timeout=45.0, what=""):
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


def main():
    n_shards = len(SHARDS)
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    procs = {n: spawn(n) for n in INITIAL_NODES}
    rpc = RPCClient(timeout=1.5)

    def pd_leader(exclude=()):
        for n in PD_NODES:
            if n in exclude:
                continue
            host, port = NODES[n]
            r = rpc.call(host, port, "pd_status", {})
            res = r.get("result") if r.get("ok") else None
            if isinstance(res, dict) and res.get("ok") and res.get("leader"):
                return res["leader"]
        return None

    try:
        client = ClusterClient()
        wait_until(lambda: pd_leader() is not None, what="initial PD leader elected")
        leader1 = pd_leader()
        print(f"PD Raft group up; leader = {leader1} (PD replicas: {PD_NODES})")

        for i in range(12):
            assert client.put(f"key{i}", f"val{i}").get("ok"), f"put key{i}"
        print("  wrote 12 keys")

        print(f"\nkilling the PD LEADER {leader1} (also a data node) — the control plane must survive")
        procs[leader1].kill()
        procs[leader1].wait()

        wait_until(lambda: pd_leader(exclude={leader1}) not in (None, leader1), timeout=30,
                   what="a new PD leader is elected after the old one died")
        leader2 = pd_leader(exclude={leader1})
        print(f"  new PD leader elected from the surviving replicas: {leader2}")
        assert leader2 and leader2 != leader1

        # the new PD leader must finish the job: detect the dead node and restore RF, no human action
        def healed():
            h = hosts_of(client)
            if leader1 in h:
                return False
            c = Counter(s for ns in h.values() for s in ns)
            return len(c) == n_shards and all(v == 3 for v in c.values())

        wait_until(healed, what="new PD leader re-replicates the dead node's shards to restore RF")
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12)), "data intact after PD failover"
        print("  RF restored to 3 on survivors; all 12 keys intact")

        print("\nPD FAILOVER OK: the PD leader died; a surviving PD replica took over from the durable "
              "Raft log and finished healing the dead node's data — the control plane is no longer a SPOF.")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
