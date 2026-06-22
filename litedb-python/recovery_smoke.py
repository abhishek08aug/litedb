"""
recovery_smoke.py — proves 2PC coordinator-failure recovery.

Scenario: a coordinator decides COMMIT (all participants prepared, decision fsync'd) and then dies
before sending the commits. On restart it must finish the transaction.

We stage that exactly: PREPARE a cross-shard txn directly on the two shard leaders (they hold the
prepared writes), drop a "committing" record into a THIRD node's txn log, then kill+restart that
node and verify it drives the commits — the staged writes become visible.
"""
import json
import os
import subprocess
import sys
import time

from cluster_client import ClusterClient
from cluster_config import DATA_ROOT, NODES, make_partitioner
from rpc import RPCClient


def wait_until(fn, timeout=15.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(0.2)
    raise AssertionError("timeout: " + what)


def leaders(client):
    m = {}
    for st in client.status():
        for sh in st.get("shards", []):
            if sh["role"] == "leader" and sh.get("ready"):
                m[sh["group"]] = sh["node"]
    return m


def main():
    import shutil
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    procs = {nid: subprocess.Popen([sys.executable, "node.py", nid],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
             for nid in NODES}
    rpc = RPCClient(timeout=3.0)
    part = make_partitioner()
    try:
        client = ClusterClient()
        wait_until(lambda: len(leaders(client)) == 6, what="6 leaders")
        lm = leaders(client)

        # two shards with DIFFERENT leaders, and a third node to be the coordinator
        pair = None
        items = list(lm.items())
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                if items[i][1] != items[j][1]:
                    pair = (items[i], items[j])
                    break
            if pair:
                break
        (shard_a, la), (shard_b, lb) = pair
        coord = next(n for n in NODES if n not in (la, lb))
        print(f"participants: {shard_a}@{la}, {shard_b}@{lb}; coordinator (will crash): {coord}")

        def key_for(shard):
            return next(f"rk{i}" for i in range(2000) if part.shard_for(f"rk{i}") == shard)
        ka, kb = key_for(shard_a), key_for(shard_b)

        # PREPARE the txn on both leaders (they stage + hold locks), but do NOT commit
        txn_id = "txn-recovery-demo-1"
        commit_ts = client.begin()
        for shard, leader, k, v in [(shard_a, la, ka, "A=committed"), (shard_b, lb, kb, "B=committed")]:
            host, port = NODES[leader]
            r = rpc.call(host, port, "shard_prepare", {"shard": shard, "txn_id": txn_id,
                         "writes": {k: v}, "read_ts": commit_ts, "commit_ts": commit_ts})
            assert r.get("ok") and r["result"].get("ok"), f"prepare {shard}: {r}"
        print("both participants PREPARED (staged, holding locks, not committed)")

        assert client.get(ka) is None and client.get(kb) is None
        print("  → keys not yet visible (prepared only) ✓")

        # the coordinator decided COMMIT and fsync'd the record, then crashed before committing
        d = os.path.join(DATA_ROOT, coord, "txnlog")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, txn_id + ".json"), "w") as f:
            json.dump({"txn_id": txn_id, "status": "committing",
                       "participants": [[la, shard_a], [lb, shard_b]], "commit_ts": commit_ts}, f)
        print(f"dropped a 'committing' record into {coord}'s txn log, then killing {coord}...")
        procs[coord].kill()
        procs[coord].wait()

        print(f"restarting {coord} — its recovery sweep should drive the commits...")
        procs[coord] = subprocess.Popen([sys.executable, "node.py", coord],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        wait_until(lambda: client.get(ka) == "A=committed" and client.get(kb) == "B=committed",
                   timeout=15, what="recovery to commit the in-doubt txn")
        print(f"  → after restart, both keys committed: {ka}={client.get(ka)}, {kb}={client.get(kb)} ✓")
        assert not os.path.exists(os.path.join(d, txn_id + ".json")), "record should be cleared"
        print("  → coordinator cleared its txn-log record ✓")
        print("\nRECOVERY OK: coordinator died after deciding COMMIT, restarted, and finished the 2PC.")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
