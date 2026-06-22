"""
participant_recovery_smoke.py — proves participant-crash recovery via Raft-replicated intents.

A participant leader holds a prepared 2PC intent, then crashes. Because the intent was replicated
through that shard's Raft group, the NEW leader has it — so (a) isolation is preserved (a conflicting
write is still rejected by the new leader) and (b) the transaction can still commit on the new leader.
This is the property the in-memory/local-disk approach could NOT provide.
"""
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

        def key_for(shard):
            return next(f"pk{i}" for i in range(3000) if part.shard_for(f"pk{i}") == shard)
        ka, kb = key_for(shard_a), key_for(shard_b)
        print(f"prepare on {shard_a}@{la} ({ka}) and {shard_b}@{lb} ({kb}); will kill {la}")

        txn_id = "txn-participant-demo-1"
        commit_ts = client.begin()
        for shard, leader, k, v in [(shard_a, la, ka, "A=ok"), (shard_b, lb, kb, "B=ok")]:
            host, port = NODES[leader]
            r = rpc.call(host, port, "shard_prepare", {"shard": shard, "txn_id": txn_id,
                         "writes": {k: v}, "read_ts": commit_ts, "commit_ts": commit_ts,
                         "coordinator": "test"})
            assert r.get("ok") and r["result"].get("ok"), f"prepare {shard}: {r}"
        print("both PREPARED (intents replicated through each shard's Raft group)")

        assert client.get(ka) is None and client.get(kb) is None
        c = client.put(ka, "conflict")
        assert not c.get("ok") and c.get("error") == "locked", f"expected locked, got {c}"
        print("  → key is locked by the intent (conflicting write rejected) ✓")

        print(f"killing the participant leader {la} of {shard_a}...")
        procs[la].kill()
        procs[la].wait()
        wait_until(lambda: leaders(client).get(shard_a) not in (None, la), what="new leader for shard_a")
        new_leader = leaders(client)[shard_a]
        print(f"  → {shard_a} re-elected: new leader is {new_leader} (≠ {la})")

        # the NEW leader must have the replicated intent: a conflicting write is STILL rejected
        c2 = client.put(ka, "conflict2")
        assert not c2.get("ok") and c2.get("error") == "locked", \
            f"new leader lost the intent! isolation broken: {c2}"
        print("  → new leader still rejects the conflicting write — intent survived leadership change ✓")

        # commit on the CURRENT leaders → the new leader applies the intent it inherited
        for shard, k in [(shard_a, ka), (shard_b, kb)]:
            leader = leaders(client)[shard]
            host, port = NODES[leader]
            r = rpc.call(host, port, "shard_commit", {"shard": shard, "txn_id": txn_id})
            assert r.get("ok") and r["result"].get("ok"), f"commit {shard}: {r}"

        wait_until(lambda: client.get(ka) == "A=ok" and client.get(kb) == "B=ok",
                   what="commit on the inherited intent")
        print(f"  → committed on the new leader: {ka}={client.get(ka)}, {kb}={client.get(kb)} ✓")
        print("\nPARTICIPANT RECOVERY OK: prepared intent survived a leader crash (replicated via "
              "Raft) — isolation preserved and the txn committed on the new leader.")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
