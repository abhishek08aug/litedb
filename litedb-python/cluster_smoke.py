"""Scratch test: 3 real node processes, driven by ClusterClient. Phases 4-7 end to end."""
import shutil
import subprocess
import sys
import time

from cluster_client import ClusterClient
from cluster_config import DATA_ROOT, NODES, make_partitioner


def wait_until(fn, timeout=10.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(0.2)
    raise AssertionError(f"timeout waiting for {what}")


def leaders_map(client):
    """shard -> leader node, as reported by status."""
    m = {}
    for node in client.status():
        if not node.get("alive"):
            continue
        for sh in node.get("shards", []):
            if sh["role"] == "leader":
                m[sh["group"]] = sh["node"]
    return m


def main():
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    procs = {}
    for nid in NODES:
        procs[nid] = subprocess.Popen([sys.executable, "node.py", nid],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        client = ClusterClient()
        part = make_partitioner()

        print("Waiting for all 6 shards to elect leaders...")
        wait_until(lambda: len(leaders_map(client)) == 6, timeout=15, what="6 leaders")
        lm = leaders_map(client)
        from collections import Counter
        spread = Counter(lm.values())
        print(f"  leaders per node: {dict(spread)}  (multi-raft spreads leadership)")
        assert len(spread) >= 2, "leadership should spread across nodes"

        print("\nWriting 12 keys (routed to their shards by consistent hashing):")
        for i in range(12):
            r = client.put(f"key{i}", f"val{i}")
            assert r.get("ok"), f"put key{i} failed: {r}"
        by_shard = {}
        for i in range(12):
            by_shard.setdefault(part.shard_for(f"key{i}"), []).append(f"key{i}")
        print(f"  keys landed across {len(by_shard)} shards: "
              f"{ {s: len(ks) for s, ks in sorted(by_shard.items())} }")

        print("\nReading them all back (contacting random nodes; they forward to leaders):")
        ok = all(client.get(f"key{i}") == f"val{i}" for i in range(12))
        print(f"  all 12 reads correct -> {ok}")
        assert ok

        print("\nCross-shard transaction (atomic transfer across two shards):")
        # pick two keys on different shards
        a, b = "key0", None
        for i in range(1, 12):
            if part.shard_for(f"key{i}") != part.shard_for("key0"):
                b = f"key{i}"
                break
        print(f"  {a} (shard {part.shard_for(a)})  +  {b} (shard {part.shard_for(b)})")
        r = client.txn({a: "alice=900", b: "bob=100"})
        print(f"  2PC result -> ok={r.get('ok')} shards={sorted(r.get('shards', []))}")
        assert r.get("ok") and len(r.get("shards", [])) == 2
        assert client.get(a) == "alice=900" and client.get(b) == "bob=100"

        print("\nSnapshot isolation across the cluster:")
        snap = client.begin()
        client.put(a, "alice=CHANGED")
        old = client.get(a, read_ts=snap)
        new = client.get(a)
        print(f"  @snapshot={old!r}  latest={new!r}")
        assert old == "alice=900" and new == "alice=CHANGED"

        print("\nKilling a node and verifying failover + continued service:")
        victim = max(spread, key=spread.get)  # node leading the most shards
        print(f"  killing {victim} (was leading {spread[victim]} shards)")
        procs[victim].kill()
        procs[victim].wait()
        wait_until(lambda: len(leaders_map(client)) == 6, timeout=15,
                   what="re-election of victim's shards onto survivors")
        lm2 = leaders_map(client)
        assert victim not in set(lm2.values()), "dead node still shown as leader"
        print(f"  re-elected: leaders now on {sorted(set(lm2.values()))}")

        print("\nWrites still succeed after failover:")
        r = client.put("after-failover", "ok")
        assert r.get("ok"), r
        assert client.get("after-failover") == "ok"
        assert client.get(a) == "alice=CHANGED", "data from before the crash survived"
        print("  write + read after failover OK; pre-crash data intact")

        print("\nPHASES 4-7 OK: partitioning, multi-raft, routing, cross-shard 2PC, "
              "snapshot isolation, and live failover — across 3 real processes.")
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
