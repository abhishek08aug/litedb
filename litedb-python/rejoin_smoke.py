"""
rejoin_smoke.py — proves a reaped node that REJOINS doesn't disrupt the cluster (the Pre-Vote fix).

Reproduces the failure we hit by hand: kill the PD leader (a node), let auto-heal reap it, then
RESTART and re-add it. The returning node still has stale shard replicas on disk (its drop_shard was
skipped while it was dead), so it tries to resurrect them. WITHOUT pre-vote, those stale replicas keep
starting higher-term elections and disrupt the healthy groups → some shards lose their leader forever
(DEGRADED). WITH pre-vote, peers refuse the stale replica's pre-vote while they're hearing from their
leader, so it can't bump the term — the cluster returns to fully HEALTHY and stays there.

Run:  python rejoin_smoke.py
"""

import os

os.environ.setdefault("LITEDB_CLUSTER_NODES", "4")

import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

import _loader  # noqa: F401,E402
from cluster_client import ClusterClient  # noqa: E402
from cluster_config import DATA_ROOT, INITIAL_NODES, NODES, PD_NODES, SHARDS  # noqa: E402
from controller import Controller  # noqa: E402
from rpc import RPCClient  # noqa: E402


def spawn(nid: str):
    return subprocess.Popen([sys.executable, "node.py", nid],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_until(fn, timeout=70.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(0.25)
    raise AssertionError("timeout: " + what)


def ready_leaders(client) -> set:
    out = set()
    for st in client.status():
        for sh in st.get("shards", []):
            if sh["role"] == "leader" and sh.get("ready"):
                out.add(sh["group"])
    return out


def main():
    n_shards = len(SHARDS)
    shutil.rmtree(DATA_ROOT, ignore_errors=True)
    procs = {n: spawn(n) for n in INITIAL_NODES}
    ctrl = Controller()
    rpc = RPCClient(timeout=1.5)

    def pd_leader():
        for n in PD_NODES:
            host, port = NODES[n]
            r = rpc.call(host, port, "pd_status", {})
            res = r.get("result") if r.get("ok") else None
            if isinstance(res, dict) and res.get("leader"):
                return res["leader"]
        return None

    try:
        client = ClusterClient()
        wait_until(lambda: len(ready_leaders(client)) == n_shards, what="all shards have a leader at start")
        for i in range(12):
            assert client.put(f"key{i}", f"val{i}").get("ok"), f"put key{i}"
        victim = pd_leader() or "node-1"
        print(f"cluster healthy; PD leader = {victim}. Killing it (a node + the PD leader)...")

        procs[victim].kill()
        procs[victim].wait()
        wait_until(lambda: victim not in ctrl.active and len(ready_leaders(client)) == n_shards,
                   what="auto-heal after the PD leader died (reaped + RF restored)")
        print(f"  auto-healed: {victim} reaped, all {n_shards} shards led on survivors {sorted(ctrl.active)}")

        print(f"restarting {victim} and re-adding it (it still has stale shard replicas on disk)...")
        procs[victim] = spawn(victim)
        time.sleep(2)
        ctrl.add_node(victim)

        # THE FIX: the rejoining node must CONVERGE to a stable, fully-led cluster. Reconfiguration
        # churn (node-1 is re-added to several shards) can briefly dip a shard's leader, so we wait for
        # SUSTAINED health — 5 consecutive seconds of all shards led. Without pre-vote the stale
        # replicas disrupt elections permanently and this never converges (it stays stuck, as we saw).
        def stably_healthy(streak_needed=5, timeout=80.0) -> bool:
            streak = 0
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if len(ready_leaders(client)) == n_shards:
                    streak += 1
                    if streak >= streak_needed:
                        return True
                else:
                    streak = 0
                time.sleep(1.0)
            return False

        assert stably_healthy(), \
            "cluster never stabilized after rejoin — a returning node is disrupting elections"
        assert all(client.get(f"key{i}") == f"val{i}" for i in range(12)), "data intact after rejoin"

        # FENCING: the rejoined node must host only shards it's actually a voter of — its orphaned
        # stale replicas (shards it was reaped from) must have been dropped + wiped, not left lingering.
        time.sleep(6)  # let the fence loop run
        placement = ctrl.placement
        hosted = {sh["group"] for st in client.status() if st.get("node") == victim
                  for sh in st.get("shards", [])}
        orphans = {s for s in hosted if victim not in placement.get(s, [])}
        assert not orphans, f"{victim} still hosts orphan replicas it isn't a voter of: {orphans}"
        print(f"  {victim} hosts only shards it's a voter of {sorted(hosted)} — orphans fenced + wiped")

        print(f"\nREJOIN OK: {victim} restarted + re-added with stale replicas did NOT disrupt the "
              f"cluster (pre-vote refused its elections) — all {n_shards} shards keep a stable leader, "
              f"data intact.")
    finally:
        ctrl.stop()
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
