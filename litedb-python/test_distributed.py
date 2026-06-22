"""
test_distributed.py — fast, in-process tests for the distributed layer.

These wire the real components (RaftGroup, ShardReplica) over real TCP sockets but inside one
process, so they run in a few seconds and are deterministic enough for CI. The full 3-OS-process
end-to-end demo lives in cluster_smoke.py.
"""

import shutil
import tempfile
import time

from hlc import HLC
from raft_node import RPC_TIMEOUT, RaftGroup
from rpc import RPCClient, RPCServer
from shard_replica import ShardReplica

HOST = "127.0.0.1"


def _wait(predicate, timeout=6.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        val = predicate()
        if val:
            return val
        time.sleep(0.05)
    return None


# --------------------------------------------------------------------------- #
#  RPC                                                                          #
# --------------------------------------------------------------------------- #

def test_rpc_call_error_and_reconnect():
    srv = RPCServer(HOST, 19401, {"add": lambda p: {"sum": p["a"] + p["b"]}})
    srv.start()
    time.sleep(0.1)
    cli = RPCClient(timeout=1.0)
    try:
        assert cli.call(HOST, 19401, "add", {"a": 2, "b": 3})["result"]["sum"] == 5
        assert cli.call(HOST, 19401, "missing", {})["ok"] is False
    finally:
        srv.stop()
    # dead server -> graceful failure, not an exception
    assert cli.call(HOST, 19401, "add", {"a": 1, "b": 1})["ok"] is False
    # restart -> client transparently reconnects
    srv2 = RPCServer(HOST, 19401, {"add": lambda p: {"sum": p["a"] + p["b"]}})
    srv2.start()
    time.sleep(0.2)
    try:
        assert cli.call(HOST, 19401, "add", {"a": 10, "b": 10})["result"]["sum"] == 20
    finally:
        srv2.stop()


# --------------------------------------------------------------------------- #
#  Raft: election, replication, failover, restart-recovery                     #
# --------------------------------------------------------------------------- #

def _raft_cluster(tmp, ports):
    client = RPCClient(timeout=RPC_TIMEOUT)
    reps, servers, applied = {}, {}, {}

    def make(nid):
        applied[nid] = {}
        peers = [p for p in ports if p != nid]
        g = RaftGroup(
            node_id=nid, group_id="g", peers=peers,
            send_fn=lambda peer, kind, payload, c=client: c.call(HOST, ports[peer], kind, payload, timeout=RPC_TIMEOUT),
            apply_fn=lambda idx, cmd, a=applied[nid]: a.__setitem__(cmd["key"], cmd["value"]) if "key" in cmd else None,
            data_dir=f"{tmp}/{nid}",
        )
        s = RPCServer(HOST, ports[nid], {"vote": g.handle_vote, "append": g.handle_append})
        return g, s

    for nid in ports:
        g, s = make(nid)
        reps[nid], servers[nid] = g, s
        s.start()
        g.start()
    return client, reps, servers, applied, make


def test_raft_election_replication_failover():
    tmp = tempfile.mkdtemp(prefix="t_raft_")
    ports = {"A": 19411, "B": 19412, "C": 19413}
    client, reps, servers, applied, make = _raft_cluster(tmp, ports)
    try:
        leader = _wait(lambda: next((r for r in reps.values() if r.is_ready()), None))
        assert leader is not None, "no leader elected"

        for i in range(5):
            idx = leader.propose({"op": "set", "key": f"k{i}", "value": f"v{i}"})
            assert idx is not None and leader.wait_commit(idx, timeout=3.0)
        time.sleep(0.4)
        for nid in ports:
            assert applied[nid] == {f"k{i}": f"v{i}" for i in range(5)}, f"{nid} diverged"

        # kill the leader -> new leader elected, more writes converge on survivors
        dead = leader.node_id
        reps[dead].stop()
        servers[dead].stop()
        survivors = [r for nid, r in reps.items() if nid != dead]
        new_leader = _wait(lambda: next((r for r in survivors if r.is_ready()), None))
        assert new_leader is not None, "no failover"
        for i in range(5, 8):
            idx = new_leader.propose({"op": "set", "key": f"k{i}", "value": f"v{i}"})
            assert idx is not None and new_leader.wait_commit(idx, timeout=3.0)
        time.sleep(0.4)
        for nid in ports:
            if nid != dead:
                assert applied[nid] == {f"k{i}": f"v{i}" for i in range(8)}
    finally:
        for r in reps.values():
            r.stop()
        for s in servers.values():
            s.stop()
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------- #
#  Replicated MVCC through Raft                                                 #
# --------------------------------------------------------------------------- #

def test_shard_replicated_mvcc_and_snapshot_isolation():
    tmp = tempfile.mkdtemp(prefix="t_shard_")
    ports = {"A": 19421, "B": 19422, "C": 19423}
    client = RPCClient(timeout=RPC_TIMEOUT)
    reps, servers = {}, {}
    try:
        for nid in ports:
            peers = [p for p in ports if p != nid]
            rep = ShardReplica(
                node_id=nid, shard_id="s", peers=peers,
                send_fn=lambda peer, kind, payload, c=client: c.call(HOST, ports[peer], kind, payload, timeout=RPC_TIMEOUT),
                data_dir=f"{tmp}/{nid}", hlc=HLC(),
            )
            srv = RPCServer(HOST, ports[nid], {"vote": rep.raft.handle_vote, "append": rep.raft.handle_append})
            reps[nid], servers[nid] = rep, srv
            srv.start()
            rep.start()

        leader = _wait(lambda: next((r for r in reps.values() if r.is_ready()), None))
        assert leader is not None

        r = leader.commit_write({"alice": "100"})
        snap_old = r["commit_ts"]
        assert leader.commit_write({"alice": "175"})["ok"]
        assert leader.commit_write({"bob": "50"})["ok"]
        assert leader.commit_write({"bob": None})["ok"]  # delete
        time.sleep(0.4)

        assert leader.read("alice") == "175"
        assert leader.read("bob") is None
        assert leader.read("alice", read_ts=snap_old) == "100"  # snapshot isolation
        for nid in ports:
            ts = reps[nid].snapshot_ts()
            assert reps[nid].store.read("alice", ts) == "175"
            assert reps[nid].store.read("bob", ts) is None
    finally:
        for r in reps.values():
            r.stop()
        for s in servers.values():
            s.stop()
        shutil.rmtree(tmp, ignore_errors=True)
