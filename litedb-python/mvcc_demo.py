"""mvcc_demo.py — snapshot isolation, conflict detection, tombstone deletes, GC, persistence."""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402
from lsm_engine import LSMEngine  # type: ignore
from mvcc import ConflictException, MVCCEngine  # type: ignore


def line(s):
    print("\n" + s + "\n" + "-" * len(s))


def main():
    d = tempfile.mkdtemp(prefix="litedb_mvcc_")
    engine = LSMEngine(d)
    mvcc = MVCCEngine(engine)

    seed = mvcc.begin(); seed.put("alice", "1000"); seed.put("bob", "500"); seed.commit()

    line("1) SNAPSHOT ISOLATION — read an old version while an update commits")
    txA = mvcc.begin(); txB = mvcc.begin()
    print("  txB.get(alice) before txA commits =", txB.get("alice"))
    txA.put("alice", "800"); txA.put("bob", "700"); c = txA.commit()
    print("  txA committed (alice 1000->800) at ts =", c)
    print("  txB.get(alice) AFTER txA commit   =", txB.get("alice"), "  <- still its snapshot")
    print("  txB.get(bob)                      =", txB.get("bob"))
    txC = mvcc.begin()
    print("  txC.get(alice) (fresh snapshot)   =", txC.get("alice"), "   <- sees the commit")
    txB.rollback(); txC.rollback()

    line("2) CONFLICT DETECTION — concurrent writes can't lose an update")
    t1 = mvcc.begin(); t2 = mvcc.begin()
    t1.put("alice", "900"); print("  t1 commit -> ts =", t1.commit())
    t2.put("alice", "1234")
    try:
        t2.commit(); print("  t2 commit -> (unexpected: should have conflicted)")
    except ConflictException as e:
        print("  t2 commit -> ABORTED:", e)

    line("3) DELETE (tombstone) — old snapshots still see the value")
    before = mvcc.begin()
    dtx = mvcc.begin(); dtx.delete("alice"); dtx.commit()
    print("  after delete, fresh read alice  =", mvcc.begin().get("alice"))
    print("  pre-delete snapshot reads alice =", before.get("alice"))
    before.rollback()

    line("4) GARBAGE COLLECTION — reclaim versions no snapshot can see")
    print("  versions before vacuum =", mvcc.version_count())
    collected = mvcc.vacuum(mvcc.last_commit_ts())
    print("  vacuum collected       =", collected, "old versions")
    print("  versions after vacuum  =", mvcc.version_count())

    engine.close()

    line("5) PERSISTENCE — reopen, timestamp + data recovered")
    e2 = LSMEngine(d); m2 = MVCCEngine(e2)
    print("  recovered last_commit_ts =", m2.last_commit_ts())
    print("  fresh read alice =", m2.begin().get("alice"), " (deleted)")
    print("  fresh read bob   =", m2.begin().get("bob"))
    e2.close()

    shutil.rmtree(d, ignore_errors=True)
    print("\n[MVCC demo complete]")


if __name__ == "__main__":
    main()
