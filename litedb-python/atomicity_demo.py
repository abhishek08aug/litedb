"""atomicity_demo.py — shows LSMEngine.write_batch is all-or-nothing across a crash."""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402
from lsm_engine import LSMEngine  # type: ignore
from storage_engine import WriteOp  # type: ignore


def main():
    d = tempfile.mkdtemp(prefix="litedb_atomic_")
    print("=== Atomic write batches (WAL BEGIN/COMMIT) ===\n")

    e1 = LSMEngine(d)
    e1.write_batch([WriteOp.put("a", "1"), WriteOp.put("b", "2")])
    print("committed batch:                      a=1, b=2")
    e1.write_batch_simulate_crash([WriteOp.put("c", "3"), WriteOp.put("d", "4")])
    print("logged but NOT committed (crash sim): c=3, d=4")
    print("...crash: no clean close\n")
    # deliberately do not close e1

    e2 = LSMEngine(d)
    print("after recovery:")
    print("  get a =", e2.get("a"), "  (committed   -> present)")
    print("  get b =", e2.get("b"), "  (committed   -> present)")
    print("  get c =", e2.get("c"), "(uncommitted -> discarded)")
    print("  get d =", e2.get("d"), "(uncommitted -> discarded)")
    e2.close()

    shutil.rmtree(d, ignore_errors=True)
    print("\n[Atomicity demo complete]")


if __name__ == "__main__":
    main()
