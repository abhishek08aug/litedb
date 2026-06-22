"""
txn_log.py — the coordinator's durable transaction log for 2PC recovery.

The 2-phase commit coordinator writes a record here at two points:
  - "preparing"  : before sending PREPARE (records the participants; the txn is still undecided)
  - "committing" : once every participant has voted YES (this fsync is the COMMIT POINT)
and deletes it once all commits are acknowledged.

On restart a node sweeps this log and finishes any in-doubt transaction it was coordinating:
  - "committing" → re-send COMMIT to each participant (idempotent)
  - "preparing"  → the coordinator died before deciding, so ABORT

This resolves coordinator failure (assuming participants are alive). Participant-side restart
recovery — persisting a participant's prepared writes so they survive its own crash — is a further
step (see ROADMAP).
"""

import json
import os


class TxnLog:
    """A directory of durable JSON records keyed by id (fsync'd, atomic replace). Used by the
    coordinator (`txnlog`) and by each shard participant (`shard-<id>-prepared`)."""

    def __init__(self, data_dir: str, subdir: str = "txnlog"):
        self.dir = os.path.join(data_dir, subdir)
        os.makedirs(self.dir, exist_ok=True)

    def _path(self, txn_id: str) -> str:
        return os.path.join(self.dir, txn_id + ".json")

    def write(self, txn_id: str, record: dict) -> None:
        path = self._path(txn_id)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(record, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def remove(self, txn_id: str) -> None:
        try:
            os.remove(self._path(txn_id))
        except FileNotFoundError:
            pass

    def pending(self) -> list[dict]:
        out = []
        for fn in sorted(os.listdir(self.dir)):
            if fn.endswith(".json"):
                with open(os.path.join(self.dir, fn)) as f:
                    out.append(json.load(f))
        return out
