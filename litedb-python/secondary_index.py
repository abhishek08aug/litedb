"""
secondary_index.py — a value index over an opaque key->value store.

Maps stored VALUE -> the primary keys holding it, so a query can find keys by value
(and by value range) without scanning the whole keyspace.

Backed by a B+Tree keyed on a composite (value + SEP + primaryKey). The composite supports
duplicate values (the primary key makes each index entry unique, like a real non-unique
index) and sorts by value first, so a range scan over values is a B+Tree range scan.

The separator is U+0000, which text values and keys never contain, so the composite is
unambiguous. Range queries over-capture at the boundary and then filter on the parsed value,
which stays correct regardless of where separators fall.

Maintained by the owning engine on every set/delete. Held in memory and rebuilt from the
base data on startup (it is not itself persisted).
"""

from btree import BPlusTree  # type: ignore

_SEP = chr(0)          # U+0000 — never appears in text values/keys
_HI = chr(0x10FFFF)    # max code point — high-range sentinel for over-capture


class SecondaryIndex:
    def __init__(self):
        self._tree = BPlusTree(order=16)
        self._entries = 0

    @staticmethod
    def _composite(value: str, key: str) -> str:
        return value + _SEP + key

    def add(self, key: str, value: str) -> None:
        comp = self._composite(value, key)
        if self._tree.get(comp) is None:
            self._entries += 1
        self._tree.set(comp, key)

    def remove(self, key: str, value: str) -> None:
        if self._tree.delete(self._composite(value, key)):
            self._entries -= 1

    def update(self, key: str, old_value, new_value: str) -> None:
        if old_value is not None and old_value != new_value:
            self.remove(key, old_value)
        self.add(key, new_value)

    def keys_in_value_range(self, low_value: str, high_value: str) -> list[str]:
        """Primary keys whose value is within [low_value, high_value] (inclusive)."""
        keys = []
        for comp, primary_key in self._tree.scan(low_value, high_value + _HI):
            idx = comp.find(_SEP)
            value = comp[:idx] if idx >= 0 else comp
            if low_value <= value <= high_value:
                keys.append(primary_key)
        return keys

    def size(self) -> int:
        return self._entries
