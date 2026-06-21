"""index_def.py — a named secondary index on one column of a table."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import row_codec  # type: ignore


class IndexDef:
    __slots__ = ("name", "table", "column")

    def __init__(self, name: str, table: str, column: str):
        self.name = name
        self.table = table
        self.column = column

    def serialize(self) -> str:
        return row_codec.encode([self.name, self.table, self.column])

    @staticmethod
    def deserialize(s: str) -> "IndexDef":
        f = row_codec.decode(s)
        return IndexDef(f[0], f[1], f[2])

    def __repr__(self):
        return f"{self.name} ON {self.table}({self.column})"
