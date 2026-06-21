"""table_schema.py — a table's name and typed columns (first column is the primary key)."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import row_codec        # type: ignore
from column import Column   # type: ignore


class TableSchema:
    def __init__(self, name: str, columns):
        if not columns:
            raise ValueError(f"table {name!r} needs at least one column")
        self.name = name
        self.columns = columns

    def primary_key(self) -> Column:
        return self.columns[0]   # convention: first column

    def column_index(self, name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == name:
                return i
        return -1

    def column_type(self, name: str):
        i = self.column_index(name)
        return None if i < 0 else self.columns[i].type

    def column_names(self):
        return [c.name for c in self.columns]

    def serialize(self) -> str:
        fields = [self.name]
        for c in self.columns:
            fields += [c.name, c.type]
        return row_codec.encode(fields)

    @staticmethod
    def deserialize(s: str) -> "TableSchema":
        f = row_codec.decode(s)
        cols = [Column(f[i], f[i + 1]) for i in range(1, len(f), 2)]
        return TableSchema(f[0], cols)

    def __repr__(self):
        return f"{self.name}{self.columns} PK={self.primary_key().name}"
