"""
catalog.py — the system catalog (table schemas + index definitions), persisted under reserved
key namespaces and MVCC-versioned like all other data. Cached in memory; mutated through the
owning statement's transaction.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from index_def import IndexDef  # type: ignore
from table_schema import TableSchema  # type: ignore

TABLE_PREFIX = "__catalog__/table/"
INDEX_PREFIX = "__catalog__/index/"
_HI = chr(0x10FFFF)


class Catalog:
    def __init__(self, mvcc):
        self._tables = {}
        self._indexes = {}
        tx = mvcc.begin()
        try:
            for _k, v in tx.scan(TABLE_PREFIX, TABLE_PREFIX + _HI):
                s = TableSchema.deserialize(v)
                self._tables[s.name] = s
            for _k, v in tx.scan(INDEX_PREFIX, INDEX_PREFIX + _HI):
                d = IndexDef.deserialize(v)
                self._indexes[d.name] = d
        finally:
            tx.rollback()

    # reads (from cache)
    def has_table(self, name):  return name in self._tables
    def get_table(self, name):  return self._tables.get(name)
    def table_names(self):      return list(self._tables.keys())
    def has_index(self, name):  return name in self._indexes
    def get_index(self, name):  return self._indexes.get(name)

    def indexes_for_table(self, table):
        return [d for d in self._indexes.values() if d.table == table]

    def index_for_column(self, table, column):
        for d in self._indexes.values():
            if d.table == table and d.column == column:
                return d
        return None

    # mutations (staged on the statement's transaction)
    def create_table(self, schema, tx):
        tx.put(TABLE_PREFIX + schema.name, schema.serialize())
        self._tables[schema.name] = schema

    def drop_table(self, name, tx):
        tx.delete(TABLE_PREFIX + name)
        self._tables.pop(name, None)

    def create_index(self, d, tx):
        tx.put(INDEX_PREFIX + d.name, d.serialize())
        self._indexes[d.name] = d

    def drop_index(self, name, tx):
        tx.delete(INDEX_PREFIX + name)
        self._indexes.pop(name, None)
