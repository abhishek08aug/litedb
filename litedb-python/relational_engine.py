"""
relational_engine.py — a SQL layer over an MVCCEngine.

Every statement runs in an MVCC transaction: reads at a consistent snapshot, writes buffered and
committed atomically with write-write conflict detection. Statements auto-commit by default;
explicit BEGIN / COMMIT / ROLLBACK group several statements (so concurrent sessions get snapshot
isolation). Catalog, rows, and index entries are all MVCC-versioned keys:

  __catalog__/table/<t> , __catalog__/index/<name> , <table>/<pk> ,
  __idx__/<table>/<column>/<encodedValue>\\0<pk>
"""

import functools
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402
import row_codec  # type: ignore
import sql_parser as sp  # type: ignore
import type_codec  # type: ignore
from catalog import Catalog  # type: ignore
from column import Column  # type: ignore
from index_def import IndexDef  # type: ignore
from mvcc import ConflictException, MVCCEngine, Transaction  # type: ignore
from table_schema import TableSchema  # type: ignore

SEP = chr(0)
_HI = chr(0x10FFFF)
_CREATE_INDEX = re.compile(r"(?i)\s*CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(\s*(\w+)\s*\)\s*;?\s*")
_DROP_INDEX = re.compile(r"(?i)\s*DROP\s+INDEX\s+(\w+)\s*;?\s*")


class RelationalEngine:
    def __init__(self, mvcc: MVCCEngine):
        self._mvcc = mvcc
        self._catalog = Catalog(mvcc)
        self._current: Optional[Transaction] = None   # active explicit transaction, or None (auto-commit)

    @property
    def catalog(self):
        return self._catalog

    # ---- statement entry point + transaction control ---------------------

    def execute(self, sql: str) -> str:
        kw = re.sub(r";\s*$", "", sql.strip()).upper()
        if kw == "BEGIN":
            if self._current is not None:
                return "ERROR: already in a transaction"
            self._current = self._mvcc.begin()
            return f"OK: BEGIN (snapshot ts={self._current.read_ts})"
        if kw == "COMMIT":
            if self._current is None:
                return "ERROR: no active transaction"
            try:
                ts = self._current.commit(); self._current = None; return f"OK: COMMIT (ts={ts})"
            except ConflictException as e:
                self._current = None; return f"ERROR: {e}"
        if kw == "ROLLBACK":
            if self._current is None:
                return "ERROR: no active transaction"
            self._current.rollback(); self._current = None; return "OK: ROLLBACK"

        auto = self._current is None
        tx = self._mvcc.begin() if auto else self._current
        assert tx is not None  # fresh when auto; the active txn otherwise
        try:
            result = self._dispatch(sql, tx)
        except ConflictException as e:
            tx.rollback()
            if not auto:
                self._current = None
            return f"ERROR: {e}"
        if auto:
            if result.startswith("ERROR") or not tx.has_writes():
                tx.rollback(); return result
            try:
                tx.commit()
            except ConflictException as e:
                return f"ERROR: {e}"
        return result

    def _dispatch(self, sql, tx):
        m = _CREATE_INDEX.match(sql)
        if m:
            return self._create_index(m.group(1), m.group(2), m.group(3), tx)
        m = _DROP_INDEX.match(sql)
        if m:
            return self._drop_index(m.group(1), tx)
        try:
            stmt = sp.parse_sql(sql)
        except Exception as e:
            return f"ERROR: parse: {e}"
        if isinstance(stmt, sp.CreateTableStmt): return self._create_table(stmt, tx)
        if isinstance(stmt, sp.DropTableStmt):   return self._drop_table(stmt, tx)
        if isinstance(stmt, sp.InsertStmt):      return self._insert(stmt, tx)
        if isinstance(stmt, sp.SelectStmt):      return self._select(stmt, tx)
        if isinstance(stmt, sp.DeleteStmt):      return self._delete(stmt, tx)
        return f"ERROR: {type(stmt).__name__} not supported yet"

    # ---- key helpers ------------------------------------------------------

    @staticmethod
    def _row_key(table, pk):          return f"{table}/{pk}"
    @staticmethod
    def _index_prefix(table, column): return f"__idx__/{table}/{column}/"
    @classmethod
    def _index_key(cls, table, col, value, pk):
        return cls._index_prefix(table, col) + value + SEP + pk

    # ---- DDL --------------------------------------------------------------

    def _create_table(self, s, tx):
        if self._catalog.has_table(s.table):
            return f"ERROR: table already exists: {s.table}"
        cols = [Column(cd.name, cd.type) for cd in s.columns]
        schema = TableSchema(s.table, cols)
        self._catalog.create_table(schema, tx)
        return f"OK: created table {s.table} (PK={schema.primary_key().name})"

    def _drop_table(self, s, tx):
        if not self._catalog.has_table(s.table):
            return f"ERROR: no such table: {s.table}"
        for idx in self._catalog.indexes_for_table(s.table):
            self._delete_prefix(self._index_prefix(s.table, idx.column), tx)
            self._catalog.drop_index(idx.name, tx)
        deleted = 0
        for k, _v in list(self._scan_table(s.table, tx)):
            tx.delete(k); deleted += 1
        self._catalog.drop_table(s.table, tx)
        return f"OK: dropped table {s.table} ({deleted} rows removed)"

    def _create_index(self, name, table, column, tx):
        schema = self._catalog.get_table(table)
        if schema is None:                    return f"ERROR: no such table: {table}"
        if schema.column_index(column) < 0:   return f"ERROR: no such column: {column}"
        if self._catalog.has_index(name):     return f"ERROR: index already exists: {name}"
        if self._catalog.index_for_column(table, column) is not None:
            return f"ERROR: column already indexed: {table}({column})"
        self._catalog.create_index(IndexDef(name, table, column), tx)
        ci = schema.column_index(column); typ = schema.column_type(column); built = 0
        for _k, v in self._scan_table(table, tx):
            row = row_codec.decode(v)
            tx.put(self._index_key(table, column, type_codec.encode(typ, row[ci]), row[0]), row[0])
            built += 1
        return f"OK: created index {name} ON {table}({column}) — {built} entries"

    def _drop_index(self, name, tx):
        d = self._catalog.get_index(name)
        if d is None:
            return f"ERROR: no such index: {name}"
        self._delete_prefix(self._index_prefix(d.table, d.column), tx)
        self._catalog.drop_index(name, tx)
        return f"OK: dropped index {name}"

    # ---- DML --------------------------------------------------------------

    def _insert(self, s, tx):
        schema = self._catalog.get_table(s.table)
        if schema is None:
            return f"ERROR: no such table: {s.table}"
        row = self._build_row(schema, s.columns, s.values)
        if row is None:
            return f"ERROR: column/value mismatch for INSERT into {s.table}"
        pk = row[0]
        if tx.get(self._row_key(s.table, pk)) is not None:
            return f"ERROR: duplicate primary key: {pk}"
        tx.put(self._row_key(s.table, pk), row_codec.encode(row))
        for idx in self._catalog.indexes_for_table(s.table):
            enc = type_codec.encode(schema.column_type(idx.column), row[schema.column_index(idx.column)])
            tx.put(self._index_key(s.table, idx.column, enc, pk), pk)
        return "OK: 1 row inserted"

    def _select(self, s, tx):
        schema = self._catalog.get_table(s.table)
        if schema is None:
            return f"ERROR: no such table: {s.table}"
        proj = schema.column_names() if (len(s.columns) == 1 and s.columns[0] == "*") else s.columns
        for c in proj:
            if schema.column_index(c) < 0:
                return f"ERROR: no such column: {c}"

        pred = self._simple_predicate(s.where)
        idx = self._catalog.index_for_column(s.table, pred[0]) if pred else None
        if idx is not None:
            rows = self._index_scan(schema, idx, pred, tx)
            plan = f"index-scan on {pred[0]} ({idx.name})"
        else:
            rows = []
            for _k, v in self._scan_table(s.table, tx):
                row = row_codec.decode(v)
                if s.where is None or self._eval(s.where, schema, row):
                    rows.append(row)
            plan = "full-scan" + (f" (no index on {pred[0]})" if pred else "")

        if s.order_by is not None:
            oi = schema.column_index(s.order_by)
            if oi >= 0:
                otype = schema.columns[oi].type
                rows.sort(key=functools.cmp_to_key(lambda a, b: type_codec.compare(otype, a[oi], b[oi])))
                if s.order_desc:
                    rows.reverse()
        if s.limit is not None and len(rows) > s.limit:
            rows = rows[:s.limit]

        out = [[row[schema.column_index(c)] for c in proj] for row in rows]
        return "-- plan: " + plan + "\n" + self._render(proj, out)

    def _delete(self, s, tx):
        schema = self._catalog.get_table(s.table)
        if schema is None:
            return f"ERROR: no such table: {s.table}"
        victims = []
        for k, v in self._scan_table(s.table, tx):
            row = row_codec.decode(v)
            if s.where is None or self._eval(s.where, schema, row):
                victims.append((k, row))
        for k, row in victims:
            tx.delete(k); pk = row[0]
            for idx in self._catalog.indexes_for_table(s.table):
                enc = type_codec.encode(schema.column_type(idx.column), row[schema.column_index(idx.column)])
                tx.delete(self._index_key(s.table, idx.column, enc, pk))
        n = len(victims)
        return f"OK: {n} row{'s' if n != 1 else ''} deleted"

    # ---- scan + planner helpers ------------------------------------------

    def _scan_table(self, table, tx):
        return tx.scan(f"{table}/", f"{table}/" + _HI)

    def _delete_prefix(self, prefix, tx):
        for k, _v in list(tx.scan(prefix, prefix + _HI)):
            tx.delete(k)

    @staticmethod
    def _simple_predicate(where):
        """(column, op, value_str) if where is a single indexable comparison, else None."""
        if isinstance(where, sp.BinaryExpr) and where.op in ("=", "<", ">", "<=", ">="):
            if isinstance(where.left, sp.ColumnRef) and isinstance(where.right, sp.Literal):
                return (where.left.name, where.op, str(where.right.value))
        return None

    def _index_scan(self, schema, idx, pred, tx):
        _col, op, val = pred
        prefix = self._index_prefix(schema.name, idx.column)
        v = type_codec.encode(schema.column_type(idx.column), val)
        if op == "=":
            lo, hi = prefix + v, prefix + v + _HI
        elif op in (">", ">="):
            lo, hi = prefix + v, prefix + _HI
        else:  # <, <=
            lo, hi = prefix, prefix + v + _HI
        rows = []
        for k, pk in tx.scan(lo, hi):
            sep = k.find(SEP, len(prefix))
            if sep < 0:
                continue
            enc_col = k[len(prefix):sep]
            if not self._op_match(enc_col, op, v):
                continue
            rowval = tx.get(self._row_key(schema.name, pk))
            if rowval is not None:
                rows.append(row_codec.decode(rowval))
        return rows

    @staticmethod
    def _op_match(a, op, b):
        if op == "=":  return a == b
        if op == "<":  return a < b
        if op == ">":  return a > b
        if op == "<=": return a <= b
        if op == ">=": return a >= b
        return False

    # ---- expression eval, row building, render ---------------------------

    def _eval(self, expr, schema, row) -> bool:
        if isinstance(expr, sp.BinaryExpr):
            if expr.op in ("AND", "OR"):
                left = self._eval(expr.left, schema, row)
                right = self._eval(expr.right, schema, row)
                return (left and right) if expr.op == "AND" else (left or right)
            if isinstance(expr.left, sp.ColumnRef) and isinstance(expr.right, sp.Literal):
                ci = schema.column_index(expr.left.name)
                if ci < 0:
                    return False
                cmp = type_codec.compare(schema.columns[ci].type, row[ci], str(expr.right.value))
                op = expr.op
                return {"=": cmp == 0, "!=": cmp != 0, "<": cmp < 0,
                        ">": cmp > 0, "<=": cmp <= 0, ">=": cmp >= 0}.get(op, False)
            return False
        if isinstance(expr, sp.UnaryExpr):   # NOT
            return not self._eval(expr.operand, schema, row)
        return False

    @staticmethod
    def _build_row(schema, cols, vals):
        if vals is None:
            return None
        row = [""] * len(schema.columns)
        if not cols:
            if len(vals) != len(schema.columns):
                return None
            for i, v in enumerate(vals):
                row[i] = str(v)
        else:
            if len(cols) != len(vals):
                return None
            for i, c in enumerate(cols):
                ci = schema.column_index(c)
                if ci < 0:
                    return None
                row[ci] = str(vals[i])
        return row

    @staticmethod
    def _render(cols, rows):
        lines = [" | ".join(cols)]
        for r in rows:
            lines.append(" | ".join(r))
        n = len(rows)
        lines.append(f"({n} row{'s' if n != 1 else ''})")
        return "\n".join(lines)


# ======================================================================= #
#  DEMO — SQL through MVCC, plus snapshot isolation across two sessions      #
# ======================================================================= #

def _main():
    import shutil
    import tempfile

    from lsm_engine import LSMEngine  # type: ignore

    d = tempfile.mkdtemp(prefix="litedb_rel_")
    lsm = LSMEngine(d)
    mvcc = MVCCEngine(lsm)
    db = RelationalEngine(mvcc)

    print("=== SQL through MVCC (auto-commit) ===\n")
    script = [
        "CREATE TABLE nums (id INT, n INT)",
        "INSERT INTO nums (id, n) VALUES (1, 5)",
        "INSERT INTO nums (id, n) VALUES (2, 100)",
        "INSERT INTO nums (id, n) VALUES (3, 9)",
        "CREATE INDEX idx_n ON nums(n)",
        "SELECT * FROM nums WHERE n > 9",
        "SELECT * FROM nums ORDER BY n",
    ]
    for sql in script:
        print("db>", sql)
        print(db.execute(sql), "\n")

    print("=== Snapshot isolation across two sessions ===\n")
    a = RelationalEngine(mvcc)
    b = RelationalEngine(mvcc)
    print("A> BEGIN                :", a.execute("BEGIN"))
    print("A> SELECT (snapshot)    :\n" + a.execute("SELECT id FROM nums ORDER BY id"))
    print("B> INSERT id=4 (commits):", b.execute("INSERT INTO nums (id, n) VALUES (4, 7)"))
    print("A> SELECT again         :\n" + a.execute("SELECT id FROM nums ORDER BY id")
          + "   <- A still on its snapshot, no id=4")
    print("A> COMMIT               :", a.execute("COMMIT"))
    print("A> SELECT (new snapshot):\n" + a.execute("SELECT id FROM nums ORDER BY id")
          + "   <- now sees id=4")

    lsm.close()
    shutil.rmtree(d, ignore_errors=True)
    print("\n[MVCC-backed SQL demo complete]")


if __name__ == "__main__":
    _main()
