"""
sql_parser.py — SQL Parser & Executor

CONCEPT:
  A SQL parser converts text like:
    SELECT name, age FROM users WHERE age > 25 AND city = 'NYC'
  into an Abstract Syntax Tree (AST), then executes it against storage.

  Pipeline:
    SQL text → Tokenizer → Token stream → Parser → AST → Executor → Results

  Supported statements:
    CREATE TABLE t (col1 TEXT, col2 INT, ...)
    INSERT INTO t (col1, col2) VALUES ('v1', 42)
    SELECT col1, col2 FROM t [WHERE expr] [ORDER BY col] [LIMIT n]
    UPDATE t SET col1 = val [WHERE expr]
    DELETE FROM t [WHERE expr]
    DROP TABLE t

  WHERE expressions:
    col = 'value'
    col != 'value'
    col > value  /  col >= value  /  col < value  /  col <= value
    expr AND expr
    expr OR expr
    NOT expr

  How real databases parse SQL:
    1. Lexer (tokenizer): text → token stream
       "SELECT name FROM users" → [SELECT, IDENT(name), FROM, IDENT(users)]

    2. Parser: token stream → AST (recursive descent or LALR)
       We use recursive descent — simple and readable

    3. Planner: AST → query plan (choose indexes, join order, etc.)
       We skip this — just execute directly

    4. Executor: query plan → result rows
       We implement a simple table scan with filter pushdown
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Any, Optional, Iterator


# ======================================================================= #
#  Tokenizer                                                               #
# ======================================================================= #

class TokenType:
    # Keywords
    SELECT = "SELECT"; INSERT = "INSERT"; UPDATE = "UPDATE"
    DELETE = "DELETE"; CREATE = "CREATE"; DROP = "DROP"
    FROM = "FROM"; INTO = "INTO"; TABLE = "TABLE"
    WHERE = "WHERE"; SET = "SET"; VALUES = "VALUES"
    AND = "AND"; OR = "OR"; NOT = "NOT"
    ORDER = "ORDER"; BY = "BY"; LIMIT = "LIMIT"
    ASC = "ASC"; DESC = "DESC"
    # Literals
    IDENT = "IDENT"; STRING = "STRING"; NUMBER = "NUMBER"
    STAR = "STAR"
    # Operators
    EQ = "EQ"; NEQ = "NEQ"; LT = "LT"; LTE = "LTE"; GT = "GT"; GTE = "GTE"
    # Punctuation
    LPAREN = "LPAREN"; RPAREN = "RPAREN"; COMMA = "COMMA"; SEMI = "SEMI"
    EOF = "EOF"


KEYWORDS = {
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
    "FROM", "INTO", "TABLE", "WHERE", "SET", "VALUES",
    "AND", "OR", "NOT", "ORDER", "BY", "LIMIT", "ASC", "DESC",
    "TEXT", "INT", "INTEGER", "FLOAT", "BOOLEAN",
}


@dataclass
class Token:
    type: str
    value: Any
    pos: int = 0

    def __repr__(self):
        return f"Token({self.type}, {self.value!r})"


def tokenize(sql: str) -> list[Token]:
    """
    Convert SQL string into a list of tokens.
    Handles: keywords, identifiers, string literals, numbers, operators.
    """
    tokens = []
    i = 0
    n = len(sql)

    while i < n:
        # Skip whitespace
        if sql[i].isspace():
            i += 1
            continue

        # Single-line comment
        if sql[i:i+2] == "--":
            while i < n and sql[i] != "\n":
                i += 1
            continue

        # String literal: 'value' or "value"
        if sql[i] in ("'", '"'):
            quote = sql[i]
            j = i + 1
            while j < n and sql[j] != quote:
                if sql[j] == "\\" and j + 1 < n:
                    j += 2
                else:
                    j += 1
            tokens.append(Token(TokenType.STRING, sql[i+1:j], i))
            i = j + 1
            continue

        # Number
        if sql[i].isdigit() or (sql[i] == "-" and i+1 < n and sql[i+1].isdigit()):
            j = i
            if sql[j] == "-":
                j += 1
            while j < n and (sql[j].isdigit() or sql[j] == "."):
                j += 1
            raw = sql[i:j]
            val = float(raw) if "." in raw else int(raw)
            tokens.append(Token(TokenType.NUMBER, val, i))
            i = j
            continue

        # Identifier or keyword
        if sql[i].isalpha() or sql[i] == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            word = sql[i:j]
            upper = word.upper()
            if upper in KEYWORDS:
                tokens.append(Token(upper, upper, i))
            else:
                tokens.append(Token(TokenType.IDENT, word, i))
            i = j
            continue

        # Operators and punctuation
        two = sql[i:i+2]
        if two == "!=":
            tokens.append(Token(TokenType.NEQ, "!=", i)); i += 2
        elif two == "<=":
            tokens.append(Token(TokenType.LTE, "<=", i)); i += 2
        elif two == ">=":
            tokens.append(Token(TokenType.GTE, ">=", i)); i += 2
        elif sql[i] == "=":
            tokens.append(Token(TokenType.EQ, "=", i)); i += 1
        elif sql[i] == "<":
            tokens.append(Token(TokenType.LT, "<", i)); i += 1
        elif sql[i] == ">":
            tokens.append(Token(TokenType.GT, ">", i)); i += 1
        elif sql[i] == "*":
            tokens.append(Token(TokenType.STAR, "*", i)); i += 1
        elif sql[i] == "(":
            tokens.append(Token(TokenType.LPAREN, "(", i)); i += 1
        elif sql[i] == ")":
            tokens.append(Token(TokenType.RPAREN, ")", i)); i += 1
        elif sql[i] == ",":
            tokens.append(Token(TokenType.COMMA, ",", i)); i += 1
        elif sql[i] == ";":
            tokens.append(Token(TokenType.SEMI, ";", i)); i += 1
        else:
            raise SyntaxError(f"Unexpected character {sql[i]!r} at position {i}")

    tokens.append(Token(TokenType.EOF, None, n))
    return tokens


# ======================================================================= #
#  AST Nodes                                                               #
# ======================================================================= #

@dataclass
class ColumnDef:
    name: str
    type: str  # TEXT, INT, FLOAT, BOOLEAN

@dataclass
class CreateTableStmt:
    table: str
    columns: list[ColumnDef]

@dataclass
class DropTableStmt:
    table: str

@dataclass
class InsertStmt:
    table: str
    columns: list[str]
    values: list[Any]

@dataclass
class SelectStmt:
    table: str
    columns: list[str]   # ["*"] for SELECT *
    where: Optional["Expr"] = None
    order_by: Optional[str] = None
    order_desc: bool = False
    limit: Optional[int] = None

@dataclass
class UpdateStmt:
    table: str
    assignments: dict[str, Any]  # col → new_value
    where: Optional["Expr"] = None

@dataclass
class DeleteStmt:
    table: str
    where: Optional["Expr"] = None

# WHERE expression nodes
@dataclass
class BinaryExpr:
    op: str   # =, !=, <, <=, >, >=, AND, OR
    left: Any
    right: Any

@dataclass
class UnaryExpr:
    op: str   # NOT
    operand: Any

@dataclass
class ColumnRef:
    name: str

@dataclass
class Literal:
    value: Any

Expr = BinaryExpr | UnaryExpr | ColumnRef | Literal


# ======================================================================= #
#  Parser (recursive descent)                                              #
# ======================================================================= #

class Parser:
    """
    Recursive descent SQL parser.

    Grammar (simplified):
      stmt     → select | insert | update | delete | create | drop
      select   → SELECT cols FROM IDENT [WHERE expr] [ORDER BY IDENT [ASC|DESC]] [LIMIT NUMBER]
      insert   → INSERT INTO IDENT (cols) VALUES (vals)
      update   → UPDATE IDENT SET assignments [WHERE expr]
      delete   → DELETE FROM IDENT [WHERE expr]
      create   → CREATE TABLE IDENT (col_defs)
      drop     → DROP TABLE IDENT
      expr     → or_expr
      or_expr  → and_expr (OR and_expr)*
      and_expr → not_expr (AND not_expr)*
      not_expr → NOT not_expr | compare
      compare  → term (op term)?
      term     → IDENT | STRING | NUMBER | (expr)
    """

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def consume(self, expected_type: str = None) -> Token:
        tok = self.tokens[self.pos]
        if expected_type and tok.type != expected_type:
            raise SyntaxError(
                f"Expected {expected_type} but got {tok.type} ({tok.value!r}) at pos {tok.pos}"
            )
        self.pos += 1
        return tok

    def match(self, *types: str) -> bool:
        return self.peek().type in types

    def parse(self):
        stmt = self._parse_stmt()
        # Consume optional semicolon
        if self.match(TokenType.SEMI):
            self.consume()
        return stmt

    def _parse_stmt(self):
        tok = self.peek()
        if tok.type == "SELECT":
            return self._parse_select()
        elif tok.type == "INSERT":
            return self._parse_insert()
        elif tok.type == "UPDATE":
            return self._parse_update()
        elif tok.type == "DELETE":
            return self._parse_delete()
        elif tok.type == "CREATE":
            return self._parse_create()
        elif tok.type == "DROP":
            return self._parse_drop()
        else:
            raise SyntaxError(f"Unknown statement starting with {tok.value!r}")

    def _parse_select(self):
        self.consume("SELECT")
        columns = self._parse_column_list()
        self.consume("FROM")
        table = self.consume(TokenType.IDENT).value
        where = None
        if self.match("WHERE"):
            self.consume("WHERE")
            where = self._parse_expr()
        order_by = None
        order_desc = False
        if self.match("ORDER"):
            self.consume("ORDER")
            self.consume("BY")
            order_by = self.consume(TokenType.IDENT).value
            if self.match("DESC"):
                self.consume("DESC")
                order_desc = True
            elif self.match("ASC"):
                self.consume("ASC")
        limit = None
        if self.match("LIMIT"):
            self.consume("LIMIT")
            limit = int(self.consume(TokenType.NUMBER).value)
        return SelectStmt(table, columns, where, order_by, order_desc, limit)

    def _parse_column_list(self) -> list[str]:
        if self.match(TokenType.STAR):
            self.consume()
            return ["*"]
        cols = [self.consume(TokenType.IDENT).value]
        while self.match(TokenType.COMMA):
            self.consume()
            cols.append(self.consume(TokenType.IDENT).value)
        return cols

    def _parse_insert(self):
        self.consume("INSERT")
        self.consume("INTO")
        table = self.consume(TokenType.IDENT).value
        self.consume(TokenType.LPAREN)
        columns = [self.consume(TokenType.IDENT).value]
        while self.match(TokenType.COMMA):
            self.consume()
            columns.append(self.consume(TokenType.IDENT).value)
        self.consume(TokenType.RPAREN)
        self.consume("VALUES")
        self.consume(TokenType.LPAREN)
        values = [self._parse_literal_value()]
        while self.match(TokenType.COMMA):
            self.consume()
            values.append(self._parse_literal_value())
        self.consume(TokenType.RPAREN)
        return InsertStmt(table, columns, values)

    def _parse_update(self):
        self.consume("UPDATE")
        table = self.consume(TokenType.IDENT).value
        self.consume("SET")
        assignments = {}
        col = self.consume(TokenType.IDENT).value
        self.consume(TokenType.EQ)
        assignments[col] = self._parse_literal_value()
        while self.match(TokenType.COMMA):
            self.consume()
            col = self.consume(TokenType.IDENT).value
            self.consume(TokenType.EQ)
            assignments[col] = self._parse_literal_value()
        where = None
        if self.match("WHERE"):
            self.consume("WHERE")
            where = self._parse_expr()
        return UpdateStmt(table, assignments, where)

    def _parse_delete(self):
        self.consume("DELETE")
        self.consume("FROM")
        table = self.consume(TokenType.IDENT).value
        where = None
        if self.match("WHERE"):
            self.consume("WHERE")
            where = self._parse_expr()
        return DeleteStmt(table, where)

    def _parse_create(self):
        self.consume("CREATE")
        self.consume("TABLE")
        table = self.consume(TokenType.IDENT).value
        self.consume(TokenType.LPAREN)
        col_defs = [self._parse_col_def()]
        while self.match(TokenType.COMMA):
            self.consume()
            col_defs.append(self._parse_col_def())
        self.consume(TokenType.RPAREN)
        return CreateTableStmt(table, col_defs)

    def _parse_col_def(self) -> ColumnDef:
        name = self.consume(TokenType.IDENT).value
        type_tok = self.consume()
        return ColumnDef(name, type_tok.value.upper())

    def _parse_drop(self):
        self.consume("DROP")
        self.consume("TABLE")
        table = self.consume(TokenType.IDENT).value
        return DropTableStmt(table)

    def _parse_literal_value(self) -> Any:
        tok = self.peek()
        if tok.type == TokenType.STRING:
            self.consume()
            return tok.value
        elif tok.type == TokenType.NUMBER:
            self.consume()
            return tok.value
        elif tok.type == TokenType.IDENT:
            # Could be NULL, TRUE, FALSE
            self.consume()
            if tok.value.upper() == "NULL":
                return None
            elif tok.value.upper() == "TRUE":
                return True
            elif tok.value.upper() == "FALSE":
                return False
            return tok.value
        raise SyntaxError(f"Expected literal value, got {tok.type} ({tok.value!r})")

    # WHERE expression parsing (recursive descent)
    def _parse_expr(self) -> Expr:
        return self._parse_or()

    def _parse_or(self) -> Expr:
        left = self._parse_and()
        while self.match("OR"):
            self.consume()
            right = self._parse_and()
            left = BinaryExpr("OR", left, right)
        return left

    def _parse_and(self) -> Expr:
        left = self._parse_not()
        while self.match("AND"):
            self.consume()
            right = self._parse_not()
            left = BinaryExpr("AND", left, right)
        return left

    def _parse_not(self) -> Expr:
        if self.match("NOT"):
            self.consume()
            return UnaryExpr("NOT", self._parse_not())
        return self._parse_compare()

    def _parse_compare(self) -> Expr:
        left = self._parse_term()
        op_map = {
            TokenType.EQ: "=", TokenType.NEQ: "!=",
            TokenType.LT: "<", TokenType.LTE: "<=",
            TokenType.GT: ">", TokenType.GTE: ">=",
        }
        if self.peek().type in op_map:
            op = op_map[self.consume().type]
            right = self._parse_term()
            return BinaryExpr(op, left, right)
        return left

    def _parse_term(self) -> Expr:
        tok = self.peek()
        if tok.type == TokenType.LPAREN:
            self.consume()
            expr = self._parse_expr()
            self.consume(TokenType.RPAREN)
            return expr
        elif tok.type == TokenType.IDENT:
            self.consume()
            return ColumnRef(tok.value)
        elif tok.type == TokenType.STRING:
            self.consume()
            return Literal(tok.value)
        elif tok.type == TokenType.NUMBER:
            self.consume()
            return Literal(tok.value)
        raise SyntaxError(f"Unexpected token in expression: {tok.type} ({tok.value!r})")


def parse_sql(sql: str):
    """Parse a SQL string and return an AST node."""
    tokens = tokenize(sql)
    parser = Parser(tokens)
    return parser.parse()


# ======================================================================= #
#  In-memory Table (simple row store)                                      #
# ======================================================================= #

@dataclass
class Row:
    data: dict[str, Any]

    def get(self, col: str) -> Any:
        return self.data.get(col)


class Table:
    """Simple in-memory table with column schema."""

    def __init__(self, name: str, columns: list[ColumnDef]):
        self.name = name
        self.columns = {c.name: c for c in columns}
        self.rows: list[Row] = []
        self._next_rowid = 1

    def insert(self, col_names: list[str], values: list[Any]) -> int:
        row_data = {"_rowid": self._next_rowid}
        self._next_rowid += 1
        for col, val in zip(col_names, values):
            if col not in self.columns:
                raise ValueError(f"Column {col!r} not in table {self.name!r}")
            row_data[col] = val
        self.rows.append(Row(row_data))
        return row_data["_rowid"]

    def scan(self, predicate=None) -> Iterator[Row]:
        for row in self.rows:
            if predicate is None or predicate(row):
                yield row

    def __repr__(self):
        return f"Table({self.name!r}, {len(self.rows)} rows)"


# ======================================================================= #
#  Expression Evaluator                                                    #
# ======================================================================= #

def eval_expr(expr: Expr, row: Row) -> Any:
    """Evaluate a WHERE expression against a row."""
    if isinstance(expr, Literal):
        return expr.value
    elif isinstance(expr, ColumnRef):
        return row.get(expr.name)
    elif isinstance(expr, UnaryExpr):
        if expr.op == "NOT":
            return not eval_expr(expr.operand, row)
    elif isinstance(expr, BinaryExpr):
        if expr.op == "AND":
            return bool(eval_expr(expr.left, row)) and bool(eval_expr(expr.right, row))
        if expr.op == "OR":
            return bool(eval_expr(expr.left, row)) or bool(eval_expr(expr.right, row))
        left = eval_expr(expr.left, row)
        right = eval_expr(expr.right, row)
        # Type coercion: compare numbers as numbers
        try:
            if isinstance(right, (int, float)) and isinstance(left, str):
                left = float(left)
            elif isinstance(left, (int, float)) and isinstance(right, str):
                right = float(right)
        except (ValueError, TypeError):
            pass
        if expr.op == "=":   return left == right
        if expr.op == "!=":  return left != right
        if expr.op == "<":   return left < right
        if expr.op == "<=":  return left <= right
        if expr.op == ">":   return left > right
        if expr.op == ">=":  return left >= right
    return None


# ======================================================================= #
#  SQL Executor                                                            #
# ======================================================================= #

class SQLDatabase:
    """
    In-memory SQL database.
    Executes parsed SQL AST nodes against a collection of Tables.
    """

    def __init__(self):
        self.tables: dict[str, Table] = {}

    def execute(self, sql: str) -> list[dict]:
        """Parse and execute a SQL statement. Returns list of result rows."""
        ast = parse_sql(sql)
        return self._execute_ast(ast)

    def _execute_ast(self, ast) -> list[dict]:
        if isinstance(ast, CreateTableStmt):
            return self._create_table(ast)
        elif isinstance(ast, DropTableStmt):
            return self._drop_table(ast)
        elif isinstance(ast, InsertStmt):
            return self._insert(ast)
        elif isinstance(ast, SelectStmt):
            return self._select(ast)
        elif isinstance(ast, UpdateStmt):
            return self._update(ast)
        elif isinstance(ast, DeleteStmt):
            return self._delete(ast)
        raise ValueError(f"Unknown AST node: {type(ast)}")

    def _create_table(self, stmt: CreateTableStmt) -> list[dict]:
        if stmt.table in self.tables:
            raise ValueError(f"Table {stmt.table!r} already exists")
        self.tables[stmt.table] = Table(stmt.table, stmt.columns)
        return [{"result": f"Table {stmt.table!r} created"}]

    def _drop_table(self, stmt: DropTableStmt) -> list[dict]:
        if stmt.table not in self.tables:
            raise ValueError(f"Table {stmt.table!r} does not exist")
        del self.tables[stmt.table]
        return [{"result": f"Table {stmt.table!r} dropped"}]

    def _insert(self, stmt: InsertStmt) -> list[dict]:
        table = self._get_table(stmt.table)
        rowid = table.insert(stmt.columns, stmt.values)
        return [{"rowid": rowid, "result": "1 row inserted"}]

    def _select(self, stmt: SelectStmt) -> list[dict]:
        table = self._get_table(stmt.table)

        # Build predicate from WHERE clause
        predicate = None
        if stmt.where:
            predicate = lambda row: eval_expr(stmt.where, row)

        # Scan rows
        rows = list(table.scan(predicate))

        # ORDER BY
        if stmt.order_by:
            rows.sort(
                key=lambda r: (r.get(stmt.order_by) is None, r.get(stmt.order_by)),
                reverse=stmt.order_desc
            )

        # LIMIT
        if stmt.limit is not None:
            rows = rows[:stmt.limit]

        # Project columns
        results = []
        for row in rows:
            if stmt.columns == ["*"]:
                # All columns except internal _rowid
                results.append({k: v for k, v in row.data.items() if not k.startswith("_")})
            else:
                results.append({col: row.get(col) for col in stmt.columns})

        return results

    def _update(self, stmt: UpdateStmt) -> list[dict]:
        table = self._get_table(stmt.table)
        count = 0
        for row in table.rows:
            if stmt.where is None or eval_expr(stmt.where, row):
                for col, val in stmt.assignments.items():
                    row.data[col] = val
                count += 1
        return [{"result": f"{count} rows updated"}]

    def _delete(self, stmt: DeleteStmt) -> list[dict]:
        table = self._get_table(stmt.table)
        before = len(table.rows)
        if stmt.where:
            table.rows = [r for r in table.rows if not eval_expr(stmt.where, r)]
        else:
            table.rows = []
        deleted = before - len(table.rows)
        return [{"result": f"{deleted} rows deleted"}]

    def _get_table(self, name: str) -> Table:
        if name not in self.tables:
            raise ValueError(f"Table {name!r} does not exist")
        return self.tables[name]

    def show_tables(self) -> list[str]:
        return list(self.tables.keys())


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

def run_sql(db: SQLDatabase, sql: str):
    """Execute SQL and print results."""
    print(f"\n  SQL: {sql}")
    try:
        results = db.execute(sql)
        if results:
            for row in results:
                print(f"       → {row}")
        else:
            print("       → (no results)")
    except Exception as e:
        print(f"       ✗ ERROR: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("SQL PARSER & EXECUTOR DEMO")
    print("=" * 60)

    db = SQLDatabase()

    # --- Step 1: Create tables ---
    print("\n[Step 1] CREATE TABLE")
    run_sql(db, "CREATE TABLE users (id INT, name TEXT, age INT, city TEXT)")
    run_sql(db, "CREATE TABLE orders (id INT, user_id INT, product TEXT, amount FLOAT)")

    # --- Step 2: INSERT ---
    print("\n[Step 2] INSERT INTO")
    run_sql(db, "INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 30, 'New York')")
    run_sql(db, "INSERT INTO users (id, name, age, city) VALUES (2, 'Bob', 25, 'London')")
    run_sql(db, "INSERT INTO users (id, name, age, city) VALUES (3, 'Carol', 35, 'New York')")
    run_sql(db, "INSERT INTO users (id, name, age, city) VALUES (4, 'Dave', 28, 'Tokyo')")
    run_sql(db, "INSERT INTO users (id, name, age, city) VALUES (5, 'Eve', 22, 'London')")

    run_sql(db, "INSERT INTO orders (id, user_id, product, amount) VALUES (1, 1, 'Laptop', 999.99)")
    run_sql(db, "INSERT INTO orders (id, user_id, product, amount) VALUES (2, 2, 'Phone', 599.99)")
    run_sql(db, "INSERT INTO orders (id, user_id, product, amount) VALUES (3, 1, 'Tablet', 449.99)")
    run_sql(db, "INSERT INTO orders (id, user_id, product, amount) VALUES (4, 3, 'Monitor', 299.99)")

    # --- Step 3: SELECT * ---
    print("\n[Step 3] SELECT *")
    run_sql(db, "SELECT * FROM users")

    # --- Step 4: SELECT with WHERE ---
    print("\n[Step 4] SELECT with WHERE")
    run_sql(db, "SELECT name, age FROM users WHERE city = 'New York'")
    run_sql(db, "SELECT name, age FROM users WHERE age > 25")
    run_sql(db, "SELECT name, city FROM users WHERE age >= 28 AND city != 'Tokyo'")

    # --- Step 5: SELECT with OR ---
    print("\n[Step 5] SELECT with OR / NOT")
    run_sql(db, "SELECT name, city FROM users WHERE city = 'London' OR city = 'Tokyo'")
    run_sql(db, "SELECT name, age FROM users WHERE NOT age > 28")

    # --- Step 6: ORDER BY + LIMIT ---
    print("\n[Step 6] ORDER BY + LIMIT")
    run_sql(db, "SELECT name, age FROM users ORDER BY age DESC")
    run_sql(db, "SELECT name, age FROM users ORDER BY age ASC LIMIT 3")

    # --- Step 7: UPDATE ---
    print("\n[Step 7] UPDATE")
    run_sql(db, "UPDATE users SET city = 'San Francisco' WHERE name = 'Bob'")
    run_sql(db, "SELECT name, city FROM users WHERE name = 'Bob'")

    # --- Step 8: DELETE ---
    print("\n[Step 8] DELETE")
    run_sql(db, "DELETE FROM users WHERE age < 25")
    run_sql(db, "SELECT * FROM users")

    # --- Step 9: SELECT specific columns ---
    print("\n[Step 9] SELECT specific columns from orders")
    run_sql(db, "SELECT product, amount FROM orders WHERE amount > 400")
    run_sql(db, "SELECT product, amount FROM orders ORDER BY amount DESC LIMIT 2")

    # --- Step 10: Error handling ---
    print("\n[Step 10] Error handling")
    run_sql(db, "SELECT * FROM nonexistent")
    run_sql(db, "INSERT INTO users (id, bad_col) VALUES (99, 'x')")

    # --- Step 11: DROP TABLE ---
    print("\n[Step 11] DROP TABLE")
    run_sql(db, "DROP TABLE orders")
    print(f"  Tables remaining: {db.show_tables()}")

    print("\n[Done] SQL parser demo complete.")
    print("\nKey insights:")
    print("  1. Tokenizer converts text → token stream (handles strings, numbers, ops)")
    print("  2. Recursive descent parser builds AST from token stream")
    print("  3. Executor walks AST and applies operations to storage")
    print("  4. WHERE expressions evaluated per-row (table scan)")
    print("  5. Production: add indexes, query planner, join support")