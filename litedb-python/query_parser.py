"""
query_parser.py — Query Parser & Command Executor

CONCEPT:
  A query parser takes raw text input from a client and converts it
  into structured commands that the storage engine can execute.

  This is a simplified version of what MySQL/PostgreSQL do with SQL.
  Our command language is Redis-like (simple text protocol):

  Commands:
    SET <key> <value>           → store key-value pair
    GET <key>                   → retrieve value
    DELETE <key>                → delete key
    SCAN <start_key> <end_key>  → range scan
    STATS                       → engine statistics
    PING                        → health check
    QUIT                        → close connection

  Response format:
    OK                          → success (no value)
    VALUE <value>               → success with value
    NOT_FOUND                   → key doesn't exist
    SCAN_START                  → beginning of scan results
    ROW <key> <value>           → one scan result row
    SCAN_END <count>            → end of scan results
    ERROR <message>             → command failed
    PONG                        → response to PING

  Wire protocol (text-based, like Redis):
    Each command is a single line terminated by \n
    Responses are one or more lines terminated by \n
"""

from __future__ import annotations
import shlex
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lsm_engine import LSMEngine  # type: ignore


class QueryResult:
    """Structured result from executing a command."""

    def __init__(self, status: str, value: str | None = None, rows: list[tuple[str, str]] | None = None):
        self.status = status   # "OK", "VALUE", "NOT_FOUND", "ERROR", "PONG", "SCAN"
        self.value = value     # for VALUE responses
        self.rows = rows or [] # for SCAN responses

    def to_wire(self) -> str:
        """Serialize result to wire protocol string."""
        if self.status == "VALUE":
            return f"VALUE {self.value}\n"
        elif self.status == "SCAN":
            lines = ["SCAN_START\n"]
            for k, v in self.rows:
                lines.append(f"ROW {k} {v}\n")
            lines.append(f"SCAN_END {len(self.rows)}\n")
            return "".join(lines)
        elif self.status == "ERROR":
            return f"ERROR {self.value}\n"
        else:
            return f"{self.status}\n"

    def __repr__(self) -> str:
        if self.status == "VALUE":
            return f"QueryResult(VALUE, {self.value!r})"
        elif self.status == "SCAN":
            return f"QueryResult(SCAN, {len(self.rows)} rows)"
        return f"QueryResult({self.status})"


class QueryParser:
    """
    Parses and executes LiteDB commands against an LSMEngine.

    This is the layer between the network (raw text) and the storage engine.
    In a real database, this layer would also handle:
      - Authentication / authorization
      - Query planning and optimization
      - Transaction management
      - Schema validation
    """

    def __init__(self, engine):
        self._engine = engine

    def execute(self, raw_command: str) -> QueryResult:
        """
        Parse and execute a single command string.
        Returns a QueryResult.
        """
        raw_command = raw_command.strip()
        if not raw_command:
            return QueryResult("ERROR", "Empty command")

        try:
            # Use shlex to handle quoted values with spaces
            # e.g.: SET name "Alice Smith" → ["SET", "name", "Alice Smith"]
            tokens = shlex.split(raw_command)
        except ValueError as e:
            return QueryResult("ERROR", f"Parse error: {e}")

        if not tokens:
            return QueryResult("ERROR", "Empty command")

        command = tokens[0].upper()

        try:
            return self._dispatch(command, tokens[1:])
        except Exception as e:
            return QueryResult("ERROR", f"Execution error: {e}")

    def _dispatch(self, command: str, args: list[str]) -> QueryResult:
        """Route command to the appropriate handler."""

        if command == "SET":
            return self._cmd_set(args)
        elif command == "GET":
            return self._cmd_get(args)
        elif command == "DELETE" or command == "DEL":
            return self._cmd_delete(args)
        elif command == "SCAN":
            return self._cmd_scan(args)
        elif command == "STATS":
            return self._cmd_stats(args)
        elif command == "PING":
            return QueryResult("PONG")
        elif command == "QUIT" or command == "EXIT":
            return QueryResult("QUIT")
        elif command == "HELP":
            return self._cmd_help()
        else:
            return QueryResult("ERROR", f"Unknown command: {command!r}. Type HELP for commands.")

    # ------------------------------------------------------------------ #
    #  Command handlers                                                    #
    # ------------------------------------------------------------------ #

    def _cmd_set(self, args: list[str]) -> QueryResult:
        """SET <key> <value>"""
        if len(args) < 2:
            return QueryResult("ERROR", "Usage: SET <key> <value>")

        key = args[0]
        value = " ".join(args[1:])  # allow spaces in value

        if not self._valid_key(key):
            return QueryResult("ERROR", f"Invalid key: {key!r}. Keys cannot contain spaces.")

        self._engine.set(key, value)
        return QueryResult("OK")

    def _cmd_get(self, args: list[str]) -> QueryResult:
        """GET <key>"""
        if len(args) != 1:
            return QueryResult("ERROR", "Usage: GET <key>")

        key = args[0]
        value = self._engine.get(key)

        if value is None:
            return QueryResult("NOT_FOUND")
        return QueryResult("VALUE", value)

    def _cmd_delete(self, args: list[str]) -> QueryResult:
        """DELETE <key>"""
        if len(args) != 1:
            return QueryResult("ERROR", "Usage: DELETE <key>")

        key = args[0]
        self._engine.delete(key)
        return QueryResult("OK")

    def _cmd_scan(self, args: list[str]) -> QueryResult:
        """SCAN <start_key> <end_key>"""
        if len(args) != 2:
            return QueryResult("ERROR", "Usage: SCAN <start_key> <end_key>")

        start_key, end_key = args[0], args[1]
        if start_key > end_key:
            return QueryResult("ERROR", "start_key must be <= end_key")

        rows = list(self._engine.scan(start_key, end_key))
        return QueryResult("SCAN", rows=rows)

    def _cmd_stats(self, args: list[str]) -> QueryResult:
        """STATS — show engine statistics"""
        stats = self._engine.stats()
        lines = [f"{k}={v}" for k, v in stats.items()]
        return QueryResult("VALUE", " ".join(lines))

    def _cmd_help(self) -> QueryResult:
        help_text = (
            "Commands: "
            "SET <key> <value> | "
            "GET <key> | "
            "DELETE <key> | "
            "SCAN <start> <end> | "
            "STATS | "
            "PING | "
            "QUIT"
        )
        return QueryResult("VALUE", help_text)

    def _valid_key(self, key: str) -> bool:
        """Keys must be non-empty and not contain spaces."""
        return bool(key) and " " not in key


# ======================================================================= #
#  DEMO — standalone (no engine needed, uses a mock)                      #
# ======================================================================= #

class MockEngine:
    """Simple in-memory mock engine for testing the parser."""

    def __init__(self):
        self._store: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def scan(self, start_key: str, end_key: str):
        for k in sorted(self._store.keys()):
            if start_key <= k <= end_key:
                yield k, self._store[k]

    def stats(self) -> dict:
        return {"entries": len(self._store), "engine": "MockEngine"}


if __name__ == "__main__":
    print("=" * 60)
    print("QUERY PARSER DEMO")
    print("=" * 60)

    engine = MockEngine()
    parser = QueryParser(engine)

    test_commands = [
        "PING",
        "SET name Alice",
        "SET age 30",
        'SET city "New York"',
        "SET country USA",
        "GET name",
        "GET age",
        "GET missing_key",
        "SCAN a z",
        "SCAN c d",
        "DELETE age",
        "GET age",
        "STATS",
        "HELP",
        "BADCOMMAND foo",
        "SET",                    # missing args
        "GET",                    # missing args
        "SCAN only_one_arg",      # missing end key
    ]

    print()
    for cmd in test_commands:
        result = parser.execute(cmd)
        wire = result.to_wire().rstrip("\n")
        print(f"  > {cmd:<35} → {wire}")

    print("\n[Done] Query parser demo complete.")
    print("\nKey insights:")
    print("  1. Parser tokenizes raw text → structured command + args")
    print("  2. Each command dispatches to a typed handler")
    print("  3. Errors return structured ERROR responses (not exceptions)")
    print("  4. Wire protocol is text-based (easy to test with netcat)")