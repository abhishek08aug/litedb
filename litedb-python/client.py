"""
client.py — TCP client for the LiteDB server (server.py).

Two modes:
  - interactive (default): a REPL — type commands, see responses
  - --demo: runs a fixed command sequence (handy as a smoke test)

The server speaks a text protocol (like Redis): each command is a single line;
the server replies with one line, except SCAN which streams rows until
"SCAN_END <count>".

Usage:
  python client.py [--host 127.0.0.1] [--port 7379] [--demo]

Commands: PING, SET <k> <v>, GET <k>, DELETE <k>, SCAN <start> <end>, STATS, HELP, QUIT
"""

import argparse
import socket


class LiteDBClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def run(self, demo: bool = False):
        with socket.create_connection((self.host, self.port)) as sock:
            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            print(reader.readline().rstrip("\n"))  # server banner
            if demo:
                self._run_demo(sock, reader)
            else:
                self._run_interactive(sock, reader)

    # -- protocol helpers -------------------------------------------------- #

    def _send(self, sock: socket.socket, cmd: str):
        sock.sendall((cmd + "\n").encode("utf-8"))

    def _read_response(self, reader, cmd: str) -> str:
        """One reply, except SCAN which streams rows until SCAN_END."""
        if cmd.strip().upper().startswith("SCAN"):
            parts = []
            while True:
                line = reader.readline()
                if not line:
                    break
                line = line.rstrip("\n")
                parts.append(line)
                if line.startswith("SCAN_END"):
                    break
            return " | ".join(parts)
        line = reader.readline()
        return line.rstrip("\n") if line else "(connection closed)"

    # -- modes ------------------------------------------------------------- #

    def _run_interactive(self, sock: socket.socket, reader):
        try:
            while True:
                try:
                    cmd = input("litedb> ").strip()
                except EOFError:
                    break
                if not cmd:
                    continue
                self._send(sock, cmd)
                print(self._read_response(reader, cmd))
                if cmd.upper() in ("QUIT", "EXIT"):
                    break
        except (BrokenPipeError, ConnectionResetError):
            print("(server closed the connection)")

    def _run_demo(self, sock: socket.socket, reader):
        commands = [
            "PING", "SET name Alice", "SET age 30", "GET name", "GET missing",
            "SCAN a z", "DELETE age", "GET age", "STATS", "QUIT",
        ]
        for cmd in commands:
            self._send(sock, cmd)
            print(f"  > {cmd:<22} -> {self._read_response(reader, cmd)}")


def main():
    p = argparse.ArgumentParser(description="LiteDB client")
    p.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=7379, help="Server port (default: 7379)")
    p.add_argument("--demo", action="store_true", help="Run a scripted command sequence")
    args = p.parse_args()
    LiteDBClient(args.host, args.port).run(demo=args.demo)


if __name__ == "__main__":
    main()
