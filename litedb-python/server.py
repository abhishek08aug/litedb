"""
server.py — TCP Server

CONCEPT:
  The server accepts TCP connections from clients and processes
  LiteDB commands using the query parser and LSM engine.

  Architecture:
    - One thread per client connection (simple, not production-grade)
    - Production databases use async I/O (epoll/kqueue) or thread pools
    - Each client gets its own QueryParser instance
    - The LSMEngine is shared across all clients (thread-safe)

  Usage:
    python server.py --port 7379 --data-dir ./data/primary

  Connect:
    python client.py --port 7379   # interactive client (or --demo)
    nc localhost 7379              # or any TCP client
"""

import sys
import os
import socket
import threading
import argparse
import signal

# Add current directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _loader  # noqa: F401, E402  — registers numbered files under short names

from lsm_engine import LSMEngine          # type: ignore
from btree_engine import BTreeEngine      # type: ignore
from query_parser import QueryParser      # type: ignore


class ClientHandler(threading.Thread):
    """
    Handles a single client connection in its own thread.
    Reads commands line by line, executes them, sends responses.
    """

    def __init__(self, conn: socket.socket, addr: tuple, parser: QueryParser, client_id: int):
        super().__init__(daemon=True)
        self.conn = conn
        self.addr = addr
        self.parser = parser
        self.client_id = client_id

    def run(self):
        print(f"[Server] Client #{self.client_id} connected from {self.addr}")
        self.conn.sendall(b"LiteDB 1.0 ready. Type HELP for commands.\n")

        buffer = ""
        try:
            while True:
                data = self.conn.recv(4096)
                if not data:
                    break  # client disconnected

                buffer += data.decode("utf-8", errors="replace")

                # Process all complete lines in the buffer
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue

                    result = self.parser.execute(line)

                    if result.status == "QUIT":
                        self.conn.sendall(b"BYE\n")
                        return

                    response = result.to_wire()
                    self.conn.sendall(response.encode("utf-8"))

        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            print(f"[Server] Client #{self.client_id} error: {e}")
        finally:
            self.conn.close()
            print(f"[Server] Client #{self.client_id} disconnected")


class LiteDBServer:
    """
    TCP server for LiteDB.
    Accepts connections and spawns a ClientHandler thread per client.
    """

    def __init__(self, host: str, port: int, engine):
        self.host = host
        self.port = port
        self.data_dir = getattr(engine, "data_dir", "")
        self._client_count = 0
        self._running = False

        # Shared storage engine (thread-safe), built by the caller
        self._engine = engine

        # TCP socket
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(128)

        print(f"[Server] LiteDB listening on {host}:{port}")
        print(f"[Server] Engine: {self._engine.name()}   Data directory: {self.data_dir!r}")
        print(f"[Server] Connect with: nc {host} {port}")

    def serve_forever(self):
        """Accept connections in a loop until stopped."""
        self._running = True
        self._sock.settimeout(1.0)  # allow checking _running flag

        try:
            while self._running:
                try:
                    conn, addr = self._sock.accept()
                except socket.timeout:
                    continue

                self._client_count += 1
                parser = QueryParser(self._engine)
                handler = ClientHandler(conn, addr, parser, self._client_count)
                handler.start()
        finally:
            self._sock.close()
            self._engine.close()
            print("[Server] Shutdown complete.")

    def stop(self):
        self._running = False


def main():
    parser = argparse.ArgumentParser(description="LiteDB Server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7379, help="Port (default: 7379)")
    parser.add_argument("--data-dir", default="./data/primary", help="Data directory")
    parser.add_argument("--engine", default="lsm", choices=["lsm", "btree"],
                        help="Storage engine (default: lsm)")
    args = parser.parse_args()

    engine = LSMEngine(args.data_dir) if args.engine == "lsm" else BTreeEngine(args.data_dir)
    server = LiteDBServer(args.host, args.port, engine)

    # Graceful shutdown on Ctrl+C
    def handle_signal(sig, frame):
        print("\n[Server] Shutting down...")
        server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    server.serve_forever()


if __name__ == "__main__":
    main()