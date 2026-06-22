"""
rpc.py — minimal length-framed JSON-over-TCP RPC.

Every cluster interaction (Raft vote/append, client routing, 2PC) rides on this. A message is
a 4-byte big-endian length prefix followed by a UTF-8 JSON body:

    request:  {"method": <str>, "payload": <obj>}
    response: {"ok": true, "result": <obj>} | {"ok": false, "error": <str>}

The server is threaded (one thread per accepted connection, requests served sequentially on
that connection). The client keeps one persistent connection per target address and transparently
reconnects once on failure — so a peer that dies and is restarted heals without caller changes.
"""

import json
import socket
import struct
import threading
from typing import Any, Callable, Optional

_HEADER = struct.Struct(">I")  # 4-byte unsigned length prefix

Handler = Callable[[dict], Any]


def _send_msg(sock: socket.socket, obj: dict) -> None:
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(_HEADER.pack(len(data)) + data)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed connection")
        buf.extend(chunk)
    return bytes(buf)


def _recv_msg(sock: socket.socket) -> dict:
    (n,) = _HEADER.unpack(_recv_exact(sock, 4))
    return json.loads(_recv_exact(sock, n).decode("utf-8"))


class RPCServer:
    """Threaded RPC server. `handlers` maps a method name to a callable(payload) -> result."""

    def __init__(self, host: str, port: int, handlers: dict[str, Handler]):
        self._host = host
        self._port = port
        self._handlers = handlers
        self._sock: Optional[socket.socket] = None
        self._running = False
        self._conns: set[socket.socket] = set()
        self._conns_lock = threading.Lock()

    def start(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self._host, self._port))
        s.listen(128)
        self._sock = s
        self._running = True
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self) -> None:
        assert self._sock is not None
        while self._running:
            try:
                conn, _addr = self._sock.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_conn, args=(conn,), daemon=True).start()

    def _serve_conn(self, conn: socket.socket) -> None:
        with self._conns_lock:
            self._conns.add(conn)
        try:
            while self._running:
                req = _recv_msg(conn)
                if not self._running:
                    break
                method = req.get("method")
                handler = self._handlers.get(method) if method else None
                if handler is None:
                    resp = {"ok": False, "error": f"unknown method: {method!r}"}
                else:
                    try:
                        result = handler(req.get("payload") or {})
                        resp = {"ok": True, "result": result}
                    except Exception as e:  # surface as an error response, don't kill the conn
                        resp = {"ok": False, "error": f"{type(e).__name__}: {e}"}
                _send_msg(conn, resp)
        except (ConnectionError, OSError, json.JSONDecodeError):
            pass
        finally:
            with self._conns_lock:
                self._conns.discard(conn)
            try:
                conn.close()
            except OSError:
                pass

    def stop(self) -> None:
        self._running = False
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        # close in-flight connections so blocked recv() calls unwind immediately
        with self._conns_lock:
            conns = list(self._conns)
            self._conns.clear()
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                c.close()
            except OSError:
                pass


class RPCClient:
    """Pooled RPC client: one persistent connection per (host, port), reconnect-once on failure."""

    def __init__(self, timeout: float = 2.0):
        self._timeout = timeout
        self._conns: dict[tuple[str, int], socket.socket] = {}
        self._locks: dict[tuple[str, int], threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, addr: tuple[str, int]) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(addr)
            if lock is None:
                lock = threading.Lock()
                self._locks[addr] = lock
            return lock

    def call(self, host: str, port: int, method: str, payload: dict,
             timeout: Optional[float] = None) -> dict:
        """Send one request, return the response dict. Never raises — failures come back as
        {"ok": False, "error": ...} so callers (Raft loops) treat a dead peer as a failed RPC."""
        addr = (host, port)
        to = timeout if timeout is not None else self._timeout
        with self._lock_for(addr):
            last_err = "unknown"
            for attempt in (1, 2):
                sock = self._conns.get(addr)
                try:
                    if sock is None:
                        sock = socket.create_connection(addr, timeout=to)
                        sock.settimeout(to)
                        self._conns[addr] = sock
                    _send_msg(sock, {"method": method, "payload": payload})
                    return _recv_msg(sock)
                except (ConnectionError, OSError, json.JSONDecodeError) as e:
                    last_err = f"{type(e).__name__}: {e}"
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                    self._conns.pop(addr, None)
            return {"ok": False, "error": f"rpc to {host}:{port} failed: {last_err}"}

    def close(self) -> None:
        with self._guard:
            for sock in self._conns.values():
                try:
                    sock.close()
                except OSError:
                    pass
            self._conns.clear()
