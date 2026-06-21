"""
type_codec.py — order-preserving ("sortable") encoding of typed column values.

Maps a typed value to a string whose LEXICOGRAPHIC order equals its NATURAL order, so index
range-scans and WHERE comparisons are correct on a string-sorted store (otherwise "10" < "9"
and "-5" > "3").

  INT   — bias the signed value into the unsigned range (x + 2**63), then fixed-width 16-hex.
  FLOAT — IEEE-754 bits sort right for positives but reversed for negatives; flip the sign bit
          for positives and all bits for negatives, then fixed-width 16-hex.
  TEXT/other — natural string order is already correct, so encode as-is.

Rows stay human-readable; only index keys and comparisons use this encoding. Malformed
numerics fall back to the raw string (a teaching-scope limitation).
"""

import struct

_U64 = (1 << 64) - 1


def encode(type_: str, value) -> str:
    if value is None:
        value = ""
    t = (type_ or "").upper()
    try:
        if t == "INT":
            x = int(str(value).strip())
            return format(x + (1 << 63), "016x")          # bias signed -> unsigned, monotonic
        if t in ("FLOAT", "DOUBLE"):
            bits = struct.unpack(">Q", struct.pack(">d", float(str(value).strip())))[0]
            bits = (bits ^ _U64) if (bits & (1 << 63)) else (bits ^ (1 << 63))
            return format(bits, "016x")
        return str(value)                                  # TEXT / BOOL: natural order
    except (ValueError, OverflowError):
        return str(value)


def compare(type_: str, a, b) -> int:
    ea, eb = encode(type_, a), encode(type_, b)
    return (ea > eb) - (ea < eb)
