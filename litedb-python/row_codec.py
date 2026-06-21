"""
row_codec.py — length-prefixed, collision-free encoding of a list of string fields.

Each field is written as "<length>:<chars>", so values may contain ':' or spaces. Used for
table rows (column values in schema order) and for serializing schemas/index defs.
"""


def encode(fields) -> str:
    return "".join(f"{len(f)}:{f}" for f in fields)


def decode(s: str):
    out = []
    i = 0
    while i < len(s):
        colon = s.index(":", i)
        length = int(s[i:colon])
        start = colon + 1
        out.append(s[start:start + length])
        i = start + length
    return out
