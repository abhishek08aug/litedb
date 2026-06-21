"""column.py — a typed column in a table schema."""


class Column:
    __slots__ = ("name", "type")

    def __init__(self, name: str, type_: str):
        self.name = name
        self.type = type_   # INT | TEXT | FLOAT | BOOLEAN

    def __repr__(self):
        return f"{self.name} {self.type}"
