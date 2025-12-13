"""
Very small in-memory equality index.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable, Iterable

Scalar = str | int | float | bool | None


class InMemoryIndex:
    def __init__(self, fields: Iterable[str]) -> None:
        self.fields = set(fields)
        # field -> value -> set[offsets]
        self._index: dict[str, dict[Scalar, set[int]]] = {
            field: defaultdict(set) for field in self.fields
        }

    def add(self, doc: dict[str, object], offset: int) -> None:
        for field in self.fields:
            if field in doc:
                value = doc[field]
                if not isinstance(value, Hashable):
                    continue
                self._index[field][value].add(offset)

    def lookup(self, field: str, value: Scalar) -> list[int]:
        if field not in self._index:
            return []
        return list(self._index[field].get(value, []))

    def clear(self) -> None:
        for field in self._index.values():
            field.clear()
