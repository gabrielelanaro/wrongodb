"""
Very small in-memory equality index.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, Iterable, List, Set


class InMemoryIndex:
    def __init__(self, fields: Iterable[str]) -> None:
        self.fields = set(fields)
        # field -> value -> set[offsets]
        self._index: Dict[str, DefaultDict[Any, Set[int]]] = {
            field: defaultdict(set) for field in self.fields
        }

    def add(self, doc: Dict, offset: int) -> None:
        for field in self.fields:
            if field in doc:
                self._index[field][doc[field]].add(offset)

    def lookup(self, field: str, value: Any) -> List[int]:
        if field not in self._index:
            return []
        return list(self._index[field].get(value, []))

    def clear(self) -> None:
        for field in self._index.values():
            field.clear()
