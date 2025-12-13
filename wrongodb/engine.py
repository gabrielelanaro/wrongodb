"""
Core database facade that wires together storage and indexing.
"""

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

from .storage import AppendOnlyStorage
from .index import InMemoryIndex, Scalar
from .document import normalize_document


@dataclass
class _Record:
    offset: int
    doc: dict[str, object]


class WrongoDB:
    """
    Lightweight document store with append-only persistence and in-memory indexing.
    """

    def __init__(
        self,
        path: str | Path = "data/db.log",
        index_fields: Iterable[str] | None = None,
        sync_every_write: bool = False,
    ) -> None:
        self.path = Path(path)
        self.storage = AppendOnlyStorage(self.path, sync_every_write=sync_every_write)
        self.index = InMemoryIndex(index_fields or [])
        self._docs: list[_Record] = []
        self._doc_by_offset: dict[int, dict[str, object]] = {}
        self._load_existing()

    def insert_one(self, doc: dict[str, object]) -> dict[str, object]:
        """
        Validate, persist, and index a single document.
        """
        normalized = normalize_document(doc)
        offset = self.storage.append(normalized)
        self.index.add(normalized, offset)
        self._docs.append(_Record(offset=offset, doc=normalized))
        self._doc_by_offset[offset] = normalized
        # Return a shallow copy to prevent external mutation.
        return dict(normalized)

    def find(self, filter: dict[str, Scalar] | None = None) -> list[dict[str, object]]:
        """
        Return all documents matching a simple equality filter.
        """
        filter = filter or {}

        if not filter:
            return [dict(record.doc) for record in self._docs]

        # Pick an indexed field if possible to reduce scan.
        indexed_field = next((f for f in filter.keys() if f in self.index.fields), None)

        if indexed_field:
            offsets = self.index.lookup(indexed_field, filter[indexed_field])
            candidates = (self._doc_by_offset[o] for o in offsets if o in self._doc_by_offset)
        else:
            candidates = (record.doc for record in self._docs)

        results: list[dict[str, object]] = []
        for doc in candidates:
            if all(doc.get(k) == v for k, v in filter.items()):
                results.append(dict(doc))
        return results

    def find_one(self, filter: dict[str, object] | None = None) -> dict[str, object] | None:
        """
        Convenience wrapper that returns the first matching document or None.
        """
        results = self.find(filter)
        return results[0] if results else None

    # --- internal helpers -------------------------------------------------

    def _load_existing(self) -> None:
        """
        Read the existing log file (if any) and rebuild in-memory state.
        """
        for offset, doc in self.storage.read_all():
            self._docs.append(_Record(offset=offset, doc=doc))
            self._doc_by_offset[offset] = doc
            self.index.add(doc, offset)


# Backward-compat alias.
MiniMongo = WrongoDB
