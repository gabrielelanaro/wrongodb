"""
Core database facade that wires together storage and indexing.
"""

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .storage import AppendOnlyStorage
from .index import InMemoryIndex
from .document import normalize_document


class MiniMongo:
    """
    Lightweight document store with append-only persistence and in-memory indexing.
    """

    def __init__(
        self,
        path: str | Path = "data/db.log",
        index_fields: Optional[Iterable[str]] = None,
        sync_every_write: bool = False,
    ) -> None:
        self.path = Path(path)
        self.storage = AppendOnlyStorage(self.path, sync_every_write=sync_every_write)
        self.index = InMemoryIndex(index_fields or [])

    def insert_one(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate, persist, and index a single document.
        """
        raise NotImplementedError

    def find(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Return all documents matching a simple equality filter.
        """
        raise NotImplementedError

    def find_one(self, filter: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Convenience wrapper that returns the first matching document or None.
        """
        results = self.find(filter)
        return results[0] if results else None
