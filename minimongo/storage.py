"""
Append-only file-backed storage for documents.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Iterator


class AppendOnlyStorage:
    def __init__(self, path: Path, sync_every_write: bool = False) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sync_every_write = sync_every_write

    def append(self, doc: Dict) -> int:
        """
        Append a JSON document to disk.
        Returns the byte offset where the document starts.
        """
        raise NotImplementedError

    def read_all(self) -> Iterator[Dict]:
        """
        Iterate over all documents from disk.
        """
        raise NotImplementedError
