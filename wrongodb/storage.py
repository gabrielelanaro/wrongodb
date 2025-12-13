"""
Append-only file-backed storage for documents.
"""

from __future__ import annotations

import json
from pathlib import Path
from collections.abc import Iterable, Iterator


class AppendOnlyStorage:
    def __init__(self, path: Path, sync_every_write: bool = False) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.sync_every_write = sync_every_write

    def append(self, doc: dict[str, object]) -> int:
        """
        Append a JSON document to disk and return the starting byte offset.
        """
        payload = (json.dumps(doc, separators=(",", ":")) + "\n").encode("utf-8")
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self.path.open("ab") as fh:
            fh.seek(0, 2)  # ensure end of file
            offset = fh.tell()
            fh.write(payload)
            fh.flush()
            if self.sync_every_write:
                fh.flush()
                try:
                    # Only available on real file objects.
                    import os

                    os.fsync(fh.fileno())
                except OSError:
                    pass
        return offset

    def read_all(self) -> Iterator[tuple[int, dict[str, object]]]:
        """
        Iterate over all documents on disk, yielding (offset, document).
        """
        if not self.path.exists():
            return iter(())

        def _iter() -> Iterator[tuple[int, dict[str, object]]]:
            with self.path.open("rb") as fh:
                while True:
                    offset = fh.tell()
                    line = fh.readline()
                    if not line:
                        break
                    if not line.strip():
                        continue
                    yield offset, json.loads(line)

        return _iter()
