"""
Document validation and normalization helpers.
"""

from __future__ import annotations

import copy
import uuid

from .errors import DocumentValidationError


def normalize_document(doc: dict[str, object]) -> dict[str, object]:
    """
    Ensure the document is JSON-serializable-ish and has an _id.
    """
    if not isinstance(doc, dict):
        raise DocumentValidationError("document must be a dict")

    for key in doc.keys():
        if not isinstance(key, str):
            raise DocumentValidationError("document keys must be strings")

    normalized = copy.deepcopy(doc)
    normalized.setdefault("_id", str(uuid.uuid4()))
    return normalized
