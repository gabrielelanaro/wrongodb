"""
Compatibility layer for the old `minimongo` import path.

New code should import from `wrongodb`.
"""

from wrongodb import MiniMongo, WrongoDB

__all__ = ["MiniMongo", "WrongoDB"]

