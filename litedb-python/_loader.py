"""
_loader.py — Module path loader

Ensures the litedb/ directory is on sys.path so that all modules
(wal, memtable, sstable, lsm_engine, …) can be imported by their
plain names from any working directory.

Usage: import _loader  (before any other local imports)
"""

import sys
import os

_DIR = os.path.dirname(os.path.abspath(__file__))

if _DIR not in sys.path:
    sys.path.insert(0, _DIR)