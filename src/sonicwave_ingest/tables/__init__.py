"""SonicWave ingestion pipeline — raw daily source drops to typed Silver tables.

All pipeline logic lives in this package. The scripts under ``entrypoints/``
only parse arguments and call in here.
"""

from __future__ import annotations

__version__ = "0.1.0"
