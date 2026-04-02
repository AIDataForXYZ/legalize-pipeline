"""Portugal (PT) — DRE legislative fetcher components.

Two client implementations:
- DREClient: SQLite-based, for bootstrap (reads tretas.org dump)
- DREHttpClient: HTTP-based, for daily updates (fetches from diariodarepublica.pt)
"""

from legalize.fetcher.pt.client import DREClient, DREHttpClient
from legalize.fetcher.pt.discovery import DREDiscovery
from legalize.fetcher.pt.parser import DREMetadataParser, DRETextParser

__all__ = [
    "DREClient",
    "DREHttpClient",
    "DREDiscovery",
    "DRETextParser",
    "DREMetadataParser",
]
