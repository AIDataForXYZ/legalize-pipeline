"""Belgium (BE) -- Justel legislative fetcher components."""

from legalize.fetcher.be.client import JustelClient
from legalize.fetcher.be.discovery import JustelDiscovery
from legalize.fetcher.be.parser import JustelMetadataParser, JustelTextParser

__all__ = [
    "JustelClient",
    "JustelDiscovery",
    "JustelTextParser",
    "JustelMetadataParser",
]
