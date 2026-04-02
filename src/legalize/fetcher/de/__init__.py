"""Germany (DE) — gesetze-im-internet.de legislative fetcher components."""

from legalize.fetcher.de.client import GIIClient
from legalize.fetcher.de.discovery import GIIDiscovery
from legalize.fetcher.de.parser import GIIMetadataParser, GIITextParser

__all__ = [
    "GIIClient",
    "GIIDiscovery",
    "GIITextParser",
    "GIIMetadataParser",
]
