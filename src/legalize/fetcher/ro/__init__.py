"""Romania (RO) -- legislative fetcher components.

Source: legislatie.just.ro (Ministry of Justice portal).
Strategy: SOAP API for discovery + HTML scraping for text and version history.
"""

from legalize.fetcher.ro.client import RoClient
from legalize.fetcher.ro.discovery import RoDiscovery
from legalize.fetcher.ro.parser import RoMetadataParser, RoTextParser

__all__ = ["RoClient", "RoDiscovery", "RoTextParser", "RoMetadataParser"]
