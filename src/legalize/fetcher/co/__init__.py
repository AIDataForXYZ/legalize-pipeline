"""Colombia (CO) — SUIN-Juriscol legislative fetcher."""

from legalize.fetcher.co.client import SuinClient
from legalize.fetcher.co.discovery import SuinDiscovery
from legalize.fetcher.co.parser import SuinMetadataParser, SuinTextParser

__all__ = ["SuinClient", "SuinDiscovery", "SuinTextParser", "SuinMetadataParser"]
