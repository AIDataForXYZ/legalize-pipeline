"""Chile (CL) — BCN Ley Chile legislative fetcher components."""

from legalize.fetcher.cl.client import BCNClient
from legalize.fetcher.cl.discovery import BCNDiscovery
from legalize.fetcher.cl.parser import CLMetadataParser, CLTextParser

__all__ = [
    "BCNClient",
    "BCNDiscovery",
    "CLTextParser",
    "CLMetadataParser",
]
