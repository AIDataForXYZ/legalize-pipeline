"""Andorra (AD) — legislative fetcher.

Source: BOPA (Butlletí Oficial del Principat d'Andorra) — the official
gazette of Andorra, electronic version is the legally authoritative
publication since Llei 25/2014.

Architecture:
- BOPAClient: Azure Functions API + blob storage fetch with rate limiting
- BOPADiscovery: walks all 3,464+ butlletins, filters Lleis/Reglaments/
  Legislació delegada/Constitució organismes
- BOPATextParser: parses two HTML formats (modern InDesign 2015+ and
  legacy plain-text 1989-2014)
- BOPAMetadataParser: builds canonical identifiers like BOPA-L-2024-18

Sources:
- API: https://bopaazurefunctions.azurewebsites.net/api/
- Blob: https://bopadocuments.blob.core.windows.net/bopa-documents/
"""

from legalize.fetcher.ad.client import BOPAClient
from legalize.fetcher.ad.discovery import BOPADiscovery
from legalize.fetcher.ad.parser import BOPAMetadataParser, BOPATextParser

__all__ = [
    "BOPAClient",
    "BOPADiscovery",
    "BOPATextParser",
    "BOPAMetadataParser",
]
