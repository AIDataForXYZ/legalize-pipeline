"""Mexico (MX) — legislative fetcher.

Scaffold only. The source has not been wired up yet — see ADDING_A_COUNTRY.md
Step 0 (research, fixtures, version-history spike) before fleshing out the
client/discovery/parser. Candidate sources to evaluate:

- Cámara de Diputados — Leyes Federales Vigentes (https://www.diputados.gob.mx/LeyesBiblio/)
- Diario Oficial de la Federación (https://www.dof.gob.mx)
- Orden Jurídico Nacional (https://www.ordenjuridico.gob.mx)
"""

from legalize.fetcher.mx.client import MXClient
from legalize.fetcher.mx.discovery import MXDiscovery
from legalize.fetcher.mx.parser import MXMetadataParser, MXTextParser

__all__ = ["MXClient", "MXDiscovery", "MXTextParser", "MXMetadataParser"]
