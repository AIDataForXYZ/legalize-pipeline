"""Discovery of Portuguese legislation via dre.tretas.org SQLite dump."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.pt.client import DREClient

logger = logging.getLogger(__name__)

# Series I major legislative types for bootstrap scope.
# These are the primary legal acts published in the Diario da Republica.
MAJOR_DOC_TYPES = (
    "LEI",
    "LEI CONSTITUCIONAL",
    "LEI ORGÂNICA",
    "DECRETO LEI",
    "DECRETO-LEI",
    "DECRETO REGULAMENTAR",
    "DECRETO LEGISLATIVO REGIONAL",
    "DECRETO REGULAMENTAR REGIONAL",
    "DECRETO",
    "PORTARIA",
    "RESOLUÇÃO",
)


class DREDiscovery(NormDiscovery):
    """Discovers Portuguese legislation from the tretas.org SQLite database.

    Discovery yields claint values (dre.pt internal IDs) as norm_id strings.
    The MetadataParser later converts these into human-readable identifiers.
    """

    def __init__(self, doc_types: tuple[str, ...] = MAJOR_DOC_TYPES) -> None:
        self._doc_types = doc_types

    @classmethod
    def create(cls, source: dict) -> DREDiscovery:
        """Create DREDiscovery, optionally with custom doc_types."""
        return cls()

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all claint IDs for Series I major legislative types.

        Filters to series=1 (main legislative acts) and major doc_types.
        Ordered by date ascending so bootstrap commits are chronological.
        """
        assert isinstance(client, DREClient)
        placeholders = ",".join("?" for _ in self._doc_types)
        # Normalize doc_type comparison: strip and upper
        cursor = client._conn.execute(
            f"""
            SELECT DISTINCT d.claint
            FROM dreapp_document d
            JOIN dreapp_documenttext dt ON dt.document_id = d.id
            WHERE d.series = 1
              AND UPPER(TRIM(d.doc_type)) IN ({placeholders})
            ORDER BY d.date ASC
            """,
            tuple(t.upper() for t in self._doc_types),
        )
        count = 0
        for row in cursor:
            yield str(row[0])
            count += 1
            if count % 1000 == 0:
                logger.info("Discovered %d norms so far...", count)

        logger.info("Discovery complete: %d norms found", count)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield claint IDs for documents published on target_date.

        For daily updates with a fresh SQLite dump, this finds new documents.
        """
        assert isinstance(client, DREClient)
        placeholders = ",".join("?" for _ in self._doc_types)
        date_str = target_date.isoformat()

        cursor = client._conn.execute(
            f"""
            SELECT DISTINCT d.claint
            FROM dreapp_document d
            JOIN dreapp_documenttext dt ON dt.document_id = d.id
            WHERE d.series = 1
              AND UPPER(TRIM(d.doc_type)) IN ({placeholders})
              AND d.date = ?
            ORDER BY d.claint ASC
            """,
            (*tuple(t.upper() for t in self._doc_types), date_str),
        )
        for row in cursor:
            yield str(row[0])
