"""Norm discovery for Andorran legislation via the BOPA API.

Walks the full BOPA newsletter (3,464+ butlletins since 1989), and for each
butlletí calls ``GetDocumentsByBOPA`` to find documents that belong to one of
our four target organismes:

* Lleis (02. Consell General → Lleis)
* Constitució del Principat d'Andorra (02. Consell General → Constitució)
* Legislació delegada (03. Govern → Legislació delegada)
* Reglaments (03. Govern → Reglaments)

Yields ``norm_id`` strings of the form ``"{anyButlleti}/{numButlleti}/{nomDocument}"``
which encode every coordinate the client needs to fetch the document later.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING

from legalize.fetcher.ad.client import BOPAClient
from legalize.fetcher.base import LegislativeClient, NormDiscovery

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Stable organisme GUIDs returned by GetFilters. Verified live 2026-04-07.
GUID_LLEIS = "47bded20-8156-4a8d-8df5-99871b0d343f"
GUID_CONSTITUCIO = "b795497a-f008-4bb5-8cc5-f7b3d447cfaf"
GUID_LEGISLACIO_DELEGADA = "64b04189-16c5-4c51-a409-5e8e2b60b36e"
GUID_REGLAMENTS = "94c941c1-64f5-496f-a788-08c45d44626e"

TARGET_ORGANISME_GUIDS = frozenset(
    {
        GUID_LLEIS,
        GUID_CONSTITUCIO,
        GUID_LEGISLACIO_DELEGADA,
        GUID_REGLAMENTS,
    }
)

# The API returns the human organisme name (with "NN. " prefix on the parent),
# not the GUID, in GetDocumentsByBOPA. We match by name as a fallback for older
# documents whose GUIDs may have changed historically.
TARGET_ORGANISME_NAMES = frozenset(
    {
        "Lleis",
        "Constitució del Principat d’Andorra",  # note: typographic apostrophe
        "Constitució del Principat d'Andorra",  # ASCII apostrophe variant
        "Legislació delegada",
        "Reglaments",
    }
)


def _is_target_document(doc: dict) -> bool:
    """Return True if a BOPA document belongs to one of our four target organismes."""
    organisme = doc.get("organisme", "")
    return organisme in TARGET_ORGANISME_NAMES


def _make_norm_id(doc: dict) -> str:
    """Build a ``norm_id`` from a BOPA document dict.

    Format: ``"{anyButlleti}/{numButlleti}/{nomDocument}"``
    """
    return f"{doc['anyButlleti']}/{doc['numButlleti']}/{doc['nomDocument']}"


def _parse_iso_date(value: str | None) -> date | None:
    """Parse an ISO 8601 date string (with or without time/TZ) into a ``date``."""
    if not value:
        return None
    try:
        # Strip timezone if present, keep only the date part
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


class BOPADiscovery(NormDiscovery):
    """Discovers Andorran legislation by walking the full BOPA newsletter.

    Implementation notes:

    * ``discover_all`` walks every butlletí (oldest first) and yields target
      documents in publication order. This guarantees that when a modificadora
      law arrives, the document it modifies has already been ingested.
    * ``discover_daily`` filters the same newsletter to butlletins published
      on or after ``target_date`` — used by the daily incremental flow.
    """

    def discover_all(
        self,
        client: LegislativeClient,
        **kwargs,
    ) -> Iterator[str]:
        """Yield norm IDs for every Llei/Reglament/LegDel/Constitució document
        across the entire BOPA history (1989 → today)."""
        if not isinstance(client, BOPAClient):
            raise TypeError(f"BOPADiscovery requires BOPAClient, got {type(client).__name__}")

        butlletins = client.get_paginated_newsletter()
        # Sort by publication date ascending so reformes appear after the laws
        # they modify in the iteration order.
        butlletins.sort(key=lambda b: (b.get("dataPublicacio") or "", str(b.get("numBOPA"))))

        total_yielded = 0
        for butlleti in butlletins:
            year = (butlleti.get("dataPublicacio") or "")[:4]
            num = str(butlleti.get("numBOPA") or "")
            if not year or not num:
                continue

            try:
                docs = client.get_butlleti_documents(num=num, year=year)
            except Exception as exc:  # noqa: BLE001 — keep walking on per-butlletí errors
                logger.warning("Failed to fetch butlletí %s/%s: %s", year, num, exc)
                continue

            for doc in docs:
                if _is_target_document(doc):
                    total_yielded += 1
                    yield _make_norm_id(doc)

        logger.info("BOPADiscovery: yielded %d target documents", total_yielded)

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """Yield norm IDs for documents published in butlletins on or after ``target_date``.

        Uses the full newsletter list and filters locally — more reliable than
        ``GetMonthButlletins``, which has a confusing rolling-window behaviour.
        """
        if not isinstance(client, BOPAClient):
            raise TypeError(f"BOPADiscovery requires BOPAClient, got {type(client).__name__}")

        butlletins = client.get_paginated_newsletter()
        butlletins.sort(key=lambda b: (b.get("dataPublicacio") or "", str(b.get("numBOPA"))))

        for butlleti in butlletins:
            pub_date = _parse_iso_date(butlleti.get("dataPublicacio"))
            if pub_date is None or pub_date != target_date:
                continue

            year = (butlleti.get("dataPublicacio") or "")[:4]
            num = str(butlleti.get("numBOPA") or "")
            if not year or not num:
                continue

            try:
                docs = client.get_butlleti_documents(num=num, year=year)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to fetch butlletí %s/%s: %s", year, num, exc)
                continue

            for doc in docs:
                if _is_target_document(doc):
                    yield _make_norm_id(doc)
