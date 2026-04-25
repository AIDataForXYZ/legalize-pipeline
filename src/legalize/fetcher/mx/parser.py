"""Mexico parser — multi-source scaffold.

Routes parsing to per-source helpers based on the norm_id prefix
(``DIP-…`` → Diputados, ``DOF-…`` → DOF, ``OJN-…`` → Orden Jurídico Nacional).
Each source's concrete parser lands once Step 0 research is done.
"""

from __future__ import annotations

from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import NormMetadata


def _prefix_of(norm_id: str) -> str:
    return norm_id.split("-", 1)[0] if "-" in norm_id else norm_id


class MXTextParser(TextParser):
    """Parse Mexican consolidated text. Dispatches per source via norm_id prefix."""

    def parse_text(self, data: bytes, norm_id: str | None = None) -> list[Any]:
        prefix = _prefix_of(norm_id) if norm_id else None
        raise NotImplementedError(
            f"MX text parser not wired for source prefix {prefix!r}; "
            "implement the per-source helper before calling."
        )


class MXMetadataParser(MetadataParser):
    """Parse Mexican norm metadata. Dispatches per source via norm_id prefix."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        prefix = _prefix_of(norm_id)
        raise NotImplementedError(
            f"MX metadata parser not wired for source prefix {prefix!r}; "
            "implement the per-source helper before calling."
        )
