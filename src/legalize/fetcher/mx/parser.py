"""Mexico parser — scaffold.

Concrete parsing waits on Step 0 research (RESEARCH-MX.md, 5 fixtures, version
spike). The classes below satisfy the registry contract so imports and CLI
dispatch work; calling them raises until the source is wired up.
"""

from __future__ import annotations

from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import NormMetadata


class MXTextParser(TextParser):
    """Parse Mexican consolidated text into Block/Version objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        raise NotImplementedError("MX parser is a scaffold; wire the source first.")


class MXMetadataParser(MetadataParser):
    """Parse Mexican norm metadata into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        raise NotImplementedError("MX parser is a scaffold; wire the source first.")
