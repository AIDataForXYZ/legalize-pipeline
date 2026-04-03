"""BOE-specific text and metadata parsers.

Wraps the existing xml_parser.py and metadata.py modules
behind the abstract TextParser and MetadataParser interfaces.
"""

from __future__ import annotations

from typing import Any

from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import NormMetadata


class BOETextParser(TextParser):
    """Parse BOE consolidated text XML into Block objects."""

    def parse_text(self, data: bytes) -> list[Any]:
        from legalize.transformer.xml_parser import parse_text_xml

        return parse_text_xml(data)


class BOEMetadataParser(MetadataParser):
    """Parse BOE metadata XML into NormMetadata."""

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        from legalize.fetcher.es.metadata import parse_metadata

        return parse_metadata(data, norm_id)
