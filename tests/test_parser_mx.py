"""Mexico fetcher — scaffold tests.

The MX fetcher is a placeholder pending Step 0 research (see
ADDING_A_COUNTRY.md). These tests pin the registry contract and the
NotImplementedError stubs so future contributors know which surface to keep
stable while the source gets wired up.
"""

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.mx.parser import MXMetadataParser, MXTextParser


def test_registry_dispatch():
    text_parser = get_text_parser("mx")
    metadata_parser = get_metadata_parser("mx")
    assert isinstance(text_parser, MXTextParser)
    assert isinstance(metadata_parser, MXMetadataParser)


def test_text_parser_is_scaffold():
    with pytest.raises(NotImplementedError):
        MXTextParser().parse_text(b"")


def test_metadata_parser_is_scaffold():
    with pytest.raises(NotImplementedError):
        MXMetadataParser().parse(b"", "STUB-1")
