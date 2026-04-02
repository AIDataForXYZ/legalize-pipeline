"""Discovery of German federal legislation via gesetze-im-internet.de TOC."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date
from xml.etree import ElementTree as ET

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.de.client import GIIClient

logger = logging.getLogger(__name__)


class GIIDiscovery(NormDiscovery):
    """Discovers all federal laws from the GII table of contents XML.

    The TOC at https://www.gesetze-im-internet.de/gii-toc.xml lists ~6900 laws.
    Each <item> has a <link> pointing to the ZIP download URL.
    The norm_id is the URL slug extracted from the link.
    """

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all URL slugs from the GII TOC."""
        assert isinstance(client, GIIClient)
        toc = client.get_toc()
        root = ET.fromstring(toc)

        for item in root.findall(".//item"):
            link = item.find("link")
            if link is None or not link.text:
                continue
            slug = self._extract_slug(link.text)
            if slug:
                yield slug

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """GII has no date-based filtering.

        For daily updates, we would need to re-download the full TOC and compare
        builddate attributes against cached state. For now, yields nothing.
        """
        return iter(())

    @staticmethod
    def _extract_slug(url: str) -> str | None:
        """Extract the law slug from a GII ZIP URL.

        Example: 'http://www.gesetze-im-internet.de/gg/xml.zip' -> 'gg'
        """
        match = re.search(r"gesetze-im-internet\.de/([^/]+)/xml\.zip", url)
        return match.group(1) if match else None
