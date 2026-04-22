"""Shared helpers for the ES fidelity loop."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

import requests

WORKDIR = Path("/tmp/es-audit")
WORKDIR.mkdir(exist_ok=True)
SANDBOX = WORKDIR / "sandbox"
SANDBOX.mkdir(exist_ok=True)
CATALOG_CACHE = WORKDIR / "catalog.json"
LOG_CSV = WORKDIR / "fidelity-log.csv"
DEFECTS_DIR = WORKDIR / "defects"
DEFECTS_DIR.mkdir(exist_ok=True)

BOE_BASE = "https://www.boe.es"
API = f"{BOE_BASE}/datosabiertos/api/legislacion-consolidada"

USER_AGENT = "legalize-bot/1.0 fidelity-loop (+https://github.com/legalize-dev/legalize)"


def http_get(url: str, headers: dict | None = None, tries: int = 3, pause: float = 0.6) -> bytes:
    """GET with retries + light rate limiting. Returns body bytes or b'' on 404."""
    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)
    for attempt in range(tries):
        r = requests.get(url, headers=hdrs, timeout=60)
        if r.status_code == 404:
            return b""
        if r.status_code == 200:
            time.sleep(pause)
            return r.content
        time.sleep(pause * (2**attempt))
    r.raise_for_status()
    return b""


def fetch_texto_xml(boe_id: str) -> bytes:
    """Consolidated XML text. Empty bytes if 404 (norm not in consolidada)."""
    return http_get(f"{API}/id/{boe_id}/texto", headers={"Accept": "application/xml"})


def fetch_metadatos_xml(boe_id: str) -> bytes:
    return http_get(f"{API}/id/{boe_id}/metadatos", headers={"Accept": "application/xml"})


def fetch_diario_xml(boe_id: str) -> bytes:
    """Raw diary XML. Richer content (tables, imgs, analisis) but no version history."""
    return http_get(f"{BOE_BASE}/diario_boe/xml.php?id={boe_id}")


def fetch_consolidada_html(boe_id: str) -> bytes:
    """BOE's own rendered consolidated text. Reference for fidelity comparison."""
    return http_get(f"{BOE_BASE}/buscar/act.php?id={boe_id}")


@dataclass
class CatalogEntry:
    boe_id: str
    title: str
    rango: str
    rango_code: str
    ambito: str
    ambito_code: str
    dept: str
    dept_code: str
    pub_date: str

    @property
    def decade(self) -> str:
        return self.pub_date[:3] + "0s"


def load_catalog(force: bool = False) -> list[CatalogEntry]:
    """Fetch and cache the full BOE consolidated catalog."""
    if CATALOG_CACHE.exists() and not force:
        raw = json.loads(CATALOG_CACHE.read_text())
        return [CatalogEntry(**e) for e in raw]

    entries: list[dict] = []
    offset = 0
    batch = 1000
    while True:
        url = f"{API}?limit={batch}&offset={offset}"
        body = http_get(url, headers={"Accept": "application/json"})
        data = json.loads(body).get("data", [])
        if not data:
            break
        entries.extend(data)
        offset += batch
        if len(data) < batch:
            break

    parsed: list[CatalogEntry] = []
    for e in entries:
        parsed.append(
            CatalogEntry(
                boe_id=e.get("identificador", ""),
                title=e.get("titulo", "")[:200],
                rango=e.get("rango", {}).get("texto", ""),
                rango_code=e.get("rango", {}).get("codigo", ""),
                ambito=e.get("ambito", {}).get("texto", ""),
                ambito_code=e.get("ambito", {}).get("codigo", ""),
                dept=e.get("departamento", {}).get("texto", ""),
                dept_code=e.get("departamento", {}).get("codigo", ""),
                pub_date=e.get("fecha_publicacion", "00000000"),
            )
        )
    CATALOG_CACHE.write_text(json.dumps([e.__dict__ for e in parsed]))
    return parsed


def strip_nbsp(s: str) -> str:
    """Normalize whitespace for fair comparison."""
    s = s.replace(" ", " ").replace(" ", " ").replace(" ", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def word_seq(s: str) -> list[str]:
    """Tokenize to a word sequence for sequence-match comparison."""
    return re.findall(r"\w+", s.lower(), re.UNICODE)
