"""Portugal DRE (Diario da Republica Eletronico) clients.

Two client implementations:
1. DREClient (SQLite) — reads from dre.tretas.org weekly dump. For bootstrap.
2. DREHttpClient (HTTP) — fetches directly from diariodarepublica.pt. For daily.

The HTTP client accesses the OutSystems API endpoints of diariodarepublica.pt,
the official Portuguese legislation portal. Protocol details learned from the
dre.tretas.org open source project (GPLv3, https://gitlab.com/hgg/dre).
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from pathlib import Path

import requests

from legalize.fetcher.base import LegislativeClient

logger = logging.getLogger(__name__)

# ─── OutSystems API endpoints (diariodarepublica.pt) ───

_BASE = "https://diariodarepublica.pt/dr"
_MODULE_VERSION_URL = f"{_BASE}/moduleservices/moduleversioninfo"
_OUTSYSTEMS_JS_URL = f"{_BASE}/scripts/OutSystems.js"
_DRS_BY_DATE_URL = f"{_BASE}/screenservices/dr/Home/home/DataActionGetDRByDataCalendario"
_DOC_LIST_URL = (
    f"{_BASE}/screenservices/dr/Legislacao_Conteudos"
    "/Conteudo_Det_Diario/DataActionGetDadosAndApplicationSettings"
)
_DOC_DETAIL_URL = (
    f"{_BASE}/screenservices/dr/Legislacao_Conteudos"
    "/Conteudo_Detalhe/DataActionGetConteudoDataAndApplicationSettings"
)

_RATE_LIMIT_DELAY = 0.5  # seconds between requests


class DREHttpClient(LegislativeClient):
    """HTTP client for Portuguese legislation via diariodarepublica.pt.

    Uses the OutSystems internal API to fetch document lists and full text.
    Works without any local data — suitable for CI/daily updates.
    """

    @classmethod
    def create(cls, country_config):
        """Create DREHttpClient from CountryConfig."""
        source = country_config.source
        timeout = source.get("request_timeout", 30)
        return cls(timeout=timeout)

    def __init__(self, timeout: int = 30) -> None:
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"
                ),
                "Content-Type": "application/json; charset=UTF-8",
            }
        )
        self._csrf_token: str = ""
        self._module_version: str = ""
        self._api_version: str = ""
        self._request_count = 0
        self._init_session()

    def _init_session(self) -> None:
        """Initialize session: fetch CSRF token and module version."""
        # 1. Get CSRF token from OutSystems.js
        r = self._session.get(_OUTSYSTEMS_JS_URL, timeout=self._timeout)
        r.raise_for_status()
        match = re.search(r'"X-CSRFToken","([^"]+)"', r.text)
        if match:
            self._csrf_token = match.group(1)
        else:
            # Fallback: look for csrfTokenValue
            match = re.search(r'csrfTokenValue\s*=\s*"([^"]+)"', r.text)
            if match:
                self._csrf_token = match.group(1)
        logger.info(
            "CSRF token obtained: %s...", self._csrf_token[:8] if self._csrf_token else "NONE"
        )

        # 2. Get module version
        r = self._session.get(_MODULE_VERSION_URL, timeout=self._timeout)
        r.raise_for_status()
        version_data = r.json()
        if isinstance(version_data, dict):
            self._module_version = version_data.get("versionToken", "")
            self._api_version = version_data.get("apiVersion", "")
        elif isinstance(version_data, list) and version_data:
            self._module_version = version_data[0].get("versionToken", "")
            self._api_version = version_data[0].get("apiVersion", "")

        logger.info(
            "Module version: %s", self._module_version[:20] if self._module_version else "NONE"
        )

    def _post(self, url: str, payload: dict) -> dict:
        """POST JSON to an OutSystems endpoint with CSRF token."""
        self._request_count += 1

        # Refresh session every 100 requests
        if self._request_count % 100 == 0:
            logger.info("Refreshing session after %d requests", self._request_count)
            self._init_session()

        headers = {}
        if self._csrf_token:
            headers["X-CSRFToken"] = self._csrf_token

        # Inject version info
        payload.setdefault("versionInfo", {})
        if self._module_version:
            payload["versionInfo"]["moduleVersion"] = self._module_version
        if self._api_version:
            payload["versionInfo"]["apiVersion"] = self._api_version

        r = self._session.post(url, json=payload, headers=headers, timeout=self._timeout)
        r.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return r.json()

    def get_journals_by_date(self, date_str: str) -> list[dict]:
        """Get journal (Diario da Republica) entries for a date.

        Args:
            date_str: Date in YYYY-MM-DD format.

        Returns:
            List of journal dicts with series, number, date info.
        """
        payload = {
            "viewName": "Home.home",
            "screenData": {
                "variables": {
                    "DataCalendario": date_str,
                    "_dataCalendarioInDataFetchStatus": 1,
                    "HasSerie1": True,
                    "HasSerie2": True,
                    "IsRendered": True,
                }
            },
        }
        result = self._post(_DRS_BY_DATE_URL, payload)
        data = result.get("data", {})

        journals = []
        # Extract Series I journals
        serie1 = data.get("SerieI", {})
        if isinstance(serie1, dict) and serie1.get("List"):
            journals.extend(serie1["List"])
        elif isinstance(serie1, list):
            journals.extend(serie1)

        return journals

    def get_documents_by_journal(self, journal_id: int, is_serie1: bool = True) -> list[dict]:
        """Get all documents from a journal issue.

        Args:
            journal_id: Internal journal ID.
            is_serie1: Whether this is Series I (main legislation).

        Returns:
            List of document dicts with metadata.
        """
        payload = {
            "viewName": "Legislacao_Conteudos.Conteudo_Detalhe",
            "screenData": {
                "variables": {
                    "DiarioId": journal_id,
                    "_diarioIdInDataFetchStatus": 1,
                    "IsSerieI": is_serie1,
                    "_isSerieIInDataFetchStatus": 1,
                    "NumeroDeResultadosPorPagina": 2500,
                    "ParteId": "0",
                    "_parteIdInDataFetchStatus": 1,
                }
            },
        }
        result = self._post(_DOC_LIST_URL, payload)
        data = result.get("data", {})

        docs = data.get("DetalheConteudo2", {})
        if isinstance(docs, dict):
            return docs.get("List", [])
        elif isinstance(docs, list):
            return docs

        return []

    def get_document_detail(self, diploma_id: str) -> dict:
        """Fetch full document detail including text.

        Args:
            diploma_id: Internal document content ID (DiplomaConteudoId).

        Returns:
            Dict with document details including Texto/TextoFormatado.
        """
        payload = {
            "viewName": "Legislacao_Conteudos.Conteudo_Detalhe",
            "screenData": {"variables": {}},
            "clientVariables": {
                "DiplomaConteudoId": str(diploma_id),
            },
        }
        result = self._post(_DOC_DETAIL_URL, payload)
        return result.get("data", {}).get("DetalheConteudo", {})

    def get_text(self, diploma_id: str) -> bytes:
        """Fetch the full text of a document.

        Returns HTML text as UTF-8 bytes, compatible with DRETextParser.
        """
        detail = self.get_document_detail(diploma_id)
        text = detail.get("Texto", "").strip()
        if not text:
            text = detail.get("TextoFormatado", "").strip()
        if not text:
            raise ValueError(f"No text found for diploma_id={diploma_id}")
        return text.encode("utf-8")

    def get_metadata(self, diploma_id: str) -> bytes:
        """Fetch metadata for a document.

        Returns JSON bytes compatible with DREMetadataParser.
        Includes ELI URI and vigencia status from the API.
        """
        detail = self.get_document_detail(diploma_id)

        # Vigencia: "NAO_VIGENTE" means repealed
        vigencia = detail.get("Vigencia", "")
        in_force = vigencia != "NAO_VIGENTE"

        # ELI URI (European Legislation Identifier) — preferred source URL
        eli = detail.get("ELI", "")

        meta = {
            "claint": detail.get("ConteudoId", diploma_id),
            "doc_type": detail.get("TipoActo", "").strip().upper(),
            "number": detail.get("Numero", "").strip(),
            "emiting_body": detail.get("Entidade", "").strip(),
            "source": "Serie I",
            "date": detail.get("DataPublicacao", "")[:10],
            "notes": detail.get("Sumario", "").strip(),
            "in_force": in_force,
            "series": 1,
            "dr_number": detail.get("DiarioNumero", ""),
            "dre_pdf": detail.get("URL_PDF", ""),
            "dre_key": "",
            "eli": eli,
            "parte": detail.get("Parte", ""),
        }
        return json.dumps(meta, ensure_ascii=False).encode("utf-8")

    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()


# ─── SQLite client (for bootstrap) ───


class DREClient(LegislativeClient):
    """Client for Portuguese legislation via dre.tretas.org SQLite dump.

    The tretas.org project publishes weekly SQLite exports (~1.4 GB bzip2)
    containing all legislation from the Diario da Republica since 2011.

    Tables used:
    - dreapp_document: metadata (claint, doc_type, number, date, etc.)
    - dreapp_documenttext: full HTML text (text field)
    """

    @classmethod
    def create(cls, country_config):
        """Create DREClient from CountryConfig.

        Expects config.yaml:
            pt:
              source:
                db_path: "/path/to/dre_tretas.db"  # SQLite dump
        """
        db_path = country_config.source.get("db_path", "")
        if not db_path:
            raise ValueError(
                "Portugal requires source.db_path in config.yaml "
                "pointing to the dre.tretas.org SQLite dump. "
                "Download from https://dre.tretas.org/about/"
            )
        return cls(db_path=db_path)

    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        if not self._db_path.exists():
            raise FileNotFoundError(
                f"SQLite database not found: {self._db_path}. "
                "Download the tretas.org dump and decompress it."
            )
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        logger.info("Opened DRE SQLite database: %s", self._db_path)

    def get_text(self, claint: str) -> bytes:
        """Fetch the HTML text for a document by its claint (dre.pt ID).

        Returns the raw HTML from dreapp_documenttext as UTF-8 bytes.
        """
        cursor = self._conn.execute(
            """
            SELECT dt.text
            FROM dreapp_documenttext dt
            JOIN dreapp_document d ON dt.document_id = d.id
            WHERE d.claint = ?
            ORDER BY dt.id DESC
            LIMIT 1
            """,
            (int(claint),),
        )
        row = cursor.fetchone()
        if not row or not row["text"]:
            raise ValueError(f"No text found for claint={claint}")
        return row["text"].encode("utf-8")

    def get_metadata(self, claint: str) -> bytes:
        """Fetch metadata for a document by its claint.

        Returns a JSON dict with Document fields as UTF-8 bytes.
        """
        cursor = self._conn.execute(
            """
            SELECT claint, doc_type, number, emiting_body, source, date,
                   notes, in_force, series, dr_number, dre_pdf, dre_key
            FROM dreapp_document
            WHERE claint = ?
            """,
            (int(claint),),
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No document found for claint={claint}")

        data = dict(row)
        # SQLite returns date as string — keep it as-is for parser
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn:
            self._conn.close()
            logger.info("Closed DRE SQLite database")
