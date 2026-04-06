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
from pathlib import Path

from legalize.fetcher.base import HttpClient, LegislativeClient

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

# Screen MVC JS files containing per-endpoint apiVersion hashes.
# OutSystems requires a per-action apiVersion that changes on each deploy.
_SCREEN_JS_MAP: dict[str, str] = {
    "DataActionGetDRByDataCalendario": f"{_BASE}/scripts/dr.Home.home.mvc.js",
    "DataActionGetDadosAndApplicationSettings": (
        f"{_BASE}/scripts/dr.Legislacao_Conteudos.Conteudo_Det_Diario.mvc.js"
    ),
    "DataActionGetConteudoDataAndApplicationSettings": (
        f"{_BASE}/scripts/dr.Legislacao_Conteudos.Conteudo_Detalhe.mvc.js"
    ),
}


def _nested_get(d: dict, *keys: str, default: str = "") -> str:
    """Safely traverse nested dicts: _nested_get(d, 'a', 'b') → d['a']['b']."""
    current = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
    return str(current) if current != default else default


class DREHttpClient(HttpClient):
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
        super().__init__(
            request_timeout=timeout,
            requests_per_second=2.0,
            extra_headers={"Content-Type": "application/json; charset=UTF-8"},
        )
        self._csrf_token: str = ""
        self._module_version: str = ""
        self._api_versions: dict[str, str] = {}  # action_name → apiVersion hash
        self._request_count = 0
        self._init_session()

    def _init_session(self) -> None:
        """Initialize session: fetch CSRF token, module version, and API versions.

        OutSystems requires a per-action apiVersion hash that changes on each
        platform deploy. We extract these from the screen MVC JavaScript files.
        """
        # 1. Get CSRF token from OutSystems.js
        resp = self._request("GET", _OUTSYSTEMS_JS_URL)
        for pattern in [
            r'AnonymousCSRFToken\s*=\s*"([^"]+)"',  # Current format (2025+)
            r'"X-CSRFToken","([^"]+)"',  # Legacy format
            r'csrfTokenValue\s*=\s*"([^"]+)"',  # Older fallback
        ]:
            match = re.search(pattern, resp.text)
            if match:
                self._csrf_token = match.group(1)
                break
        logger.info(
            "CSRF token obtained: %s...", self._csrf_token[:8] if self._csrf_token else "NONE"
        )

        # 2. Get module version
        resp = self._request("GET", _MODULE_VERSION_URL)
        version_data = resp.json()
        if isinstance(version_data, dict):
            self._module_version = version_data.get("versionToken", "")
        elif isinstance(version_data, list) and version_data:
            self._module_version = version_data[0].get("versionToken", "")
        logger.info(
            "Module version: %s", self._module_version[:20] if self._module_version else "NONE"
        )

        # 3. Extract per-action apiVersion hashes from screen MVC JS files.
        # Each callDataAction("ActionName", "url", "apiVersionHash", ...) in the JS
        # contains the hash required for that specific server action.
        fetched_js: dict[str, str] = {}  # url → text (avoid duplicate fetches)
        for action_name, js_url in _SCREEN_JS_MAP.items():
            if js_url not in fetched_js:
                try:
                    js_resp = self._request("GET", js_url)
                    fetched_js[js_url] = js_resp.text
                except Exception:
                    logger.warning("Failed to fetch screen JS: %s", js_url)
                    fetched_js[js_url] = ""

            js_text = fetched_js[js_url]
            # Pattern: callDataAction("ActionName", "endpoint/url", "apiHash", ...)
            pattern = (
                rf'callDataAction\s*\(\s*"{re.escape(action_name)}"\s*,\s*"[^"]+"\s*,\s*"([^"]+)"'
            )
            match = re.search(pattern, js_text)
            if match:
                self._api_versions[action_name] = match.group(1)
                logger.info("API version for %s: %s", action_name, match.group(1)[:20])
            else:
                logger.warning("Could not extract apiVersion for %s", action_name)

    def _action_name_from_url(self, url: str) -> str:
        """Extract the DataAction name from a full endpoint URL."""
        return url.rsplit("/", 1)[-1] if "/" in url else url

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

        # Inject version info with per-action apiVersion
        action_name = self._action_name_from_url(url)
        api_version = self._api_versions.get(action_name, "")

        payload.setdefault("versionInfo", {})
        if self._module_version:
            payload["versionInfo"]["moduleVersion"] = self._module_version
        if api_version:
            payload["versionInfo"]["apiVersion"] = api_version

        # Required since DRE OutSystems migration (2025)
        payload.setdefault("clientVariables", {})

        resp = self._request("POST", url, json=payload, headers=headers)
        return resp.json()

    @staticmethod
    def _parse_json_out(data: dict, key: str = "Json_Out") -> dict:
        """Parse a Json_Out Elasticsearch response string from the API.

        Since the 2025 DRE migration, many endpoints return Elasticsearch
        results wrapped in a JSON string field instead of structured data.
        """
        raw = data.get(key, "")
        if isinstance(raw, str) and raw:
            return json.loads(raw)
        return {}

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
                    # Sentinel date required for Elasticsearch date filtering
                    "DataUltimaPublicacao": "2099-11-26",
                    "HasSerie1": True,
                    "HasSerie2": True,
                    "IsRendered": True,
                }
            },
            "clientVariables": {
                "Data": date_str,
            },
        }
        result = self._post(_DRS_BY_DATE_URL, payload)
        data = result.get("data", {})

        # New format (2025+): Elasticsearch response in Json_Out
        es_data = self._parse_json_out(data)
        if es_data:
            hits = es_data.get("hits", {}).get("hits", [])
            journals = []
            for hit in hits:
                source = hit.get("_source", {})
                title = source.get("conteudoTitle", "")
                journals.append(
                    {
                        "Id": source.get("dbId"),
                        "DiarioId": source.get("dbId"),
                        "Numero": source.get("numero", ""),
                        "DataPublicacao": source.get("dataPublicacao", ""),
                        "conteudoTitle": title,
                    }
                )
            return journals

        # Legacy format: structured SerieI.List
        serie1 = data.get("SerieI", {})
        if isinstance(serie1, dict) and serie1.get("List"):
            return serie1["List"]
        elif isinstance(serie1, list):
            return serie1

        return []

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
                    "DetalheConteudo2": {"List": [], "EmptyListItem": {}},
                    "ParteIdAux": "0",
                    "IsFinished": False,
                    "DiplomaIds": {"List": [], "EmptyListItem": "0"},
                    "NumeroDeResultadosPorPagina": 2500,
                    "DiarioIdAux": journal_id,
                    "DiarioId": journal_id,
                    "_diarioIdInDataFetchStatus": 1,
                    "ParteId": "0",
                    "_parteIdInDataFetchStatus": 1,
                    "IsSerieI": is_serie1,
                    "_isSerieIInDataFetchStatus": 1,
                    "Diario_DetalheConteudo": {
                        "Id": "",
                        "Titulo": "",
                        "DataPublicacao": "",
                    },
                    "_diario_DetalheConteudoInDataFetchStatus": 1,
                }
            },
            "clientVariables": {
                "Data": "",
                "DiplomaConteudoId": "",
            },
        }
        result = self._post(_DOC_LIST_URL, payload)
        data = result.get("data", {})

        # Try structured response: DetalheConteudo.List (current format)
        for key in ("DetalheConteudo", "DetalheConteudo2"):
            container = data.get(key, {})
            if isinstance(container, dict) and container.get("List"):
                return container["List"]
            elif isinstance(container, list):
                return container

        # Elasticsearch response fallback
        es_data = self._parse_json_out(data)
        if es_data:
            hits = es_data.get("hits", {}).get("hits", [])
            return [hit.get("_source", {}) for hit in hits]

        return []

    def get_document_detail(self, diploma_id: str) -> dict:
        """Fetch full document detail including text.

        Args:
            diploma_id: Internal document legislation ID (DipLegisId).

        Returns:
            Dict with document details including Texto/TextoFormatado.
            Field names follow the new DRE API (2025+):
            TipoDiploma, Emissor, ELI, Vigencia, etc.
        """
        payload = {
            "viewName": "Legislacao_Conteudos.Conteudo_Detalhe",
            "screenData": {
                "variables": {
                    "DipLegisId": str(diploma_id),
                },
            },
            "clientVariables": {
                "DiplomaConteudoId": "",
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
        Handles both legacy and new (2025+) field names from the API.
        """
        detail = self.get_document_detail(diploma_id)

        # Vigencia: "NAO_VIGENTE" means repealed
        vigencia = detail.get("Vigencia", "")
        in_force = vigencia != "NAO_VIGENTE"

        # ELI URI (European Legislation Identifier) — preferred source URL
        eli = detail.get("ELI", "")

        # Map field names — new API (2025+) uses different names
        # New: TipoDiploma, Emissor, Id  |  Old: TipoActo, Entidade, ConteudoId
        doc_type = (
            (
                detail.get("TipoActo", "")
                or detail.get("TipoDiploma", "")
                or detail.get("TipoDiplomaExterno", "")
            )
            .strip()
            .upper()
        )

        emiting_body = (detail.get("Entidade", "") or detail.get("Emissor", "")).strip()

        dr_number = detail.get("DiarioNumero", "") or _nested_get(
            detail, "DiarioRepublica", "Numero", default=""
        )

        meta = {
            "claint": detail.get("ConteudoId", detail.get("Id", diploma_id)),
            "doc_type": doc_type,
            "number": detail.get("Numero", "").strip(),
            "emiting_body": emiting_body,
            "source": "Serie I",
            "date": detail.get("DataPublicacao", "")[:10],
            "notes": (detail.get("Sumario", "") or detail.get("Resumo", "")).strip(),
            "in_force": in_force,
            "series": 1,
            "dr_number": dr_number,
            "dre_pdf": detail.get("URL_PDF", ""),
            "dre_key": "",
            "eli": eli,
            "parte": detail.get("Parte", ""),
        }
        return json.dumps(meta, ensure_ascii=False).encode("utf-8")


# ─── SQLite client (for bootstrap) ───


class DREClient(LegislativeClient):
    """Client for Portuguese legislation via dre.tretas.org SQLite dump.

    The tretas.org project publishes weekly SQLite exports (~12 GB decompressed)
    containing all legislation from the Diario da Republica since 1911.

    Tables used:
    - dreapp_document: metadata (id, doc_type, number, date, etc.)
    - dreapp_documenttext: full HTML text (text field)
    """

    @classmethod
    def create(cls, country_config):
        """Create DREClient from CountryConfig.

        Expects config.yaml:
            pt:
              source:
                db_path: "/path/to/YYYY-MM-DD-DRE.sqlite3"
        """
        db_path = country_config.source.get("db_path", "")
        if not db_path:
            raise ValueError(
                "Portugal requires source.db_path in config.yaml "
                "pointing to the dre.tretas.org SQLite dump. "
                "Download from https://uploads.tretas.org/"
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

    def get_text(self, norm_id: str) -> bytes:
        """Fetch the HTML text for a document by its ID.

        Returns the raw HTML from dreapp_documenttext as UTF-8 bytes.
        """
        cursor = self._conn.execute(
            """
            SELECT dt.text
            FROM dreapp_documenttext dt
            WHERE dt.document_id = ?
            ORDER BY dt.id DESC
            LIMIT 1
            """,
            (int(norm_id),),
        )
        row = cursor.fetchone()
        if not row or not row["text"]:
            raise ValueError(f"No text found for id={norm_id}")
        return row["text"].encode("utf-8")

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata for a document by its ID.

        Returns a JSON dict with Document fields as UTF-8 bytes.
        """
        cursor = self._conn.execute(
            """
            SELECT id, doc_type, number, emiting_body, source, date,
                   notes, in_force, series, dr_number, dre_pdf
            FROM dreapp_document
            WHERE id = ?
            """,
            (int(norm_id),),
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No document found for id={norm_id}")

        data = dict(row)
        # Alias 'id' as 'claint' for parser compatibility
        data["claint"] = data["id"]
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn:
            self._conn.close()
            logger.info("Closed DRE SQLite database")
