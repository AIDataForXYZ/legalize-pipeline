"""Normattiva Open Data API client for Italian legislation.

Production API: https://api.normattiva.it/t/normattiva.api
Documentation: https://dati.normattiva.it/assets/come_fare_per/API_Normattiva_OpenData.pdf

Two fetch paths:
- URN API (``dettaglio-atto-urn``) — works for Legge, D.Lgs., DL, Decreto, etc.
- Web scrape (``normattiva.it/uri-res/N2Ls?...``) — fallback for DPR, DPCM
  where the URN endpoint returns 404.

Both paths return the same JSON structure so the parser is agnostic.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://api.normattiva.it/t/normattiva.api"
_BFF_PREFIX = "/bff-opendata/v1/api/v1"


class NormattivaClient(HttpClient):
    """HTTP client for Italian legislation via Normattiva Open Data API."""

    def __init__(
        self,
        *,
        api_base: str = DEFAULT_API_BASE,
        web_base: str = "https://www.normattiva.it",
        data_dir: str = "",
        request_timeout: int = 30,
        max_retries: int = 5,
        requests_per_second: float = 2.0,
    ) -> None:
        super().__init__(
            base_url=api_base,
            request_timeout=request_timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
            extra_headers={
                "Origin": "https://dati.normattiva.it",
                "Accept": "application/json",
            },
        )
        self._web_base = web_base.rstrip("/")
        self._data_dir = data_dir
        self._discovery_meta: dict[str, dict] | None = None

    @classmethod
    def create(cls, country_config: CountryConfig) -> NormattivaClient:
        source = country_config.source or {}
        return cls(
            api_base=source.get("api_base", DEFAULT_API_BASE),
            web_base=source.get("web_base", "https://www.normattiva.it"),
            data_dir=country_config.data_dir,
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 5),
            requests_per_second=source.get("requests_per_second", 2.0),
        )

    # ── Core interface ──

    def get_text(self, norm_id: str) -> bytes:
        """Fetch consolidated text for an act.

        Tries the URN API first, falls back to web scraping for act types
        the API doesn't support (DPR, DPCM).
        """
        act_meta = self._get_act_meta(norm_id)
        urn = _act_to_urn(act_meta)

        # Try URN API
        if urn:
            resp = self._fetch_urn(urn)
            if resp and resp.get("data", {}).get("atto"):
                return json.dumps(resp, ensure_ascii=False).encode("utf-8")

        # Fallback: web scrape
        logger.info("URN API unavailable for %s, using web scrape", norm_id)
        return self._fetch_via_web(norm_id, act_meta)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata for an act (0 API calls if discovery cache exists)."""
        act_meta = self._get_act_meta(norm_id)
        atto = _search_result_to_atto(act_meta)
        return json.dumps({"data": {"atto": atto}}, ensure_ascii=False).encode("utf-8")

    # ── Discovery metadata ──

    def _get_act_meta(self, norm_id: str) -> dict:
        """Get discovery metadata for an act.

        Lazy-loads discovery_meta.json once per client instance.
        Falls back to search API if the cache doesn't exist or misses.
        """
        if self._discovery_meta is None:
            from legalize.fetcher.it.discovery import NormattivaDiscovery

            self._discovery_meta = NormattivaDiscovery.load_meta_cache(self._data_dir)
            if self._discovery_meta:
                logger.info("Loaded discovery metadata for %d acts", len(self._discovery_meta))

        meta = self._discovery_meta.get(norm_id) if self._discovery_meta else None
        if meta:
            return meta

        # Fallback: search API (for manual single-norm fetches)
        result = self.search_simple(text=norm_id, order="recente", page=1, per_page=1)
        acts = result.get("listaAtti", [])
        if not acts:
            raise ValueError(f"Act {norm_id} not found")
        return acts[0]

    # ── Web scraping fallback ──

    def _fetch_via_web(self, norm_id: str, act_meta: dict) -> bytes:
        """Fetch act content by scraping the normattiva.it web page.

        Used for DPR/DPCM where the URN API returns 404. The web page
        contains the same AKN HTML as the API.
        """
        urn = _act_to_urn(act_meta)
        if not urn:
            raise ValueError(f"Cannot build URN for {norm_id}")

        url = f"{self._web_base}/uri-res/N2Ls?{urn}!vig="
        page_html = self._get(url).decode("utf-8", errors="replace")

        # Extract <div class="bodyTesto">...</div> with balanced tag matching
        article_html = _extract_body_testo(page_html)

        # Convert relative links to absolute
        if article_html:
            article_html = article_html.replace(
                'href="/uri-res/', f'href="{self._web_base}/uri-res/'
            )

        atto = _search_result_to_atto(act_meta)
        atto["articoloHtml"] = article_html

        return json.dumps(
            {"code": None, "message": None, "data": {"atto": atto}, "success": True},
            ensure_ascii=False,
        ).encode("utf-8")

    # ── Version walking (reform history) ──

    def walk_article_versions(self, urn_base: str, art_num: str) -> list[dict]:
        """Walk all temporal versions of a single article.

        Uses @originale for the first version, then iterates using
        !vig=<day after articoloDataFineVigenza> until fineVigenza=99999999.
        """
        from datetime import date as date_cls, timedelta

        versions: list[dict] = []
        urn = f"{urn_base}~art{art_num}@originale"

        resp = self._fetch_urn(urn)
        if not resp or not resp.get("data", {}).get("atto"):
            urn = f"{urn_base}~art{art_num}"
            resp = self._fetch_urn(urn)
            if not resp or not resp.get("data", {}).get("atto"):
                return versions

        atto = resp["data"]["atto"]
        versions.append(
            {
                "article_num": art_num,
                "html": atto.get("articoloHtml", ""),
                "vigenza_inizio": atto.get("articoloDataInizioVigenza", ""),
                "vigenza_fine": atto.get("articoloDataFineVigenza", ""),
            }
        )

        max_versions = 50
        while len(versions) < max_versions:
            fine = versions[-1]["vigenza_fine"]
            if not fine or fine == "99999999" or len(fine) != 8:
                break
            try:
                d = date_cls(int(fine[:4]), int(fine[4:6]), int(fine[6:8]))
                next_day = (d + timedelta(days=1)).strftime("%Y-%m-%d")
            except ValueError:
                break

            resp = self._fetch_urn(f"{urn_base}~art{art_num}!vig={next_day}")
            if not resp or not resp.get("data", {}).get("atto"):
                break
            atto = resp["data"]["atto"]
            if not atto.get("articoloHtml"):
                break
            versions.append(
                {
                    "article_num": art_num,
                    "html": atto["articoloHtml"],
                    "vigenza_inizio": atto.get("articoloDataInizioVigenza", ""),
                    "vigenza_fine": atto.get("articoloDataFineVigenza", ""),
                }
            )

        return versions

    # ── Search helpers ──

    def search_simple(
        self,
        text: str = "*",
        order: str = "vecchio",
        page: int = 1,
        per_page: int = 100,
        filters: dict[str, Any] | None = None,
    ) -> dict:
        """Ricerca semplice — paginated search with optional facet filters."""
        body: dict[str, Any] = {
            "testoRicerca": text,
            "orderType": order,
            "paginazione": {
                "paginaCorrente": page,
                "numeroElementiPerPagina": per_page,
            },
        }
        if filters:
            body["filtriMap"] = filters
        url = f"{self._base_url}{_BFF_PREFIX}/ricerca/semplice"
        return json.loads(self._request("POST", url, json=body).content)

    def search_updated(self, date_from: str, date_to: str) -> dict:
        """Ricerca atti aggiornati — acts modified between two ISO timestamps."""
        url = f"{self._base_url}{_BFF_PREFIX}/ricerca/aggiornati"
        return json.loads(
            self._request(
                "POST",
                url,
                json={
                    "dataInizioAggiornamento": date_from,
                    "dataFineAggiornamento": date_to,
                },
            ).content
        )

    # ── Internal ──

    def _fetch_urn(self, urn: str) -> dict | None:
        """POST to dettaglio-atto-urn and return parsed JSON."""
        url = f"{self._base_url}{_BFF_PREFIX}/atto/dettaglio-atto-urn"
        try:
            data = json.loads(self._request("POST", url, json={"urn": urn}).content)
            if data.get("code") and data["code"] != "null":
                return None
            return data
        except Exception:
            logger.warning("Failed to fetch URN %s", urn, exc_info=True)
            return None


# ── Pure functions (no state, no side effects) ──


def _act_to_urn(act: dict) -> str | None:
    """Build URN from a search result dict."""
    tipo_desc = act.get("denominazioneAtto", "")
    code = TIPO_TO_CODE.get(tipo_desc)
    if not code:
        return None

    if code == "COS":
        return "urn:nir:stato:costituzione:1947-12-27;1"

    urn_type = URN_TYPE_MAP.get(code)
    if not urn_type:
        return None

    anno = int(act.get("annoProvvedimento", 0) or 0)
    mese = int(act.get("meseProvvedimento", 0) or 0)
    giorno = int(act.get("giornoProvvedimento", 0) or 0)
    numero = act.get("numeroProvvedimento", "")

    if not anno or not numero:
        return None

    numero = int(numero) if isinstance(numero, str) and numero.isdigit() else numero

    if mese and giorno:
        date_str = f"{anno:04d}-{mese:02d}-{giorno:02d}"
    else:
        data_gu = act.get("dataGU", "")
        if data_gu and len(data_gu) >= 10:
            date_str = data_gu[:10]
        else:
            return None

    return f"urn:nir:stato:{urn_type}:{date_str};{numero}"


def _search_result_to_atto(act: dict) -> dict:
    """Convert a search result dict to the atto format expected by the parser."""
    tipo_code = TIPO_TO_CODE.get(act.get("denominazioneAtto", ""), "")
    anno_gu = mese_gu = giorno_gu = 0
    data_gu = act.get("dataGU", "")
    if data_gu and len(data_gu) >= 10:
        parts = data_gu.split("-")
        if len(parts) == 3:
            anno_gu, mese_gu, giorno_gu = int(parts[0]), int(parts[1]), int(parts[2])

    return {
        "titolo": act.get("descrizioneAtto", ""),
        "sottoTitolo": act.get("titoloAtto", ""),
        "articoloHtml": "",
        "tipoProvvedimentoDescrizione": act.get("denominazioneAtto", ""),
        "tipoProvvedimentoCodice": tipo_code,
        "annoProvvedimento": int(act.get("annoProvvedimento", 0) or 0),
        "meseProvvedimento": int(act.get("meseProvvedimento", 0) or 0),
        "giornoProvvedimento": int(act.get("giornoProvvedimento", 0) or 0),
        "numeroProvvedimento": int(act.get("numeroProvvedimento", 0) or 0),
        "tipoSupplementoCode": act.get("tipoSupplemento", "NO"),
        "numeroSupplemento": int(act.get("numeroSupplemento", 0) or 0),
        "annoGU": anno_gu,
        "meseGU": mese_gu,
        "giornoGU": giorno_gu,
        "numeroGU": int(act.get("numeroGU", 0) or 0),
        "articoloDataInizioVigenza": "",
        "articoloDataFineVigenza": "99999999",
    }


def _extract_body_testo(page_html: str) -> str:
    """Extract <div class="bodyTesto">...</div> from a web page.

    Uses balanced tag matching to handle nested divs.
    """
    start = page_html.find('<div class="bodyTesto">')
    if start < 0:
        return ""

    depth = 0
    i = start
    while i < len(page_html):
        if page_html[i : i + 4] == "<div":
            depth += 1
        elif page_html[i : i + 6] == "</div>":
            depth -= 1
            if depth == 0:
                return page_html[start : i + 6]
        i += 1
    return ""


# ── Mappings ──

URN_TYPE_MAP: dict[str, str] = {
    "COS": "costituzione",
    "PLC": "legge.costituzionale",
    "PLE": "legge",
    "PLL": "decreto.legislativo",
    "PDL": "decreto.legge",
    "PPR": "decreto.del.presidente.della.repubblica",
    "PCM_DPC": "decreto.del.presidente.del.consiglio.dei.ministri",
    "DCT": "decreto",
    "PDM": "decreto.ministeriale",
    "POR": "ordinanza",
    "DEL": "deliberazione",
    "D10": "regolamento",
    "PRD": "regio.decreto",
    "PRL": "regio.decreto.legge",
    "PLU": "decreto.luogotenenziale",
    "RDL": "regio.decreto.legislativo",
    "PLG": "decreto.legislativo.luogotenenziale",
    "DCS": "decreto.legislativo.del.capo.provvisorio.dello.stato",
    "PCS": "decreto.del.capo.provvisorio.dello.stato",
    "DLL": "decreto.legge.luogotenenziale",
    "PZP": "decreto.legislativo.presidenziale",
    "SNI": "decreto.reale",
    "DDD": "decreto.del.duce",
    "PCG": "decreto.del.capo.del.governo",
    "FAC": "decreto.del.duce.del.fascismo.capo.del.governo",
    "DPP": "decreto.presidenziale",
    "3NA": "decreto.del.capo.del.governo.primo.ministro.segretario.di.stato",
    "8ZL": "determinazione.intercommissariale",
    "GRC": "determinazione.del.commissario.per.le.finanze",
    "DPB": "determinazione.del.commissario.per.la.produzione.bellica",
}

TIPO_TO_CODE: dict[str, str] = {
    "COSTITUZIONE": "COS",
    "LEGGE COSTITUZIONALE": "PLC",
    "LEGGE": "PLE",
    "DECRETO LEGISLATIVO": "PLL",
    "DECRETO-LEGGE": "PDL",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA": "PPR",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "PCM_DPC",
    "DECRETO": "DCT",
    "DECRETO MINISTERIALE": "PDM",
    "ORDINANZA": "POR",
    "DELIBERAZIONE": "DEL",
    "REGOLAMENTO": "D10",
    "REGIO DECRETO": "PRD",
    "REGIO DECRETO-LEGGE": "PRL",
    "DECRETO LUOGOTENENZIALE": "PLU",
    "REGIO DECRETO LEGISLATIVO": "RDL",
    "DECRETO LEGISLATIVO LUOGOTENENZIALE": "PLG",
    "DECRETO LEGISLATIVO DEL CAPO PROVVISORIO DELLO STATO": "DCS",
    "DECRETO DEL CAPO PROVVISORIO DELLO STATO": "PCS",
    "DECRETO-LEGGE LUOGOTENENZIALE": "DLL",
    "DECRETO LEGISLATIVO PRESIDENZIALE": "PZP",
    "DECRETO REALE": "SNI",
    "DECRETO DEL DUCE": "DDD",
    "DECRETO DEL CAPO DEL GOVERNO": "PCG",
    "DECRETO DEL DUCE DEL FASCISMO, CAPO DEL GOVERNO": "FAC",
    "DECRETO PRESIDENZIALE": "DPP",
    "DECRETO DEL CAPO DEL GOVERNO, PRIMO MINISTRO SEGRETARIO DI STATO": "3NA",
    "DETERMINAZIONE DEL COMMISSARIO PER LE FINANZE": "GRC",
    "DETERMINAZIONE DEL COMMISSARIO PER LA PRODUZIONE BELLICA": "DPB",
    "DETERMINAZIONE INTERCOMMISSARIALE": "8ZL",
}
