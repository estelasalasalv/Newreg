"""CNMC scraper — consultas públicas de energía.

Target: https://www.cnmc.es/consultas-publicas/energia
Tipo: consulta (no regulación)
Excluye: audiovisual, telecomunicaciones, postal, ferroviario
"""
import logging
import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

CNMC_URL  = "https://www.cnmc.es/consultas-publicas/energia"
CNMC_BASE = "https://www.cnmc.es"

EXCLUDED_KEYWORDS = [
    "audiovisual", "telecomunicaciones", "postal",
    "ferroviario", "ferrocarril", "telecomunicacion",
]

_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}


def _is_excluded(text: str) -> bool:
    return any(kw in text.lower() for kw in EXCLUDED_KEYWORDS)


def _fetch(url: str, page: int = 0) -> Optional[BeautifulSoup]:
    params = {"page": page} if page > 0 else {}
    try:
        resp = requests.get(url, params=params, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as exc:
        logger.error("CNMC request failed (page=%d): %s", page, exc)
        return None


def _fetch_plazo(url: str) -> str:
    """Obtiene el plazo de la página individual de la consulta."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        # Busca el span con "Desde … hasta …"
        for el in soup.find_all(string=re.compile(r"hasta\s+\d", re.IGNORECASE)):
            text = el.strip()
            if text:
                return text
        # Fallback: frase de plazo
        for el in soup.find_all(string=re.compile(r"plazo.*finaliza", re.IGNORECASE)):
            return el.strip()[:120]
    except Exception as exc:
        logger.debug("No se pudo obtener plazo de %s: %s", url, exc)
    return ""


def _extract_entries(soup: BeautifulSoup) -> List[Dict]:
    entries = []
    rows = (
        soup.select("div.view-content .views-row")
        or soup.select("article.node--type-consulta-publica")
        or soup.select("div.views-row")
        or soup.select("div.border-bott.views-row")
    )

    for row in rows:
        link_el = row.select_one("a[href]")
        if not link_el:
            continue
        title = link_el.get_text(strip=True)
        href  = link_el.get("href", "")
        if href.startswith("/"):
            href = CNMC_BASE + href

        if _is_excluded(title):
            continue

        slug        = href.rstrip("/").split("/")[-1]
        external_id = f"cnmc-{slug}"

        entries.append({
            "source":         "CNMC",
            "tipo":           "consulta",
            "external_id":    external_id,
            "title":          title,
            "published_date": None,
            "url":            href,
            "section":        "Consultas públicas CNMC",
            "department":     "CNMC",
            "summary":        None,
            "plazo":          None,   # se rellena después
        })

    return entries


def scrape(max_pages: int = 5, fetch_plazos: bool = True) -> List[Dict]:
    """Scrape CNMC consultas públicas de energía."""
    results: List[Dict] = []

    for page in range(max_pages):
        soup = _fetch(CNMC_URL, page)
        if soup is None:
            break
        entries = _extract_entries(soup)
        if not entries:
            break
        results.extend(entries)
        if not soup.select_one("a[title='Página siguiente'], a.pager__item--next, li.pager-next a"):
            break

    # Obtener plazo de cada consulta
    if fetch_plazos:
        for e in results:
            e["plazo"] = _fetch_plazo(e["url"])
            logger.debug("Plazo [%s]: %s", e["title"][:40], e["plazo"])

    logger.info("CNMC consultas: %d entradas", len(results))
    return results
