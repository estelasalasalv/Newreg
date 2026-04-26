"""CNMC RSS scraper — https://www.cnmc.es/rss.xml

Descarga el feed, limpia el HTML de la descripción,
aplica el filtro de palabras clave energéticas y excluye
los sectores no energéticos.
"""
import re
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Reutiliza las keywords del scraper BOE
from scraper.boe import _find_keywords, _norm

logger = logging.getLogger(__name__)

CNMC_RSS_URL = "https://www.cnmc.es/rss.xml"

EXCLUDED = ["audiovisual", "telecomunicacion", "postal", "ferroviario", "ferrocarril"]

DC_NS = "http://purl.org/dc/elements/1.1/"


def _strip_html(html: str) -> str:
    """Devuelve texto plano a partir de HTML."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    return " ".join(soup.get_text(" ", strip=True).split())


def _enrich_title(title: str, text: str) -> str:
    """Si el título es genérico añade el nombre del expediente encontrado en el texto."""
    # El formato CNMC es siempre: "Expediente NOMBRE_CASO- Metadatos"
    m = re.search(r"Expediente\s+(.+?)\s*-\s*Metadatos", text)
    if m:
        exp = m.group(1).strip()
        if exp and len(exp) > 3:
            return f"{title}: {exp}"
    return title


def _parse_date(pub_date: str) -> Optional[str]:
    """Convierte 'Fri, 24 Apr 2026 17:30:19 +0200' → '2026-04-24'."""
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).strftime("%Y-%m-%d")
    except Exception:
        return None


def _is_excluded(text: str) -> bool:
    t = _norm(text)
    return any(kw in t for kw in EXCLUDED)


def _is_energy_relevant(title: str, summary: str) -> bool:
    """True si alguna keyword energética aparece en título o resumen."""
    return bool(_find_keywords(title + " " + summary))


def scrape() -> List[Dict]:
    """Descarga el RSS de CNMC y devuelve entradas energéticas filtradas."""
    try:
        resp = requests.get(
            CNMC_RSS_URL,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error al descargar CNMC RSS: %s", exc)
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        logger.error("Error al parsear CNMC RSS XML: %s", exc)
        return []

    results: List[Dict] = []
    items = root.findall(".//item")
    logger.info("CNMC RSS: %d ítems en el feed", len(items))

    for item in items:
        title   = (item.findtext("title")   or "").strip()
        link    = (item.findtext("link")    or "").strip()
        pub_raw = (item.findtext("pubDate") or "").strip()
        desc_html = item.findtext("description") or ""
        guid    = (item.findtext("guid")    or link).strip()

        # Limpiar descripción HTML
        full_text = _strip_html(desc_html)
        summary   = full_text[:500]

        # Extraer external_id del guid (ej. "420014 at https://www.cnmc.es")
        node_id     = guid.split()[0] if guid else link
        external_id = f"cnmc-rss-{node_id}"

        # Filtros
        if _is_excluded(title + " " + full_text):
            logger.debug("Excluido (sector): %s", title[:60])
            continue

        if not _is_energy_relevant(title, full_text):
            logger.debug("Sin keywords energéticas: %s", title[:60])
            continue

        published_date  = _parse_date(pub_raw)
        enriched_title  = _enrich_title(title, full_text)

        results.append({
            "source":         "CNMC",
            "external_id":    external_id,
            "title":          enriched_title,
            "published_date": published_date,
            "url":            link,
            "section":        "CNMC RSS",
            "department":     "CNMC",
            "summary":        summary if summary else None,
        })

    logger.info("CNMC RSS: %d/%d entradas relevantes", len(results), len(items))
    return results
