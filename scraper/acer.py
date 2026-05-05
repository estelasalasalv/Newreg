"""ACER scraper — Agency for the Cooperation of Energy Regulators.

Fuentes:
  - RSS:       https://www.acer.europa.eu/rss.xml  (noticias)
  - Decisions: https://www.acer.europa.eu/documents/official-documents/individual-decisions

El RSS de ACER tiene formato no estándar:
  - <title> contiene un <a href="..."> HTML en lugar de texto plano
  - <link> contiene la URL del <a> codificada como HTML
  - <pubDate> usa formato propio "Thu, 04/30/2026 - 10:14"
"""
import re
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

ACER_RSS_URL = "https://www.acer.europa.eu/rss.xml"
ACER_DEC_URL = "https://www.acer.europa.eu/documents/official-documents/individual-decisions"
ACER_BASE    = "https://www.acer.europa.eu"
_HEADERS     = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

# Fecha RSS: "Thu, 04/30/2026 - 10:14"  →  "2026-04-30"
_RSS_DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")

# Fecha decisiones HTML: "29.04.2026" → "2026-04-29"
_DOT_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _parse_rss_date(pub_date: str) -> Optional[str]:
    m = _RSS_DATE_RE.search(pub_date or "")
    if m:
        month, day, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def _parse_dot_date(text: str) -> Optional[str]:
    m = _DOT_DATE_RE.search(text or "")
    if m:
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"
    return None


def _is_recent(fecha_iso: Optional[str], cutoff: date) -> bool:
    if not fecha_iso:
        return True
    try:
        return date.fromisoformat(fecha_iso) >= cutoff
    except ValueError:
        return True


def _detect_tipo(title: str, section: str) -> str:
    t = title.lower()
    if "decision" in t:
        return "Decisión ACER"
    if "report" in t or "monitoring" in t or "assessment" in t:
        return "Informe ACER"
    if "opinion" in t:
        return "Opinión ACER"
    if "consultation" in t or "guidelines" in t:
        return "Consulta ACER"
    if section == "ACER Decisión":
        return "Decisión ACER"
    return "Publicación ACER"


def _is_importante(tipo: str) -> str:
    return "Sí" if tipo in ("Decisión ACER", "Informe ACER") else "No"


def _make_ext_id(prefix: str, text: str) -> str:
    return f"{prefix}-{re.sub(r'[^a-z0-9]', '', text[-55:].lower())}"


def scrape_rss(days_back: int = 2) -> List[Dict]:
    """Descarga el RSS de ACER y extrae noticias recientes."""
    cutoff = date.today() - timedelta(days=days_back)

    try:
        resp = requests.get(ACER_RSS_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error al descargar ACER RSS: %s", exc)
        return []

    # Parsear el XML (el <title> contiene HTML, lo tratamos con BeautifulSoup)
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        logger.error("Error al parsear ACER RSS XML: %s", exc)
        return []

    results: List[Dict] = []
    items = root.findall(".//item")
    logger.info("ACER RSS: %d ítems en el feed", len(items))

    for item in items:
        # El <title> contiene un <a href="...">Texto</a> como elemento XML hijo.
        # ET.findtext() solo devuelve el texto directo (vacío), hay que usar tostring.
        title_el  = item.find("title")
        desc      = item.findtext("description") or ""
        pub_raw   = item.findtext("pubDate") or ""

        if title_el is not None:
            # Serializar el elemento completo y parsear con BeautifulSoup
            raw_xml   = ET.tostring(title_el, encoding="unicode")
            title_soup = BeautifulSoup(raw_xml, "html.parser")
            a_tag = title_soup.find("a")
            if a_tag:
                title = a_tag.get_text(strip=True)
                href  = a_tag.get("href", "")
            else:
                title = title_soup.get_text(strip=True)
                href  = ""
        else:
            title = ""
            href  = ""

        if not title:
            continue

        fecha    = _parse_rss_date(pub_raw)
        full_url = (ACER_BASE + href) if href and not href.startswith("http") else href
        if not full_url:
            full_url = ACER_BASE

        if not _is_recent(fecha, cutoff):
            continue

        tipo = _detect_tipo(title, "ACER Noticias")
        results.append({
            "source":         "ACER",
            "external_id":    _make_ext_id("acer-rss", href or title),
            "title":          title,
            "published_date": fecha,
            "url":            full_url,
            "section":        "ACER Noticias",
            "department":     "ACER",
            "summary":        desc[:500] if desc else None,
            "tipo":           "regulacion",
            "plazo":          None,
            "estado":         "Abierta",
            "sector":         "electricidad",
            "tramitaciones":  "No",
            "importante":     _is_importante(tipo),
        })

    logger.info("ACER RSS: %d entradas recientes (desde %s)", len(results), cutoff)
    return results


def scrape_decisions(days_back: int = 2, max_pages: int = 3) -> List[Dict]:
    """Scrape de decisiones individuales ACER recientes."""
    cutoff  = date.today() - timedelta(days=days_back)
    results: List[Dict] = []

    for page in range(max_pages):
        url = f"{ACER_DEC_URL}?page={page}" if page > 0 else ACER_DEC_URL
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Error al descargar ACER Decisions p%d: %s", page, exc)
            break

        soup = BeautifulSoup(resp.text, "lxml")
        # Estructura: div.document > div.department_date > div.date
        #                           > div.title > a
        documents = soup.find_all("div", class_="document")
        if not documents:
            break

        page_had_recent = False
        for doc in documents:
            # Fecha
            date_div = doc.find("div", class_="date")
            fecha    = _parse_dot_date(date_div.get_text() if date_div else "")

            # Solo incluir el documento principal (div.title), no los anexos
            title_div = doc.find("div", class_="title")
            if not title_div:
                continue
            a_tag = title_div.find("a")
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            href  = a_tag.get("href", "")

            if not title or len(title) < 10:
                continue

            if _is_recent(fecha, cutoff):
                page_had_recent = True
                full_url  = href if href.startswith("http") else ACER_BASE + href
                tipo      = _detect_tipo(title, "ACER Decisión")
                results.append({
                    "source":         "ACER",
                    "external_id":    _make_ext_id("acer-dec", href),
                    "title":          title,
                    "published_date": fecha,
                    "url":            full_url,
                    "section":        "ACER Decisión",
                    "department":     "ACER",
                    "summary":        None,
                    "tipo":           "regulacion",
                    "plazo":          None,
                    "estado":         "Abierta",
                    "sector":         "electricidad",
                    "tramitaciones":  "No",
                    "importante":     _is_importante(tipo),
                })

        if not page_had_recent:
            break  # No hay más entradas recientes en páginas siguientes

    # Deduplicar
    seen: set = set()
    unique = []
    for r in results:
        if r["external_id"] not in seen:
            seen.add(r["external_id"])
            unique.append(r)

    logger.info("ACER Decisions: %d entradas recientes (desde %s)", len(unique), cutoff)
    return unique


def scrape(days_back: int = 2) -> List[Dict]:
    """Combina RSS y decisiones recientes, deduplica por external_id."""
    rss  = scrape_rss(days_back=days_back)
    decs = scrape_decisions(days_back=days_back, max_pages=3)

    seen: set = set()
    combined = []
    for e in rss + decs:
        if e["external_id"] not in seen:
            seen.add(e["external_id"])
            combined.append(e)

    logger.info("ACER total: %d entradas nuevas", len(combined))
    return combined
