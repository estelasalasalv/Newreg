"""ACER scraper — Agency for the Cooperation of Energy Regulators.

Fuentes:
  - RSS:       https://www.acer.europa.eu/rss.xml  (noticias)
  - Decisions: https://www.acer.europa.eu/documents/official-documents/individual-decisions

Solo carga entradas publicadas en los últimos days_back días (por defecto 1 = ayer).
Las entradas antiguas ya están en BD; el upsert garantiza que no se duplican.
"""
import re
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

ACER_RSS_URL = "https://www.acer.europa.eu/rss.xml"
ACER_DEC_URL = "https://www.acer.europa.eu/documents/official-documents/individual-decisions"
ACER_BASE    = "https://www.acer.europa.eu"
_HEADERS     = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}


def _parse_rss_date(pub_date: str) -> Optional[str]:
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).strftime("%Y-%m-%d")
    except Exception:
        return None


def _parse_dot_date(text: str) -> Optional[str]:
    """Convierte '29.04.2026' → '2026-04-29'."""
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text or "")
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def _is_recent(fecha_iso: Optional[str], cutoff: date) -> bool:
    if not fecha_iso:
        return True  # sin fecha → incluir por si acaso
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


def scrape_rss(days_back: int = 1) -> List[Dict]:
    """Descarga el RSS de ACER y devuelve entradas recientes."""
    cutoff = date.today() - timedelta(days=days_back)

    try:
        resp = requests.get(ACER_RSS_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error al descargar ACER RSS: %s", exc)
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as exc:
        logger.error("Error al parsear ACER RSS: %s", exc)
        return []

    results: List[Dict] = []
    items = root.findall(".//item")
    logger.info("ACER RSS: %d ítems en el feed", len(items))

    for item in items:
        title   = (item.findtext("title")       or "").strip()
        link    = (item.findtext("link")        or "").strip()
        pub_raw = (item.findtext("pubDate")     or "").strip()
        desc    = (item.findtext("description") or "").strip()

        if not title or not link:
            continue

        fecha = _parse_rss_date(pub_raw)
        if not _is_recent(fecha, cutoff):
            continue

        full_url = link if link.startswith("http") else ACER_BASE + link
        tipo     = _detect_tipo(title, "ACER Noticias")

        results.append({
            "source":         "ACER",
            "external_id":    _make_ext_id("acer-rss", link),
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


def scrape_decisions(days_back: int = 1, max_pages: int = 2) -> List[Dict]:
    """Scrape de decisiones individuales ACER recientes."""
    cutoff  = date.today() - timedelta(days=days_back)
    results: List[Dict] = []

    for page in range(max_pages):
        url = f"{ACER_DEC_URL}?page={page}"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Error al descargar ACER Decisions p%d: %s", page, exc)
            break

        soup = BeautifulSoup(resp.text, "lxml")
        items = soup.select(".decision-item, .views-row")
        found_any = False

        if items:
            for it in items:
                a_tag = it.find("a")
                if not a_tag:
                    continue
                title    = a_tag.get_text(strip=True)
                href     = a_tag.get("href", "")
                date_tag = it.find(class_=re.compile(r"date"))
                fecha    = _parse_dot_date(date_tag.get_text() if date_tag else it.get_text())

                if not _is_recent(fecha, cutoff):
                    continue  # más antiguo que el corte → no añadir (pero seguir por si hay saltos)

                found_any = True
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
        else:
            # Fallback HTML libre
            for a in soup.find_all("a", href=re.compile(r"[Dd]ecision", re.I)):
                title = a.get_text(strip=True)
                href  = a.get("href", "")
                if not title or len(title) < 10:
                    continue
                parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
                fecha    = _parse_dot_date(parent_text)
                if not _is_recent(fecha, cutoff):
                    continue
                found_any = True
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
                    "importante":     "Sí",
                })

        if not found_any:
            break  # página sin entradas recientes → parar

    # Deduplicar
    seen: set = set()
    unique = []
    for r in results:
        if r["external_id"] not in seen:
            seen.add(r["external_id"])
            unique.append(r)

    logger.info("ACER Decisions: %d entradas recientes (desde %s)", len(unique), cutoff)
    return unique


def scrape(days_back: int = 1) -> List[Dict]:
    """Combina RSS y decisiones recientes, deduplica por external_id."""
    rss  = scrape_rss(days_back=days_back)
    decs = scrape_decisions(days_back=days_back, max_pages=2)

    seen: set = set()
    combined = []
    for e in rss + decs:
        if e["external_id"] not in seen:
            seen.add(e["external_id"])
            combined.append(e)

    logger.info("ACER total: %d entradas nuevas", len(combined))
    return combined
