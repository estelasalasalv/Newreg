"""MITERD scraper — participación pública en energía.

Target: https://www.miteco.gob.es/es/energia/participacion.html
"""
import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

MITERD_URL  = "https://www.miteco.gob.es/es/energia/participacion.html"
_HEADERS    = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

_MONTHS = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
}

def _parse_date(text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text, re.IGNORECASE)
    if m:
        mon = _MONTHS.get(m.group(2).lower())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    return None

def scrape() -> List[Dict]:
    try:
        resp = requests.get(MITERD_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("MITERD request failed: %s", exc)
        return []

    soup    = BeautifulSoup(resp.text, "lxml")
    results = []

    for body in soup.select("div.public-participation-search__body"):
        link_el = body.select_one("h2.public-participation-search__title a")
        if not link_el:
            continue
        title = link_el.get("title", link_el.get_text(strip=True))
        href  = link_el.get("href", "")
        if not title or not href:
            continue

        # Categoría
        cat_el = body.select_one("div.public-participation-search__content")
        category = cat_el.get_text(strip=True) if cat_el else ""

        # Fechas — las dos fechas en <strong>
        date_el = body.select_one("div.public-participation-search__date")
        pub_date = cierre_date = plazo_str = None
        if date_el:
            strongs = [s.get_text(strip=True) for s in date_el.select("strong")]
            if len(strongs) >= 2:
                pub_date    = _parse_date(strongs[0])
                cierre_date = _parse_date(strongs[1])
                plazo_str   = date_el.get_text(" ", strip=True)
                plazo_str   = re.sub(r"\s+", " ", plazo_str)[:150]

        # Estado: si la fecha de cierre ya pasó → Cerrada
        estado = "Abierta"
        if cierre_date:
            from datetime import date
            try:
                cierre_dt = date.fromisoformat(cierre_date)
                if cierre_dt < date.today():
                    estado = "Cerrada"
            except Exception:
                pass

        slug        = href.rstrip("/").split("/")[-1]
        external_id = f"miterd-{slug}"

        results.append({
            "source":         "MITERD",
            "tipo":           "consulta",
            "external_id":    external_id,
            "title":          title,
            "published_date": pub_date,
            "url":            href,
            "section":        category or "Consultas MITERD",
            "department":     "MITERD",
            "summary":        None,
            "plazo":          plazo_str,
            "estado":         estado,
        })

    logger.info("MITERD: %d consultas scrapeadas", len(results))
    return results
