"""CNMC Actuaciones scraper (CNMC_N) — novedades de energía.

Fuente: https://www.cnmc.es/somos-cnmc/transparencia/actuaciones
Filtro: field_exp_sectores=energía, datefrom/dateto = hoy

Estructura HTML por item (.views-row):
  col-sm-4 .gey-3  → tipo de procedimiento
  col-sm-4 .red    → ámbito/sector
  col-sm-8 .h2 a   → nº expediente + título, href → /expedientes/XXXX
  col-sm-8 p (último) → "tipo de acto | DD Mes YYYY"
"""
import re
import logging
import requests
from datetime import date, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BASE_URL  = "https://www.cnmc.es"
ACTU_URL  = (
    "https://www.cnmc.es/somos-cnmc/transparencia/actuaciones"
    "?field_exp_sectores=energ%C3%ADa&datefrom={datefrom}&dateto={dateto}"
    "&t=&idambito=All&idprocedim=All&idtipoexp=All&field_exp_numero="
)
_HEADERS  = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

_MESES = {
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
    "jan":1,"apr":4,"aug":8,"dec":12,
}

def _parse_cnmc_date(text: str) -> Optional[str]:
    """'27 Abr 2026' → '2026-04-27'"""
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,4})\s+(\d{4})", text or "")
    if m:
        day, mon, yr = m.groups()
        mes = _MESES.get(mon.lower()[:3])
        if mes:
            return f"{yr}-{mes:02d}-{int(day):02d}"
    return None


def _scrape_page(url: str) -> List[Dict]:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("CNMC_N error al descargar %s: %s", url, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    items = soup.select(".views-row")
    results = []

    for item in items:
        col4 = item.find("div", class_="col-sm-4")
        col8 = item.find("div", class_="col-sm-8")
        if not col8:
            continue

        a_tag = col8.find("a")
        if not a_tag:
            continue

        title_full = a_tag.get_text(strip=True)   # "UM/023/26 - SERVICIOS..."
        href       = a_tag.get("href", "")
        full_url   = BASE_URL + href if href.startswith("/") else href

        # Separar nº expediente del título
        exp_match = re.match(r"^([A-Z0-9/]+)\s*[-–]\s*(.+)$", title_full)
        if exp_match:
            expediente = exp_match.group(1).strip()
            title      = exp_match.group(2).strip()
        else:
            expediente = ""
            title      = title_full

        # Última línea: "Tipo de acto | DD Mes YYYY"
        ps         = col8.find_all("p")
        last_p     = ps[-1].get_text(strip=True) if ps else ""
        parts      = [p.strip() for p in last_p.split("|")]
        tipo_acto  = parts[0] if parts else ""
        fecha_iso  = _parse_cnmc_date(parts[1] if len(parts) > 1 else last_p)

        # Procedimiento y ámbito de la columna izquierda
        procedimiento = ""
        ambito        = ""
        if col4:
            p_gey = col4.find("p", class_=lambda c: c and "gey" in " ".join(c))
            p_red = col4.find("p", class_=lambda c: c and "red" in " ".join(c))
            procedimiento = p_gey.get_text(strip=True) if p_gey else ""
            ambito        = p_red.get_text(strip=True) if p_red else ""

        ext_id = f"cnmc-n-{href.strip('/').replace('/', '-')}" if href else f"cnmc-n-{re.sub(r'[^a-z0-9]','',title[:40].lower())}"

        results.append({
            "source":         "CNMC_N",
            "external_id":    ext_id,
            "title":          f"{expediente} — {title}" if expediente else title,
            "published_date": fecha_iso,
            "url":            full_url,
            "section":        tipo_acto or "Actuación CNMC",
            "department":     "CNMC",
            "summary":        f"Procedimiento: {procedimiento} | Ámbito: {ambito}" if procedimiento or ambito else None,
            "tipo":           "regulacion",
            "plazo":          None,
            "estado":         "Abierta",
            "sector":         "electricidad",
            "tramitaciones":  "No",
            "importante":     "No",
            "expediente":     expediente,
        })

    return results


def scrape(days_back: int = 1) -> List[Dict]:
    """Descarga actuaciones CNMC energéticas de los últimos days_back días."""
    today    = date.today()
    cutoff   = today - timedelta(days=days_back - 1)
    all_entries: List[Dict] = []
    seen: set = set()

    for delta in range(days_back):
        target = today - timedelta(days=delta)
        date_str = target.strftime("%d/%m/%Y")
        url = ACTU_URL.format(datefrom=date_str, dateto=date_str)
        logger.info("CNMC_N: scraping %s", date_str)
        entries = _scrape_page(url)
        for e in entries:
            if e["external_id"] not in seen:
                seen.add(e["external_id"])
                all_entries.append(e)

    # Si hoy no hay resultados, intentar también sin filtro de fecha (página 1)
    if not all_entries:
        logger.info("CNMC_N: sin resultados con filtro de fecha, intentando sin filtro...")
        url_sin_fecha = (
            "https://www.cnmc.es/somos-cnmc/transparencia/actuaciones"
            "?field_exp_sectores=energ%C3%ADa&datefrom=&dateto="
        )
        entries = _scrape_page(url_sin_fecha)
        for e in entries:
            if e["published_date"] and e["published_date"] >= cutoff.isoformat():
                if e["external_id"] not in seen:
                    seen.add(e["external_id"])
                    all_entries.append(e)

    logger.info("CNMC_N: %d actuaciones energéticas", len(all_entries))
    return all_entries
