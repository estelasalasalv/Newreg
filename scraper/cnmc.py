"""CNMC scraper — consultas públicas (todas las secciones energéticas).

Target: https://www.cnmc.es/participa/consultas-publicas
Filtra por términos energéticos y excluye sectores no energéticos.
"""
import logging
import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from scraper.boe import _detect_tramitaciones

logger = logging.getLogger(__name__)

CNMC_URL  = "https://www.cnmc.es/consultas-publicas/energia"
CNMC_BASE = "https://www.cnmc.es"

EXCLUDED = ["audiovisual", "telecomunicacion", "postal", "ferroviario", "ferrocarril"]
ENERGY_TERMS = [
    "energí","energi","eléctri","electri","electricidad","gas","renovable","fotovoltaic",
    "hidrógen","hidrogen","almacenamiento","red de transporte","sistema eléctrico",
    "tarifa","peaje","retribuc","generación","generacion","transporte eléctrico",
    "circular","cir/de","rap/de","cnmc/de",
]


_GAS_RE = re.compile(r'gas natural|regasificaci|distribuc.*gas|transporte.*gas|biometano|gnl|gasif|peajes.*gas|pr.rroga.*gas', re.IGNORECASE)
_TELCO_RE = re.compile(r'telecomunicaci|audiovisual|postal|ferroviario', re.IGNORECASE)

def _detect_sector(title: str) -> str:
    if _GAS_RE.search(title): return 'gas'
    if _TELCO_RE.search(title): return 'otros'
    return 'electricidad'

_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

_MONTHS = {
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
}

def _is_excluded(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in EXCLUDED)

def _is_energy(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in ENERGY_TERMS)

def _parse_date_cnmc(raw: str) -> Optional[str]:
    """Convierte '09 Abr 2026' o '09/04/2026' → '2026-04-09'."""
    m = re.search(r"(\d{1,2})\s+(\w{3,})\s+(\d{4})", raw)
    if m:
        mon = _MONTHS.get(m.group(2).lower()[:3])
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", raw)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return None

def _fetch(url: str, page: int = 0) -> Optional[BeautifulSoup]:
    params = {"page": page} if page > 0 else {}
    try:
        resp = requests.get(url, params=params, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as exc:
        logger.error("CNMC request failed (page=%d): %s", page, exc)
        return None

def _fetch_sentencia_expediente(url: str) -> str:
    """Obtiene el título del expediente de la página de detalle de una sentencia CNMC."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        wrapper = soup.select_one("div.event-bottom-title-wrapper")
        if wrapper:
            h2 = wrapper.find("h2")
            if h2:
                h2.extract()
            text = wrapper.get_text(strip=True)
            if text:
                return text
    except Exception:
        pass
    return ""


def _fetch_plazo(url: str) -> str:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        for el in soup.find_all(string=re.compile(r"hasta\s+\d", re.IGNORECASE)):
            t = el.strip()
            if t: return t
        for el in soup.find_all(string=re.compile(r"plazo.*finaliza", re.IGNORECASE)):
            return el.strip()[:120]
    except Exception:
        pass
    return ""

def _extract(soup: BeautifulSoup) -> List[Dict]:
    entries = []
    rows = (
        soup.select("div.border-bott.views-row")
        or soup.select("div.view-content .views-row")
        or soup.select("div.views-row")
    )
    for row in rows:
        link_el = row.select_one("a[href]")
        if not link_el:
            continue
        title = link_el.get_text(strip=True)
        href  = link_el.get("href", "")
        if href.startswith("/"): href = CNMC_BASE + href

        if _is_excluded(title):
            continue

        # Estado desde la etiqueta de color
        tag = row.select_one("span.tag, span.green-tag, [class*=tag]")
        tag_text = tag.get_text(strip=True).lower() if tag else ""
        if "marcha" in tag_text or "abierta" in tag_text or "curso" in tag_text:
            estado = "Abierta"
        elif "cerrada" in tag_text or "finalizada" in tag_text:
            estado = "Cerrada"
        else:
            estado = "Abierta"

        slug        = href.rstrip("/").split("/")[-1]
        external_id = f"cnmc-{slug}"

        entries.append({
            "source":         "CNMC_C",
            "tipo":           "consulta",
            "external_id":    external_id,
            "title":          title,
            "published_date": None,
            "url":            href,
            "section":        "Consultas CNMC",
            "department":     "CNMC",
            "summary":        None,
            "plazo":          None,
            "estado":         estado,
            "sector":         _detect_sector(title),
            "tramitaciones":  _detect_tramitaciones(title),
        })
    return entries

def scrape(max_pages: int = 5, fetch_plazos: bool = True) -> List[Dict]:
    results: List[Dict] = []
    for page in range(max_pages):
        soup = _fetch(CNMC_URL, page)
        if not soup:
            break
        entries = _extract(soup)
        if not entries:
            break
        results.extend(entries)
        if not soup.select_one("a[title='Página siguiente'], a.pager__item--next"):
            break

    if fetch_plazos:
        for e in results:
            if not e.get("plazo") or "hasta" not in (e["plazo"] or "").lower():
                e["plazo"] = _fetch_plazo(e["url"]) or e["plazo"]

    # Enriquecer títulos de sentencias con el expediente de la página de detalle
    for e in results:
        if e["title"].lower().startswith("sentencia") and " | " not in e["title"]:
            exp = _fetch_sentencia_expediente(e["url"])
            if exp:
                e["title"] = f"{e['title']} | {exp}"

    logger.info("CNMC consultas: %d entradas", len(results))
    return results
