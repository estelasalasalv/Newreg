"""[Inclusión nuevas consultas] Consultas públicas de otros organismos.

Todo lo relativo a esta novedad está marcado con "Inclusión nuevas consultas"
(buscar ese texto en el repo para localizarlo o revertirlo).

Fuentes:
  - MPR (Ministerio de la Presidencia, Justicia y Relaciones con las Cortes):
    https://www.mpr.gob.es/servicios/participacion/audienciapublica/Paginas/index.aspx
    Solo proyectos de Real Decreto / Ley / Reglamento sobre incendios en
    instalaciones o establecimientos industriales abiertos desde MPR_DESDE.
  - Ministerio de Transportes y Movilidad Sostenible (buscador de participación pública):
    Solo Reales Decretos, Leyes o Reglamentos del ámbito ferroviario (generales).
"""
import re
import logging
from datetime import date
from typing import List, Dict, Optional
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
_MONTH_NAMES = {v: k for k, v in _MONTHS.items()}

# Proyectos normativos de rango RD / Ley / Reglamento (anteproyectos incluidos)
_RANGO_RE = re.compile(r"real\s+decreto|\bley\b|reglamento", re.IGNORECASE)


def _fmt_plazo(inicio: Optional[date], fin: Optional[date]) -> Optional[str]:
    """Formato compatible con daysRemaining() de la web: '... hasta 12 de noviembre de 2026'."""
    def f(d: date) -> str:
        return f"{d.day} de {_MONTH_NAMES[d.month]} de {d.year}"
    if inicio and fin:
        return f"Desde {f(inicio)} hasta {f(fin)}"
    if fin:
        return f"Hasta {f(fin)}"
    return None


def _estado(fin: Optional[date], fallback_abierta: bool) -> str:
    if fin:
        return "Abierta" if fin >= date.today() else "Cerrada"
    return "Abierta" if fallback_abierta else "Cerrada"


# ── MPR ─────────────────────────────────────────────────────────────────────
MPR_URL  = "https://www.mpr.gob.es/servicios/participacion/audienciapublica/Paginas/index.aspx"
MPR_BASE = "https://www.mpr.gob.es"
# Solo trámites abiertos a partir de la fecha en que se añadió esta fuente
MPR_DESDE = date(2026, 10, 26)
_MPR_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

# Reglamento de seguridad contra incendios en establecimientos/instalaciones industriales
_INCENDIOS_RE   = re.compile(r"incendi", re.IGNORECASE)
_INDUSTRIAL_RE  = re.compile(r"(establecimiento|instalaci[oó]n|instalaciones)\w*\s+industrial", re.IGNORECASE)


def _mpr_relevante(title: str) -> bool:
    return bool(_RANGO_RE.search(title) and _INCENDIOS_RE.search(title) and _INDUSTRIAL_RE.search(title))


def _parse_ddmmyyyy(text: str) -> Optional[date]:
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", text or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def scrape_mpr() -> List[Dict]:
    try:
        resp = requests.get(MPR_URL, headers=_MPR_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("MPR request failed: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    results: List[Dict] = []
    total = 0
    for block in soup.select("div.SummaryList"):
        h2 = block.select_one("h2.SummaryListTitulo")
        lista_abierta = bool(h2 and "abiertos" in h2.get_text(strip=True).lower())
        for li in block.select("li.SummaryListLI"):
            a = li.select_one("a[href]")
            if not a:
                continue
            total += 1
            title = (a.get("title") or a.get_text(" ", strip=True)).strip()
            if not _mpr_relevante(title):
                continue
            fechas = [_parse_ddmmyyyy(s.get_text()) for s in li.select("p.SummaryListP2 span.strong")]
            inicio = fechas[0] if fechas else None
            fin    = fechas[1] if len(fechas) > 1 else None
            if not inicio or inicio < MPR_DESDE:
                continue
            href = urljoin(MPR_BASE, quote(a["href"], safe="/:%?=&"))
            path = a["href"].split("/Paginas/")[-1].replace(".aspx", "")
            slug = re.sub(r"[^a-z0-9]+", "-", path.lower()).strip("-")[:180]
            results.append({
                "source":         "MPR",
                "tipo":           "consulta",
                "external_id":    f"mpr-{slug}",
                "title":          title,
                "published_date": inicio.isoformat(),
                "url":            href,
                "section":        "Audiencia e información pública - MPR",
                "department":     "Ministerio de la Presidencia, Justicia y Relaciones con las Cortes",
                "summary":        None,
                "plazo":          _fmt_plazo(inicio, fin),
                "estado":         _estado(fin, lista_abierta),
                "sector":         "otros",
            })

    logger.info("MPR: %d trámites en la página, %d relevantes (incendios industriales desde %s)",
                total, len(results), MPR_DESDE.isoformat())
    return results


# ── Ministerio de Transportes ───────────────────────────────────────────────
# URL facilitada (ámbitos Ferroviario/Expropiaciones/Estudios/Otras) + filtro por
# tipo de participación normativa (666 Proyecto Normativo, 669 Consulta pública
# previa, 670 Audiencia e Información pública) para descartar las expropiaciones
# ADIF ("Proyecto No Normativo"), que son la gran mayoría de resultados.
TRANSPORTES_URL = (
    "https://www.transportes.gob.es/ministerio/buen-gobierno/participacion-publica/buscador"
    "?search_api_fulltext=&field_state_public_participation=open"
    "&field_areas_action%5B640%5D=640&field_areas_action%5B642%5D=642"
    "&field_areas_action%5B645%5D=645&field_areas_action%5B647%5D=647"
    "&field_participation_type%5B666%5D=666&field_participation_type%5B669%5D=669"
    "&field_participation_type%5B670%5D=670"
)
TRANSPORTES_BASE = "https://www.transportes.gob.es"
# Además de las abiertas, se siguen las iniciadas desde esta fecha para actualizar su estado
TRANSPORTES_DESDE = date(2026, 9, 1)
TRANSPORTES_MAX_PAGES = 2   # 10 resultados/página, ordenados por fecha descendente
# La web devuelve "Página web bloqueada" con User-Agent de bot; se usa uno de navegador
_MT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}
_MT_EXCLUIR_RE = re.compile(r"expropiaci|subvenci|bases reguladoras", re.IGNORECASE)


def _mt_relevante(title: str, rango: str, ambito: str) -> bool:
    if "ferroviari" not in ambito.lower():
        return False
    if _MT_EXCLUIR_RE.search(title):
        return False
    # Rango RD/Ley, o cualquier rango si el título es un reglamento
    return bool(re.search(r"real\s+decreto|\bley\b", rango, re.IGNORECASE)
                or re.search(r"\breglamento\b", title, re.IGNORECASE))


def _parse_es_date(text: str) -> Optional[date]:
    """'2026-Octubre-21' o 'Martes, 1 Diciembre 2026'."""
    text = text or ""
    m = re.search(r"(\d{4})-([A-Za-zé]+)-(\d{1,2})", text)
    if m:
        y, mon, d = m.group(1), m.group(2), m.group(3)
    else:
        m = re.search(r"(\d{1,2})\s+(?:de\s+)?([A-Za-zé]+)\s+(?:de\s+)?(\d{4})", text)
        if not m:
            return None
        d, mon, y = m.group(1), m.group(2), m.group(3)
    mon_n = _MONTHS.get(mon.lower())
    if not mon_n:
        return None
    try:
        return date(int(y), mon_n, int(d))
    except ValueError:
        return None


def scrape_transportes() -> List[Dict]:
    results: List[Dict] = []
    seen: set = set()
    total = 0
    for page in range(TRANSPORTES_MAX_PAGES):
        url = TRANSPORTES_URL + (f"&page={page}" if page else "")
        try:
            resp = requests.get(url, headers=_MT_HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("TRANSPORTES request failed (page %d): %s", page, exc)
            break
        soup = BeautifulSoup(resp.text, "lxml")
        arts = soup.select("article.node--type-public-participation")
        if not arts:
            if page == 0:
                logger.warning("TRANSPORTES: 0 resultados (¿bloqueo o cambio de HTML?)")
            break

        def txt(art, sel):
            el = art.select_one(sel)
            return el.get_text(" ", strip=True) if el else ""

        for art in arts:
            total += 1
            title  = txt(art, "h3")
            link   = art.select_one("a.pp-teaser-link[href]")
            if not title or not link:
                continue
            state  = txt(art, ".field--name-field-state-public-participation")
            rango  = txt(art, ".field--name-field-regulatory-range")
            tipo_p = txt(art, ".field--name-field-participation-type")
            ambito = txt(art, ".field--name-field-areas-action")
            t_el   = art.select_one("time")
            inicio = _parse_es_date(t_el.get("datetime", "") if t_el else "")
            counter = art.select_one(".counter-days[data-close-date]")
            fin = (date.fromisoformat(counter["data-close-date"]) if counter
                   else _parse_es_date(txt(art, ".field--name-field-date .field__item")))
            abierta = state.lower().startswith("abiert")

            if not _mt_relevante(title, rango, ambito):
                continue
            if not abierta and not (inicio and inicio >= TRANSPORTES_DESDE):
                continue

            href = urljoin(TRANSPORTES_BASE, link["href"])
            slug = re.sub(r"[^a-z0-9]+", "-", link["href"].rstrip("/").split("/")[-1].lower()).strip("-")[:170]
            external_id = f"transportes-{slug}"
            if external_id in seen:
                continue
            seen.add(external_id)
            results.append({
                "source":         "TRANSPORTES",
                "tipo":           "consulta",
                "external_id":    external_id,
                "title":          title,
                "published_date": inicio.isoformat() if inicio else None,
                "url":            href,
                "section":        " · ".join(x for x in (tipo_p, rango) if x) or "Participación pública - Transportes",
                "department":     "Ministerio de Transportes y Movilidad Sostenible",
                "summary":        None,
                "plazo":          _fmt_plazo(inicio, fin),
                "estado":         _estado(fin, abierta),
                "sector":         "otros",
            })

    logger.info("TRANSPORTES: %d participaciones revisadas, %d relevantes (ferroviario RD/Ley/Reglamento)",
                total, len(results))
    return results


def scrape() -> List[Dict]:
    """[Inclusión nuevas consultas] Combina MPR + Transportes."""
    return scrape_mpr() + scrape_transportes()
