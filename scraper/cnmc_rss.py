"""CNMC RSS scraper — https://www.cnmc.es/rss.xml

Descarga el feed, limpia el HTML de la descripción,
aplica el filtro de palabras clave energéticas y excluye
los sectores no energéticos.
Cuando el título es genérico navega a la página individual
para extraer el nombre del expediente/empresa.
"""
import re
import logging
import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

from scraper.boe import _find_keywords, _norm, _detect_tramitaciones

logger = logging.getLogger(__name__)

CNMC_RSS_URL = "https://www.cnmc.es/rss.xml"
EXCLUDED     = ["audiovisual", "telecomunicacion", "postal", "ferroviario", "ferrocarril"]
_HEADERS     = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

# Títulos genéricos que requieren navegar a la página individual
_GENERIC_TITLES = re.compile(
    r"^(acuerdo de la direcci[oó]n|resoluci[oó]n|auto|providencia|notificaci[oó]n|"
    r"informe del consejo|acuerdo del consejo)",
    re.IGNORECASE,
)


def _strip_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    return " ".join(soup.get_text(" ", strip=True).split())


def _parse_date(pub_date: str) -> Optional[str]:
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).strftime("%Y-%m-%d")
    except Exception:
        return None


def _is_excluded(text: str) -> bool:
    return any(kw in _norm(text) for kw in EXCLUDED)


def _is_energy_relevant(title: str, summary: str) -> bool:
    return bool(_find_keywords(title + " " + summary))


def _enrich_from_description(title: str, text: str) -> str:
    """Intenta extraer el nombre del expediente del HTML del RSS."""
    m = re.search(r"Expediente\s+(.+?)\s*-\s*Metadatos", text)
    if m:
        exp = m.group(1).strip()
        if exp and len(exp) > 3:
            return f"{title}: {exp}"
    return title


_ART64_RE    = re.compile(r"art[ií]culo\s+64|art\.?\s*64", re.IGNORECASE)
_RIESGO_GS   = re.compile(r"riesgo.*garant[ií]a.*suministro|garant[ií]a.*suministro.*riesgo|"
                           r"riesgo.*seguridad.*suministro|riesgo.*suministro", re.IGNORECASE)
_RIESGO_DG   = re.compile(r"da[ñn]o\s+grave|riesgo.*da[ñn]o|perjuicio.*grave", re.IGNORECASE)
_SIN_RIESGO  = re.compile(r"sin\s+riesgo", re.IGNORECASE)


def _clasificar_riesgo(text: str, titulo: str) -> str:
    """Detecta la clasificación de riesgo para infracciones del Art. 64 LSE."""
    combined = (text or "") + " " + (titulo or "")
    if _SIN_RIESGO.search(combined):
        return "Art.64 — Sin riesgo GS"
    if _RIESGO_GS.search(combined):
        return "Art.64 — Con riesgo GS"
    if _RIESGO_DG.search(combined):
        return "Art.64 — Con daño grave"
    if _ART64_RE.search(combined):
        return "Art.64 — Sin clasificar"
    return ""


def _fetch_page_info(url: str) -> Dict[str, str]:
    """
    Navega a la página individual del acuerdo y extrae:
    - expediente: nombre del caso/empresa (ej. 'IBERDROLA S.A. PRESUNTA INFRACCIÓN...')
    - num_expediente: número de referencia (ej. 'SNC/DE/079/26')
    - fecha: fecha del acuerdo
    - ambito: ámbito sectorial
    - riesgo: clasificación Art.64 si aplica
    """
    info = {"expediente": "", "num_expediente": "", "fecha": "", "ambito": "", "riesgo": ""}
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        text = BeautifulSoup(resp.text, "lxml").get_text(" ", strip=True)

        # Título descriptivo del expediente (en mayúsculas, tras el último "Expediente")
        # Patrón: "...Expediente TITULO EN MAYÚSCULAS Documentos|NW Menu"
        # Puede haber dos "Expediente": primero el número, luego el título
        # El título descriptivo aparece después de "Fecha DD Mes YYYY Expediente"
        desc_matches = re.findall(
            r"Fecha\s+\d+\s+\w+\s+\d{4}\s+Expediente\s+(.+?)"
            r"\s+(?:Documentos asociados|NW Menu)",
            text
        )
        if desc_matches:
            # Quedarnos con el más largo/descriptivo
            info["expediente"] = max(desc_matches, key=len).strip()
        else:
            # Fallback: cualquier cosa antes de "NW Menu"
            m = re.search(r"Expediente\s+(.+?)\s+NW Menu", text)
            if m:
                info["expediente"] = m.group(1).strip()

        # Número de expediente: "Nº Expediente XXX/YY/ZZZ/NN"
        m = re.search(r"N[ºo°]\s*Expediente\s+(\S+)", text, re.IGNORECASE)
        if m:
            info["num_expediente"] = m.group(1).strip()

        # Fecha del acuerdo
        m = re.search(r"Fecha\s+(\d{1,2}\s+\w+\s+\d{4})", text)
        if m:
            info["fecha"] = m.group(1).strip()

        # Clasificación de riesgo Art.64
        info["riesgo"] = _clasificar_riesgo(text, info.get("expediente",""))

        # Ámbito
        m = re.search(r"[AÁ]mbito\s+(\w[\w\s]*?)(?:\s+Tipo|\s+Fecha|\s+Expediente)", text)
        if m:
            info["ambito"] = m.group(1).strip()

    except Exception as exc:
        logger.debug("Error al navegar %s: %s", url, exc)

    return info


def scrape() -> List[Dict]:
    """Descarga el RSS de CNMC y devuelve entradas energéticas filtradas."""
    try:
        resp = requests.get(CNMC_RSS_URL, timeout=30, headers=_HEADERS)
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
        title     = (item.findtext("title")   or "").strip()
        link      = (item.findtext("link")    or "").strip()
        pub_raw   = (item.findtext("pubDate") or "").strip()
        desc_html =  item.findtext("description") or ""
        guid      = (item.findtext("guid")    or link).strip()

        full_text = _strip_html(desc_html)
        summary   = full_text[:500]

        node_id     = guid.split()[0] if guid else link
        external_id = f"cnmc-rss-{node_id}"

        if _is_excluded(title + " " + full_text):
            logger.debug("Excluido (sector): %s", title[:60])
            continue

        if not _is_energy_relevant(title, full_text):
            logger.debug("Sin keywords energéticas: %s", title[:60])
            continue

        published_date = _parse_date(pub_raw)

        # Intentar enriquecer desde la descripción RSS
        enriched = _enrich_from_description(title, full_text)

        # Si el título sigue siendo genérico, navegar a la página individual
        if enriched == title and _GENERIC_TITLES.match(title):
            logger.info("Navegando a %s para obtener expediente…", link)
            info = _fetch_page_info(link)
            if info["expediente"]:
                enriched = f"{title}: {info['expediente']}"
                if info["num_expediente"]:
                    enriched += f" ({info['num_expediente']})"
            elif info["num_expediente"]:
                enriched = f"{title} ({info['num_expediente']})"

            # Enriquecer el summary con la info de la página
            extra = " | ".join(filter(None, [
                f"Ref: {info['num_expediente']}" if info["num_expediente"] else "",
                f"Fecha: {info['fecha']}"        if info["fecha"]          else "",
                f"Ámbito: {info['ambito']}"      if info["ambito"]         else "",
            ]))
            if extra:
                summary = extra + (" | " + summary[:300] if summary else "")

        # Detectar infracción Art.64 también desde el título enriquecido
        riesgo = ""
        if _GENERIC_TITLES.match(title):
            riesgo = _clasificar_riesgo(summary or "", enriched)
        else:
            riesgo = _clasificar_riesgo("", enriched)

        # Art.64 → siempre importante
        es_importante_art64 = bool(riesgo)

        results.append({
            "source":         "CNMC",
            "external_id":    external_id,
            "title":          enriched,
            "published_date": published_date,
            "url":            link,
            "section":        "CNMC RSS",
            "department":     "CNMC",
            "summary":        summary[:500] if summary else None,
            "impacto_ree":    riesgo if riesgo else None,
            "tipo":           "regulacion",
            "plazo":          None,
            "estado":         "Abierta",
            "sector":         "electricidad",
            "tramitaciones":  _detect_tramitaciones(enriched),
        })

    logger.info("CNMC RSS: %d/%d entradas relevantes", len(results), len(items))
    return results
