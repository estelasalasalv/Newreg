"""BOE Sección V — Anuncios de información pública energéticos.

La API JSON del BOE no expone la Sección V (Anuncios),
por lo que se hace scraping del HTML del índice diario.
Captura principalmente anuncios de Subdelegaciones del
Gobierno de Industria y Energía sobre proyectos energéticos.
"""
import re
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BOE_INDEX   = "https://www.boe.es/boe/dias/{y}/{m}/{d}/index.php"
BOE_TXT_URL = "https://www.boe.es/diario_boe/txt.php?id={id}"
BOE_BASE    = "https://www.boe.es"

_ENERGY_RE = re.compile(
    r"industria y energ[ií]a|fotovoltai|solar|e[oó]lic|renovable|electr[ií]c|"
    r"l[ií]nea.*transporte|l[ií]nea.*a[eé]rea|subestaci[oó]n|almacenamiento|"
    r"hidroel[eé]ctric|biog[aá]s|hidr[oó]geno|biomasa|parque.*energ|"
    r"planta.*energ|autorizaci[oó]n.*instalaci|informaci[oó]n p[uú]blica.*energ",
    re.IGNORECASE,
)

_EXCLUDED_RE = re.compile(
    r"suministro.*pan|correos|estadística|tarragona.*subasta|"
    r"carretera|ferrocarril|balasto|prisi[oó]n|penitenci|"
    r"sanidad.*defensa|defensa.*sanidad|adif|renfe|"
    r"licitaci[oó]n.*informatica|licitaci[oó]n.*limpieza|"
    r"licitaci[oó]n.*seguridad|licitaci[oó]n.*vigilancia",
    re.IGNORECASE,
)

# Para ser admitido, el anuncio debe contener términos específicamente energéticos
_ENERGY_SPECIFIC_RE = re.compile(
    r"industria y energ[ií]a|fotovoltai|solar|e[oó]lic|"
    r"l[ií]nea.*el[eé]ctric|l[ií]nea.*alta tensi[oó]n|l[ií]nea.*transporte|"
    r"subestaci[oó]n|parque.*energ|planta.*energ|planta.*solar|"
    r"autorizaci[oó]n.*instalaci.*el[eé]ctric|"
    r"almacenamiento.*energ|hidroel[eé]ctric|hidr[oó]geno|biomasa|biog[aá]s|"
    r"aerogenerador|molino.*viento|energ[ií]a.*renovable",
    re.IGNORECASE,
)

_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}


def _fetch_full_title(boe_id: str) -> str:
    """Obtiene el título completo del anuncio desde su página individual."""
    try:
        url = BOE_TXT_URL.format(id=boe_id)
        r = requests.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        # El título suele estar en h3 o en la primera línea del texto
        for sel in ["h3.documento-tit", "h2", "h3", ".documento-tit"]:
            el = soup.select_one(sel)
            if el:
                t = el.get_text(" ", strip=True)
                if len(t) > 20:
                    return t
        # Fallback: primera línea significativa del texto
        text = soup.get_text(" ", strip=True)
        lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 40]
        return lines[0][:400] if lines else ""
    except Exception as exc:
        logger.debug("No se pudo obtener título de %s: %s", boe_id, exc)
        return ""


def scrape(date_obj=None, days_back: int = 1) -> List[Dict]:
    """Scrape anuncios energéticos (Sección V) del HTML del BOE."""
    from datetime import date as date_type
    results: List[Dict] = []

    if date_obj is None:
        dates = [datetime.now().date() - timedelta(days=i) for i in range(days_back)]
    else:
        dates = [date_obj]

    session = requests.Session()
    session.headers.update(_HEADERS)

    for d in dates:
        if d.weekday() == 6:  # Sin domingos
            continue
        url = BOE_INDEX.format(y=d.year, m=f"{d.month:02d}", d=f"{d.day:02d}")
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("BOE Anuncios %s: %s", d, exc)
            continue

        soup = BeautifulSoup(r.text, "lxml")
        in_section_v = False

        for el in soup.find_all(["h3", "h4", "ul", "li"]):
            text = el.get_text(" ", strip=True)

            # Detectar inicio/fin de Sección V
            if el.name in ("h3", "h4"):
                if re.search(r"V\.\s*Anuncios|ANUNCIOS", text, re.IGNORECASE):
                    in_section_v = True
                elif re.search(r"^(I|II|III|IV|VI)\b", text) and "V." not in text:
                    in_section_v = False
                continue

            if not in_section_v or el.name != "li":
                continue

            # Buscar link al PDF/HTML del anuncio
            link = el.find("a", href=re.compile(r"BOE-[AB]-\d{4}-\d+"))
            if not link:
                continue

            href = link.get("href", "")
            # Extraer ID del BOE
            m = re.search(r"(BOE-[AB]-\d{4}-\d+)", href)
            if not m:
                continue
            boe_id = m.group(1)

            # Obtener texto del ítem para filtrar
            item_text = text

            if _EXCLUDED_RE.search(item_text):
                continue

            # Requiere keyword específicamente energética (no solo "electr" genérico)
            if not _ENERGY_SPECIFIC_RE.search(item_text):
                continue

            # Obtener título completo si es un anuncio BOE-B (abreviados en el índice)
            titulo = item_text
            if boe_id.startswith("BOE-B"):
                full = _fetch_full_title(boe_id)
                if full and len(full) > len(titulo):
                    titulo = full

            results.append({
                "external_id":  boe_id,
                "fecha":        d.isoformat(),
                "fuente":       "BOE",
                "seccion":      "V. Anuncios",
                "tipo":         "Anuncio",
                "organismo":    _extract_organismo(titulo),
                "subseccion":   "Información pública",
                "texto":        titulo,
                "enlace":       BOE_TXT_URL.format(id=boe_id),
                "palabras_clave": "",
                "resumen":      None,
                "importante":   "No",
                "acceso_conexion": _detect_acceso(titulo),
                "publicable":   "NO",
            })
            logger.debug("Anuncio: %s", titulo[:60])

    logger.info("BOE Anuncios Sección V: %d entradas (%d días)", len(results), len(dates))
    return results


def _extract_organismo(titulo: str) -> str:
    m = re.search(r"(Subdelegaci[oó]n del Gobierno en [\w\s]+?)[,\s]+(por|del|de la)",
                  titulo, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(Delegaci[oó]n del Gobierno[\w\s,]+?)[,\s]+(por|del)",
                  titulo, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return "MITERD - Subdelegaciones"


def _detect_acceso(titulo: str) -> str:
    if re.search(r"acceso|conexi[oó]n|peaje|interconex", titulo, re.IGNORECASE):
        return "Acceso/Conexion"
    if re.search(r"l[ií]nea.*transporte|l[ií]nea.*a[eé]rea|subestaci[oó]n|red.*transporte",
                 titulo, re.IGNORECASE):
        return "Transporte/Operador"
    return "No"
