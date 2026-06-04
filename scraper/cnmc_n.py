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
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from email.utils import parsedate_to_datetime

# Patrón para detectar actuaciones de gas en CNMC_S
_CNMC_GAS_RE = re.compile(
    r'gas natural|gasoducto|biometano|\bgnl\b|regasif|metaniz|almacen.*gas|'
    r'red de gas|enagas|redexis|nedgia|\bGTS\b|cuotas gts|mercado mayorista de gas|'
    r'mercado minorista de gas|sistema gasista|\bgasista\b|acceso.*instalaciones de gas|'
    r'hidr[oó]geno|electrolizador|pila de combustible|\bRFNBO\b|biog[aá]s|biometano',
    re.IGNORECASE,
)
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BASE_URL  = "https://www.cnmc.es"
CNMC_RSS_URL = "https://www.cnmc.es/rss.xml"


def _fetch_rss_pub_dates() -> dict:
    """Descarga el RSS de CNMC y devuelve {node_id: pubDate_ISO} para los 10 items más recientes.
    El guid del RSS tiene la forma '420665 at https://www.cnmc.es', el link es /node/420665.
    """
    pub_dates: dict = {}
    try:
        r = requests.get(CNMC_RSS_URL, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for item in root.findall(".//item"):
            link    = (item.findtext("link") or "").strip()
            pub_raw = (item.findtext("pubDate") or "").strip()
            if not link or not pub_raw:
                continue
            # Extraer node_id del link: https://www.cnmc.es/node/420665
            m = re.search(r"/node/(\d+)", link)
            if not m:
                continue
            node_id = m.group(1)
            try:
                pub_iso = parsedate_to_datetime(pub_raw).strftime("%Y-%m-%d")
                pub_dates[node_id] = pub_iso
            except Exception:
                pass
    except Exception as exc:
        logger.debug("CNMC RSS pub_dates error: %s", exc)
    return pub_dates
# idambito=9 → Energía. Formato fecha: YYYY-MM-DD (no DD/MM/YYYY)
ACTU_URL  = (
    "https://www.cnmc.es/somos-cnmc/transparencia/actuaciones"
    "?t=&idambito=9&idprocedim=All&idtipoexp=All&field_exp_numero="
    "&field_exp_sectores=&datefrom={datefrom}&dateto={dateto}"
)
# URL sin filtro de fecha para fallback
ACTU_URL_BASE = (
    "https://www.cnmc.es/somos-cnmc/transparencia/actuaciones"
    "?t=&idambito=9&idprocedim=All&idtipoexp=All&field_exp_numero="
    "&field_exp_sectores=&datefrom=&dateto="
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


def _scrape_actuaciones(days_back: int = 7) -> List[Dict]:
    """Descarga actuaciones CNMC energéticas recientes.

    La CNMC indexa los actos con la fecha del acuerdo/resolución, no la fecha
    de publicación web. Los actos más recientes pueden tardar semanas en aparecer.
    Se descarga siempre sin filtro de fecha para capturar lo nuevo.
    """
    all_entries: List[Dict] = []
    seen: set = set()

    logger.info("CNMC_N: descargando actuaciones recientes (sin filtro de fecha)...")
    entries = _scrape_page(ACTU_URL_BASE)
    for e in entries:
        if e["external_id"] not in seen:
            seen.add(e["external_id"])
            all_entries.append(e)

    logger.info("CNMC_N: %d actuaciones energéticas", len(all_entries))
    return all_entries


# ── Noticias de energía CNMC ─────────────────────────────────────────────────

NEWS_URL = (
    "https://www.cnmc.es/prensa/noticias"
    "?field_tags_target_id=9"          # Energía = 9
    "&created[min]={date_from}"
    "&created[max]={date_to}"
    "&page={page}"
)
NEWS_BASE = "https://www.cnmc.es"


def _parse_iso_date(dt_attr: str) -> Optional[str]:
    """'2026-05-06T13:48:10+02:00' → '2026-05-06'"""
    if dt_attr and len(dt_attr) >= 10:
        return dt_attr[:10]
    return None


def scrape_noticias(days_back: int = 2) -> List[Dict]:
    """Descarga noticias de energía CNMC de los últimos days_back días."""
    today   = date.today()
    cutoff  = today - timedelta(days=days_back - 1)
    results: List[Dict] = []
    seen: set = set()

    for page in range(5):  # máximo 5 páginas = 50 noticias
        url = NEWS_URL.format(
            date_from=cutoff.strftime("%Y-%m-%d"),
            date_to=today.strftime("%Y-%m-%d"),
            page=page,
        )
        try:
            r = requests.get(url, headers=_HEADERS, timeout=30)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.error("CNMC_N noticias error p%d: %s", page, exc)
            break

        soup  = BeautifulSoup(r.text, "lxml")
        items = soup.select(".views-row")
        if not items:
            break

        for it in items:
            time_el  = it.find("time")
            a_el     = it.find("a", href=True)
            sector_d = it.find("div", class_="views-field-field-tags")

            if not a_el:
                continue

            fecha_iso = _parse_iso_date(time_el.get("datetime", "") if time_el else "")
            title     = a_el.get_text(strip=True)
            href      = a_el.get("href", "").split("?")[0]  # quitar ?back=news
            full_url  = NEWS_BASE + href if href.startswith("/") else href
            sector_t  = sector_d.get_text(strip=True) if sector_d else "Energía"

            ext_id = f"cnmc-n-noticia-{href.strip('/').replace('/', '-')}"
            if ext_id in seen:
                continue
            seen.add(ext_id)

            results.append({
                "source":         "CNMC_N",
                "external_id":    ext_id,
                "title":          title,
                "published_date": fecha_iso,
                "url":            full_url,
                "section":        f"Noticia CNMC — {sector_t}",
                "department":     "CNMC",
                "summary":        None,
                "tipo":           "regulacion",
                "plazo":          None,
                "estado":         "Abierta",
                "sector":         "electricidad",
                "tramitaciones":  "No",
                "importante":     "No",
                "expediente":     "",
            })

        # Si hay menos items que una página completa, no hay más páginas
        if len(items) < 10:
            break

    logger.info("CNMC_N noticias: %d entradas (desde %s)", len(results), cutoff)
    return results


def scrape_cnmc_s(max_pages: int = 10) -> List[Dict]:
    """Descarga TODAS las actuaciones energéticas CNMC (idambito=9) — fuente CNMC_S.
    Sin filtro de fecha — descarga las max_pages primeras páginas.
    Cada página tiene ~33 items."""
    results: List[Dict] = []
    seen: set = set()

    for page in range(max_pages):
        if page == 0:
            url = ACTU_URL_BASE
        else:
            # Paginación CNMC: parámetro page con offset de comas
            url = ACTU_URL_BASE + f"&page={','.join([''] * page + [''])}"
            # Intentar con ?page=N simple
            url = ACTU_URL_BASE + f"&page={page}"

        try:
            r = requests.get(url, headers=_HEADERS, timeout=30)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.error("CNMC_S page %d error: %s", page, exc)
            break

        soup = BeautifulSoup(r.text, "lxml")
        items = soup.select(".views-row")
        if not items:
            break

        for item in items:
            col4 = item.find("div", class_="col-sm-4")
            col8 = item.find("div", class_="col-sm-8")
            if not col8:
                continue
            a_tag = col8.find("a")
            if not a_tag:
                continue

            title_full = a_tag.get_text(strip=True)
            href       = a_tag.get("href", "")
            full_url   = BASE_URL + href if href.startswith("/") else href

            exp_match = re.match(r"^([A-Z0-9/]+)\s*[-–]\s*(.+)$", title_full)
            if exp_match:
                expediente = exp_match.group(1).strip()
                title      = exp_match.group(2).strip()
            else:
                expediente = ""
                title      = title_full

            ps        = col8.find_all("p")
            last_p    = ps[-1].get_text(strip=True) if ps else ""
            parts     = [p.strip() for p in last_p.split("|")]
            tipo_acto = parts[0] if parts else ""
            fecha_iso = _parse_cnmc_date(parts[1] if len(parts) > 1 else last_p)

            procedimiento = ""
            ambito        = ""
            if col4:
                p_gey = col4.find("p", class_=lambda c: c and "gey" in " ".join(c))
                p_red = col4.find("p", class_=lambda c: c and "red" in " ".join(c))
                procedimiento = p_gey.get_text(strip=True) if p_gey else ""
                ambito        = p_red.get_text(strip=True) if p_red else ""

            ext_id = f"cnmc-s-{href.strip('/').replace('/', '-')}" if href else f"cnmc-s-{re.sub(r'[^a-z0-9]','',title[:40].lower())}"
            if ext_id in seen:
                continue
            seen.add(ext_id)

            results.append({
                "source":         "CNMC_S",
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
                "sector":         "gas" if _CNMC_GAS_RE.search(f"{expediente} {title} {tipo_acto} {procedimiento} {ambito}") else "electricidad",
                "tramitaciones":  "No",
                "importante":     "No",
                "expediente":     expediente,
            })

        if len(items) < 30:  # última página si menos de 30 items
            break

    logger.info("CNMC_S: %d actuaciones energéticas (%d páginas)", len(results), page + 1)
    return results


def _fetch_expediente_detail(url: str) -> Optional[str]:
    """Obtiene el texto descriptivo del detalle de un expediente CNMC."""
    import re as _re
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        from bs4 import BeautifulSoup as _BS
        soup = _BS(r.text, "lxml")
        main = soup.find("main")
        if not main:
            return None
        text = main.get_text(" ", strip=True)
        m = _re.search(
            r"Cronolog[ií]a.*?(?:Mostrar detalle\s*)?((?:\d{1,2}\s+\w+\s+\d{4}\s+)?[A-ZÁÉÍÓÚ][^\n]{30,}?)(?:Documentos asociados|$)",
            text, _re.DOTALL
        )
        if m:
            detalle = m.group(1).strip()
            # Eliminar prefijo "del Consejo TIPO del Consejo [Periodo: X]"
            detalle = _re.sub(
                r"^(?:del\s+Consejo\s+|de\s+la\s+Direcci[oó]n\s+|del\s+Secretario\s+)?"
                r"(?:Resoluci[oó]n|Informe|Acuerdo|Sentencia|Auto|Providencia|Circular)"
                r"(?:\s+del\s+Consejo|\s+de\s+la\s+Direcci[oó]n|\s+del\s+Secretario(?:\s+del\s+Consejo)?)?"
                r"(?:\s+Periodo:\s+\S+(?:\s+\S+){0,3})?\s+",
                "", detalle, flags=_re.IGNORECASE
            ).strip()
            if detalle:
                detalle = detalle[0].upper() + detalle[1:]
            return detalle[:500] if len(detalle) > 20 else None
        return None
    except Exception:
        return None


def scrape_cnmc_s_hoy(days_back: int = 2) -> List[Dict]:
    """Descarga las actuaciones energéticas CNMC_S publicadas hoy (o días recientes).
    Usa idambito=9 con filtro de fecha — solo las novedades del día."""
    today   = date.today()
    cutoff  = today - timedelta(days=days_back - 1)
    results: List[Dict] = []
    seen: set = set()

    for delta in range(days_back):
        target = today - timedelta(days=delta)
        if target.weekday() == 6:
            continue
        date_str = target.strftime("%Y-%m-%d")
        url = ACTU_URL.format(datefrom=date_str, dateto=date_str)
        try:
            r = requests.get(url, headers=_HEADERS, timeout=30)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.error("CNMC_S hoy error %s: %s", date_str, exc)
            continue

        soup  = BeautifulSoup(r.text, "lxml")
        items = soup.select(".views-row")
        for item in items:
            col4 = item.find("div", class_="col-sm-4")
            col8 = item.find("div", class_="col-sm-8")
            if not col8: continue
            a_tag = col8.find("a")
            if not a_tag: continue

            title_full = a_tag.get_text(strip=True)
            href       = a_tag.get("href", "")
            full_url   = BASE_URL + href if href.startswith("/") else href
            exp_match  = re.match(r"^([A-Z0-9/]+)\s*[-–]\s*(.+)$", title_full)
            if exp_match:
                expediente = exp_match.group(1).strip()
                title      = exp_match.group(2).strip()
            else:
                expediente, title = "", title_full

            ps        = col8.find_all("p")
            last_p    = ps[-1].get_text(strip=True) if ps else ""
            parts     = [p.strip() for p in last_p.split("|")]
            tipo_acto = parts[0] if parts else ""
            fecha_iso = _parse_cnmc_date(parts[1] if len(parts) > 1 else last_p) or target.isoformat()

            procedimiento, ambito = "", ""
            if col4:
                p_gey = col4.find("p", class_=lambda c: c and "gey" in " ".join(c))
                p_red = col4.find("p", class_=lambda c: c and "red" in " ".join(c))
                procedimiento = p_gey.get_text(strip=True) if p_gey else ""
                ambito        = p_red.get_text(strip=True) if p_red else ""

            ext_id = f"cnmc-s-{href.strip('/').replace('/', '-')}" if href else f"cnmc-s-{re.sub(r'[^a-z0-9]','',title[:40].lower())}"
            if ext_id in seen: continue
            seen.add(ext_id)

            results.append({
                "source": "CNMC_S", "external_id": ext_id,
                "title": f"{expediente} — {title}" if expediente else title,
                "published_date": fecha_iso, "url": full_url,
                "section": tipo_acto or "Actuación CNMC", "department": "CNMC",
                "summary": f"Procedimiento: {procedimiento} | Ámbito: {ambito}" if procedimiento or ambito else None,
                "tipo": "regulacion", "plazo": None, "estado": "Abierta",
                "sector": "gas" if _CNMC_GAS_RE.search(f"{expediente} {title} {tipo_acto} {procedimiento}") else "electricidad",
                "tramitaciones": "No", "importante": "No",
                "expediente": expediente,
                "summary": _fetch_expediente_detail(full_url),
            })

    logger.info("CNMC_S hoy: %d actuaciones (días_back=%d)", len(results), days_back)
    return results


def scrape(days_back: int = 2) -> List[Dict]:
    """Combina actuaciones y noticias energéticas CNMC recientes."""
    actuaciones = _scrape_actuaciones(days_back)
    noticias    = scrape_noticias(days_back)

    seen: set = set()
    combined = []
    for e in actuaciones + noticias:
        if e["external_id"] not in seen:
            seen.add(e["external_id"])
            combined.append(e)

    logger.info("CNMC_N total: %d entradas", len(combined))
    return combined
