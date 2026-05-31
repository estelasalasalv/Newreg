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


# Traducciones de términos frecuentes en títulos ACER (inglés → español)
_TRADUCCIONES = [
    (re.compile(r"\bACER calls for\b", re.I),          "ACER solicita"),
    (re.compile(r"\bACER recommends\b", re.I),          "ACER recomienda"),
    (re.compile(r"\bACER launches\b", re.I),            "ACER lanza"),
    (re.compile(r"\bACER publishes\b", re.I),           "ACER publica"),
    (re.compile(r"\bACER approves\b", re.I),            "ACER aprueba"),
    (re.compile(r"\bACER welcomes\b", re.I),            "ACER acoge favorablemente"),
    (re.compile(r"\bACER provides\b", re.I),            "ACER emite"),
    (re.compile(r"\bACER warns\b", re.I),               "ACER advierte"),
    (re.compile(r"\bACER to amend\b", re.I),            "ACER modificará"),
    (re.compile(r"\bACER will consult\b", re.I),        "ACER consultará sobre"),
    (re.compile(r"\bDecision No\b", re.I),              "Decisión n.º"),
    (re.compile(r"\belectricity\b", re.I),              "electricidad"),
    (re.compile(r"\bnatural gas\b", re.I),              "gas natural"),
    (re.compile(r"\bgas storage\b", re.I),              "almacenamiento de gas"),
    (re.compile(r"\bLNG\b"),                             "GNL"),
    (re.compile(r"\bnetwork tariffs\b", re.I),          "tarifas de red"),
    (re.compile(r"\btransmission tariffs\b", re.I),     "tarifas de transporte"),
    (re.compile(r"\bday-ahead\b", re.I),                "mercado a día siguiente"),
    (re.compile(r"\bcapacity calculation\b", re.I),     "cálculo de capacidad"),
    (re.compile(r"\binterconnection\b", re.I),          "interconexión"),
    (re.compile(r"\bsecurity of supply\b", re.I),       "seguridad de suministro"),
    (re.compile(r"\benergy markets\b", re.I),           "mercados energéticos"),
    (re.compile(r"\bgrid\b", re.I),                     "red eléctrica"),
    (re.compile(r"\btransparency\b", re.I),             "transparencia"),
    (re.compile(r"\bmeasures\b", re.I),                 "medidas"),
    (re.compile(r"\bimprovement[s]?\b", re.I),          "mejoras"),
    (re.compile(r"\bmonitoring\b", re.I),               "seguimiento"),
    (re.compile(r"\bregion\b", re.I),                   "región"),
    (re.compile(r"\breserve needs\b", re.I),            "necesidades de reserva"),
    (re.compile(r"\badequacy assessment\b", re.I),      "evaluación de adecuación"),
    (re.compile(r"\bEuropean Resource Adequacy Assessment\b", re.I),
     "Evaluación Europea de Adecuación de Recursos"),
    (re.compile(r"\binvestment\b", re.I),               "inversión"),
    (re.compile(r"\bcost[s]? and benefit[s]?\b", re.I),"costes y beneficios"),
    (re.compile(r"\bderogation[s]?\b", re.I),          "derogaciones"),
    (re.compile(r"\benforcement\b", re.I),              "aplicación normativa"),
    (re.compile(r"\btrading intermediar\w+\b", re.I),  "intermediarios comerciales"),
    (re.compile(r"\bquarterly\b", re.I),                "trimestral"),
    (re.compile(r"\bis out\b", re.I),                   "ya disponible"),
    (re.compile(r"\bhighlights\b", re.I),              "destaca"),
    (re.compile(r"\bmitigate\b", re.I),                "mitigar"),
    (re.compile(r"\bstress\b", re.I),                  "tensión"),
    (re.compile(r"\bspikes?\b", re.I),                 "picos"),
    (re.compile(r"\bsurveillance\b", re.I),            "vigilancia"),
    (re.compile(r"\bmarket risks\b", re.I),            "riesgos de mercado"),
    (re.compile(r"\bexpansion\b", re.I),               "expansión"),
    (re.compile(r"\brecord high\b", re.I),             "máximos históricos"),
    (re.compile(r"\bfilling\b", re.I),                 "llenado de"),
    (re.compile(r"\bcongestion\b", re.I),              "congestión"),
    (re.compile(r"\bequilibrium\b", re.I),             "equilibrio"),
    (re.compile(r"\blower\b", re.I),                   "menores"),
    (re.compile(r"\bpoint[s]? to\b", re.I),            "apuntan a"),
    (re.compile(r"\bnew equilibrium\b", re.I),         "nuevo equilibrio"),
    (re.compile(r"\bgas market\b", re.I),              "mercado de gas"),
    (re.compile(r"\bbiennial\b", re.I),                "bienal"),
    (re.compile(r"\bcontractual\b", re.I),             "contractual"),
    (re.compile(r"\bassessing\b", re.I),               "evaluando"),
    (re.compile(r"\bcovers?\b", re.I),                 "cubre"),
    (re.compile(r"\bprice spikes?\b", re.I),           "picos de precios"),
    (re.compile(r"\bsystem stress\b", re.I),           "tensión del sistema"),
    (re.compile(r"\bdistribution\b", re.I),            "distribución"),
    (re.compile(r"\bprice[s]?\b", re.I),               "precios"),
    (re.compile(r"\bframework\b", re.I),               "marco regulatorio"),
    (re.compile(r"\bopinions? on\b", re.I),            "dictámenes sobre"),
    (re.compile(r"\bgas network codes?\b", re.I),      "códigos de red de gas"),
    (re.compile(r"\bthird countries?\b", re.I),        "terceros países"),
    (re.compile(r"\bpoints?\b", re.I),                 "puntos"),
    (re.compile(r"\bstrengthens?\b", re.I),            "refuerza"),
    (re.compile(r"\btrust\b", re.I),                   "confianza"),
    (re.compile(r"\bvisible\b", re.I),                 "notable"),
    (re.compile(r"\brise\b", re.I),                    "aumento"),
    (re.compile(r"\bcloser\b", re.I),                  "más estrecho"),
    (re.compile(r"\boptimis\w+\b", re.I),              "optimizar"),
    (re.compile(r"\bramp.?up\b", re.I),                "aceleración"),
    (re.compile(r"\bupdated\b", re.I),                 "actualizado"),
]


def _traducir(titulo: str) -> str:
    """Aplica traducciones parciales de términos frecuentes de ACER al español."""
    for pat, reemplazo in _TRADUCCIONES:
        titulo = pat.sub(reemplazo, titulo)
    # Capitalizar primera letra
    return titulo[0].upper() + titulo[1:] if titulo else titulo


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
            # ET parsea el <a> hijo directamente (html.parser trataba <title> como RCDATA)
            a_el = title_el.find("a")
            if a_el is not None:
                title = (a_el.text or "").strip()
                href  = a_el.get("href", "")
            else:
                # Título plano sin <a> hijo
                title = "".join(title_el.itertext()).strip()
                href  = ""
            # Seguridad: si el título sigue teniendo HTML, limpiar con regex
            if "<" in title:
                import re as _re
                href_m = _re.search(r'href="([^"]+)"', title)
                href   = href_m.group(1) if href_m and not href else href
                title  = _re.sub(r"<[^>]+>", "", title).strip()
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

        # Clave estable: slug final de la URL (último segmento del path)
        slug = href.rstrip("/").split("/")[-1] if href else ""
        tipo = _detect_tipo(title, "ACER Noticias")
        results.append({
            "source":         "ACER",
            "external_id":    _make_ext_id("acer-rss", slug or title),
            "title":          _traducir(title),
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
                    "title":          _traducir(title),
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
