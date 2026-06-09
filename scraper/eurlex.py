"""EUR-Lex scraper — normativa europea energética publicada en el DOUE.

Fuente: SPARQL endpoint del Publications Office de la UE
        https://publications.europa.eu/webapi/rdf/sparql

Captura Reglamentos, Directivas y Decisiones de la UE publicadas
en el Diario Oficial de la Unión Europea (DOUE) con keywords energéticas.
"""
import re
import json
import logging
import requests
from datetime import date, timedelta
from typing import List, Dict, Optional
from scraper.boe import _detect_tramitaciones
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"

# Tipos de acto UE por patrón en el título
TIPO_MAP = [
    (re.compile(r"Implementing Regulation|Reglamento de Ejecuci",   re.I), "Reglamento de Ejecución (UE)"),
    (re.compile(r"Delegated Regulation|Reglamento Delegado",        re.I), "Reglamento Delegado (UE)"),
    (re.compile(r"\bRegulation\b|Reglamento",                       re.I), "Reglamento (UE)"),
    (re.compile(r"\bDirective\b|Directiva",                        re.I), "Directiva (UE)"),
    (re.compile(r"Implementing Decision|Decisión de Ejecución",    re.I), "Decisión de Ejecución (UE)"),
    (re.compile(r"Delegated Decision|Decisión Delegada",            re.I), "Decisión Delegada (UE)"),
    (re.compile(r"\bDecision\b|Decisión|Decisi.n",                 re.I), "Decisión (UE)"),
    (re.compile(r"Recommendation|Recomendación",                    re.I), "Recomendación (UE)"),
    (re.compile(r"Commission Notice|Aviso de la Comisi[oó]n",      re.I), "Aviso/Comunicación UE"),
    (re.compile(r"Court of Auditors|Tribunal de Cuentas",          re.I), "Dictamen Tribunal de Cuentas (UE)"),
    (re.compile(r"Committee of the Regions|Comité de las Regiones", re.I), "Acto Comité de las Regiones (UE)"),
    (re.compile(r"European Parliament|Parlamento Europeo",          re.I), "Acto Parlamento Europeo"),
    (re.compile(r"Economic.*Social Committee|Comité Económico.*Social|CESE\b|EESC\b", re.I), "Dictamen CESE (UE)"),
    (re.compile(r"Corrigendum|Corrección de errores",               re.I), "Corrección de errores"),
]

EURLEX_BASE = "https://eur-lex.europa.eu/legal-content/ES/TXT/?uri=uriserv:OJ.L_.{year}.{oj}.01.0001.01.SPA"
CELLAR_BASE = "https://publications.europa.eu/resource/cellar/"


def _detect_tipo(titulo: str) -> str:
    for pat, label in TIPO_MAP:
        if pat.search(titulo):
            return label
    return "Acto UE"


def _is_importante(tipo: str) -> str:
    return "No"  # importante solo lo marca el usuario manualmente


_NON_LEGIS_RE = re.compile(
    r"^(REPORT FROM|COMMISSION STAFF WORKING|COMMUNICATION FROM|"
    r"ANNEX TO|WORKING DOCUMENT|CORRIGENDUM TO|CORRIGENDUM$|"
    r"Decreto-Lei|Decreto Legislativo|Lei n\.|Orden |Real Decreto|"
    r"SUMMARY RECORD|MINUTES OF|AGENDA OF)",
    re.IGNORECASE,
)

# Documentos serie C permitidos aunque no lleven (UE) en el título
_SERIES_C_ALLOWED_RE = re.compile(
    r"^COMMISSION NOTICE|^AVISO DE LA COMISI[OÓ]N"
    r"|^Opinion.*Court of Auditors|^Dictamen.*Tribunal de Cuentas"
    r"|^Special Report.*Court of Auditors|^Informe Especial.*Tribunal de Cuentas"
    # Organismos consultivos UE: Comité de las Regiones, Parlamento Europeo, CESE
    r"|^Declaration.*Committee of the Regions|^Declaraci[oó]n.*Comit[eé] de las Regiones"
    r"|^Resolution.*Committee of the Regions|^Resoluci[oó]n.*Comit[eé] de las Regiones"
    r"|^Opinion.*Committee of the Regions|^Dictamen.*Comit[eé] de las Regiones"
    r"|^Resolution.*European Parliament|^Resoluci[oó]n.*Parlamento Europeo"
    r"|^Opinion.*European Parliament|^Dictamen.*Parlamento Europeo"
    r"|^Opinion.*Economic.*Social Committee|^Dictamen.*Comit[eé] Econ[oó]mico.*Social"
    r"|^Opinion.*EESC|^Dictamen.*CESE",
    re.IGNORECASE,
)

EU_ACT_STRICT_RE = re.compile(
    r"\(UE\)|\(EU\)|\(Euratom\)|\(EURATOM\)|\(UE, Euratom\)",
    re.IGNORECASE,
)

def _sparql_query(date_from: str, date_to: str) -> List[Dict]:
    """Ejecuta SPARQL para obtener actos UE energéticos en el rango de fechas.
    Obtiene todos los títulos disponibles por acto para poder escoger el español."""
    query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?work ?title ?date WHERE {{
  ?work cdm:work_date_document ?date ;
        cdm:work_title ?title .
  FILTER(?date >= "{date_from}"^^xsd:date AND ?date <= "{date_to}"^^xsd:date)
  FILTER(
    CONTAINS(STR(?title), "(UE)") OR CONTAINS(STR(?title), "(EU)") OR
    CONTAINS(STR(?title), "(EURATOM)") OR CONTAINS(STR(?title), "(Euratom)") OR
    CONTAINS(STR(?title), "COMMISSION NOTICE") OR CONTAINS(STR(?title), "Commission notice") OR
    CONTAINS(STR(?title), "Court of Auditors") OR CONTAINS(STR(?title), "Tribunal de Cuentas") OR
    CONTAINS(STR(?title), "Committee of the Regions") OR CONTAINS(STR(?title), "Comité de las Regiones") OR
    CONTAINS(STR(?title), "European Parliament") OR CONTAINS(STR(?title), "Parlamento Europeo") OR
    CONTAINS(STR(?title), "Economic and Social Committee") OR CONTAINS(STR(?title), "Comité Económico y Social")
  )
  FILTER(
    CONTAINS(LCASE(STR(?title)), "energ") OR
    CONTAINS(LCASE(STR(?title)), "electr") OR
    CONTAINS(LCASE(STR(?title)), "renew") OR
    CONTAINS(LCASE(STR(?title)), "renovable") OR
    CONTAINS(LCASE(STR(?title)), "hidrog") OR
    CONTAINS(LCASE(STR(?title)), "hydrogen") OR
    CONTAINS(LCASE(STR(?title)), "emission") OR
    CONTAINS(LCASE(STR(?title)), "emisi") OR
    CONTAINS(LCASE(STR(?title)), "climate") OR
    CONTAINS(LCASE(STR(?title)), "solar") OR
    CONTAINS(LCASE(STR(?title)), "wind") OR
    CONTAINS(LCASE(STR(?title)), "natural gas") OR
    CONTAINS(LCASE(STR(?title)), "gas natural") OR
    CONTAINS(LCASE(STR(?title)), "carbon") OR
    CONTAINS(LCASE(STR(?title)), "decarboni") OR
    CONTAINS(LCASE(STR(?title)), "biofuel") OR
    CONTAINS(LCASE(STR(?title)), "biomass") OR
    CONTAINS(LCASE(STR(?title)), "net zero") OR
    CONTAINS(LCASE(STR(?title)), "storage") OR
    CONTAINS(LCASE(STR(?title)), "grid") OR
    CONTAINS(LCASE(STR(?title)), "power") OR
    CONTAINS(LCASE(STR(?title)), "nuclear") OR
    CONTAINS(LCASE(STR(?title)), "eficiencia") OR
    CONTAINS(LCASE(STR(?title)), "efficiency") OR
    CONTAINS(LCASE(STR(?title)), "greenhouse") OR
    CONTAINS(LCASE(STR(?title)), "invernadero") OR
    CONTAINS(LCASE(STR(?title)), "fotovoltai") OR
    CONTAINS(LCASE(STR(?title)), "offshore") OR
    CONTAINS(LCASE(STR(?title)), "eolic") OR
    CONTAINS(LCASE(STR(?title)), "aerogen") OR
    CONTAINS(LCASE(STR(?title)), "warming potential") OR
    CONTAINS(LCASE(STR(?title)), "life-cycle") OR
    CONTAINS(LCASE(STR(?title)), "taxonomy") OR
    CONTAINS(LCASE(STR(?title)), "remit") OR
    CONTAINS(LCASE(STR(?title)), "omnibus")
  )
}} ORDER BY ?work DESC(?date) LIMIT 2000
"""
    try:
        r = requests.post(
            SPARQL_ENDPOINT,
            data={"query": query, "format": "json"},
            headers={
                "Accept": "application/sparql-results+json",
                "User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)",
            },
            timeout=60,
        )
        r.raise_for_status()
        return json.loads(r.text).get("results", {}).get("bindings", [])
    except Exception as exc:
        logger.error("SPARQL EUR-Lex error: %s", exc)
        return []


def _process_bindings(bindings: List[Dict]) -> List[Dict]:
    """Convierte los bindings SPARQL en entradas para la BD.
    Agrupa por work URI y elige el título en español cuando está disponible."""
    # Agrupar todos los títulos por work URI
    work_titles: dict = {}
    work_dates:  dict = {}
    for b in bindings:
        work  = b.get("work",  {}).get("value", "")
        title = b.get("title", {}).get("value", "").strip()
        fecha = b.get("date",  {}).get("value", "")[:10]
        if work not in work_titles:
            work_titles[work] = []
            work_dates[work]  = fecha
        work_titles[work].append((b.get("title",{}).get("xml:lang","") or
                                  b.get("title",{}).get("lang",""), title))

    results = []
    seen    = set()

    for work, titles_list in work_titles.items():
        fecha = work_dates.get(work, "")

        # Preferir español (es, spa, es-*), luego inglés, luego lo que haya
        _ES = {"es", "spa"}
        _EN = {"en", "eng"}
        title = ""
        for lang_set in [_ES, _EN, None]:
            if lang_set is None:
                title = titles_list[0][1] if titles_list else ""
                break
            matches = [
                t for l, t in titles_list
                if l.lower() in lang_set or l.lower().startswith("es-")
            ]
            if matches:
                title = matches[0]
                break
        if not title:
            continue

        # Filtrar documentos no legislativos y legislación nacional no UE
        if _NON_LEGIS_RE.search(title):
            continue
        # Requiere marcador UE/EU, salvo documentos serie C permitidos (Notices, ECA Opinions)
        if not EU_ACT_STRICT_RE.search(title) and not _SERIES_C_ALLOWED_RE.search(title):
            continue
        # Excluir claramente legislación nacional que se coló
        _NATIONAL_START = re.compile(
            r"^(Decreto-Lei|Decreto Legislativo|Lei n\.|Arrêté|Ordonnance|"
            r"Verordnung|Besluit|Wet van|Attuazione|Umsetzung|"
            r"Proposal for a |Proposta di |Projeto de )",
            re.IGNORECASE
        )
        if _NATIONAL_START.match(title):
            continue
        if re.search(r"eficiencia del servicio p[uú]blico", title, re.IGNORECASE):
            continue

        # Deduplicar por título
        key = re.sub(r"\s+", " ", title[:100])
        if key in seen:
            continue
        seen.add(key)

        # Construir URL al documento
        cellar_id = work.replace(CELLAR_BASE, "").split(".")[0] if CELLAR_BASE in work else ""
        enlace = f"https://eur-lex.europa.eu/resource.html?uri=cellar:{cellar_id}" if cellar_id else work

        tipo          = _detect_tipo(title)
        importante    = _is_importante(tipo)
        tramitaciones = _detect_tramitaciones(title)
        ext_id        = f"eu-{cellar_id[:30]}" if cellar_id else f"eu-{re.sub(r'[^a-z0-9]','', title[:40].lower())}"

        results.append({
            "external_id":     ext_id,
            "fecha":           fecha,
            "fuente":          "DOUE",
            "seccion":         "DOUE — Serie L",
            "tipo":            tipo,
            "organismo":       "Unión Europea",
            "subseccion":      "",
            "texto":           title,
            "enlace":          enlace,
            "palabras_clave":  "",
            "resumen":         None,
            "importante":      importante,
            "acceso_conexion": "No",
            "tramitaciones":   tramitaciones,
            "publicable":      "NO",
        })

    return results


_DAILY_BASE = "https://eur-lex.europa.eu/oj/daily-view/{serie}-series/default.html?ojDate={fecha}"
_EURLEX_BASE_URL = "https://eur-lex.europa.eu"

# Keywords energéticas para filtrar el índice diario (mismas que el SPARQL)
_DAILY_ENERGY_RE = re.compile(
    r"energ|electr|renew|renovable|hidrog|hydrogen|emission|emisi|climate|solar|"
    r"wind|gas natural|natural gas|carbon|decarboni|biofuel|biomass|net.zero|"
    r"storage|almacen|\bgrid\b|\bpower\b|nuclear|eficiencia|efficiency|greenhouse|"
    r"invernadero|fotovoltai|offshore|eolic|aerogen|taxonomy|remit|omnibus|"
    r"accelerateu|energy.union|affordable.and.secure.energy",
    re.IGNORECASE,
)


def _scrape_daily_index(target_date: date) -> List[Dict]:
    """Scrape del índice HTML diario del DOUE (series C y L).

    Complementa el SPARQL para capturar documentos recientes que aún no están
    indexados en el endpoint RDF (el lag habitual es de 3-10 días hábiles).
    """
    results: List[Dict] = []
    fecha_str = target_date.strftime("%d%m%Y")  # formato ojDate: DDMMYYYY
    fecha_iso = target_date.isoformat()

    for serie in ("L", "C"):
        url = _DAILY_BASE.format(serie=serie, fecha=fecha_str)
        try:
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)", "Accept-Language": "es,en"},
                timeout=20,
            )
            if r.status_code != 200:
                logger.debug("EUR-Lex daily %s %s: HTTP %d", serie, fecha_iso, r.status_code)
                continue
        except requests.RequestException as exc:
            logger.warning("EUR-Lex daily %s %s: %s", serie, fecha_iso, exc)
            continue

        soup = BeautifulSoup(r.text, "lxml")

        # Estructura: div.col-md-2 contiene la referencia (p.ej. "C/2026/3099")
        # El div.col-md-* hermano siguiente contiene el enlace con el título
        ref_re = re.compile(rf"^[{serie}]/\d{{4}}/\d+$")

        for ref_div in soup.find_all("div", class_="col-md-2"):
            ref_text = ref_div.get_text(strip=True)
            if not ref_re.match(ref_text):
                continue

            # Buscar el div hermano con el título/enlace
            row = ref_div.parent  # div.row o similar
            if not row:
                continue
            a_tag = row.find("a", href=True)
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            href  = a_tag.get("href", "")

            if not title or len(title) < 15:
                continue

            # Filtro temático energético
            if not _DAILY_ENERGY_RE.search(title):
                continue

            # Filtro de tipo: solo actos con marcador UE/EU/organismos permitidos
            if not EU_ACT_STRICT_RE.search(title) and not _SERIES_C_ALLOWED_RE.search(title):
                continue

            # Filtro: excluir no-legislativos
            if _NON_LEGIS_RE.search(title):
                continue

            full_url = href if href.startswith("http") else _EURLEX_BASE_URL + href
            # Normalizar a versión ES si viene en EN
            full_url = re.sub(r"/legal-content/[A-Z]{2}/TXT/", "/legal-content/ES/TXT/", full_url)

            ext_id = f"eu-daily-{re.sub(r'[^a-z0-9]', '', ref_text.lower())}"
            tipo = _detect_tipo(title)
            tramitaciones = _detect_tramitaciones(title)

            results.append({
                "external_id":     ext_id,
                "fecha":           fecha_iso,
                "fuente":          "DOUE",
                "seccion":         f"DOUE — Serie {serie}",
                "tipo":            tipo,
                "organismo":       "Unión Europea",
                "subseccion":      "",
                "texto":           title,
                "enlace":          full_url,
                "palabras_clave":  "",
                "resumen":         None,
                "importante":      "No",
                "acceso_conexion": "No",
                "tramitaciones":   tramitaciones,
                "publicable":      "NO",
            })

    if results:
        logger.info("EUR-Lex daily index %s: %d actos energéticos", fecha_iso, len(results))
    return results


def scrape(days_back: int = 1) -> List[Dict]:
    """Scrape normativa europea energética del día actual (o últimos N días).

    Combina dos métodos:
    1. SPARQL (days_back=7): captura actos ya indexados en el RDF del Publications Office.
    2. Índice HTML diario (days_back días): captura actos recientes aún no en el SPARQL.
    Los resultados se deduplicán por external_id antes de devolverse.
    """
    today     = date.today()
    date_from = (today - timedelta(days=max(days_back, 7))).isoformat()
    date_to   = today.isoformat()

    # Método 1: SPARQL (siempre 7 días para cubrir el lag de indexación)
    bindings = _sparql_query(date_from, date_to)
    results  = _process_bindings(bindings)
    logger.info("EUR-Lex SPARQL: %d actos UE energéticos", len(results))

    # Método 2: índice HTML diario (últimos days_back días)
    # Captura los documentos recientes que el SPARQL aún no tiene indexados
    seen_ids = {e["external_id"] for e in results}
    # También registrar títulos normalizados para evitar duplicados con distinto ID
    seen_titles = {re.sub(r"\s+", " ", e["texto"][:80]).lower() for e in results}

    for i in range(days_back):
        day = today - timedelta(days=i)
        if day.weekday() == 6:  # sin domingos (el DOUE no se publica)
            continue
        daily = _scrape_daily_index(day)
        for e in daily:
            title_key = re.sub(r"\s+", " ", e["texto"][:80]).lower()
            if e["external_id"] not in seen_ids and title_key not in seen_titles:
                results.append(e)
                seen_ids.add(e["external_id"])
                seen_titles.add(title_key)

    logger.info("EUR-Lex total (SPARQL + daily): %d actos UE energéticos", len(results))
    return results


def scrape_backfill(year_from: int = 2020) -> List[Dict]:
    """Backfill histórico: descarga normativa europea energética desde year_from."""
    all_results = []
    current_year = date.today().year

    for year in range(year_from, current_year + 1):
        date_from = f"{year}-01-01"
        date_to   = f"{year}-12-31" if year < current_year else date.today().isoformat()
        logger.info("EUR-Lex backfill %d (%s → %s)...", year, date_from, date_to)
        bindings = _sparql_query(date_from, date_to)
        entries  = _process_bindings(bindings)
        logger.info("  %d: %d actos UE energéticos", year, len(entries))
        all_results.extend(entries)

    logger.info("EUR-Lex backfill total: %d actos", len(all_results))
    return all_results
