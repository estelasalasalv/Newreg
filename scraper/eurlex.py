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
    return "Sí" if tipo in (
        "Reglamento (UE)", "Directiva (UE)",
        "Reglamento de Ejecución (UE)", "Reglamento Delegado (UE)",
    ) else "No"


_NON_LEGIS_RE = re.compile(
    r"^(REPORT FROM|COMMISSION STAFF WORKING|COMMUNICATION FROM|"
    r"ANNEX TO|WORKING DOCUMENT|NOTICE |CORRIGENDUM TO|CORRIGENDUM$|"
    r"Decreto-Lei|Decreto Legislativo|Lei n\.|Orden |Real Decreto|"
    r"SUMMARY RECORD|MINUTES OF|AGENDA OF)",
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
    CONTAINS(STR(?title), "(EURATOM)") OR CONTAINS(STR(?title), "(Euratom)")
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
    CONTAINS(LCASE(STR(?title)), "net zero")
  )
}} ORDER BY ?work DESC(?date) LIMIT 1000
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

        # Preferir español, luego inglés, luego el primero disponible
        title = ""
        for lang_pref in ["es", "en", ""]:
            matches = [t for l, t in titles_list if l.lower().startswith(lang_pref)]
            if matches:
                title = matches[0]
                break
        if not title and titles_list:
            title = titles_list[0][1]
        if not title:
            continue

        # Filtrar documentos no legislativos y legislación nacional no UE
        if _NON_LEGIS_RE.search(title):
            continue
        # Requiere marcador UE/EU
        if not EU_ACT_STRICT_RE.search(title):
            continue
        # Requiere año numérico (actos adoptados) o número de acto
        if not re.search(r"\b(20\d\d)[/\\]\d+|\b(20\d\d/\d+)", title):
            continue
        # El título debe EMPEZAR por el tipo de acto UE (no por legislación nacional)
        EU_TITLE_START = re.compile(
            r"^(Regulation|Commission Regulation|Commission Implementing Regulation|"
            r"Commission Delegated Regulation|Delegated Regulation|"
            r"Directive|Commission Directive|Implementing Directive|"
            r"Decision|Commission Decision|Commission Implementing Decision|"
            r"Commission Delegated Decision|Council Decision|Council Regulation|"
            r"Reglamento|Directiva|Decisi[oó]n|"
            r"Recommendation|Recomendaci[oó]n)",
            re.IGNORECASE
        )
        if not EU_TITLE_START.match(title):
            continue

        # Deduplicar por título
        key = re.sub(r"\s+", " ", title[:100])
        if key in seen:
            continue
        seen.add(key)

        # Construir URL al documento
        cellar_id = work.replace(CELLAR_BASE, "").split(".")[0] if CELLAR_BASE in work else ""
        enlace = f"https://eur-lex.europa.eu/resource.html?uri=cellar:{cellar_id}" if cellar_id else work

        tipo       = _detect_tipo(title)
        importante = _is_importante(tipo)
        ext_id     = f"eu-{cellar_id[:30]}" if cellar_id else f"eu-{re.sub(r'[^a-z0-9]','', title[:40].lower())}"

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
            "publicable":      "NO",
        })

    return results


def scrape(days_back: int = 1) -> List[Dict]:
    """Scrape normativa europea energética del día actual (o últimos N días)."""
    today     = date.today()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to   = today.isoformat()
    bindings  = _sparql_query(date_from, date_to)
    results   = _process_bindings(bindings)
    logger.info("EUR-Lex diario: %d actos UE energéticos", len(results))
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
