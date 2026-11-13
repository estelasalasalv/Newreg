"""[Inclusión consultas CE] Consultas públicas de la Comisión Europea ("Have your say").

Todo lo relativo a esta novedad está marcado con "Inclusión consultas CE"
(buscar ese texto en el repo para localizarlo o revertirlo).

Fuente: API pública que usa la propia web
  https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives_es
  - /brpapi/searchInitiatives?topic=ENER  → iniciativas del tema Energía
  - /brpapi/groupInitiatives/{id}         → detalle (DG, resumen en español)

Cada fase con periodo de comentarios (convocatoria de datos, consulta pública,
proyecto de acto...) se guarda como una consulta distinta, igual que en España
son entradas distintas la consulta previa y la audiencia pública.
"""
import logging
from datetime import date, datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

API_BASE = "https://ec.europa.eu/info/law/better-regulation/brpapi"
WEB_BASE = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives"
_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)", "Accept": "application/json"}
TOPIC = "ENER"
PAGE_SIZE = 100
# Además de las abiertas, se guardan las iniciadas desde esta fecha (para ver cerradas recientes)
CE_DESDE = date(2026, 9, 1)

_MONTH_NAMES = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio",
                8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}

# Fase de la iniciativa en la que se recogen comentarios (frontEndStage)
_FASES = {
    "PLANNING_WORKFLOW": "Convocatoria de datos",
    "OPC_LAUNCHED":      "Consulta pública",
    "ISC_WORKFLOW":      "Proyecto de acto",
    "ADOPTION_WORKFLOW": "Propuesta adoptada por la Comisión",
}
# Tipo de acto previsto (foreseenActType)
_TIPOS_ACTO = {
    "PROP_REG": "Propuesta de Reglamento", "PROP_DIR": "Propuesta de Directiva",
    "PROP_DEC": "Propuesta de Decisión", "REG": "Reglamento", "DIR": "Directiva",
    "DEC": "Decisión", "REG_DEL": "Reglamento Delegado", "DIR_DEL": "Directiva Delegada",
    "DEC_DEL": "Decisión Delegada", "REG_IMPL": "Reglamento de Ejecución",
    "DIR_IMPL": "Directiva de Ejecución", "DEC_IMPL": "Decisión de Ejecución",
    "COMMUNIC": "Comunicación", "REPORT": "Informe", "EVL": "Evaluación",
    "RECOMMENDATION": "Recomendación", "WP": "Programa de trabajo",
}


def _get(path: str, **params) -> dict:
    resp = requests.get(API_BASE + path, params=params, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_dt(s: Optional[str]) -> Optional[date]:
    """'2026/10/26 10:17:00' → date."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y/%m/%d").date()
    except ValueError:
        return None


def _fmt_plazo(inicio: Optional[date], fin: Optional[date]) -> Optional[str]:
    """Mismo formato que nuevas_consultas: compatible con daysRemaining() de la web."""
    def f(d: date) -> str:
        return f"{d.day} de {_MONTH_NAMES[d.month]} de {d.year}"
    if inicio and fin:
        return f"Desde {f(inicio)} hasta {f(fin)}"
    if fin:
        return f"Hasta {f(fin)}"
    return None


def _traduccion(translations: list, field: str, lang: str = "ES") -> Optional[str]:
    for t in translations or []:
        if t.get("language") == lang and t.get("field") == field and t.get("value"):
            return t["value"].strip()
    return None


def _estado_actual(item: dict) -> dict:
    estados = item.get("currentStatuses") or []
    return next((s for s in estados if s.get("isCurrent")), estados[0] if estados else {})


def scrape() -> List[Dict]:
    # 1. Listado de iniciativas del tema Energía (paginado)
    items: List[dict] = []
    page = 0
    while True:
        try:
            data = _get("/searchInitiatives", topic=TOPIC, size=PAGE_SIZE, page=page, language="ES")
        except (requests.RequestException, ValueError) as exc:
            logger.error("CE Have your say: error en el listado (página %d): %s", page, exc)
            break
        pg = data.get("initiativeResultDtoPage") or {}
        items.extend(pg.get("content") or [])
        if pg.get("last", True):
            break
        page += 1

    results: List[Dict] = []
    for it in items:
        st = _estado_actual(it)
        inicio = _parse_dt(st.get("feedbackStartDate"))
        fin    = _parse_dt(st.get("feedbackEndDate"))
        if not fin:
            continue   # iniciativa solo prevista: aún no hay periodo de comentarios
        abierta = st.get("receivingFeedbackStatus") == "OPEN"
        if not abierta and not (inicio and inicio >= CE_DESDE):
            continue

        iid  = int(it["id"])
        fase = st.get("frontEndStage") or "OTRA"
        # 2. Detalle: DG responsable y resumen en español
        detalle: dict = {}
        try:
            detalle = _get(f"/groupInitiatives/{iid}", language="ES")
        except (requests.RequestException, ValueError) as exc:
            logger.warning("CE Have your say: sin detalle para %d: %s", iid, exc)

        titulo = (_traduccion(it.get("initiativeTranslations"), "SHORT_TITLE")
                  or _traduccion(detalle.get("initiativeTranslations"), "SHORT_TITLE")
                  or it.get("shortTitle") or "").strip()
        resumen = (_traduccion(detalle.get("initiativeTranslations"), "SUMMARY")
                   or detalle.get("dossierSummary"))
        if resumen:   # la API devuelve el resumen con etiquetas HTML (<p>...)
            resumen = BeautifulSoup(resumen, "lxml").get_text(" ", strip=True) or None
        tipo_acto = _TIPOS_ACTO.get(it.get("foreseenActType"), it.get("foreseenActType") or "")
        dg = detalle.get("dg") or ""

        results.append({
            "source":         "CE",
            "tipo":           "consulta",
            "external_id":    f"ce-{iid}-{fase.lower()}",
            "title":          titulo,
            "published_date": inicio.isoformat() if inicio else None,
            # La fase va en el fragmento para que cada fase tenga URL propia (la deduplicación
            # de upsert_entries es por URL); el fragmento no altera la página oficial.
            "url":            f"{WEB_BASE}/{iid}_es#{fase.lower()}",
            "section":        " · ".join(x for x in (_FASES.get(fase, fase), tipo_acto) if x),
            "department":     f"Comisión Europea · DG {dg}" if dg else "Comisión Europea",
            "summary":        resumen,
            "plazo":          _fmt_plazo(inicio, fin),
            "estado":         "Abierta" if abierta else "Cerrada",
            "sector":         "otros",
        })

    logger.info("CE Have your say: %d iniciativas de energía, %d consultas guardables (abiertas o desde %s)",
                len(items), len(results), CE_DESDE.isoformat())
    return results
