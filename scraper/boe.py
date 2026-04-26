"""BOE scraper using the official open-data API.

Docs: https://www.boe.es/datosabiertos/
API:  https://www.boe.es/datosabiertos/api/boe/sumario/{YYYYMMDD}
"""
import os
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)

BOE_API = "https://www.boe.es/datosabiertos/api/boe/sumario/{fecha}"
BOE_ITEM_URL = "https://www.boe.es/diario_boe/txt.php?id={id}"

DEFAULT_KEYWORDS = [
    "energía", "energia", "eléctrico", "electrico",
    "gas natural", "renovable", "hidrocarburo",
    "tarifa eléctrica", "mercado eléctrico",
    "CNMC", "REE", "regulación energética",
]


def _keywords() -> List[str]:
    raw = os.environ.get("BOE_KEYWORDS", "")
    if raw:
        return [k.strip().lower() for k in raw.split(",") if k.strip()]
    return [k.lower() for k in DEFAULT_KEYWORDS]


def _to_list(val) -> list:
    if val is None:
        return []
    return val if isinstance(val, list) else [val]


def _parse_sumario(data: dict, fecha_str: str) -> List[Dict]:
    """Walk the BOE JSON sumario and return flat list of items."""
    items = []
    try:
        sumario = data["data"]["sumario"]
        diario = sumario.get("diario", {})
        # diario can be a dict (single day) or a list
        diarios = _to_list(diario)
        for d in diarios:
            for seccion in _to_list(d.get("seccion")):
                sec_nombre = seccion.get("@nombre", "")
                for dept in _to_list(seccion.get("departamento")):
                    dept_nombre = dept.get("@nombre", "")
                    for epigrafe in _to_list(dept.get("epigrafe")):
                        for item in _to_list(epigrafe.get("item")):
                            doc_id = item.get("identificador", "")
                            items.append({
                                "source": "BOE",
                                "external_id": doc_id,
                                "title": item.get("titulo", "").strip(),
                                "published_date": fecha_str,
                                "url": BOE_ITEM_URL.format(id=doc_id),
                                "section": sec_nombre,
                                "department": dept_nombre,
                                "summary": None,
                            })
    except (KeyError, TypeError) as exc:
        logger.warning("Unexpected BOE response structure: %s", exc)
    return items


def _is_energy_relevant(entry: Dict, keywords: List[str]) -> bool:
    text = " ".join([
        entry.get("title", ""),
        entry.get("department", ""),
        entry.get("section", ""),
    ]).lower()
    return any(kw in text for kw in keywords)


def scrape(days_back: int = 1) -> List[Dict]:
    """Fetch BOE summaries for the last *days_back* days and filter by energy keywords."""
    keywords = _keywords()
    results: List[Dict] = []
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    for offset in range(days_back):
        date = datetime.now() - timedelta(days=offset)
        # Skip weekends (BOE not published Saturday/Sunday)
        if date.weekday() >= 5:
            continue
        fecha = date.strftime("%Y%m%d")
        url = BOE_API.format(fecha=fecha)
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.info("No BOE sumario for %s (404)", fecha)
                continue
            resp.raise_for_status()
            items = _parse_sumario(resp.json(), date.strftime("%Y-%m-%d"))
            before = len(items)
            items = [i for i in items if _is_energy_relevant(i, keywords)]
            logger.info("BOE %s: %d/%d items match energy keywords", fecha, len(items), before)
            results.extend(items)
        except requests.RequestException as exc:
            logger.error("BOE request failed for %s: %s", fecha, exc)

    return results
