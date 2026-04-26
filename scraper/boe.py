"""BOE scraper — API oficial de datos abiertos.

Campos extraídos por documento:
  fecha, fuente, seccion (h3), departamento (h4), tipo (h5/epígrafe),
  titulo, url, importante, acceso_conexion
"""
import os
import re
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BOE_API      = "https://www.boe.es/datosabiertos/api/boe/sumario/{fecha}"
BOE_ITEM_URL = "https://www.boe.es/diario_boe/txt.php?id={id}"

DEFAULT_KEYWORDS = [
    "energía", "energia", "eléctrico", "electrico",
    "gas natural", "renovable", "hidrocarburo",
    "tarifa eléctrica", "mercado eléctrico",
    "cnmc", "ree", "regulación energética", "red eléctrica",
    "sistema eléctrico", "instalaciones eléctricas", "sector eléctrico",
]

# Palabras que activan el campo "acceso_conexion"
ACCESS_WORDS = [
    "acceso", "conexión", "conexion",
    "interconexión", "interconexion",
    "peaje de acceso", "acceso a la red",
    "red de transporte", "red de distribución", "red de distribucion",
    "capacidad de acceso",
]

# Patrones que marcan un documento como "importante"
IMPORTANT_PATTERNS = [
    (r"\bley org[aá]nica\b",    "Ley Orgánica"),
    (r"\bley\b",                 "Ley"),
    (r"\breal decreto[-‐–]ley\b","Real Decreto-ley"),
    (r"\breal decreto\b",        "Real Decreto"),
    (r"\bordre?n\b",             "Orden"),
    (r"\bcircular\b",            "Circular CNMC"),
    (r"\bresoluc[ií][oó]n\b",    "Resolución"),
]


def _keywords() -> List[str]:
    raw = os.environ.get("BOE_KEYWORDS", "")
    if raw:
        return [k.strip().lower() for k in raw.split(",") if k.strip()]
    return DEFAULT_KEYWORDS


def _to_list(val) -> list:
    if val is None:
        return []
    return val if isinstance(val, list) else [val]


def _detect_importante(titulo: str, departamento: str) -> str:
    """Return document type if it's a law/decree/circular, else empty string."""
    text = titulo.lower()
    for pattern, label in IMPORTANT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            # Circular sólo es importante si viene de CNMC o es regulatoria
            if label == "Circular CNMC":
                if "cnmc" in departamento.lower() or "comisión" in departamento.lower():
                    return label
                continue
            return label
    return ""


def _detect_acceso_conexion(titulo: str) -> str:
    """Return space-separated list of access/connection keywords found in title (no duplicates)."""
    text = titulo.lower()
    # Normalize accents for matching
    normalized = (text
        .replace("á","a").replace("é","e").replace("í","i")
        .replace("ó","o").replace("ú","u").replace("ü","u")
    )
    found = []
    seen = set()
    for word in ACCESS_WORDS:
        key = word.lower().replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
        if key not in seen and key in normalized:
            found.append(word)
            seen.add(key)
    return ", ".join(found)


def _parse_sumario(data: dict, fecha_str: str) -> List[Dict]:
    """Walk the BOE JSON sumario and return flat list of items with all 9 fields."""
    items = []
    try:
        sumario = data["data"]["sumario"]
        for d in _to_list(sumario.get("diario")):
            for seccion in _to_list(d.get("seccion")):
                sec_nombre = seccion.get("@nombre", "")          # h3
                for dept in _to_list(seccion.get("departamento")):
                    dept_nombre = dept.get("@nombre", "")        # h4
                    for epigrafe in _to_list(dept.get("epigrafe")):
                        tipo = epigrafe.get("@nombre", "")       # h5
                        for item in _to_list(epigrafe.get("item")):
                            doc_id = item.get("identificador", "")
                            titulo = item.get("titulo", "").strip()
                            items.append({
                                "fecha":           fecha_str,
                                "fuente":          "BOE",
                                "seccion":         sec_nombre,
                                "departamento":    dept_nombre,
                                "tipo":            tipo,
                                "titulo":          titulo,
                                "url":             BOE_ITEM_URL.format(id=doc_id),
                                "external_id":     doc_id,
                                "importante":      _detect_importante(titulo, dept_nombre),
                                "acceso_conexion": _detect_acceso_conexion(titulo),
                            })
    except (KeyError, TypeError) as exc:
        logger.warning("Unexpected BOE response structure: %s", exc)
    return items


def _is_energy_relevant(entry: Dict, keywords: List[str]) -> bool:
    text = " ".join([
        entry.get("titulo", ""),
        entry.get("departamento", ""),
        entry.get("seccion", ""),
        entry.get("tipo", ""),
    ]).lower()
    return any(kw in text for kw in keywords)


def scrape(days_back: int = 1) -> List[Dict]:
    """Fetch BOE summaries for the last *days_back* days, filter by energy keywords."""
    keywords = _keywords()
    results: List[Dict] = []
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    for offset in range(days_back):
        date = datetime.now() - timedelta(days=offset)
        if date.weekday() >= 5:   # BOE no se publica sábado ni domingo
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
            energy = [i for i in items if _is_energy_relevant(i, keywords)]
            logger.info("BOE %s: %d/%d items relevantes energía", fecha, len(energy), len(items))
            results.extend(energy)
        except requests.RequestException as exc:
            logger.error("BOE request failed for %s: %s", fecha, exc)

    return results
