"""BOE-N scraper — Suplemento de Notificaciones: Registros de la Propiedad.

Fuente: https://www.boe.es/boe_n/dias/YYYY/MM/DD/index.php?l=N

Flujo:
  Fase 1 – scrape():      inserta TODOS los anuncios de Registros de la
                          Propiedad con publicable='NO' (rápido, sin PDF).
  Fase 2 – filter_ree():  lee cada PDF en paralelo con pdfminer, marca
                          publicable='SI' los que contengan 'Red Eléctrica',
                          'REE' o 'Redeia'. Los demás quedan como 'NO'.
"""
import io
import re
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

BOE_N_INDEX = "https://www.boe.es/boe_n/dias/{y}/{m:02d}/{d:02d}/index.php?l=N"
BOE_N_PDF   = "https://www.boe.es{href}"
_HEADERS    = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

# Términos que marcan afección a Red Eléctrica / REE / Redeia
_REE_RE = re.compile(
    r"red el[eé]ctrica|\bredeia\b|\bREE\b",  # \b evita falsos positivos como 'Heredeia'
    re.IGNORECASE,
)

# ── Fase 1: scraping del índice ───────────────────────────────────────────────

def _scrape_day(target: date) -> List[Dict]:
    """Extrae todos los anuncios de Registro de la Propiedad del suplemento de ese día."""
    url = BOE_N_INDEX.format(y=target.year, m=target.month, d=target.day)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=60)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("BOE-N: error al descargar %s: %s", url, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    results = []
    seen: set = set()

    for li in soup.select("li.notif"):
        p = li.find("p")
        if not p:
            continue
        titulo = p.get_text(" ", strip=True)

        # Capturar Registros de la Propiedad Y Delegaciones del Gobierno con expropiación
        _DELEGACION_RE = re.compile(
            r"delegaci[oó]n del gobierno.*expropiaci[oó]n|"
            r"jurado.*expropiaci[oó]n|"
            r"notificaci[oó]n.*expropiaci[oó]n|"
            r"expropiaci[oó]n forzosa",
            re.IGNORECASE,
        )
        es_reg_prop  = titulo.upper().startswith("REGISTRO DE LA PROPIEDAD")
        es_delegacion = bool(_DELEGACION_RE.search(titulo))
        if not es_reg_prop and not es_delegacion:
            continue

        # Extraer link al PDF
        a = li.find("a", href=re.compile(r"not\.php"))
        if not a:
            continue
        href = a.get("href", "")
        boe_id_m = re.search(r"BOE-N-\d{4}-\d+", href)
        if not boe_id_m:
            continue
        boe_id = boe_id_m.group()

        if boe_id in seen:
            continue
        seen.add(boe_id)

        results.append({
            "external_id":  boe_id,
            "fecha":        target.isoformat(),
            "fuente":       "BOE-N",
            "seccion":      "Suplemento Notificaciones",
            "tipo":         "Anuncio Registro Propiedad",
            "organismo":    titulo.split(".")[0].strip(),
            "subseccion":   "",
            "texto":        titulo,
            "enlace":       f"https://www.boe.es{href}",
            "palabras_clave": "registro de la propiedad, tramitación",
            "resumen":      None,
            "importante":   "No",
            "acceso_conexion": "Sí",
            "tramitaciones": "Sí",
            "publicable":   "NO",   # pendiente de filtrado por contenido REE
            "impacto_ree":  None,
        })

    logger.info("BOE-N %s: %d anuncios de Registro de la Propiedad", target, len(results))
    return results


def scrape(days_back: int = 1) -> List[Dict]:
    """Descarga el suplemento BOE-N de los últimos days_back días (excluye domingos)."""
    all_entries: List[Dict] = []
    for delta in range(days_back):
        target = date.today() - timedelta(days=delta)
        if target.weekday() == 6:   # domingos no hay BOE-N
            continue
        all_entries.extend(_scrape_day(target))
    return all_entries


# ── Fase 2: filtrado por contenido REE ───────────────────────────────────────

def _extract_pdf_text(url: str, timeout: int = 20) -> str:
    """Descarga y extrae texto de un PDF del BOE-N con pdfminer."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout)
        r.raise_for_status()
        from pdfminer.high_level import extract_text
        return extract_text(io.BytesIO(r.content))
    except Exception as exc:
        logger.debug("BOE-N PDF error %s: %s", url, exc)
        return ""


def filter_ree(entries: List[Dict], max_workers: int = 8) -> Dict[str, bool]:
    """Lee los PDFs en paralelo y devuelve {external_id: tiene_ree}.

    Devuelve un dict {external_id: True/False} para cada entrada.
    Se llama después de upsert_boe_n() para actualizar publicable en BD.
    """
    result: Dict[str, bool] = {}

    def check_one(entry: Dict) -> tuple:
        text = _extract_pdf_text(entry["enlace"])
        found = bool(_REE_RE.search(text))
        if found:
            logger.info("BOE-N REE encontrado: %s — %s", entry["external_id"], entry["texto"][:60])
        return entry["external_id"], found

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(check_one, e): e for e in entries}
        for fut in as_completed(futures):
            try:
                ext_id, found = fut.result()
                result[ext_id] = found
            except Exception as exc:
                e = futures[fut]
                logger.warning("BOE-N filter error %s: %s", e.get("external_id"), exc)
                result[e.get("external_id", "")] = False

    return result
