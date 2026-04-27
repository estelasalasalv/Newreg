"""
Backfill extraordinario: normativa energética BOE 2025-2026.

Pasos:
  1. Scraping de todos los días laborables desde el 01/01/2025
  2. Por cada entrada nueva, descarga el texto del documento BOE
  3. Llama a Claude Haiku para generar resumen + impacto REE + acceso_conexion
  4. Graba todo en la BD (solo BD, sin actualizar data.json)

Uso:
  python backfill_historico.py              # procesa todo
  python backfill_historico.py --solo-boe   # solo scraping, sin Claude
  python backfill_historico.py --solo-ia    # solo genera resúmenes (entradas ya en BD)
  python backfill_historico.py --limit 50   # limita el nº de resúmenes generados
"""
import os, sys, json, re, time, logging, argparse
import requests
import anthropic
import psycopg2
from bs4 import BeautifulSoup
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
FECHA_INICIO = date(2025, 1, 1)
FECHA_FIN    = date.today()

SYSTEM_PROMPT = (
    "Eres un experto en regulación del sector eléctrico español, especializado en "
    "Red Eléctrica de España (REE). Respondes SIEMPRE en JSON válido, sin markdown."
)

USER_TEMPLATE = """\
Analiza este documento regulatorio español del BOE y proporciona:
1. resumen: 2-3 frases que expliquen QUÉ establece o modifica la norma (no copies el título).
2. impacto_ree: 1-2 frases sobre cómo afecta a REE como operador del sistema y gestor de la red de transporte. Si no afecta directamente escribe "Sin impacto directo identificado."
3. acceso_conexion: "Sí" si el texto trata sobre acceso a la red, conexión, peajes, o condiciones técnicas de conexión. Si no, "No".

Título: {titulo}
Sección: {seccion}
Organismo: {organismo}
Texto del documento (primeros 2500 caracteres):
{texto_doc}

Responde SOLO con este JSON:
{{"resumen": "...", "impacto_ree": "...", "acceso_conexion": "Sí o No"}}"""


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ── 1. Scraping BOE ───────────────────────────────────────────────────────────
def scrape_rango():
    from scraper.boe import scrape, BOE_API, _parse_sumario, _should_include, \
        _find_keywords, _detect_tipo, _is_importante, _detect_acceso, _get_url, _to_list, _get_nombre
    from db.database import upsert_boe

    total_new = 0
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    delta = (FECHA_FIN - FECHA_INICIO).days + 1

    for offset in range(delta):
        d = FECHA_INICIO + timedelta(days=offset)
        if d.weekday() == 6:     # solo domingo se salta
            continue
        fecha = d.strftime("%Y%m%d")
        url   = BOE_API.format(fecha=fecha)
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            items = _parse_sumario(resp.json(), d.strftime("%Y-%m-%d"))
            if items:
                new = upsert_boe(items)
                if new:
                    logger.info("BOE %s: %d nuevas", fecha, new)
                    total_new += new
        except Exception as exc:
            logger.warning("Error BOE %s: %s", fecha, exc)
        time.sleep(0.05)   # cortesía hacia la API del BOE

    logger.info("Scraping completado: %d entradas nuevas en BD", total_new)
    return total_new


# ── 2. Descarga texto de un documento BOE ────────────────────────────────────
def fetch_boe_text(enlace: str) -> str:
    """Descarga el HTML del documento BOE y devuelve el texto limpio (max 2500 chars)."""
    if not enlace:
        return ""
    try:
        r = requests.get(enlace, timeout=20,
                         headers={"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        # Selectores en orden de preferencia
        for sel in ["#textoxslt", "div.dispo", "div.documento-boe", "article", "main"]:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(" ", strip=True)
                text = re.sub(r"\s+", " ", text)
                return text[:2500]
        # Fallback: texto completo sin navegación
        for tag in soup(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        return re.sub(r"\s+", " ", soup.get_text(" ", strip=True))[:2500]
    except Exception as exc:
        logger.debug("No se pudo descargar %s: %s", enlace, exc)
        return ""


# ── 3. Generación con Claude Haiku ───────────────────────────────────────────
def generate_ia(client: anthropic.Anthropic, titulo: str, seccion: str,
                organismo: str, texto_doc: str) -> dict:
    prompt = USER_TEMPLATE.format(
        titulo    = titulo,
        seccion   = seccion   or "—",
        organismo = organismo or "—",
        texto_doc = texto_doc or "(texto no disponible)",
    )
    msg = client.messages.create(
        model      = "claude-haiku-4-5-20251001",
        max_tokens = 400,
        system     = SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": prompt}],
    )
    return json.loads(msg.content[0].text.strip())


def generate_summaries(limit: int = 0):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY no configurado.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    conn   = get_connection()
    cur    = conn.cursor()

    # Entradas sin resumen, ordenadas por fecha desc (más recientes primero)
    q = """
        SELECT id, texto, seccion, organismo, enlace
        FROM   boe_entries
        WHERE  (resumen IS NULL OR resumen = '')
          AND  fecha BETWEEN %s AND %s
        ORDER  BY fecha DESC
    """
    params = (FECHA_INICIO, FECHA_FIN)
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q, params)
    rows = cur.fetchall()
    logger.info("%d entradas pendientes de resumen", len(rows))

    procesadas = 0
    for id_, titulo, seccion, organismo, enlace in rows:
        try:
            texto_doc = fetch_boe_text(enlace or "")
            result    = generate_ia(client, titulo or "", seccion or "",
                                    organismo or "", texto_doc)

            cur.execute(
                "UPDATE boe_entries SET resumen=%s, impacto_ree=%s, acceso_conexion=%s WHERE id=%s",
                (result.get("resumen",""),
                 result.get("impacto_ree",""),
                 result.get("acceso_conexion","No"),
                 id_),
            )
            conn.commit()
            procesadas += 1
            logger.info("[%d/%d] %s", procesadas, len(rows), (titulo or "")[:65])

        except Exception as exc:
            logger.warning("Error en id=%d: %s", id_, exc)
            conn.rollback()
            time.sleep(2)

    conn.close()
    logger.info("Resúmenes generados: %d", procesadas)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo-boe", action="store_true")
    parser.add_argument("--solo-ia",  action="store_true")
    parser.add_argument("--limit",    type=int, default=0,
                        help="Máx. entradas a resumir por ejecución (0=todas)")
    parser.add_argument("--desde",    default=None,
                        help="Fecha inicio YYYY-MM-DD (defecto: 2025-01-01)")
    parser.add_argument("--hasta",    default=None,
                        help="Fecha fin YYYY-MM-DD (defecto: hoy)")
    args = parser.parse_args()

    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL no configurado."); sys.exit(1)

    global FECHA_INICIO, FECHA_FIN
    if args.desde:
        FECHA_INICIO = date.fromisoformat(args.desde)
    if args.hasta:
        FECHA_FIN = date.fromisoformat(args.hasta)

    if not args.solo_ia:
        logger.info("=== FASE 1: Scraping BOE %s → %s ===", FECHA_INICIO, FECHA_FIN)
        scrape_rango()

    if not args.solo_boe:
        logger.info("=== FASE 2: Generación de resúmenes con Claude Haiku ===")
        generate_summaries(limit=args.limit)

    logger.info("=== Backfill histórico completado ===")


if __name__ == "__main__":
    main()
