"""
Genera resúmenes e impacto para empresa transportista y operadora del sistema
eléctrico español para entradas sin analizar, usando Claude Haiku.

Cubre: BOE, CNMC_C, CNMC_N, ACER, MITERD, EUR-Lex (DOUE).

Uso:
  python generate_summaries.py            # procesa todas las pendientes (máx 50 por fuente)
  python generate_summaries.py --limit 20 # procesa máximo 20 por fuente
"""
import os
import sys
import json
import logging
import argparse
import anthropic
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Eres un experto en regulación energética española y europea, especializado en "
    "empresas transportistas y operadoras del sistema eléctrico (TSO/OS) como "
    "Red Eléctrica de España / REE. "
    "Respondes SIEMPRE en JSON válido, sin markdown ni texto adicional."
)

USER_TEMPLATE = """\
Analiza esta normativa regulatoria y proporciona:
1. "resumen": 2-3 frases claras sobre qué regula o establece esta norma.
2. "impacto_tso": 2-3 frases concretas sobre cómo puede afectar a una empresa \
transportista y operadora del sistema eléctrico español (REE/Red Eléctrica). \
Considera especialmente: acceso y conexión a la red de transporte, retribución \
regulada, planificación de infraestructuras, operación del sistema, peajes, \
autorizaciones administrativas, obligaciones de información/transparencia, \
cumplimiento normativo (REMIT, SERC, LSE), impactos fiscales locales \
(tasas municipales, impuestos autonómicos sobre instalaciones de transporte). \
Si no le afecta directamente escribe: "Sin impacto directo identificado."

Fuente: {fuente}
Título: {titulo}
Sección/Tipo: {seccion}
Organismo: {organismo}
Palabras clave: {keywords}

Responde SOLO con este JSON:
{{"resumen": "...", "impacto_tso": "..."}}"""


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)


# ── Queries de pendientes ─────────────────────────────────────────────────────

def fetch_pending_boe(limit: int) -> list:
    sql = """
    SELECT id, texto AS titulo, seccion, organismo, palabras_clave, 'BOE' AS fuente
    FROM   boe_entries
    WHERE  (resumen IS NULL OR resumen = '')
      AND  texto NOT ILIKE '%%eficiencia del servicio p_blico%%'
    ORDER  BY fecha DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            return [dict(r) for r in cur.fetchall()]


def fetch_pending_regulatory(limit: int, source: str) -> list:
    sql = """
    SELECT id, title AS titulo, section AS seccion, department AS organismo,
           source AS fuente, '' AS palabras_clave
    FROM   regulatory_entries
    WHERE  source = %(source)s
      AND  (summary IS NULL OR summary = '' OR summary NOT LIKE '%%resumen%%')
      AND  (impacto_ree IS NULL OR impacto_ree = '')
    ORDER  BY scraped_at DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"source": source, "limit": limit})
            return [dict(r) for r in cur.fetchall()]


def fetch_pending_eurlex(limit: int) -> list:
    sql = """
    SELECT id, texto AS titulo, seccion, organismo, palabras_clave,
           tipo AS fuente_tipo, 'DOUE' AS fuente
    FROM   eurlex_entries
    WHERE  (resumen IS NULL OR resumen = '')
    ORDER  BY fecha DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            return [dict(r) for r in cur.fetchall()]


# ── Guardado ──────────────────────────────────────────────────────────────────

def save_boe(id_: int, resumen: str, impacto: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE boe_entries SET resumen=%s, impacto_ree=%s WHERE id=%s",
                (resumen, impacto, id_),
            )
        conn.commit()


def save_regulatory(id_: int, resumen: str, impacto: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE regulatory_entries SET summary=%s, impacto_ree=%s WHERE id=%s",
                (resumen, impacto, id_),
            )
        conn.commit()


def save_eurlex(id_: int, resumen: str, impacto: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # impacto_ree añadida con ALTER TABLE IF NOT EXISTS en init_db
            cur.execute(
                "UPDATE eurlex_entries SET resumen=%s, impacto_ree=%s WHERE id=%s",
                (resumen, impacto, id_),
            )
        conn.commit()


# ── Generación ────────────────────────────────────────────────────────────────

def generate(client: anthropic.Anthropic, entry: dict) -> dict:
    prompt = USER_TEMPLATE.format(
        fuente    = entry.get("fuente", "—"),
        titulo    = entry.get("titulo", ""),
        seccion   = entry.get("seccion") or entry.get("fuente_tipo") or "—",
        organismo = entry.get("organismo") or "—",
        keywords  = entry.get("palabras_clave") or "—",
    )
    msg = client.messages.create(
        model      = "claude-haiku-4-5-20251001",
        max_tokens = 500,
        system     = SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    return json.loads(text)


def process_batch(client, rows: list, save_fn, label: str):
    ok = 0
    for row in rows:
        try:
            result = generate(client, row)
            resumen = result.get("resumen", "")
            impacto = result.get("impacto_tso", result.get("impacto_ree", ""))
            save_fn(row["id"], resumen, impacto)
            logger.info("%s %d ✓ %s", label, row["id"], row["titulo"][:60])
            ok += 1
        except Exception as exc:
            logger.warning("%s %d error: %s | titulo: %s", label, row["id"], exc, row.get("titulo","")[:40])
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50,
                        help="Máximo de entradas a procesar por fuente")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY no configurado.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    total  = 0

    # ── BOE ────────────────────────────────────────────────────────────────
    rows = fetch_pending_boe(args.limit)
    logger.info("BOE pendientes: %d", len(rows))
    total += process_batch(client, rows, save_boe, "BOE")

    # ── CNMC_C (consultas + RSS) ────────────────────────────────────────────
    rows = fetch_pending_regulatory(args.limit, "CNMC_C")
    logger.info("CNMC_C pendientes: %d", len(rows))
    total += process_batch(client, rows, save_regulatory, "CNMC_C")

    # ── CNMC_N (actuaciones + noticias) ────────────────────────────────────
    rows = fetch_pending_regulatory(args.limit, "CNMC_N")
    logger.info("CNMC_N pendientes: %d", len(rows))
    total += process_batch(client, rows, save_regulatory, "CNMC_N")

    # ── ACER ───────────────────────────────────────────────────────────────
    rows = fetch_pending_regulatory(args.limit, "ACER")
    logger.info("ACER pendientes: %d", len(rows))
    total += process_batch(client, rows, save_regulatory, "ACER")

    # ── MITERD ─────────────────────────────────────────────────────────────
    rows = fetch_pending_regulatory(args.limit, "MITERD")
    logger.info("MITERD pendientes: %d", len(rows))
    total += process_batch(client, rows, save_regulatory, "MITERD")

    # ── EUR-Lex (DOUE) ──────────────────────────────────────────────────────
    rows = fetch_pending_eurlex(args.limit)
    logger.info("EUR-Lex pendientes: %d", len(rows))
    total += process_batch(client, rows, save_eurlex, "EUR-Lex")

    logger.info("Generación completada. Total procesadas: %d", total)


if __name__ == "__main__":
    main()
