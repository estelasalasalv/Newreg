"""
Genera resúmenes e impacto en Red Eléctrica para entradas sin analizar,
usando Claude Haiku (API Anthropic). Solo procesa entradas nuevas (sin resumen).

Uso:
  python generate_summaries.py            # procesa todas las pendientes
  python generate_summaries.py --limit 20 # procesa máximo 20
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
    "Eres un experto en regulación energética española, especializado en "
    "Red Eléctrica de España (REE). Respondes SIEMPRE en JSON válido, "
    "sin markdown ni texto adicional."
)

USER_TEMPLATE = """\
Analiza esta normativa regulatoria española y proporciona:
1. resumen: 2-3 frases claras explicando de qué trata.
2. impacto_ree: 1-2 frases sobre cómo puede afectar a Red Eléctrica de España (REE) \
como operador del sistema eléctrico y gestor de la red de transporte. \
Si no le afecta directamente escribe exactamente: "Sin impacto directo identificado."

Título: {titulo}
Sección: {seccion}
Organismo: {organismo}
Palabras clave: {keywords}

Responde SOLO con este JSON (sin comillas extra, sin markdown):
{{"resumen": "...", "impacto_ree": "..."}}"""


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)


def fetch_pending_boe(limit: int) -> list:
    sql = """
    SELECT id, texto AS titulo, seccion, organismo, palabras_clave
    FROM   boe_entries
    WHERE  resumen IS NULL OR resumen = ''
    ORDER  BY fecha DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            return [dict(r) for r in cur.fetchall()]


def fetch_pending_cnmc(limit: int) -> list:
    sql = """
    SELECT id, title AS titulo, section AS seccion, department AS organismo, '' AS palabras_clave
    FROM   regulatory_entries
    WHERE  (summary IS NULL OR summary = '' OR summary NOT LIKE '%%resumen%%')
      AND  source = 'CNMC'
    ORDER  BY scraped_at DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            return [dict(r) for r in cur.fetchall()]


def save_boe(id_: int, resumen: str, impacto_ree: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE boe_entries SET resumen=%s, impacto_ree=%s WHERE id=%s",
                (resumen, impacto_ree, id_),
            )
        conn.commit()


def save_cnmc(id_: int, resumen: str, impacto_ree: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE regulatory_entries SET summary=%s, impacto_ree=%s WHERE id=%s",
                (resumen, impacto_ree, id_),
            )
        conn.commit()


def generate(client: anthropic.Anthropic, entry: dict) -> dict:
    prompt = USER_TEMPLATE.format(
        titulo    = entry.get("titulo", ""),
        seccion   = entry.get("seccion") or "—",
        organismo = entry.get("organismo") or "—",
        keywords  = entry.get("palabras_clave") or "—",
    )
    msg = client.messages.create(
        model      = "claude-haiku-4-5-20251001",
        max_tokens = 350,
        system     = SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    return json.loads(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50,
                        help="Máximo de entradas a procesar (por fuente)")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY no configurado.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # ── BOE ────────────────────────────────────────────────────────
    boe_rows = fetch_pending_boe(args.limit)
    logger.info("BOE pendientes: %d", len(boe_rows))
    for row in boe_rows:
        try:
            result = generate(client, row)
            save_boe(row["id"], result["resumen"], result["impacto_ree"])
            logger.info("BOE %d ✓ %s", row["id"], row["titulo"][:50])
        except Exception as exc:
            logger.warning("BOE %d error: %s", row["id"], exc)

    # ── CNMC ───────────────────────────────────────────────────────
    cnmc_rows = fetch_pending_cnmc(args.limit)
    logger.info("CNMC pendientes: %d", len(cnmc_rows))
    for row in cnmc_rows:
        try:
            result = generate(client, row)
            save_cnmc(row["id"], result["resumen"], result["impacto_ree"])
            logger.info("CNMC %d ✓ %s", row["id"], row["titulo"][:50])
        except Exception as exc:
            logger.warning("CNMC %d error: %s", row["id"], exc)

    logger.info("Generación completada.")


if __name__ == "__main__":
    main()
