"""
Analiza normativa nueva con OpenAI API (ChatGPT) para:
  1. Generar un resumen del documento.
  2. Determinar qué funciones de Red Eléctrica (tabla ree_funciones) se ven afectadas.

Guarda resultados en:
  - resumen / summary de la tabla origen (boe_entries / regulatory_entries / eurlex_entries)
  - ree_normativa_funciones (relación normativa ↔ funciones REE afectadas)

Las entradas que fallan se guardan en ree_analisis_pendiente para reintento posterior.

Clave API: https://platform.openai.com/api-keys
Modelo: gpt-4o-mini (rápido y económico).

Uso:
  python generate_ree_analysis.py              # procesa pendientes + nuevas (máx 30 por ejecución)
  python generate_ree_analysis.py --limit 10   # máximo 10 entradas
  python generate_ree_analysis.py --retry      # solo reintentar las pendientes
"""
import os
import re
import sys
import json
import time
import logging
import argparse
import requests
import psycopg2
from openai import OpenAI, RateLimitError
from typing import Optional
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OPENAI_MODEL  = "gpt-4o-mini"
HEADERS_WEB   = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}
MAX_DOC_CHARS = 12_000
MAX_RETRIES   = 3
RETRY_WAIT    = 10

SYSTEM_PROMPT = (
    "Eres un experto en regulación energética española y europea, especializado en "
    "Red Eléctrica de España (REE) como transportista y operador del sistema eléctrico. "
    "Respondes SIEMPRE con JSON válido, sin markdown ni texto adicional."
)


# ── Conexión BD ───────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ── Carga de funciones REE ────────────────────────────────────────────────────

def load_ree_funciones() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, categoria, actividad, descripcion, keywords "
                "FROM ree_funciones ORDER BY categoria, id"
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Descarga del texto del documento ─────────────────────────────────────────

def fetch_text(url: str, normativa_tipo: str) -> Optional[str]:
    """Descarga y extrae el texto útil del documento según la fuente."""
    if not url:
        return None
    try:
        r = requests.get(url, headers=HEADERS_WEB, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        logger.warning("Error descargando %s: %s", url, exc)
        return None

    soup = BeautifulSoup(r.text, "lxml")

    if normativa_tipo == "boe":
        # BOE: contenido principal en #textoxslt o .diariosoficiales
        main = soup.select_one("#textoxslt, .diariosoficiales, article")
        text = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
    elif normativa_tipo in ("regulatory", "eurlex"):
        # CNMC/MITERD/ACER/DOUE: texto principal
        for tag in soup(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        main = soup.select_one("main, article, .field--name-body, #content")
        text = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
    else:
        text = soup.get_text(" ", strip=True)

    # Limpiar espacios múltiples
    text = re.sub(r"\s+", " ", text).strip()
    return text[:MAX_DOC_CHARS] if text else None


# ── Llamada a Gemini ──────────────────────────────────────────────────────────

def build_prompt(titulo: str, texto: str, funciones: list[dict]) -> str:
    funciones_txt = "\n".join(
        f"- ID {f['id']} [{f['categoria']}] {f['actividad']}: {f['descripcion']}"
        for f in funciones
    )
    return f"""Analiza el siguiente documento normativo/regulatorio y responde ÚNICAMENTE con JSON válido.

TÍTULO: {titulo}

TEXTO DEL DOCUMENTO:
{texto}

FUNCIONES DE RED ELÉCTRICA DE ESPAÑA (REE):
{funciones_txt}

Responde con este JSON exacto (sin markdown, sin texto adicional):
{{
  "resumen": "2-3 frases claras sobre qué regula o establece este documento",
  "funciones_afectadas": [
    {{
      "funcion_id": <id numérico>,
      "justificacion": "1 frase explicando por qué afecta a esta función"
    }}
  ]
}}

Si no afecta a ninguna función de REE, devuelve "funciones_afectadas": [].
Solo incluye funciones con afectación real y directa, no potencial o remota."""


def call_openai(client: OpenAI, prompt: str) -> Optional[dict]:
    """Llama a OpenAI API y devuelve el JSON parseado. Reintenta hasta MAX_RETRIES."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.1,
                max_tokens=1024,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content.strip()
            return json.loads(text)
        except RateLimitError:
            wait = RETRY_WAIT * attempt
            logger.warning("Rate limit OpenAI, esperando %ds (intento %d/%d)", wait, attempt, MAX_RETRIES)
            time.sleep(wait)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Respuesta OpenAI no parseable (intento %d): %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
        except Exception as exc:
            logger.warning("Error llamando OpenAI (intento %d): %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
    return None


# ── Guardado en BD ────────────────────────────────────────────────────────────

def save_result(conn, normativa_tipo: str, normativa_id: int, resumen: str,
                funciones_afectadas: list, funcion_map: dict):
    with conn.cursor() as cur:
        # Guardar resumen en la tabla origen
        if normativa_tipo == "boe":
            cur.execute(
                "UPDATE boe_entries SET resumen=%s WHERE id=%s AND (resumen IS NULL OR resumen='')",
                (resumen, normativa_id),
            )
        elif normativa_tipo == "regulatory":
            cur.execute(
                "UPDATE regulatory_entries SET summary=%s WHERE id=%s AND (summary IS NULL OR summary='')",
                (resumen, normativa_id),
            )
        elif normativa_tipo == "eurlex":
            cur.execute(
                "UPDATE eurlex_entries SET resumen=%s WHERE id=%s AND (resumen IS NULL OR resumen='')",
                (resumen, normativa_id),
            )

        # Guardar funciones afectadas
        for fa in funciones_afectadas:
            fid = fa.get("funcion_id")
            just = fa.get("justificacion", "")
            if fid not in funcion_map:
                continue
            cat = funcion_map[fid]["categoria"]
            cur.execute(
                """INSERT INTO ree_normativa_funciones
                     (normativa_tipo, normativa_id, funcion_id, categoria, justificacion)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (normativa_tipo, normativa_id, funcion_id) DO UPDATE
                     SET justificacion = EXCLUDED.justificacion
                """,
                (normativa_tipo, normativa_id, fid, cat, just),
            )

        # Eliminar de pendientes si estaba
        cur.execute(
            "DELETE FROM ree_analisis_pendiente WHERE normativa_tipo=%s AND normativa_id=%s",
            (normativa_tipo, normativa_id),
        )
    conn.commit()


def save_pending(conn, normativa_tipo: str, normativa_id: int, url: str,
                 titulo: str, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ree_analisis_pendiente
                 (normativa_tipo, normativa_id, url, titulo, intentos, ultimo_error, ultimo_intento)
               VALUES (%s, %s, %s, %s, 1, %s, NOW())
               ON CONFLICT (normativa_tipo, normativa_id) DO UPDATE SET
                 intentos       = ree_analisis_pendiente.intentos + 1,
                 ultimo_error   = EXCLUDED.ultimo_error,
                 ultimo_intento = NOW()
            """,
            (normativa_tipo, normativa_id, url, titulo[:300], error[:500]),
        )
    conn.commit()


# ── Consultas de entradas pendientes de análisis ──────────────────────────────

def fetch_pending_queue(conn, limit: int) -> list[dict]:
    """Entradas en la cola de pendientes (fallaron antes)."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT normativa_tipo, normativa_id, url, titulo
               FROM ree_analisis_pendiente
               WHERE intentos < 5
               ORDER BY intentos ASC, ultimo_intento ASC NULLS FIRST
               LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_new_entries(conn, limit: int) -> list[dict]:
    """Entradas nuevas sin análisis REE todavía."""
    rows = []

    # BOE sin resumen_ia (campo resumen en boe_entries)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT 'boe' AS normativa_tipo, id AS normativa_id,
                      enlace AS url, texto AS titulo
               FROM boe_entries
               WHERE (resumen IS NULL OR resumen = '')
                 AND fecha >= '2026-01-01'
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_normativa_funciones r
                   WHERE r.normativa_tipo='boe' AND r.normativa_id=boe_entries.id
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_analisis_pendiente p
                   WHERE p.normativa_tipo='boe' AND p.normativa_id=boe_entries.id AND p.intentos >= 5
                 )
               ORDER BY fecha DESC NULLS LAST
               LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows += [dict(zip(cols, r)) for r in cur.fetchall()]

    remaining = limit - len(rows)
    if remaining <= 0:
        return rows

    # regulatory_entries (CNMC_RSS, CNMC_S, CNMC_N, MITERD, ACER) sin summary
    with conn.cursor() as cur:
        cur.execute(
            """SELECT 'regulatory' AS normativa_tipo, id AS normativa_id,
                      url, title AS titulo
               FROM regulatory_entries
               WHERE source IN ('CNMC_RSS','CNMC_S','CNMC_N','MITERD','ACER')
                 AND (summary IS NULL OR summary = '')
                 AND published_date >= '2026-01-01'
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_normativa_funciones r
                   WHERE r.normativa_tipo='regulatory' AND r.normativa_id=regulatory_entries.id
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_analisis_pendiente p
                   WHERE p.normativa_tipo='regulatory' AND p.normativa_id=regulatory_entries.id AND p.intentos >= 5
                 )
               ORDER BY scraped_at DESC NULLS LAST
               LIMIT %s
            """,
            (remaining,),
        )
        cols = [d[0] for d in cur.description]
        rows += [dict(zip(cols, r)) for r in cur.fetchall()]

    remaining = limit - len(rows)
    if remaining <= 0:
        return rows

    # EUR-Lex sin resumen
    with conn.cursor() as cur:
        cur.execute(
            """SELECT 'eurlex' AS normativa_tipo, id AS normativa_id,
                      enlace AS url, texto AS titulo
               FROM eurlex_entries
               WHERE (resumen IS NULL OR resumen = '')
                 AND fecha >= '2026-01-01'
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_normativa_funciones r
                   WHERE r.normativa_tipo='eurlex' AND r.normativa_id=eurlex_entries.id
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM ree_analisis_pendiente p
                   WHERE p.normativa_tipo='eurlex' AND p.normativa_id=eurlex_entries.id AND p.intentos >= 5
                 )
               ORDER BY fecha DESC NULLS LAST
               LIMIT %s
            """,
            (remaining,),
        )
        cols = [d[0] for d in cur.description]
        rows += [dict(zip(cols, r)) for r in cur.fetchall()]

    return rows


# ── Proceso principal ─────────────────────────────────────────────────────────

def process_entry(conn, client: OpenAI, entry: dict,
                  funciones: list[dict], funcion_map: dict) -> bool:
    tipo   = entry["normativa_tipo"]
    nid    = entry["normativa_id"]
    url    = entry.get("url") or ""
    titulo = entry.get("titulo") or ""

    logger.info("Analizando [%s id=%d] %s", tipo, nid, titulo[:70])

    # 1. Descargar texto
    texto = fetch_text(url, tipo)
    if not texto:
        save_pending(conn, tipo, nid, url, titulo, "No se pudo descargar el texto del documento")
        return False

    # 2. Llamar a OpenAI
    prompt = build_prompt(titulo, texto, funciones)
    result = call_openai(client, prompt)
    if not result:
        save_pending(conn, tipo, nid, url, titulo, "OpenAI no devolvió respuesta válida")
        return False

    # 3. Guardar resultado
    resumen = result.get("resumen", "")
    funciones_afectadas = result.get("funciones_afectadas", [])
    if not isinstance(funciones_afectadas, list):
        funciones_afectadas = []

    save_result(conn, tipo, nid, resumen, funciones_afectadas, funcion_map)

    n_func = len(funciones_afectadas)
    logger.info("  ✓ Resumen guardado. Funciones REE afectadas: %d", n_func)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30,
                        help="Máximo de entradas a procesar por ejecución (default: 30)")
    parser.add_argument("--retry", action="store_true",
                        help="Solo procesar entradas en cola de pendientes")
    args = parser.parse_args()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY no configurado. Obtén una clave en https://platform.openai.com/api-keys")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    funciones = load_ree_funciones()
    if not funciones:
        logger.error("No hay funciones REE en la BD. Ejecuta seed_ree_funciones.py primero.")
        sys.exit(1)
    funcion_map = {f["id"]: f for f in funciones}
    logger.info("Funciones REE cargadas: %d", len(funciones))

    conn = get_conn()

    # Obtener entradas a procesar
    entries = fetch_pending_queue(conn, args.limit)
    logger.info("Cola de pendientes: %d entradas", len(entries))

    if not args.retry:
        remaining = args.limit - len(entries)
        if remaining > 0:
            new_entries = fetch_new_entries(conn, remaining)
            logger.info("Entradas nuevas sin análisis: %d", len(new_entries))
            pending_keys = {(e["normativa_tipo"], e["normativa_id"]) for e in entries}
            for e in new_entries:
                if (e["normativa_tipo"], e["normativa_id"]) not in pending_keys:
                    entries.append(e)

    logger.info("Total a procesar: %d", len(entries))
    ok = 0
    fail = 0
    for entry in entries:
        success = process_entry(conn, client, entry, funciones, funcion_map)
        if success:
            ok += 1
        else:
            fail += 1
        time.sleep(1)

    conn.close()
    logger.info("Análisis REE completado. OK=%d  FAIL=%d (guardados en pendientes)", ok, fail)


if __name__ == "__main__":
    main()
