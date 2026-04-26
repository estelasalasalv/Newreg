"""PostgreSQL database operations."""
import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)


def init_db():
    """Recrea las tablas desde cero con el nuevo esquema."""
    sql = """
    -- Tabla BOE con esquema completo de 13 campos
    DROP TABLE IF EXISTS boe_entries CASCADE;
    CREATE TABLE boe_entries (
        id               SERIAL PRIMARY KEY,
        external_id      VARCHAR(200) UNIQUE NOT NULL,
        fecha            DATE,
        fuente           VARCHAR(10)   DEFAULT 'BOE',
        seccion          TEXT,
        tipo             TEXT,
        organismo        TEXT,
        subseccion       TEXT,
        texto            TEXT NOT NULL,
        enlace           TEXT,
        palabras_clave   TEXT,
        resumen          TEXT,
        importante       VARCHAR(3)    DEFAULT 'No',
        acceso_conexion  VARCHAR(3)    DEFAULT 'No',
        publicable       VARCHAR(3)    DEFAULT 'NO',
        scraped_at       TIMESTAMPTZ   DEFAULT NOW()
    );
    CREATE INDEX idx_boe_fecha      ON boe_entries(fecha DESC);
    CREATE INDEX idx_boe_importante ON boe_entries(importante);
    CREATE INDEX idx_boe_acceso     ON boe_entries(acceso_conexion);

    -- Tabla CNMC (sin cambios)
    CREATE TABLE IF NOT EXISTS regulatory_entries (
        id              SERIAL PRIMARY KEY,
        source          VARCHAR(20)  NOT NULL,
        external_id     VARCHAR(200) UNIQUE,
        title           TEXT         NOT NULL,
        published_date  DATE,
        url             TEXT,
        section         TEXT,
        department      TEXT,
        summary         TEXT,
        scraped_at      TIMESTAMPTZ  DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_reg_date ON regulatory_entries(published_date DESC);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Base de datos inicializada (esquema nuevo).")


def upsert_boe(entries: List[Dict]) -> int:
    """Inserta entradas BOE; ignora duplicados. Devuelve nº de filas nuevas."""
    if not entries:
        return 0
    sql = """
    INSERT INTO boe_entries
        (external_id, fecha, fuente, seccion, tipo, organismo, subseccion,
         texto, enlace, palabras_clave, resumen, importante, acceso_conexion, publicable)
    VALUES
        (%(external_id)s, %(fecha)s, %(fuente)s, %(seccion)s, %(tipo)s,
         %(organismo)s, %(subseccion)s, %(texto)s, %(enlace)s, %(palabras_clave)s,
         %(resumen)s, %(importante)s, %(acceso_conexion)s, %(publicable)s)
    ON CONFLICT (external_id) DO NOTHING
    """
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute(sql, e)
                inserted += cur.rowcount
        conn.commit()
    return inserted


def upsert_entries(entries: List[Dict]) -> int:
    """Inserta entradas CNMC/genéricas; ignora duplicados."""
    if not entries:
        return 0
    sql = """
    INSERT INTO regulatory_entries
        (source, external_id, title, published_date, url, section, department, summary)
    VALUES
        (%(source)s, %(external_id)s, %(title)s, %(published_date)s,
         %(url)s, %(section)s, %(department)s, %(summary)s)
    ON CONFLICT (external_id) DO NOTHING
    """
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute(sql, e)
                inserted += cur.rowcount
        conn.commit()
    return inserted


def fetch_boe_trimestre(days: int = 92) -> List[Dict]:
    """Devuelve entradas BOE del último trimestre con todos los campos."""
    sql = """
    SELECT
        TO_CHAR(fecha, 'DD/MM/YYYY') AS fecha,
        fuente, seccion, tipo, organismo, subseccion,
        texto, enlace, palabras_clave, resumen,
        importante, acceso_conexion, publicable,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid', 'DD/MM/YYYY HH24:MI') AS scraped_at
    FROM   boe_entries
    WHERE  fecha >= CURRENT_DATE - %(days)s
    ORDER  BY fecha DESC, scraped_at DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"days": days})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_recent(limit: int = 300) -> List[Dict]:
    """Devuelve entradas combinadas BOE+CNMC para la pestaña Todas."""
    sql = """
    SELECT
        'BOE'                                                   AS source,
        texto                                                   AS title,
        TO_CHAR(fecha, 'DD/MM/YYYY')                           AS published_date,
        enlace                                                  AS url,
        seccion                                                 AS section,
        organismo                                               AS department,
        tipo,
        importante,
        acceso_conexion,
        palabras_clave                                          AS summary,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at
    FROM boe_entries
    WHERE fecha >= CURRENT_DATE - 92

    UNION ALL

    SELECT
        source,
        title,
        TO_CHAR(published_date, 'DD/MM/YYYY')                  AS published_date,
        url,
        section,
        department,
        NULL::text                                              AS tipo,
        NULL::text                                              AS importante,
        NULL::text                                              AS acceso_conexion,
        summary,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at
    FROM regulatory_entries
    WHERE (published_date IS NULL OR published_date >= CURRENT_DATE - 92)

    ORDER BY published_date DESC NULLS LAST, scraped_at DESC
    LIMIT %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def export_to_json(path: str = "web/data.json", limit: int = 300):
    """Exporta datos al JSON que consume la web estática."""
    entries        = fetch_recent(limit)
    boe_trimestre  = fetch_boe_trimestre(92)
    payload = {
        "updated_at":     datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC"),
        "total":          len(entries),
        "entries":        entries,        # pestaña Todas
        "boe_trimestre":  boe_trimestre,  # pestaña BOE Trimestre
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Exportadas %d entradas totales / %d BOE trimestre → %s",
                len(entries), len(boe_trimestre), path)
