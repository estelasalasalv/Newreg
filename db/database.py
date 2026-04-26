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
    """Create/migrate tables."""
    sql = """
    CREATE TABLE IF NOT EXISTS boe_entries (
        id               SERIAL PRIMARY KEY,
        external_id      VARCHAR(200) UNIQUE NOT NULL,
        fecha            DATE,
        fuente           VARCHAR(20)  DEFAULT 'BOE',
        seccion          TEXT,
        departamento     TEXT,
        tipo             TEXT,
        titulo           TEXT NOT NULL,
        url              TEXT,
        importante       TEXT,
        acceso_conexion  TEXT,
        scraped_at       TIMESTAMPTZ  DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_boe_fecha      ON boe_entries(fecha DESC);
    CREATE INDEX IF NOT EXISTS idx_boe_importante ON boe_entries(importante) WHERE importante <> '';
    CREATE INDEX IF NOT EXISTS idx_boe_acceso     ON boe_entries(acceso_conexion) WHERE acceso_conexion <> '';

    -- Tabla legacy para CNMC (mantiene compatibilidad)
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
    CREATE INDEX IF NOT EXISTS idx_reg_source ON regulatory_entries(source);
    CREATE INDEX IF NOT EXISTS idx_reg_date   ON regulatory_entries(published_date DESC);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Database initialised.")


def upsert_boe(entries: List[Dict]) -> int:
    """Insert BOE entries; skip duplicates. Returns count of new rows."""
    if not entries:
        return 0
    sql = """
    INSERT INTO boe_entries
        (external_id, fecha, fuente, seccion, departamento, tipo,
         titulo, url, importante, acceso_conexion)
    VALUES
        (%(external_id)s, %(fecha)s, %(fuente)s, %(seccion)s, %(departamento)s, %(tipo)s,
         %(titulo)s, %(url)s, %(importante)s, %(acceso_conexion)s)
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
    """Insert CNMC / generic entries; skip duplicates."""
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


def fetch_recent(limit: int = 300) -> List[Dict]:
    """Return merged BOE + CNMC entries ordered by date for web export."""
    sql = """
    SELECT
        'BOE'                                                   AS source,
        external_id,
        titulo                                                  AS title,
        TO_CHAR(fecha, 'DD/MM/YYYY')                           AS published_date,
        url,
        seccion                                                 AS section,
        departamento                                            AS department,
        tipo,
        importante,
        acceso_conexion,
        NULL::text                                              AS summary,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at
    FROM boe_entries

    UNION ALL

    SELECT
        source,
        external_id,
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

    ORDER BY published_date DESC NULLS LAST, scraped_at DESC
    LIMIT %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def export_to_json(path: str = "web/data.json", limit: int = 300):
    rows = fetch_recent(limit)
    payload = {
        "updated_at": datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC"),
        "total": len(rows),
        "entries": rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Exported %d entries to %s", len(rows), path)
