"""PostgreSQL database operations."""
import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def get_connection():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def init_db():
    """Create tables if they don't exist."""
    sql = """
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
    CREATE INDEX IF NOT EXISTS idx_source     ON regulatory_entries(source);
    CREATE INDEX IF NOT EXISTS idx_date       ON regulatory_entries(published_date DESC);
    CREATE INDEX IF NOT EXISTS idx_scraped_at ON regulatory_entries(scraped_at DESC);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Database initialised.")


def upsert_entries(entries: List[Dict]) -> int:
    """Insert new entries; skip duplicates. Returns count of new rows inserted."""
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
            for entry in entries:
                cur.execute(sql, entry)
                inserted += cur.rowcount
        conn.commit()
    return inserted


def fetch_recent(limit: int = 200) -> List[Dict]:
    """Return the most recent entries for web export."""
    sql = """
    SELECT source, external_id, title,
           TO_CHAR(published_date, 'DD/MM/YYYY') AS published_date,
           url, section, department, summary,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid', 'DD/MM/YYYY HH24:MI') AS scraped_at
    FROM   regulatory_entries
    ORDER  BY published_date DESC NULLS LAST, scraped_at DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def export_to_json(path: str = "web/data.json", limit: int = 200):
    """Dump recent entries to a JSON file consumed by the static web."""
    rows = fetch_recent(limit)
    payload = {
        "updated_at": datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC"),
        "total": len(rows),
        "entries": rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Exported %d entries to %s", len(rows), path)
