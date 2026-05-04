"""PostgreSQL database operations."""
import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)


def init_db():
    """Crea las tablas si no existen. Nunca borra datos existentes."""
    sql = """
    CREATE TABLE IF NOT EXISTS boe_entries (
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
    CREATE INDEX IF NOT EXISTS idx_boe_fecha      ON boe_entries(fecha DESC);
    CREATE INDEX IF NOT EXISTS idx_boe_importante ON boe_entries(importante);
    CREATE INDEX IF NOT EXISTS idx_boe_acceso     ON boe_entries(acceso_conexion);

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
        tipo            VARCHAR(20)  DEFAULT 'regulacion',
        plazo           TEXT,
        scraped_at      TIMESTAMPTZ  DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_reg_date ON regulatory_entries(published_date DESC);
    -- Añadir columnas si no existen (migraciones seguras)
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS tipo        VARCHAR(20) DEFAULT 'regulacion';
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS plazo       TEXT;
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS impacto_ree TEXT;
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS estado      VARCHAR(20) DEFAULT 'Abierta';
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS comprobado  VARCHAR(1)  DEFAULT 'N';
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS importante  VARCHAR(3)  DEFAULT 'No';
    ALTER TABLE boe_entries        ADD COLUMN IF NOT EXISTS impacto_ree        TEXT;
    ALTER TABLE boe_entries        ADD COLUMN IF NOT EXISTS comprobado          VARCHAR(1)  DEFAULT 'N';
    ALTER TABLE boe_entries        ADD COLUMN IF NOT EXISTS tramitaciones       VARCHAR(3)  DEFAULT 'No';
    ALTER TABLE eurlex_entries     ADD COLUMN IF NOT EXISTS tramitaciones       VARCHAR(3)  DEFAULT 'No';
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS tramitaciones       VARCHAR(3)  DEFAULT 'No';
    ALTER TABLE eurlex_entries     ADD COLUMN IF NOT EXISTS comprobado          VARCHAR(1)  DEFAULT 'N';
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Base de datos inicializada (esquema nuevo).")


def purge_excluded() -> int:
    """Elimina de boe_entries y eurlex_entries las entradas cuyos títulos
    coincidan con los patrones de exclusión definidos en los scrapers."""
    sql_boe = """
    DELETE FROM boe_entries
    WHERE texto ILIKE '%eficiencia del servicio p_blico%'
    """
    sql_eu = """
    DELETE FROM eurlex_entries
    WHERE texto ILIKE '%eficiencia del servicio p_blico%'
    """
    deleted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_boe)
            deleted += cur.rowcount
            cur.execute(sql_eu)
            deleted += cur.rowcount
        conn.commit()
    if deleted:
        logger.info("purge_excluded: eliminadas %d entradas excluidas.", deleted)
    return deleted


def upsert_boe(entries: List[Dict]) -> int:
    """Inserta entradas BOE; ignora duplicados. Devuelve nº de filas nuevas."""
    if not entries:
        return 0
    sql = """
    INSERT INTO boe_entries
        (external_id, fecha, fuente, seccion, tipo, organismo, subseccion,
         texto, enlace, palabras_clave, resumen, importante, acceso_conexion,
         tramitaciones, publicable, impacto_ree)
    VALUES
        (%(external_id)s, %(fecha)s, %(fuente)s, %(seccion)s, %(tipo)s,
         %(organismo)s, %(subseccion)s, %(texto)s, %(enlace)s, %(palabras_clave)s,
         %(resumen)s, %(importante)s, %(acceso_conexion)s,
         %(tramitaciones)s, %(publicable)s, %(impacto_ree)s)
    ON CONFLICT (external_id) DO NOTHING
    """
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute(sql, {**e, "impacto_ree": e.get("impacto_ree"), "tramitaciones": e.get("tramitaciones", "No")})
                inserted += cur.rowcount
        conn.commit()
    return inserted


def upsert_entries(entries: List[Dict]) -> int:
    """Inserta entradas CNMC/genéricas; actualiza plazo si cambia."""
    if not entries:
        return 0
    sql = """
    INSERT INTO regulatory_entries
        (source, external_id, title, published_date, url, section, department, summary, tipo, plazo, estado, sector, tramitaciones)
    VALUES
        (%(source)s, %(external_id)s, %(title)s, %(published_date)s,
         %(url)s, %(section)s, %(department)s, %(summary)s,
         %(tipo)s, %(plazo)s, %(estado)s, %(sector)s, %(tramitaciones)s)
    ON CONFLICT (external_id) DO UPDATE SET
        plazo  = EXCLUDED.plazo,
        estado = EXCLUDED.estado,
        title  = EXCLUDED.title
    """
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                row = {**e, "tipo": e.get("tipo", "regulacion"), "plazo": e.get("plazo"),
                       "estado": e.get("estado", "Abierta"), "sector": e.get("sector", "electricidad"),
                       "tramitaciones": e.get("tramitaciones", "No")}
                cur.execute(sql, row)
                inserted += cur.rowcount
        conn.commit()
    return inserted


def fetch_cnmc_consultas() -> List[Dict]:
    """Devuelve consultas públicas de CNMC y MITERD."""
    sql = """
    SELECT source, title, url, plazo, summary, impacto_ree, tramitaciones,
           TO_CHAR(published_date, 'DD/MM/YYYY') AS published_date,
           COALESCE(estado, 'Abierta') AS estado,
           COALESCE(sector, 'electricidad') AS sector,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid', 'DD/MM/YYYY HH24:MI') AS scraped_at,
           (scraped_at::date = CURRENT_DATE) AS es_nuevo
    FROM   regulatory_entries
    WHERE  source IN ('CNMC', 'MITERD')
      AND  tipo = 'consulta'
    ORDER  BY
           CASE WHEN COALESCE(estado,'Abierta') = 'Abierta' THEN 0 ELSE 1 END,
           scraped_at DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_boe_trimestre(days: int = 92) -> List[Dict]:
    """Devuelve entradas BOE del último trimestre con todos los campos."""
    sql = """
    SELECT
        TO_CHAR(fecha, 'DD/MM/YYYY') AS fecha,
        fuente, seccion, tipo, organismo, subseccion,
        texto, enlace, palabras_clave, resumen, impacto_ree,
        importante, acceso_conexion, tramitaciones, publicable,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid', 'DD/MM/YYYY HH24:MI') AS scraped_at
    FROM   boe_entries
    WHERE  fecha >= CURRENT_DATE - %(days)s
      AND  texto NOT ILIKE '%%eficiencia del servicio p_blico%%'
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
        tramitaciones,
        palabras_clave                                          AS summary,
        resumen,
        impacto_ree,
        CASE
          WHEN LOWER(organismo) LIKE '%%transici%%ecol%%' OR LOWER(organismo) LIKE '%%miterd%%'
               OR LOWER(organismo) LIKE '%%miteco%%' THEN 'MITERD'
          WHEN LOWER(organismo) LIKE '%%mercados%%competencia%%'
               OR LOWER(organismo) LIKE '%%cnmc%%' THEN 'CNMC'
          ELSE 'BOE'
        END                                                     AS filtro,
        TO_CHAR(fecha, 'YYYY-MM-DD')                            AS fecha_real,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
        (scraped_at::date = CURRENT_DATE)                      AS es_nuevo
    FROM boe_entries
    WHERE texto NOT ILIKE '%%eficiencia del servicio p_blico%%'

    UNION ALL

    SELECT
        source,
        title,
        TO_CHAR(published_date, 'DD/MM/YYYY')                  AS published_date,
        url,
        section,
        department,
        NULL::text                                              AS tipo,
        COALESCE(importante, 'No')                             AS importante,
        NULL::text                                              AS acceso_conexion,
        tramitaciones,
        summary,
        NULL::text                                              AS resumen,
        impacto_ree,
        source                                                  AS filtro,
        TO_CHAR(published_date, 'YYYY-MM-DD')                   AS fecha_real,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
        (scraped_at::date = CURRENT_DATE)                      AS es_nuevo
    FROM regulatory_entries
    WHERE (tipo = 'regulacion' OR tipo IS NULL)

    ORDER BY fecha_real DESC NULLS LAST, scraped_at DESC
    LIMIT %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_acceso_conexion() -> List[Dict]:
    """Devuelve todas las entradas BOE+consultas relacionadas con acceso y conexión."""
    sql = """
    SELECT
        'BOE'                                                   AS source,
        texto                                                   AS title,
        TO_CHAR(fecha, 'DD/MM/YYYY')                           AS published_date,
        EXTRACT(YEAR FROM fecha)::int                          AS anio,
        enlace                                                  AS url,
        seccion                                                 AS section,
        organismo                                               AS department,
        tipo, importante, acceso_conexion, tramitaciones, palabras_clave AS summary,
        resumen, impacto_ree,
        CASE
          WHEN LOWER(organismo) LIKE '%%transici%%ecol%%' OR LOWER(organismo) LIKE '%%miterd%%' THEN 'MITERD'
          WHEN LOWER(organismo) LIKE '%%mercados%%competencia%%' OR LOWER(organismo) LIKE '%%cnmc%%' THEN 'CNMC'
          ELSE 'BOE'
        END AS filtro
    FROM boe_entries
    WHERE acceso_conexion != 'No'
      AND texto NOT ILIKE '%eficiencia del servicio p_blico%'
    UNION ALL
    SELECT
        source, title,
        TO_CHAR(published_date, 'DD/MM/YYYY') AS published_date,
        EXTRACT(YEAR FROM COALESCE(published_date, scraped_at::date))::int AS anio,
        url, section, department,
        NULL::text AS tipo, NULL::text AS importante, 'Acceso/Conexion' AS acceso_conexion,
        tramitaciones,
        summary, NULL::text AS resumen, impacto_ree, source AS filtro
    FROM regulatory_entries
    WHERE tipo = 'consulta'
      AND (
        LOWER(title) LIKE '%%acceso%%' OR LOWER(title) LIKE '%%conexi%%'
        OR LOWER(title) LIKE '%%peaje%%' OR LOWER(summary) LIKE '%%acceso%%'
        OR LOWER(title) LIKE '%%nudo%%transici%%' OR LOWER(title) LIKE '%%manifestaci%%inter%%nudo%%'
        OR LOWER(title) LIKE '%%precios el%%ctric%%' OR LOWER(title) LIKE '%%trf%%'
        OR LOWER(title) LIKE '%%retribuci%%transporte%%' OR LOWER(title) LIKE '%%circular%%precio%%'
        OR LOWER(summary) LIKE '%%acceso a la red%%' OR LOWER(summary) LIKE '%%conexi%%red%%'
      )
    ORDER BY published_date DESC NULLS LAST, anio DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_eurlex(limit: int = 500) -> List[Dict]:
    """Devuelve normativa europea energética del DOUE."""
    sql = """
    SELECT
        external_id, TO_CHAR(fecha,'DD/MM/YYYY') AS fecha,
        EXTRACT(YEAR FROM fecha)::int             AS anio,
        fuente, seccion, tipo, organismo, texto, enlace,
        palabras_clave, resumen, importante, acceso_conexion, tramitaciones,
        TO_CHAR(fecha,'YYYY-MM-DD')               AS fecha_real
    FROM   eurlex_entries
    ORDER  BY fecha DESC NULLS LAST
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def upsert_eurlex(entries: List[Dict]) -> int:
    if not entries:
        return 0
    sql = """
    INSERT INTO eurlex_entries
        (external_id,fecha,fuente,seccion,tipo,organismo,subseccion,
         texto,enlace,palabras_clave,resumen,importante,acceso_conexion,tramitaciones,publicable)
    VALUES
        (%(external_id)s,%(fecha)s,%(fuente)s,%(seccion)s,%(tipo)s,%(organismo)s,%(subseccion)s,
         %(texto)s,%(enlace)s,%(palabras_clave)s,%(resumen)s,%(importante)s,%(acceso_conexion)s,
         %(tramitaciones)s,%(publicable)s)
    ON CONFLICT (external_id) DO NOTHING
    """
    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute(sql, {**e, "tramitaciones": e.get("tramitaciones", "No")})
                inserted += cur.rowcount
        conn.commit()
    return inserted


def fetch_reg_espanola_q1() -> Dict:
    """Devuelve BOE y CNMC del primer trimestre del año en curso."""
    from datetime import date
    year   = date.today().year
    q1_ini = date(year, 1, 1).isoformat()
    q1_fin = date(year, 3, 31).isoformat()

    boe_sql = """
    SELECT
        TO_CHAR(fecha, 'DD/MM/YYYY') AS fecha,
        fuente, seccion, tipo, organismo, subseccion,
        texto, enlace, palabras_clave, resumen, impacto_ree, importante, acceso_conexion, tramitaciones, publicable
    FROM   boe_entries
    WHERE  fecha BETWEEN %(ini)s AND %(fin)s
      AND  texto NOT ILIKE '%%eficiencia del servicio p_blico%%'
    ORDER  BY fecha DESC
    """
    # CNMC: la web de CNMC no expone fechas por consulta, así que
    # mostramos todas las entradas disponibles (consultas activas/recientes).
    # Las fechadas se ordenan primero; las sin fecha (web scraping) al final.
    cnmc_sql = """
    SELECT
        source,
        title,
        TO_CHAR(published_date, 'DD/MM/YYYY') AS published_date,
        url,
        section,
        department,
        summary, impacto_ree,
        (LOWER(title) LIKE '%%circular%%') AS es_circular
    FROM   regulatory_entries
    WHERE  source = 'CNMC'
      AND  (tipo = 'regulacion' OR tipo IS NULL)
    ORDER  BY published_date DESC NULLS LAST, scraped_at DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(boe_sql, {"ini": q1_ini, "fin": q1_fin})
            boe_rows = [dict(r) for r in cur.fetchall()]
            cur.execute(cnmc_sql)
            cnmc_rows = [dict(r) for r in cur.fetchall()]

    return {
        "year":  year,
        "rango": f"01/01/{year} – 31/03/{year}",
        "boe":   boe_rows,
        "cnmc":  cnmc_rows,
    }


YEAR_FILTER = 2026   # Solo mostrar datos de este año hasta nuevo aviso


def _filter_year(items: list, year: int) -> list:
    """Filtra una lista de entradas por año (campo fecha_real o published_date o fecha)."""
    def _matches(e):
        for field in ("fecha_real", "published_date", "fecha"):
            v = e.get(field, "") or ""
            if str(year) in str(v):
                return True
        return False
    return [e for e in items if _matches(e)]


def backfill_sentencias() -> int:
    """Enriquece con el expediente los títulos de sentencias CNMC ya en BD que aún no lo tienen."""
    from scraper.cnmc import _fetch_sentencia_expediente
    sql_select = """
    SELECT id, title, url FROM regulatory_entries
    WHERE LOWER(title) LIKE 'sentencia%%' AND title NOT LIKE '%%|%%'
    """
    sql_update = "UPDATE regulatory_entries SET title = %s WHERE id = %s"
    updated = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_select)
            rows = cur.fetchall()
        for row in rows:
            exp = _fetch_sentencia_expediente(row["url"])
            if exp:
                new_title = f"{row['title']} | {exp}"
                with conn.cursor() as cur:
                    cur.execute(sql_update, (new_title, row["id"]))
                updated += 1
                logger.info("backfill_sentencias: %s → %s", row["title"][:60], new_title[:80])
        conn.commit()
    logger.info("backfill_sentencias: %d entradas actualizadas.", updated)
    return updated


def fetch_acer() -> List[Dict]:
    """Devuelve todas las entradas ACER (noticias + decisiones)."""
    sql = """
    SELECT source, title, url, section, department, summary, impacto_ree, tramitaciones,
           COALESCE(importante, 'No')                                        AS importante,
           TO_CHAR(published_date, 'DD/MM/YYYY')                            AS published_date,
           TO_CHAR(published_date, 'YYYY-MM-DD')                            AS fecha_real,
           EXTRACT(YEAR FROM published_date)::int                           AS anio,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
           (scraped_at::date = CURRENT_DATE)                                AS es_nuevo
    FROM   regulatory_entries
    WHERE  source = 'ACER'
    ORDER  BY published_date DESC NULLS LAST, scraped_at DESC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def export_to_json(path: str = "web/data.json", limit: int = 300):
    """Exporta datos al JSON que consume la web estática.
    Solo incluye datos del YEAR_FILTER (datos históricos se conservan en BD).
    """
    entries        = fetch_recent(limit)
    boe_trimestre  = fetch_boe_trimestre(92)
    reg_espanola   = fetch_reg_espanola_q1()
    consultas      = fetch_cnmc_consultas()
    acceso_con     = fetch_acceso_conexion()
    eurlex         = fetch_eurlex(500)
    acer           = fetch_acer()

    # Aplicar filtro de año (solo visualización — datos históricos intactos en BD)
    entries_f   = _filter_year(entries,    YEAR_FILTER)
    acceso_f    = _filter_year(acceso_con, YEAR_FILTER)
    eurlex_f    = _filter_year(eurlex,     YEAR_FILTER)

    acer_f = _filter_year(acer, YEAR_FILTER)

    payload = {
        "updated_at":    (datetime.utcnow() + timedelta(hours=2)).strftime("%d/%m/%Y %H:%M"),
        "total":         len(entries_f),
        "entries":       entries_f,      # pestaña Todas (solo 2026)
        "boe_trimestre": boe_trimestre,  # pestaña BOE Último Trimestre (ya es reciente)
        "reg_espanola":  reg_espanola,   # pestaña Regulación Española (Q1 2026)
        "cnmc_consultas": consultas,     # pestaña Consultas (siempre actuales)
        "acceso_conexion": acceso_f,     # pestaña Acceso/Conexión (solo 2026)
        "eurlex":          eurlex_f,     # pestaña Normativa Europea (solo 2026)
        "acer":            acer_f,       # pestaña ACER (solo 2026)
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Exportadas %d totales / %d BOE trim. / %d+%d Q1 → %s",
                len(entries), len(boe_trimestre),
                len(reg_espanola["boe"]), len(reg_espanola["cnmc"]), path)
