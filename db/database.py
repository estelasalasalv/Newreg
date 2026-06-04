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
    ALTER TABLE eurlex_entries     ADD COLUMN IF NOT EXISTS impacto_ree        TEXT;
    ALTER TABLE regulatory_entries ADD COLUMN IF NOT EXISTS tramitaciones       VARCHAR(3)  DEFAULT 'No';
    ALTER TABLE eurlex_entries     ADD COLUMN IF NOT EXISTS comprobado          VARCHAR(1)  DEFAULT 'N';

    -- Tabla de rechazos: normativa descartada (AENA, etc.) — conservada para auditoría
    CREATE TABLE IF NOT EXISTS boe_rechazos (
        id           SERIAL PRIMARY KEY,
        external_id  VARCHAR(200) UNIQUE NOT NULL,
        fecha        DATE,
        fuente       VARCHAR(10),
        seccion      TEXT,
        tipo         TEXT,
        organismo    TEXT,
        texto        TEXT,
        enlace       TEXT,
        motivo       TEXT,
        moved_at     TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_rechazos_motivo ON boe_rechazos(motivo);

    -- Tabla BOE-N: anuncios de Registros de la Propiedad con Red Eléctrica confirmada
    CREATE TABLE IF NOT EXISTS boe_n_entries (
        id              SERIAL PRIMARY KEY,
        external_id     VARCHAR(200) UNIQUE NOT NULL,
        fecha           DATE,
        organismo       TEXT,
        texto           TEXT NOT NULL,
        enlace          TEXT,
        scraped_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_boe_n_fecha ON boe_n_entries(fecha DESC);

    -- Tabla BOE-N descarte: sin Red Eléctrica — se puede borrar cuando convenga
    CREATE TABLE IF NOT EXISTS boe_n_descarte (
        id              SERIAL PRIMARY KEY,
        external_id     VARCHAR(200) UNIQUE NOT NULL,
        fecha           DATE,
        organismo       TEXT,
        texto           TEXT NOT NULL,
        enlace          TEXT,
        scraped_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_boe_n_desc_fecha ON boe_n_descarte(fecha DESC);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    logger.info("Base de datos inicializada (esquema nuevo).")


def upsert_boe_n_staging(entries: List[Dict]) -> int:
    """Inserta en boe_n_staging (tabla temporal) todos los anuncios del suplemento BOE-N."""
    if not entries:
        return 0
    sql = """
    INSERT INTO boe_n_entries (external_id, fecha, organismo, texto, enlace)
    VALUES (%(external_id)s, %(fecha)s, %(organismo)s, %(texto)s, %(enlace)s)
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


def promote_boe_n(all_ids: List[str], ree_ids: List[str]) -> int:
    """Clasifica anuncios BOE-N procesados:
    - ree_ids  → boe_entries (tabla principal, tramitación confirmada REE)
    - resto    → boe_n_descarte (tabla auxiliar, se puede vaciar sin riesgo)
    - Borra todos de boe_n_entries (staging)
    Devuelve el número de entradas promovidas a boe_entries.
    """
    if not all_ids:
        return 0
    ree_set = set(ree_ids)
    promoted = 0
    BATCH = 200  # commit cada 200 filas para evitar timeout de conexión

    # Obtener los datos en una sola lectura rápida
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT external_id, fecha, organismo, texto, enlace "
                "FROM boe_n_entries WHERE external_id = ANY(%s)", (all_ids,)
            )
            rows = [dict(r) for r in cur.fetchall()]

    # Procesar en lotes con conexión fresca cada vez
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in batch:
                    ext_id    = row['external_id']
                    fecha     = row['fecha']
                    organismo = row['organismo']
                    texto     = row['texto']
                    enlace    = row['enlace']
                    if ext_id in ree_set:
                        cur.execute("""
                            INSERT INTO boe_entries
                              (external_id, fecha, fuente, seccion, tipo, organismo, subseccion,
                               texto, enlace, palabras_clave, importante, acceso_conexion,
                               tramitaciones, publicable)
                            VALUES (%s,%s,'BOE-N','Suplemento Notificaciones',
                                    'Anuncio Registro Propiedad',%s,'',%s,%s,
                                    'registro de la propiedad, Red Eléctrica, tramitación',
                                    'No','Sí','Sí','NO')
                            ON CONFLICT (external_id) DO NOTHING
                        """, (ext_id, fecha, organismo, texto, enlace))
                        promoted += cur.rowcount
                    else:
                        cur.execute("""
                            INSERT INTO boe_n_descarte
                              (external_id, fecha, organismo, texto, enlace)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT (external_id) DO NOTHING
                        """, (ext_id, fecha, organismo, texto, enlace))
                # Limpiar staging del lote
                batch_ids = [r['external_id'] for r in batch]
                cur.execute("DELETE FROM boe_n_entries WHERE external_id = ANY(%s)", (batch_ids,))
            conn.commit()
    return promoted


def purge_excluded() -> int:
    """Elimina entradas duplicadas o con external_id incorrecto conocido.
    No borra por contenido; solo por external_id exacto y source."""
    _BLACKLIST = [
        # ACER: duplicados de 'lower-congestion-levels-2024-and-2025...' (ext_id variable)
        ("ACER", "acer-rss-evels2024and2025pointnewequilibriumeugasmarket"),
    ]
    # También eliminar por URL duplicada dentro de ACER (mantiene la entrada más antigua)
    sql_url_dup = """
    DELETE FROM regulatory_entries
    WHERE source = 'ACER' AND id NOT IN (
        SELECT MIN(id) FROM regulatory_entries
        WHERE source = 'ACER'
        GROUP BY url
    )
    """
    deleted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for source, ext_id in _BLACKLIST:
                cur.execute(
                    "DELETE FROM regulatory_entries WHERE source=%s AND external_id=%s",
                    (source, ext_id),
                )
                deleted += cur.rowcount
            # Eliminar duplicados ACER por URL (conserva la entrada más antigua)
            cur.execute(sql_url_dup)
            deleted += cur.rowcount
        conn.commit()
    if deleted:
        logger.info("purge_excluded: %d entradas duplicadas eliminadas.", deleted)
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


def backfill_pub_dates_from_rss() -> int:
    """Cruza el RSS de la CNMC con CNMC_S y CNMC_N para rellenar published_date con
    la fecha de publicación web real (pubDate del RSS).

    El RSS tiene max 10 items. Para cada uno navega a la página individual del node
    para extraer el nº de expediente, y actualiza published_date en regulatory_entries
    donde el title empieza por ese expediente.

    Para los registros CNMC_S/CNMC_N sin published_date, usa scraped_at como fallback.
    Devuelve el número de filas actualizadas.
    """
    import requests
    import xml.etree.ElementTree as ET
    import re as _re
    from email.utils import parsedate_to_datetime
    from bs4 import BeautifulSoup

    HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}
    updated = 0

    # 1. Descargar RSS y obtener {node_id: pub_date_iso}
    rss_dates: dict = {}
    try:
        r = requests.get("https://www.cnmc.es/rss.xml", headers=HEADERS, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for item in root.findall(".//item"):
            link    = (item.findtext("link") or "").strip()
            pub_raw = (item.findtext("pubDate") or "").strip()
            m = _re.search(r"/node/(\d+)", link)
            if not m or not pub_raw:
                continue
            try:
                pub_iso = parsedate_to_datetime(pub_raw).strftime("%Y-%m-%d")
                rss_dates[m.group(1)] = (pub_iso, link)
            except Exception:
                pass
    except Exception as exc:
        logger.warning("backfill_pub_dates_from_rss: error RSS: %s", exc)
        rss_dates = {}

    # 2. Para cada node del RSS, obtener el nº expediente de su página
    with get_connection() as conn:
        with conn.cursor() as cur:
            for node_id, (pub_iso, node_url) in rss_dates.items():
                try:
                    rp = requests.get(node_url, headers=HEADERS, timeout=15)
                    rp.raise_for_status()
                    soup = BeautifulSoup(rp.text, "lxml")
                    text = soup.get_text(" ", strip=True)
                    # Buscar nº expediente: "Nº Expediente XXX/YY/ZZZ/NN"
                    m = _re.search(r"N[ºo°]\s*Expediente\s+(\S+)", text, _re.IGNORECASE)
                    if not m:
                        continue
                    num_exp = m.group(1).strip().upper()
                    # Actualizar published_date en CNMC_S y CNMC_N donde el title empieza por ese expediente
                    cur.execute(
                        """UPDATE regulatory_entries
                           SET published_date = %(pub)s
                           WHERE source IN ('CNMC_S', 'CNMC_N')
                             AND title ILIKE %(pat)s
                             AND (published_date IS NULL OR published_date != %(pub)s)
                        """,
                        {"pub": pub_iso, "pat": f"{num_exp}%"},
                    )
                    if cur.rowcount:
                        logger.info("backfill_pub_dates: %s → %s (%d filas)", num_exp, pub_iso, cur.rowcount)
                        updated += cur.rowcount
                except Exception as exc:
                    logger.debug("backfill_pub_dates node %s error: %s", node_id, exc)

            # 3. Fallback: CNMC_S/CNMC_N sin published_date → usar scraped_at
            cur.execute(
                """UPDATE regulatory_entries
                   SET published_date = scraped_at::date
                   WHERE source IN ('CNMC_S', 'CNMC_N')
                     AND published_date IS NULL
                     AND scraped_at IS NOT NULL
                """
            )
            if cur.rowcount:
                logger.info("backfill_pub_dates fallback scraped_at: %d filas", cur.rowcount)
                updated += cur.rowcount

        conn.commit()

    logger.info("backfill_pub_dates_from_rss: %d registros actualizados", updated)
    return updated


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
        title  = CASE
                   WHEN EXCLUDED.title LIKE '<%%' THEN regulatory_entries.title
                   ELSE EXCLUDED.title
                 END
    """
    # Pre-cargar URLs ya existentes de la misma fuente para evitar duplicados por URL
    sources_in_batch = list({e.get("source") for e in entries if e.get("source")})
    # Si el batch incluye CNMC_N, también cargar URLs de CNMC_S para evitar duplicados cruzados
    sources_check = list(set(sources_in_batch) | ({'CNMC_S'} if 'CNMC_N' in sources_in_batch else set()))
    existing_urls: set = set()
    if sources_check:
        import psycopg2 as _pg2
        with _pg2.connect(os.environ["DATABASE_URL"]) as _conn:
            with _conn.cursor() as _cur:
                _cur.execute(
                    "SELECT url FROM regulatory_entries WHERE source = ANY(%s) AND url IS NOT NULL",
                    (sources_check,),
                )
                existing_urls = {r[0] for r in _cur.fetchall()}

    inserted = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                # Omitir si la URL ya existe (previene duplicados con external_id distinto)
                if e.get("url") and e["url"] in existing_urls:
                    continue
                row = {**e, "tipo": e.get("tipo", "regulacion"), "plazo": e.get("plazo"),
                       "estado": e.get("estado", "Abierta"), "sector": e.get("sector", "electricidad"),
                       "tramitaciones": e.get("tramitaciones", "No")}
                cur.execute(sql, row)
                if cur.rowcount:
                    inserted += cur.rowcount
                    existing_urls.add(e.get("url"))  # actualizar el set en memoria
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
           (scraped_at::date >= CURRENT_DATE - 7) AS es_nuevo
    FROM   regulatory_entries
    WHERE  source IN ('CNMC_C', 'MITERD')
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
               OR LOWER(organismo) LIKE '%%cnmc%%' THEN 'CNMC_C'
          ELSE 'BOE'
        END                                                     AS filtro,
        TO_CHAR(fecha, 'YYYY-MM-DD')                            AS fecha_real,
        TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
        -- El lunes (DOW=1) ampliar ventana al sábado anterior (2 días atrás)
        (fecha::date >= CURRENT_DATE - CASE WHEN EXTRACT(DOW FROM CURRENT_DATE) = 1 THEN 2 ELSE 0 END) AS es_nuevo
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
        (scraped_at::date >= CURRENT_DATE - CASE WHEN EXTRACT(DOW FROM CURRENT_DATE) = 1 THEN 2 ELSE 0 END) AS es_nuevo
    FROM regulatory_entries
    WHERE (tipo = 'regulacion' OR tipo IS NULL)
      AND source != 'ACER'   -- ACER tiene su propia pestaña y fetch_acer()

    ORDER BY fecha_real DESC NULLS LAST, scraped_at DESC
    LIMIT %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_cnmc_rss_entries(limit: int = 300) -> List[Dict]:
    """Devuelve entradas del RSS de CNMC (resoluciones, acuerdos, sentencias)."""
    sql = """
    SELECT source, title, url, section, department, summary, impacto_ree, tramitaciones,
           COALESCE(importante, 'No')                                        AS importante,
           TO_CHAR(published_date, 'DD/MM/YYYY')                            AS published_date,
           TO_CHAR(published_date, 'YYYY-MM-DD')                            AS fecha_real,
           EXTRACT(YEAR FROM published_date)::int                           AS anio,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
           (scraped_at::date >= CURRENT_DATE - 7)                           AS es_nuevo
    FROM   regulatory_entries
    WHERE  source = 'CNMC_RSS'
    ORDER  BY published_date DESC NULLS LAST, scraped_at DESC
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_cnmc_s(limit: int = 500) -> List[Dict]:
    """Devuelve actuaciones energéticas CNMC (CNMC_S — transparencia/actuaciones idambito=9)."""
    sql = """
    SELECT source, title, url, section, department, summary, impacto_ree, tramitaciones,
           COALESCE(importante, 'No')                                        AS importante,
           TO_CHAR(published_date, 'DD/MM/YYYY')                            AS published_date,
           TO_CHAR(published_date, 'YYYY-MM-DD')                            AS fecha_real,
           EXTRACT(YEAR FROM published_date)::int                           AS anio,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
           (scraped_at::date >= CURRENT_DATE - 7)                           AS es_nuevo
    FROM   regulatory_entries
    WHERE  source = 'CNMC_S'
    ORDER  BY scraped_at DESC, published_date DESC NULLS LAST
    LIMIT  %(limit)s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_cnmc_n(limit: int = 200) -> List[Dict]:
    """Devuelve actuaciones CNMC_N energéticas."""
    sql = """
    SELECT source, title, url, section, department, summary, impacto_ree, tramitaciones,
           COALESCE(importante, 'No')                                        AS importante,
           TO_CHAR(published_date, 'DD/MM/YYYY')                            AS published_date,
           TO_CHAR(published_date, 'YYYY-MM-DD')                            AS fecha_real,
           EXTRACT(YEAR FROM published_date)::int                           AS anio,
           TO_CHAR(scraped_at AT TIME ZONE 'Europe/Madrid','DD/MM/YYYY HH24:MI') AS scraped_at,
           (scraped_at::date >= CURRENT_DATE - 7)                           AS es_nuevo
    FROM   regulatory_entries
    WHERE  source = 'CNMC_N'
    ORDER  BY scraped_at DESC, published_date DESC NULLS LAST
    LIMIT  %(limit)s
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
          WHEN LOWER(organismo) LIKE '%%mercados%%competencia%%' OR LOWER(organismo) LIKE '%%cnmc%%' THEN 'CNMC_C'
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
        palabras_clave, resumen, impacto_ree, importante, acceso_conexion, tramitaciones,
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
    WHERE  source = 'CNMC_C'
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
           (scraped_at::date >= CURRENT_DATE - CASE WHEN EXTRACT(DOW FROM CURRENT_DATE) = 1 THEN 2 ELSE 0 END) AS es_nuevo
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
    cnmc_n         = fetch_cnmc_n()
    cnmc_s         = fetch_cnmc_s()
    cnmc_rss_data  = fetch_cnmc_rss_entries()

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
        "cnmc_n":          _filter_year(cnmc_n, YEAR_FILTER),  # pestaña CNMC_N actuaciones
        "cnmc_s":          cnmc_s,                              # pestaña CNMC_S (sin filtro año)
        "cnmc_rss":        _filter_year(cnmc_rss_data, YEAR_FILTER),  # pestaña CNMC RSS
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Exportadas %d totales / %d BOE trim. / %d+%d Q1 → %s",
                len(entries), len(boe_trimestre),
                len(reg_espanola["boe"]), len(reg_espanola["cnmc"]), path)
