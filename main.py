"""Scraper regulatorio: BOE (hoy) + CNMC → PostgreSQL → web/data.json + CSV stdout."""
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def main():
    from db.database import init_db, purge_excluded, backfill_sentencias, upsert_boe, upsert_entries, export_to_json
    from scraper import boe, cnmc

    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL no configurado. Copia .env.example a .env.")
        sys.exit(1)

    logger.info("=== Iniciando base de datos ===")
    init_db()
    purge_excluded()
    backfill_sentencias()

    # ── EUR-Lex (DOUE) ──────────────────────────────────────────────────
    logger.info("=== Scraping EUR-Lex (DOUE) ===")
    from scraper.eurlex import scrape as scrape_eurlex
    from db.database import upsert_eurlex
    # days_back=7 para cubrir el lag de indexación del SPARQL de la UE
    eu_entries = scrape_eurlex(days_back=7)
    eu_new     = upsert_eurlex(eu_entries)
    logger.info("EUR-Lex: %d actos, %d nuevos", len(eu_entries), eu_new)

    # El lunes ampliar a 3 días para capturar también el sábado anterior
    from datetime import date as _date
    _boe_days = 3 if _date.today().weekday() == 0 else 1

    # ── BOE Sección V Anuncios (HTML) ──────────────────────────────────
    logger.info("=== Scraping BOE Anuncios Sección V ===")
    from scraper.boe_anuncios import scrape as scrape_anuncios
    anuncios = scrape_anuncios(days_back=_boe_days)
    anuncios_new = upsert_boe(anuncios)
    logger.info("BOE Anuncios: %d encontrados, %d nuevos", len(anuncios), anuncios_new)

    # ── BOE: hoy (+ sábado si es lunes) ────────────────────────────────
    logger.info("=== Scraping BOE (days_back=%d) ===", _boe_days)
    boe_entries = boe.scrape(days_back=_boe_days)
    boe_new     = upsert_boe(boe_entries)
    logger.info("BOE: %d entradas encontradas, %d nuevas en BD", len(boe_entries), boe_new)

    # ── CSV a stdout ────────────────────────────────────────────────────
    if boe_entries:
        print("\n" + boe.to_csv(boe_entries) + "\n")
    else:
        logger.info("(Sin entradas BOE hoy — puede ser fin de semana o festivo)")

    # ── CNMC consultas (hasta 3 intentos, intercalados con otros scrapers) ─
    from scraper import miterd, cnmc_rss, cnmc_n as cnmc_n_mod

    def _scrape_cnmc() -> int:
        """Intenta scraping CNMC. Devuelve nº nuevas o -1 si falla."""
        try:
            entries = cnmc.scrape(max_pages=5)
            if not entries:
                return 0
            new = upsert_entries(entries)
            logger.info("CNMC: %d entradas, %d nuevas en BD", len(entries), new)
            return new
        except Exception as exc:
            logger.warning("CNMC fallo: %s", exc)
            return -1

    cnmc_new   = -1
    cnmc_intentos = 0
    MAX_INTENTOS  = 3

    # Intento 1 — antes del resto
    logger.info("=== Scraping CNMC consultas (intento 1/%d) ===", MAX_INTENTOS)
    cnmc_new = _scrape_cnmc()
    cnmc_intentos = 1

    # ── MITERD consultas ─────────────────────────────────────────────────
    logger.info("=== Scraping MITERD consultas ===")
    mit_entries = miterd.scrape()
    mit_new     = upsert_entries(mit_entries)
    logger.info("MITERD: %d entradas, %d nuevas en BD", len(mit_entries), mit_new)

    # Intento 2 — después de MITERD si falló antes
    if cnmc_new < 0 and cnmc_intentos < MAX_INTENTOS:
        cnmc_intentos += 1
        logger.info("=== Scraping CNMC consultas (intento %d/%d) ===", cnmc_intentos, MAX_INTENTOS)
        cnmc_new = _scrape_cnmc()

    # ── CNMC RSS ─────────────────────────────────────────────────────────
    logger.info("=== Scraping CNMC RSS ===")
    rss_entries = cnmc_rss.scrape()
    rss_new     = upsert_entries(rss_entries)
    logger.info("CNMC RSS: %d entradas, %d nuevas en BD", len(rss_entries), rss_new)

    # ── CNMC_N Actuaciones energía ────────────────────────────────────────
    # La CNMC publica actuaciones con la fecha del acto, no la de publicación web.
    # El filtro por fecha devuelve 0 resultados porque los actos recientes tardan
    # semanas en aparecer. Se usan los 50 más recientes (sin filtro fecha) y el
    # upsert por external_id evita duplicados.
    logger.info("=== Scraping CNMC_N Actuaciones (sin filtro fecha) ===")
    cnmc_n_entries = cnmc_n_mod.scrape(days_back=7)
    cnmc_n_new     = upsert_entries(cnmc_n_entries)
    logger.info("CNMC_N: %d entradas, %d nuevas en BD", len(cnmc_n_entries), cnmc_n_new)

    # ── CNMC_S Actuaciones energía (50 más recientes, idambito=9) ────────
    logger.info("=== Scraping CNMC_S Actuaciones (recientes) ===")
    cnmc_s_entries = cnmc_n_mod.scrape_cnmc_s(max_pages=2)
    cnmc_s_new     = upsert_entries(cnmc_s_entries)
    logger.info("CNMC_S: %d entradas, %d nuevas en BD", len(cnmc_s_entries), cnmc_s_new)

    # Intento 3 — después de CNMC_N si aún falló
    if cnmc_new < 0 and cnmc_intentos < MAX_INTENTOS:
        cnmc_intentos += 1
        logger.info("=== Scraping CNMC consultas (intento %d/%d) ===", cnmc_intentos, MAX_INTENTOS)
        cnmc_new = _scrape_cnmc()

    if cnmc_new < 0:
        logger.error("CNMC: falló en los %d intentos — continuando sin datos CNMC", MAX_INTENTOS)
        cnmc_new = 0

    # ── BOE-N Suplemento: Registros de la Propiedad con REE ─────────────
    logger.info("=== Scraping BOE-N Suplemento (Registros de la Propiedad) ===")
    from scraper.boe_n import scrape as scrape_boe_n, filter_ree
    from db.database import upsert_boe_n_staging, promote_boe_n
    boe_n_raw = scrape_boe_n(days_back=_boe_days)
    staged = upsert_boe_n_staging(boe_n_raw)
    logger.info("BOE-N: %d anuncios nuevos en staging", staged)
    if boe_n_raw:
        logger.info("BOE-N: leyendo PDFs en paralelo (busca Red Eléctrica/REE/Redeia)…")
        ree_result = filter_ree(boe_n_raw, max_workers=8)
        ree_ids  = [eid for eid, found in ree_result.items() if found]
        all_ids  = list(ree_result.keys())
        promoted = promote_boe_n(all_ids, ree_ids)
        logger.info("BOE-N: %d con REE → boe_entries | %d sin REE → boe_n_descarte",
                    promoted, len(all_ids) - promoted)

    # ── ACER (dos ejecuciones diarias, cubre hoy y ayer) ────────────────
    logger.info("=== Scraping ACER ===")
    from scraper import acer as acer_scraper
    acer_entries = acer_scraper.scrape(days_back=2)
    acer_new     = upsert_entries(acer_entries)
    logger.info("ACER: %d entradas, %d nuevas en BD", len(acer_entries), acer_new)

    # ── Exportar JSON para la web ───────────────────────────────────────
    logger.info("=== Exportando web/data.json ===")
    export_to_json("web/data.json", limit=3000)

    logger.info("=== Listo. BOE=%d  CNMC=%d ===", boe_new, cnmc_new)


if __name__ == "__main__":
    main()
