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
    from db.database import init_db, upsert_boe, upsert_entries, export_to_json
    from scraper import boe, cnmc

    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL no configurado. Copia .env.example a .env.")
        sys.exit(1)

    logger.info("=== Iniciando base de datos ===")
    init_db()

    # ── EUR-Lex (DOUE) ──────────────────────────────────────────────────
    logger.info("=== Scraping EUR-Lex (DOUE) ===")
    from scraper.eurlex import scrape as scrape_eurlex
    from db.database import upsert_eurlex
    # days_back=7 para cubrir el lag de indexación del SPARQL de la UE
    eu_entries = scrape_eurlex(days_back=7)
    eu_new     = upsert_eurlex(eu_entries)
    logger.info("EUR-Lex: %d actos, %d nuevos", len(eu_entries), eu_new)

    # ── BOE Sección V Anuncios (HTML) ──────────────────────────────────
    logger.info("=== Scraping BOE Anuncios Sección V ===")
    from scraper.boe_anuncios import scrape as scrape_anuncios
    anuncios = scrape_anuncios(days_back=1)
    anuncios_new = upsert_boe(anuncios)
    logger.info("BOE Anuncios: %d encontrados, %d nuevos", len(anuncios), anuncios_new)

    # ── BOE: solo el día actual ─────────────────────────────────────────
    logger.info("=== Scraping BOE (hoy) ===")
    boe_entries = boe.scrape(days_back=1)
    boe_new     = upsert_boe(boe_entries)
    logger.info("BOE: %d entradas encontradas, %d nuevas en BD", len(boe_entries), boe_new)

    # ── CSV a stdout ────────────────────────────────────────────────────
    if boe_entries:
        print("\n" + boe.to_csv(boe_entries) + "\n")
    else:
        logger.info("(Sin entradas BOE hoy — puede ser fin de semana o festivo)")

    # ── CNMC consultas ───────────────────────────────────────────────────
    logger.info("=== Scraping CNMC consultas ===")
    cnmc_entries = cnmc.scrape(max_pages=5)
    cnmc_new     = upsert_entries(cnmc_entries)
    logger.info("CNMC: %d entradas, %d nuevas en BD", len(cnmc_entries), cnmc_new)

    # ── MITERD consultas ─────────────────────────────────────────────────
    logger.info("=== Scraping MITERD consultas ===")
    from scraper import miterd
    mit_entries = miterd.scrape()
    mit_new     = upsert_entries(mit_entries)
    logger.info("MITERD: %d entradas, %d nuevas en BD", len(mit_entries), mit_new)

    # ── CNMC RSS ─────────────────────────────────────────────────────────
    logger.info("=== Scraping CNMC RSS ===")
    from scraper import cnmc_rss
    rss_entries = cnmc_rss.scrape()
    rss_new     = upsert_entries(rss_entries)
    logger.info("CNMC RSS: %d entradas, %d nuevas en BD", len(rss_entries), rss_new)

    # ── Exportar JSON para la web ───────────────────────────────────────
    logger.info("=== Exportando web/data.json ===")
    export_to_json("web/data.json", limit=3000)

    logger.info("=== Listo. BOE=%d  CNMC=%d ===", boe_new, cnmc_new)


if __name__ == "__main__":
    main()
