"""Entry point: run scrapers, save to PostgreSQL, export JSON for the web."""
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
    # Lazy import so dotenv is loaded first
    from db.database import init_db, upsert_entries, export_to_json
    from scraper import boe, cnmc

    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL not set. Copy .env.example to .env and configure it.")
        sys.exit(1)

    logger.info("=== Initialising database ===")
    init_db()

    logger.info("=== Scraping BOE ===")
    boe_entries = boe.scrape(days_back=2)
    boe_new = upsert_entries(boe_entries)
    logger.info("BOE: %d entries scraped, %d new in DB", len(boe_entries), boe_new)

    logger.info("=== Scraping CNMC ===")
    cnmc_entries = cnmc.scrape(max_pages=5)
    cnmc_new = upsert_entries(cnmc_entries)
    logger.info("CNMC: %d entries scraped, %d new in DB", len(cnmc_entries), cnmc_new)

    logger.info("=== Exporting to web/data.json ===")
    export_to_json("web/data.json", limit=300)

    logger.info("=== Done. BOE new=%d  CNMC new=%d ===", boe_new, cnmc_new)


if __name__ == "__main__":
    main()
