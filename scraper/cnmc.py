"""CNMC scraper — consultas públicas de energía.

Target: https://www.cnmc.es/consultas-publicas/energia
Excludes: audiovisual, telecomunicaciones, postal, ferroviario
"""
import logging
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

CNMC_URL = "https://www.cnmc.es/consultas-publicas/energia"
CNMC_BASE = "https://www.cnmc.es"

EXCLUDED_KEYWORDS = [
    "audiovisual", "telecomunicaciones", "postal",
    "ferroviario", "ferrocarril", "telecomunicacion",
]


def _is_excluded(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in EXCLUDED_KEYWORDS)


def _parse_date(raw: str) -> Optional[str]:
    """Try several date formats and return ISO YYYY-MM-DD or None."""
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y", "%d de %B de %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try to extract with regex  dd/mm/yyyy
    m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", raw)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _fetch_page(url: str, page: int = 0) -> Optional[BeautifulSoup]:
    params = {"page": page} if page > 0 else {}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; RegulatoryBot/1.0; "
            "+https://github.com/regulatory-bot)"
        )
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as exc:
        logger.error("CNMC request failed (page=%d): %s", page, exc)
        return None


def _extract_entries(soup: BeautifulSoup) -> List[Dict]:
    entries = []

    # CNMC uses a view with rows inside .view-content or article cards
    # Try multiple selectors for robustness
    rows = (
        soup.select("div.view-content .views-row")
        or soup.select("article.node--type-consulta-publica")
        or soup.select("div.views-row")
        or soup.select("li.views-row")
    )

    if not rows:
        # Fallback: any <article> or <li> that contains a heading link
        rows = soup.select("article") or soup.select("li")

    for row in rows:
        # --- Title + URL ---
        link_el = (
            row.select_one("h2 a, h3 a, h4 a, .field--name-title a, .views-field-title a")
        )
        if not link_el:
            continue
        title = link_el.get_text(strip=True)
        href = link_el.get("href", "")
        if href.startswith("/"):
            href = CNMC_BASE + href

        if _is_excluded(title + " " + href):
            logger.debug("Excluded CNMC entry: %s", title[:80])
            continue

        # --- Date ---
        date_el = row.select_one(
            ".field--name-field-fecha, "
            ".views-field-field-fecha, "
            "time, "
            ".date-display-single, "
            ".field-name-field-fecha"
        )
        raw_date = ""
        if date_el:
            raw_date = date_el.get("datetime", "") or date_el.get_text(strip=True)
        published_date = _parse_date(raw_date) if raw_date else None

        # --- Section / category ---
        section_el = row.select_one(
            ".field--name-field-sector, "
            ".views-field-field-sector, "
            ".field-name-field-sector"
        )
        section = section_el.get_text(strip=True) if section_el else "Energía"

        # --- Summary ---
        body_el = row.select_one(
            ".field--name-body, "
            ".views-field-body, "
            ".field-name-body, "
            "p"
        )
        summary = body_el.get_text(" ", strip=True)[:500] if body_el else None

        # Unique external_id from URL slug
        slug = href.rstrip("/").split("/")[-1]
        external_id = f"cnmc-{slug}"

        entries.append({
            "source": "CNMC",
            "external_id": external_id,
            "title": title,
            "published_date": published_date,
            "url": href,
            "section": section,
            "department": "CNMC",
            "summary": summary,
        })

    return entries


def scrape(max_pages: int = 5) -> List[Dict]:
    """Scrape CNMC energy consultations across up to *max_pages* pages."""
    results: List[Dict] = []

    for page in range(max_pages):
        soup = _fetch_page(CNMC_URL, page)
        if soup is None:
            break

        entries = _extract_entries(soup)
        if not entries:
            logger.info("CNMC: no entries on page %d — stopping.", page)
            break

        results.extend(entries)
        logger.info("CNMC page %d: %d entries collected (total %d)", page, len(entries), len(results))

        # Stop if there's no "next page" link
        if not soup.select_one("a[title='Página siguiente'], a.pager__item--next, li.pager-next a"):
            break

    logger.info("CNMC scrape finished: %d entries", len(results))
    return results
