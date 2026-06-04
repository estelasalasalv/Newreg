"""MITERD/MITECO scraper — participacion publica en energia, cambio climatico,
calidad ambiental y costas.

Fuentes:
  - https://www.miteco.gob.es/es/energia/participacion.html
  - https://www.miteco.gob.es/es/cambio-climatico/participacion-publica.html
  - https://www.miteco.gob.es/es/calidad-y-evaluacion-ambiental/participacion-publica.html
  - https://www.miteco.gob.es/es/costas/participacion-publica.html
"""
import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

MITERD_URL    = "https://www.miteco.gob.es/es/energia/participacion.html"
MITERD_CC_URL = "https://www.miteco.gob.es/es/cambio-climatico/participacion-publica.html"
MITERD_CA_URL = "https://www.miteco.gob.es/es/calidad-y-evaluacion-ambiental/participacion-publica.html"
MITERD_CO_URL = "https://www.miteco.gob.es/es/costas/participacion-publica.html"
MITERD_BASE   = "https://www.miteco.gob.es"

_GAS_RE   = re.compile(
    r'gas natural|regasificaci|distribuc.*gas|transporte.*gas|biometano|gnl|gasif|peajes.*gas|pr.rroga.*gas',
    re.IGNORECASE,
)
_TELCO_RE = re.compile(r'telecomunicaci|audiovisual|postal|ferroviario', re.IGNORECASE)

_HEADERS = {"User-Agent": "Mozilla/5.0 (RegulatoryBot/1.0)"}

_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _detect_sector(title: str) -> str:
    if _GAS_RE.search(title):
        return "gas"
    if _TELCO_RE.search(title):
        return "otros"
    return "electricidad"


def _parse_date(text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text, re.IGNORECASE)
    if m:
        mon = _MONTHS.get(m.group(2).lower())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    return None


def _fetch_plazo(url: str) -> Optional[str]:
    """Navega a la página individual de una consulta MITECO y extrae el plazo de remisión."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text(" ", strip=True)
        m = re.search(
            r"[Pp]lazo\s+(?:de\s+remisi[oó]n\s+)?(?:para\s+presentar\s+documentaci[oó]n\s+)?"
            r"[Dd]esde\s+.+?\s+[Hh]asta\s+.+?(?=\s{2,}|\Z|Presentaci[oó]n)",
            text,
        )
        if m:
            return re.sub(r"\s+", " ", m.group()).strip()[:200]
    except Exception:
        pass
    return None


def _scrape_links_list(url: str, section_label: str, sector: str = "otros") -> List[Dict]:
    """Scrape generico para paginas MITECO con estructura h2 + div.links-list."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("MITERD %s request failed: %s", section_label, exc)
        return []

    soup    = BeautifulSoup(resp.text, "lxml")
    results = []
    seen: set = set()
    current_estado = "Abierta"

    for el in soup.find_all(["h2", "div"]):
        if el.name == "h2":
            txt = el.get_text(strip=True).lower()
            if "abierto" in txt:
                current_estado = "Abierta"
            elif "cerrado" in txt:
                current_estado = "Cerrada"
            continue

        classes = " ".join(el.get("class", []))
        if "links-list" not in classes:
            continue

        for a in el.select("div.links-list__item-title a, li a"):
            title = a.get_text(strip=True)
            href  = a.get("href", "")
            if not title or not href:
                continue
            # Filtrar títulos que son cabeceras/navegación, no consultas reales
            _SKIP_TITLES = re.compile(
                r'^(anuncios anteriores|información legal|acceso al listado|ver todos|ver más|anterior|siguiente)$',
                re.IGNORECASE,
            )
            if _SKIP_TITLES.match(title) or len(title) < 15:
                continue
            if href.startswith("/"):
                href = MITERD_BASE + href
            if not href.startswith("http"):
                continue
            # Excluir la pagina indice
            base = url.split("#")[0].rstrip("/")
            if href.rstrip("/").split("#")[0].rstrip("/") == base:
                continue

            # Para Costas: el estado real está en el título como [DD/MM-DD/MM/YYYY]
            estado_entry = current_estado
            date_range = re.search(r'\[[\d/]+-(\d{2}/\d{2}/\d{4})\]', title)
            if date_range:
                from datetime import date, datetime
                try:
                    cierre = datetime.strptime(date_range.group(1), "%d/%m/%Y").date()
                    estado_entry = "Abierta" if cierre >= date.today() else "Cerrada"
                except Exception:
                    pass

            slug        = re.sub(r"[^a-z0-9]", "-", href.rstrip("/").split("/")[-1].replace(".html", "").lower())
            prefix      = section_label.lower().replace(" ", "-").replace("/", "")[:8]
            external_id = f"miterd-{prefix}-{slug}"
            if external_id in seen:
                continue
            seen.add(external_id)

            # Para entradas abiertas, intentar obtener plazo navegando a la página individual
            plazo_str = None
            if estado_entry == "Abierta":
                plazo_str = _fetch_plazo(href)
                if plazo_str:
                    # Extraer fecha de cierre para confirmar estado
                    m_cierre = re.search(r'[Hh]asta\s+el?\s+\w+,?\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', plazo_str)
                    if m_cierre:
                        from datetime import date
                        mon = _MONTHS.get(m_cierre.group(2).lower())
                        if mon:
                            try:
                                cierre = date(int(m_cierre.group(3)), mon, int(m_cierre.group(1)))
                                if cierre < date.today():
                                    estado_entry = "Cerrada"
                            except Exception:
                                pass

            results.append({
                "source":         "MITERD",
                "tipo":           "consulta",
                "external_id":    external_id,
                "title":          title,
                "published_date": None,
                "url":            href,
                "section":        section_label,
                "department":     "MITERD",
                "summary":        None,
                "plazo":          plazo_str,
                "estado":         estado_entry,
                "sector":         sector,
            })

    abiertas = sum(1 for r in results if r["estado"] == "Abierta")
    logger.info("MITERD %s: %d consultas (%d abiertas)", section_label, len(results), abiertas)
    return results


def _scrape_energia() -> List[Dict]:
    """Scrape la pagina de participacion publica de energia del MITERD."""
    try:
        resp = requests.get(MITERD_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("MITERD energia request failed: %s", exc)
        return []

    soup    = BeautifulSoup(resp.text, "lxml")
    results = []

    for body in soup.select("div.public-participation-search__body"):
        link_el = body.select_one("h2.public-participation-search__title a")
        if not link_el:
            continue
        title = link_el.get("title", link_el.get_text(strip=True))
        href  = link_el.get("href", "")
        if not title or not href:
            continue

        cat_el   = body.select_one("div.public-participation-search__content")
        category = cat_el.get_text(strip=True) if cat_el else ""

        date_el = body.select_one("div.public-participation-search__date")
        pub_date = cierre_date = plazo_str = None
        if date_el:
            strongs = [s.get_text(strip=True) for s in date_el.select("strong")]
            if len(strongs) >= 2:
                pub_date    = _parse_date(strongs[0])
                cierre_date = _parse_date(strongs[1])
                plazo_str   = re.sub(r"\s+", " ", date_el.get_text(" ", strip=True))[:150]

        estado = "Abierta"
        if cierre_date:
            from datetime import date
            try:
                if date.fromisoformat(cierre_date) < date.today():
                    estado = "Cerrada"
            except Exception:
                pass

        slug        = href.rstrip("/").split("/")[-1]
        external_id = f"miterd-{slug}"

        results.append({
            "source":         "MITERD",
            "tipo":           "consulta",
            "external_id":    external_id,
            "title":          title,
            "published_date": pub_date,
            "url":            href,
            "section":        category or "Consultas MITERD",
            "department":     "MITERD",
            "summary":        None,
            "plazo":          plazo_str,
            "estado":         estado,
            "sector":         _detect_sector(title),
        })

    logger.info("MITERD energia: %d consultas", len(results))
    return results


def scrape() -> List[Dict]:
    """Combina todas las fuentes de participacion publica del MITECO."""
    energia  = _scrape_energia()
    cc       = _scrape_links_list(MITERD_CC_URL, "Cambio Climatico - MITECO", "otros")
    ca       = _scrape_links_list(MITERD_CA_URL, "Calidad y Evaluacion Ambiental - MITECO", "otros")
    total    = energia + cc + ca
    logger.info("MITERD total: %d consultas", len(total))
    return total
