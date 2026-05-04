"""BOE scraper — API oficial de datos abiertos.

Campos extraídos por documento:
  fecha, fuente, seccion, tipo, organismo, subseccion,
  texto (título completo), enlace, palabras_clave,
  resumen, importante (Sí/No), acceso_conexion (Sí/No), publicable (NO)
"""
import re
import unicodedata
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

BOE_API = "https://www.boe.es/datosabiertos/api/boe/sumario/{fecha}"


# ── Normalización ─────────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    """Minúsculas + sin acentos para comparación."""
    return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode("ascii")


def _to_list(val) -> list:
    if val is None:
        return []
    return val if isinstance(val, list) else [val]


def _get_nombre(obj: dict) -> str:
    return (obj.get("nombre") or obj.get("@nombre") or "").strip()


def _get_url(item: dict) -> str:
    html = item.get("url_html", "")
    if isinstance(html, str) and html:
        return html
    pdf = item.get("url_pdf", {})
    if isinstance(pdf, dict) and pdf.get("texto"):
        return pdf["texto"]
    doc_id = item.get("identificador", "")
    return f"https://www.boe.es/diario_boe/txt.php?id={doc_id}" if doc_id else ""


# ── Lista de palabras clave energéticas ───────────────────────────────────────
# Tuplas (nombre_display, patron_normalizado)
# Las entradas marcadas con WB se comparan con palabra completa (\b...\b)
_KW_RAW: List[Tuple[str, str, bool]] = [
    ("Red Eléctrica de España",                         "red electrica de espana",                         False),
    ("eléctrico",                                       "electrico",                                       False),
    ("eléctrica",                                       "electrica",                                       False),
    ("electricidad",                                    "electricidad",                                    False),
    ("energía",                                         "energia",                                         False),
    ("energético",                                      "energetico",                                      False),
    ("energética",                                      "energetica",                                      False),
    ("autorización administrativa",                     "autorizacion administrativa",                     False),
    ("BombeoEMER",                                      "bombeoemr",                                       True ),
    ("bono social",                                     "bono social",                                     False),
    ("ciberseguridad",                                  "ciberseguridad",                                  False),
    ("comercializador",                                  "comercializador",                                 False),
    ("comercializadora",                                 "comercializadora",                                False),
    ("cambio de suministrador",                          "cambio de suministrador",                         False),
    ("conexión a la red",                               "conexion a la red",                               False),
    ("contabilidad regulatoria",                        "contabilidad regulatoria",                        False),
    ("coste de generación",                             "coste de generacion",                             False),
    ("declaración de utilidad pública",                 "declaracion de utilidad publica",                 False),
    ("derechos de emisión",                             "derechos de emision",                             False),
    ("eficiencia",                                      "eficiencia",                                      False),
    ("emisiones de gases de efecto invernadero",        "emisiones de gases de efecto invernadero",        False),
    ("renovables",                                      "renovable",                                       False),
    ("fotovoltaica",                                    "fotovoltaica",                                    False),
    ("generación de energía",                           "generacion de energia",                           False),
    ("generación distribuida",                          "generacion distribuida",                          False),
    ("hexafluoruro de azufre (SF6)",                    "hexafluoruro de azufre",                          False),
    ("SF6",                                             "sf6",                                             True ),
    ("hidrógeno verde",                                 "hidrogeno verde",                                 False),
    ("hidrógeno",                                       "hidrogeno",                                       False),
    ("impacto ambiental",                               "impacto ambiental",                               False),
    ("indexación de la economía",                       "indexacion de la economia",                       False),
    ("interconexión internacional",                     "interconexion internacional",                     False),
    ("jurisprudencia del sistema eléctrico",            "jurisprudencia del sistema electrico",            False),
    ("línea de transmisión",                            "linea de transmision",                            False),
    ("línea de transporte",                             "linea de transporte",                             False),
    ("línea aérea",                                     "linea aerea",                                     False),
    ("LAT",                                             "lat",                                             True ),
    ("línea subterránea de energía",                    "linea subterranea de energia",                    False),
    ("línea subterránea de electricidad",               "linea subterranea de electricidad",               False),
    ("liquidación del sistema eléctrico",               "liquidacion del sistema electrico",               False),
    ("mercado diario",                                  "mercado diario",                                  False),
    ("mercado intradiario",                             "mercado intradiario",                             False),
    ("mercado eléctrico",                               "mercado electrico",                               False),
    ("gas renovable",                                   "gas renovable",                                   False),
    ("ofertas mayoristas",                              "ofertas mayoristas",                              False),
    ("OMIE",                                            "omie",                                            True ),
    ("OMIP",                                            "omip",                                            True ),
    ("operador del sistema",                            "operador del sistema",                            False),
    ("organismo regulador energético",                  "organismo regulador energetico",                  False),
    ("peaje de acceso",                                 "peaje de acceso",                                 False),
    ("planificación de la red",                         "planificacion de la red",                         False),
    ("política energética",                             "politica energetica",                             False),
    ("producción eléctrica",                            "produccion electrica",                            False),
    ("reactancia",                                      "reactancia",                                      False),
    ("condensador",                                     "condensador",                                     False),
    ("FACTS",                                           "facts",                                           True ),
    ("transformador",                                   "transformador",                                   False),
    ("corriente continua (HVDC)",                       "corriente continua",                              False),
    ("HVDC",                                            "hvdc",                                            True ),
    ("red de transporte",                               "red de transporte",                               False),
    ("regulación de la energía eléctrica",              "regulacion de la energia electrica",              False),
    ("retribución del operador del sistema eléctrico",  "retribucion del operador del sistema electrico",  False),
    ("retribución del transporte de electricidad",      "retribucion del transporte de electricidad",      False),
    ("servicio de ajuste del sistema",                  "servicio de ajuste del sistema",                  False),
    ("sistema eléctrico",                               "sistema electrico",                               False),
    ("subestación",                                     "subestacion",                                     False),
    ("tarifa de acceso",                                "tarifa de acceso",                                False),
    ("tarifa eléctrica",                                "tarifa electrica",                                False),
    ("tasa de ocupación de dominio público",            "tasa de ocupacion de dominio publico",            False),
    ("tasa local",                                      "tasa local",                                      False),
    ("transporte de energía",                           "transporte de energia",                           False),
    ("transporte de electricidad",                      "transporte de electricidad",                      False),
    ("transportista único",                             "transportista unico",                             False),
    ("vehículo eléctrico",                              "vehiculo electrico",                              False),
]

# Pre-compilar patrones una sola vez
_KW_PATTERNS: List[Tuple[str, re.Pattern]] = [
    (display, re.compile(r"\b" + re.escape(norm) + r"\b") if wb else re.compile(re.escape(norm)))
    for display, norm, wb in _KW_RAW
]

# Palabras especiales: nombramientos/ceses, solo para ciertos organismos
_NOM_TERMS = re.compile(r"\b(nombramiento|nombramientos|cese|ceses)\b")
_NOM_DEPTS = [_norm(d) for d in [
    "presidencia del gobierno",
    "ministerio para la transicion ecologica y el reto demografico",
    "ministerio de industria y turismo",
    "comision nacional de los mercados y la competencia",
    "ministerio de politica territorial y memoria democratica",
    "ministerio de derechos sociales consumo y agenda 2030",
    "ministerio de transportes y movilidad sostenible",
    "ministerio de economia comercio y empresa",
]]

# Palabras para campo acceso_conexion
_ACCESS_RE = re.compile(r"\b(acceso|conexion|interconexion|peaje de acceso|red de transporte|red de distribucion)\b")

# Patrón para autorizaciones administrativas de construcción/previa (AAP/AAC)
_TRAMITACIONES_RE = re.compile(
    r"autorizaci[oó]n administrativa\s+(previa|de construcci[oó]n)"
    r"|\bAAP\b"
    r"|\bAAC\b",
    re.IGNORECASE,
)


# ── Detección de tipo de documento ───────────────────────────────────────────
_TIPO_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^ley organica"),       "Ley Orgánica"),
    (re.compile(r"^real decreto.?ley"),            "Real Decreto-ley"),
    (re.compile(r"^real decreto"),                 "Real Decreto"),
    (re.compile(r"^decreto foral legislativo"),    "Decreto Foral Legislativo"),
    (re.compile(r"^decreto foral"),                "Decreto Foral"),
    (re.compile(r"^decreto legislativo"),          "Decreto Legislativo"),
    (re.compile(r"^ley\b"),              "Ley"),
    (re.compile(r"^orden\b"),            "Orden"),
    (re.compile(r"^circular\b"),         "Circular"),
    (re.compile(r"^resolucion\b"),       "Resolución"),
    (re.compile(r"^anuncio\b"),          "Anuncio"),
    (re.compile(r"^acuerdo\b"),          "Acuerdo"),
    (re.compile(r"^instruccion\b"),      "Instrucción"),
    (re.compile(r"^correccion de error"),"Corrección de errores"),
    (re.compile(r"^extracto\b"),         "Extracto"),
    (re.compile(r"^convenio\b"),         "Convenio"),
    (re.compile(r"^edicto\b"),           "Edicto"),
    (re.compile(r"^declaracion\b"),      "Declaración"),
    (re.compile(r"^comunicacion\b"),     "Comunicación"),
]
_IMPORTANTES = {"Ley Orgánica", "Real Decreto-ley", "Real Decreto", "Ley", "Circular"}


def _find_keywords(texto: str, epigrafe: str = "") -> List[str]:
    """Devuelve lista de palabras clave encontradas (sin duplicados)."""
    nt = _norm(texto + " " + epigrafe)
    found, seen = [], set()
    for display, pat in _KW_PATTERNS:
        if display not in seen and pat.search(nt):
            found.append(display)
            seen.add(display)
    return found


def _detect_tipo(titulo: str, epigrafe: str = "") -> str:
    nt = _norm(titulo)
    for pat, label in _TIPO_PATTERNS:
        if pat.search(nt):
            return label
    # Fallback: usar el epigrafe
    ne = _norm(epigrafe)
    if "resolucion" in ne:
        return "Resolución"
    if "anuncio" in ne:
        return "Anuncio"
    if "nombramientos" in ne or "nombramiento" in ne:
        return "Nombramiento"
    if "ceses" in ne or "cese" in ne:
        return "Cese"
    return epigrafe.strip() or "—"


# Detección de infracciones Art.64 LSE
_ART64_RE   = re.compile(r"art[ií]culo\s+64|art\.?\s*64\b", re.IGNORECASE)
_RIESGO_GS  = re.compile(r"riesgo.*garant[ií]a.*suministro|garant[ií]a.*suministro.*riesgo|"
                          r"riesgo.*seguridad.*suministro", re.IGNORECASE)
_RIESGO_DG  = re.compile(r"da[ñn]o\s+grave|riesgo.*da[ñn]o", re.IGNORECASE)
_SIN_RIESGO_RE = re.compile(r"sin\s+riesgo", re.IGNORECASE)


def _clasificar_art64(titulo: str) -> str:
    """Devuelve la clasificación de riesgo Art.64 o cadena vacía si no aplica."""
    if not _ART64_RE.search(titulo):
        return ""
    if _SIN_RIESGO_RE.search(titulo):
        return "Art.64 — Sin riesgo GS"
    if _RIESGO_GS.search(titulo):
        return "Art.64 — Con riesgo GS"
    if _RIESGO_DG.search(titulo):
        return "Art.64 — Con daño grave"
    return "Art.64 — Sin clasificar"


_NOM_IMP_DEPTS = [
    "presidencia del gobierno",
    "ministerio para la transicion ecologica y el reto demografico",
]
_NOM_TITLE_RE = re.compile(r"\b(nombra|nombramiento|cese|dispone el cese)\b", re.IGNORECASE)

def _is_importante(tipo: str, organismo: str = "", titulo: str = "") -> str:
    if tipo not in _IMPORTANTES:
        return "No"
    # Si el documento es un nombramiento/cese, solo importante
    # si viene de Presidencia del Gobierno o Transición Ecológica
    if _NOM_TITLE_RE.search(titulo):
        org_norm = _norm(organismo)
        return "Sí" if any(d in org_norm for d in _NOM_IMP_DEPTS) else "No"
    return "Sí"


def _detect_acceso(titulo: str) -> str:
    return "Sí" if _ACCESS_RE.search(_norm(titulo)) else "No"


def _detect_tramitaciones(titulo: str) -> str:
    return "Sí" if _TRAMITACIONES_RE.search(titulo) else "No"


def _dept_is_approved(dept_norm: str) -> bool:
    return any(frag in dept_norm for frag in _NOM_DEPTS)


# Subsecciones/títulos excluidos explícitamente
_EXCLUDED_SUBSECTIONS = re.compile(r"\brtve\b|convenio", re.IGNORECASE)
_EXCLUDED_ORGANISMS   = re.compile(r"\buniversidad|telecomunicaci", re.IGNORECASE)
_EXCLUDED_TITLES = [
    re.compile(r"certificado profesional.*intercambio geoterm", re.IGNORECASE),
    re.compile(r"instituto geogr[aá]fico nacional", re.IGNORECASE),
    re.compile(r"fiscal con destino", re.IGNORECASE),
    re.compile(r"seguridad jur[ií]dica y fe p[uú]blica", re.IGNORECASE),
    re.compile(r"eficiencia del servicio p[uú]blico", re.IGNORECASE),
    re.compile(r"agencia estatal de meteorolog[ií]a", re.IGNORECASE),
]

# Organismos aprobados para libre designación
_LIBRE_DESIG_RE  = re.compile(r"libre designaci[oó]n", re.IGNORECASE)
_LIBRE_DESIG_OK  = [
    "transicion ecologica", "miterd", "miteco",
    "mercados y la competencia", "cnmc",
    "presidencia del gobierno", "jefatura del estado",
]


def _should_include(titulo: str, epigrafe: str, dept: str) -> bool:
    """True si el documento debe incluirse según las reglas."""
    # Excluir subsecciones, organismos y títulos no deseados
    if _EXCLUDED_SUBSECTIONS.search(epigrafe) or _EXCLUDED_SUBSECTIONS.search(dept):
        return False
    if _EXCLUDED_ORGANISMS.search(dept):
        return False
    if any(p.search(titulo) for p in _EXCLUDED_TITLES):
        return False
    # Libre designación solo de organismos energéticos aprobados
    if _LIBRE_DESIG_RE.search(titulo):
        dept_n = _norm(dept)
        if not any(ok in dept_n for ok in _LIBRE_DESIG_OK):
            return False

    # Keywords solo en título (no en epígrafe) para evitar falsos positivos
    # como "Impacto ambiental" que coincide con el nombre del epígrafe
    norm_titulo = _norm(titulo)
    norm_titulo_ep = _norm(titulo + " " + epigrafe)
    dept_norm = _norm(dept)

    # Regla 1: nombramientos/ceses solo de organismos aprobados
    if _NOM_TERMS.search(norm_titulo_ep):
        if _dept_is_approved(dept_norm):
            return True
        # Si solo coincide por nombramiento/cese y dept no aprobado → saltar
        # (pero puede pasar igualmente la Regla 2)

    # Regla 2: keywords energéticas solo en el título
    # (no en epígrafe para evitar que "Impacto ambiental" incluya todo lo de esa subsección)
    for _, pat in _KW_PATTERNS:
        if pat.search(norm_titulo):
            return True

    return False


# ── Parser del sumario ────────────────────────────────────────────────────────
def _parse_sumario(data: dict, fecha_str: str) -> List[Dict]:
    items = []
    try:
        sumario = data["data"]["sumario"]
        for d in _to_list(sumario.get("diario")):
            for seccion in _to_list(d.get("seccion")):
                sec_nombre = _get_nombre(seccion)
                for dept in _to_list(seccion.get("departamento")):
                    dept_nombre = _get_nombre(dept)
                    for epigrafe in _to_list(dept.get("epigrafe")):
                        ep_nombre = _get_nombre(epigrafe)
                        for item in _to_list(epigrafe.get("item")):
                            doc_id  = item.get("identificador", "")
                            titulo  = item.get("titulo", "").strip()

                            if not _should_include(titulo, ep_nombre, dept_nombre):
                                continue

                            kw       = _find_keywords(titulo, ep_nombre)
                            tipo     = _detect_tipo(titulo, ep_nombre)
                            art64    = _clasificar_art64(titulo)
                            imp      = "Sí" if art64 else _is_importante(tipo, dept_nombre, titulo)
                            acceso   = _detect_acceso(titulo)
                            autorizacion = _detect_tramitaciones(titulo)

                            items.append({
                                "external_id":   doc_id,
                                "fecha":         fecha_str,
                                "fuente":        "BOE",
                                "seccion":       sec_nombre,
                                "tipo":          tipo,
                                "organismo":     dept_nombre,
                                "subseccion":    ep_nombre,
                                "texto":         titulo,
                                "enlace":        _get_url(item),
                                "palabras_clave": ", ".join(kw),
                                "resumen":       None,
                                "importante":    imp,
                                "acceso_conexion": acceso,
                                "tramitaciones": autorizacion,
                                "impacto_ree":   art64 if art64 else None,
                                "publicable":    "NO",
                            })
    except (KeyError, TypeError) as exc:
        logger.warning("Estructura inesperada en respuesta BOE: %s", exc)
    return items


# ── Punto de entrada ──────────────────────────────────────────────────────────
def scrape(days_back: int = 1) -> List[Dict]:
    """Consulta el sumario BOE de los últimos *days_back* días laborables."""
    results: List[Dict] = []
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    for offset in range(days_back):
        date = datetime.now() - timedelta(days=offset)
        if date.weekday() == 6:   # el BOE no publica los domingos
            continue
        fecha = date.strftime("%Y%m%d")
        url   = BOE_API.format(fecha=fecha)
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.info("BOE %s: no publicado (404)", fecha)
                continue
            resp.raise_for_status()
            items = _parse_sumario(resp.json(), date.strftime("%Y-%m-%d"))
            logger.info("BOE %s: %d entradas relevantes", fecha, len(items))
            results.extend(items)
        except requests.RequestException as exc:
            logger.error("Error BOE %s: %s", fecha, exc)

    return results


# ── Formato CSV ───────────────────────────────────────────────────────────────
CSV_HEADER = (
    "Fecha;Fuente;Sección;Tipo;Organismo;Subsección;"
    "Texto;Enlace;Palabras clave;Resumen;Importante;"
    "Acceso y conexión a la red;Publicable"
)


def _csv_field(v) -> str:
    s = str(v) if v is not None else ""
    return '"' + s.replace('"', '""') + '"'


def to_csv(entries: List[Dict]) -> str:
    lines = [CSV_HEADER]
    for e in entries:
        row = ";".join(_csv_field(e.get(k)) for k in [
            "fecha", "fuente", "seccion", "tipo", "organismo", "subseccion",
            "texto", "enlace", "palabras_clave", "resumen", "importante",
            "acceso_conexion", "publicable",
        ])
        lines.append(row)
    return "\r\n".join(lines)
