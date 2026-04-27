"""
Importa resúmenes e impacto REE desde agente-regulatorio hacia la BD de Newreg.
Busca coincidencias por fecha aproximada + palabras clave del título.
"""
import os
import re
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ── Datos de agente-regulatorio (traducidos al español) ─────────────────────
ENTRIES = [
    {
        "keywords": ["grado", "cardoso", "400"],
        "date_from": "2026-03-25", "date_to": "2026-04-10",
        "resumen": "Autorización administrativa para una línea aérea de 400 kV entre las subestaciones de Grado y Cardoso en Asturias, declarada de utilidad pública.",
        "impacto_ree": "Beneficiaria directa. Autoriza la construcción de nueva infraestructura de 400 kV que refuerza la capacidad de evacuación en Asturias.",
    },
    {
        "keywords": ["villanueva", "rey", "400", "carmona"],
        "date_from": "2026-03-20", "date_to": "2026-04-10",
        "resumen": "Autorización de nueva subestación de 400 kV en Villanueva del Rey (Sevilla), línea de 58,7 km hasta Carmona y ampliación de la subestación Carmona con 3 nuevas posiciones.",
        "impacto_ree": "Autorización de infraestructura de transporte a gran escala que refuerza la integración de renovables en el occidente de Andalucía.",
    },
    {
        "keywords": ["cardoso", "400", "carreño", "asturias"],
        "date_from": "2026-03-20", "date_to": "2026-04-10",
        "resumen": "Autorización de nueva subestación Cardoso de 400 kV con 3 posiciones de operación, 6 de reserva y autotransformador 400/220 kV en Carreño.",
        "impacto_ree": "Infraestructura nodal que complementa la línea Grado-Cardoso, reforzando la capacidad de la red de transporte en el norte de la península.",
    },
    {
        "keywords": ["moraleja", "villaviciosa", "400", "doble circuito"],
        "date_from": "2026-03-15", "date_to": "2026-04-01",
        "resumen": "Autorización de aumento de capacidad en la línea de 400 kV de doble circuito en el entorno de la subestación de Villaviciosa (Madrid), con modificación de trazado de 10,2 km.",
        "impacto_ree": "Refuerzo de la capacidad de transporte en el corredor de Madrid mediante nueva infraestructura de 10,2 km.",
    },
    {
        "keywords": ["rdl", "7/2026", "oriente", "crisis"],
        "date_from": "2026-03-15", "date_to": "2026-03-28",
        "resumen": "Moviliza 5.000 millones de euros para mitigar el impacto de la crisis de Oriente Medio. Incluye medidas urgentes de energía: protección al consumidor, flexibilización del acceso a la red, aceleración de renovables y promoción del almacenamiento.",
        "impacto_ree": "El Título I afecta directamente a REE mediante la 'flexibilización de los procedimientos de acceso y conexión a la red' y acelera la tramitación de instalaciones de almacenamiento, habilitando medidas de refuerzo de red.",
    },
    {
        "keywords": ["circular", "9/2025", "trf", "recursos"],
        "date_from": "2026-03-10", "date_to": "2026-03-25",
        "resumen": "La CNMC notifica múltiples recursos contencioso-administrativos contra la Circular 9/2025, que establece la tasa de retribución financiera en el 6,58 % para el periodo regulatorio 2026-2031.",
        "impacto_ree": "Riesgo regulatorio significativo: si prosperan los recursos, podría anularse la tasa del 6,58 %, afectando directamente a los ingresos regulados de REE por transporte y operación del sistema.",
    },
    {
        "keywords": ["retribuci", "transportistas", "definitiva", "2023"],
        "date_from": "2026-03-01", "date_to": "2026-03-20",
        "resumen": "La CNMC establece la retribución definitiva de las empresas transportistas de energía eléctrica para el ejercicio 2023, con un importe total de 1.471 millones de euros y los ajustes correspondientes.",
        "impacto_ree": "Resolución crítica como principal operador. Cierra el ciclo retributivo de 2023 y liquida las diferencias entre la retribución provisional y la definitiva.",
    },
    {
        "keywords": ["iznalloz", "400", "granada", "subestaci"],
        "date_from": "2026-02-20", "date_to": "2026-03-15",
        "resumen": "Autorización de nueva subestación de 400 kV en Iznalloz con siete posiciones de operación en Granada. No se declara la utilidad pública al demostrar REE la titularidad de los terrenos.",
        "impacto_ree": "Nuevo nodo estratégico de 400 kV en la red peninsular sureste que refuerza la integración de renovables en Granada.",
    },
    {
        "keywords": ["acceso", "demanda", "nudos", "transporte", "concurso"],
        "date_from": "2026-02-18", "date_to": "2026-03-05",
        "resumen": "La Secretaría de Estado de Energía resuelve el concurso de capacidad de acceso para instalaciones de demanda en nudos específicos de la red de transporte, asignando capacidad a las instalaciones seleccionadas.",
        "impacto_ree": "Determina qué instalaciones industriales, electrolizadores y centros de datos obtienen acceso garantizado a la red, condicionando la planificación y operación de REE.",
    },
    {
        "keywords": ["déficit", "sistema eléctrico", "31/12/2025"],
        "date_from": "2026-02-18", "date_to": "2026-03-05",
        "resumen": "La Dirección General de Política Energética y Minas fija el importe del déficit pendiente de cobro del Sistema Eléctrico a 31 de diciembre de 2025 para el Fondo de Titulización.",
        "impacto_ree": "Indicador clave de la salud financiera del sistema regulado, que afecta a la solvencia del marco retributivo de REE y a la capacidad de pago futura.",
    },
    {
        "keywords": ["ibiza", "formentera", "adicional"],
        "date_from": "2026-02-15", "date_to": "2026-03-05",
        "resumen": "La Secretaría de Estado de Energía concluye el proceso competitivo de régimen retributivo adicional para el subsistema eléctrico no peninsular de Ibiza-Formentera.",
        "impacto_ree": "Afecta a la operación del sistema insular bajo la supervisión de REE, determinando qué instalaciones de generación acceden a retribución adicional en sistemas no peninsulares.",
    },
    {
        "keywords": ["canarias", "ceuta", "melilla", "mallorca", "menorca", "adicional"],
        "date_from": "2026-02-15", "date_to": "2026-03-05",
        "resumen": "La Dirección General resuelve el proceso competitivo de régimen retributivo adicional para los subsistemas no peninsulares de Canarias, Ceuta, Melilla y Baleares.",
        "impacto_ree": "Impacta en la operación de sistemas aislados bajo gestión de REE, determinando la estructura de generación que garantiza el suministro en territorios no peninsulares durante 2026-2031.",
    },
    {
        "keywords": ["rd", "88/2026", "suministro", "comercializaci"],
        "date_from": "2026-02-05", "date_to": "2026-02-20",
        "resumen": "Nuevo reglamento general de suministro, comercialización y agregación de energía eléctrica, que transpone la Directiva UE 2019/944 y establece el marco para el agregador independiente.",
        "impacto_ree": "Consolida el marco de acceso de terceros a la red para suministro y agregación. Los agregadores independientes generan nuevos flujos de potencia que REE debe gestionar como operador del sistema.",
    },
    {
        "keywords": ["ted/82", "conexión", "módulos", "generación", "almacenamiento"],
        "date_from": "2026-02-05", "date_to": "2026-02-20",
        "resumen": "Actualiza los requisitos técnicos de conexión a la red para módulos de generación y almacenamiento, introduciendo especificaciones para sistemas no peninsulares. Entrada en vigor el 12 de mayo de 2026.",
        "impacto_ree": "Obliga a adaptar los procedimientos de operación para los nuevos requisitos técnicos de conexión, afectando a los criterios de despacho y la coordinación con generadores renovables.",
    },
    {
        "keywords": ["retribuci", "distribuidoras", "provisional", "2026"],
        "date_from": "2026-01-28", "date_to": "2026-02-15",
        "resumen": "La CNMC establece provisionalmente la retribución de las empresas distribuidoras para 2026, utilizando los valores aprobados de 2022 en tanto no se dicte resolución definitiva.",
        "impacto_ree": "Relevante para el entorno regulatorio de REE; la retribución provisional de distribuidoras impacta en los flujos financieros del sistema y en la coordinación transporte-distribución.",
    },
    {
        "keywords": ["hac/56", "sf6", "gases fluorados", "subestaci"],
        "date_from": "2026-01-28", "date_to": "2026-02-15",
        "resumen": "Modifica la Orden HFP/826/2022 introduciendo un mecanismo de autoliquidación rectificativa para el Impuesto sobre los Gases Fluorados (modelo 587), con entrada en vigor el 1 de julio de 2026.",
        "impacto_ree": "Afecta a las obligaciones fiscales de REE como operador de subestaciones con SF6; modifica los procedimientos de autoliquidación del impuesto sobre gases fluorados en equipos de alta tensión.",
    },
    {
        "keywords": ["retribuci", "transportistas", "provisional", "2026"],
        "date_from": "2026-01-25", "date_to": "2026-02-10",
        "resumen": "La CNMC establece provisionalmente la retribución de las empresas transportistas para 2026, utilizando los valores aprobados de 2022 en tanto no se dicte resolución definitiva.",
        "impacto_ree": "De aplicación directa a REE como principal operador. La retribución provisional 2026 se basa en valores de 2022, pendiente de la aprobación del nuevo periodo regulatorio (TRF 6,58 %).",
    },
    {
        "keywords": ["ted/53", "parámetros retributivos", "renovables", "cogeneraci"],
        "date_from": "2026-01-25", "date_to": "2026-02-10",
        "resumen": "Actualiza los parámetros retributivos de las instalaciones de energías renovables, cogeneración y residuos para 2026-2031 (precios de mercado: 61,65 €/MWh en 2026, 59,11 en 2027, 58,65 en 2028).",
        "impacto_ree": "Los parámetros retributivos determinan el volumen y perfil de la generación renovable que REE debe integrar en la operación del sistema, la planificación de balances de potencia y la gestión de restricciones técnicas.",
    },
    {
        "keywords": ["magall", "400", "600 mva", "zaragoza"],
        "date_from": "2026-01-20", "date_to": "2026-02-05",
        "resumen": "Autorización de ampliación de la subestación de Magallón en el parque de 400 kV con un nuevo banco de transformación de 600 MVA y dos posiciones de operación en Zaragoza.",
        "impacto_ree": "Ampliación directa de infraestructura en el nodo estratégico de Magallón; el banco de 600 MVA refuerza la capacidad de transformación en el noreste del sistema, con alto crecimiento renovable.",
    },
    {
        "keywords": ["ted/30", "no peninsular", "parámetros técnicos", "económicos"],
        "date_from": "2026-01-15", "date_to": "2026-02-05",
        "resumen": "Establece los parámetros técnicos y económicos para el cálculo de la retribución de la generación no peninsular durante el periodo regulatorio 2026-2031.",
        "impacto_ree": "Define el marco retributivo para la generación en sistemas insulares y no peninsulares bajo el rol de operador del sistema de REE, condicionando la estructura de generación y los costes de operación de sistemas aislados.",
    },
    {
        "keywords": ["congestión", "españa", "francia", "vizcaya", "2024"],
        "date_from": "2026-01-10", "date_to": "2026-02-01",
        "resumen": "La CNMC fija las rentas de congestión de la interconexión eléctrica España-Francia por el Golfo de Vizcaya para 2024 en 157,544 millones de euros, destinadas a financiar los costes de inversión de REE.",
        "impacto_ree": "REE recibe directamente 157,5 millones de euros de rentas de congestión que financian los costes de inversión en la interconexión de 2024, menos los fondos CEF ya percibidos.",
    },
    {
        "keywords": ["po", "3.1", "3.2", "7.2", "tensión", "reactiva"],
        "date_from": "2026-01-10", "date_to": "2026-02-01",
        "resumen": "La CNMC modifica los procedimientos de operación 3.1 (energía reactiva), 3.2 (restricciones técnicas) y 7.2 (calidad del suministro) para facilitar la estabilización de la tensión del sistema.",
        "impacto_ree": "Afecta directamente a la operación del sistema, obligando a REE a adaptar los procedimientos operativos en tiempo real y los protocolos de coordinación con los generadores para el control de la tensión y la energía reactiva.",
    },
    {
        "keywords": ["iznalloz", "doble circuito", "línea", "entrada", "salida"],
        "date_from": "2026-01-05", "date_to": "2026-01-25",
        "resumen": "Autorización de la línea aérea de 400 kV en doble circuito de entrada/salida en la nueva subestación de Iznalloz, derivada de la línea Baza-Caparacena, con una longitud de 677 metros.",
        "impacto_ree": "Autoriza la línea de conexión de la nueva subestación de 400 kV de Iznalloz; junto con la autorización de la subestación, completa el nuevo nodo del sureste peninsular y refuerza la integración de renovables.",
    },
]


def _norm(s):
    import unicodedata
    return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode("ascii")


def find_match(cur, entry):
    """Busca en boe_entries la entrada que coincide por fecha y palabras clave."""
    kws = [_norm(k) for k in entry["keywords"]]
    cur.execute(
        "SELECT id, texto, fecha FROM boe_entries WHERE fecha BETWEEN %s AND %s",
        (entry["date_from"], entry["date_to"]),
    )
    rows = cur.fetchall()
    best = None
    best_score = 0
    for id_, texto, fecha in rows:
        normed = _norm(texto or "")
        score = sum(1 for k in kws if k in normed)
        if score > best_score and score >= max(2, len(kws) // 2):
            best_score = score
            best = (id_, texto, str(fecha))
    return best


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur  = conn.cursor()
    updated = 0

    for entry in ENTRIES:
        match = find_match(cur, entry)
        if match:
            id_, texto, fecha = match
            cur.execute(
                "UPDATE boe_entries SET resumen=%s, impacto_ree=%s WHERE id=%s",
                (entry["resumen"], entry["impacto_ree"], id_),
            )
            updated += cur.rowcount
            logger.info("✓ [%s] %s", fecha, (texto or "")[:70])
        else:
            logger.warning("✗ Sin coincidencia: %s (%s→%s)",
                           entry["keywords"], entry["date_from"], entry["date_to"])

    conn.commit()
    conn.close()
    logger.info("\nActualizadas %d entradas.", updated)


if __name__ == "__main__":
    main()
