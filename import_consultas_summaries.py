"""Importa resumenes reales de agente-regulatorio para consultas CNMC/MITERD."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONSULTAS = [
    ("cnmc-especifica-circular-precios-electricos",
     "Metodologia de calculo de derechos economicos para conexiones del sector electrico y alquiler de equipos, adaptandose a los objetivos de descarbonizacion y asegurando retornos razonables para las distribuidoras.",
     "Media - afecta a distribuidoras; metodologia de referencia para retribucion de instalaciones."),
    ("cnmc-retribucion-instalaciones-de-distribucion-energia-2023",
     "Audiencia sobre la retribucion de las empresas distribuidoras en 2023, estableciendo los niveles de ingresos regulados de las principales distribuidoras electricas espanolas.",
     "Alta - la metodologia retributiva de distribucion es referencia directa para futuras revisiones."),
    ("miterd-detalle-participacion-publica-k-810",
     "Real Decreto que actualiza los requisitos tecnicos minimos de conexion a la red, incorporando almacenamiento (objetivo 22,5 GW en 2030), renovables e hidrogeno.",
     "Muy alta - define los requisitos tecnicos de conexion que REE debe verificar como gestor de la red de transporte."),
    ("cnmc-permisos-acceso-flexibles-demanda-red-distribucion",
     "Audiencia sobre permisos de acceso flexible para la demanda a las redes de transporte y distribucion, ofreciendo opciones de acceso con limitacion de potencia y menores costes de conexion.",
     "Alta - afecta directamente a la gestion de accesos de la demanda a la red de transporte de REE."),
    ("miterd-detalle-participacion-publica-k-814",
     "Propuesta de Orden que amplia los plazos de ejecucion de proyectos de infraestructuras en municipios de transicion energetica mas alla del 30 de junio de 2026, con financiacion nacional y europea.",
     "Baja-media - afecta a infraestructuras en zonas de transicion donde REE puede tener actuaciones."),
    ("cnmc-circular-distribucion-gas-cirde00325",
     "Metodologia para la retribucion de las instalaciones de distribucion de gas natural, con foco en la adaptacion a la transicion energetica y el marco 2026-2031.",
     "Media - establece el marco retributivo del gas, sector complementario a la red electrica de REE."),
    ("cnmc-cir-transporte-y-regasificacion",
     "Circular que establece los parametros retributivos para las instalaciones de transporte de gas natural y regasificacion para el periodo regulatorio 2026-2031.",
     "Media - el transporte de gas complementa el sistema energetico coordinado por REE como operador del sistema."),
    ("cnmc-prop-de-resolucion-modificacion-po-electrico-75",
     "Propuesta de resolucion que modifica el Procedimiento de Operacion 7.5 del sistema electrico, relativo a la gestion de la respuesta de la demanda en tiempo real.",
     "Alta - afecta directamente a los procedimientos de operacion del sistema gestionados por REE."),
    ("cnmc-consulta-publica-de-modificacion-de-los-po-31-32-y-72-del-sistema",
     "Consulta publica sobre la modificacion de los procedimientos de operacion 3.1, 3.2 y 7.2, que regulan la energia reactiva, las restricciones tecnicas y la calidad del suministro.",
     "Muy alta - REE como Operador del Sistema debe adaptar los POs 3.1, 3.2 y 7.2 directamente afectados."),
    ("cnmc-ajuste-fibra-optica-2026",
     "Consulta sobre el ajuste de los valores retributivos de la fibra optica integrada en la red de transporte electrico para el periodo 2026-2031.",
     "Alta - REE gestiona la infraestructura de fibra optica integrada en la red de transporte electrico."),
    ("cnmc-informe-evaluacion-modelo-retributivo-transporte-gas-y-plantas-gnl",
     "Informe de evaluacion del modelo retributivo vigente para el transporte de gas natural y plantas de GNL, con vistas a la revision del marco regulatorio 2026-2031.",
     "Media - el modelo retributivo del gas es referencia para el diseno de marcos regulatorios del transporte electrico."),
    ("miterd-detalle-participacion-publica-k-820",
     "Consulta previa para elaborar la Orden de bases del concurso de acceso para instalaciones renovables en el nudo de transicion justa Macineira 400 kV.",
     "Alta - afecta al acceso de instalaciones renovables a un nudo de 400 kV de la red de transporte gestionada por REE."),
    ("miterd-detalle-participacion-publica-k-821",
     "Audiencia sobre el proyecto de Real Decreto que modifica el sistema de Certificados de Ahorro Energetico (CAE), actualizando requisitos de eficiencia energetica para instalaciones.",
     "Media - el ahorro energetico puede reducir la demanda total gestionada por REE en la red de transporte."),
    ("miterd-detalle-participacion-publica-k-795",
     "Manifestacion de interes para identificar nudos de la red de transporte vinculados a Transicion Justa donde priorizar la conexion de nuevas instalaciones renovables.",
     "Muy alta - REE gestiona los nudos de la red de transporte sobre los que versara la manifestacion de interes."),
]


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur  = conn.cursor()
    updated = 0
    for slug, resumen, relevancia in CONSULTAS:
        cur.execute(
            "UPDATE regulatory_entries SET summary=%s, impacto_ree=%s WHERE external_id=%s",
            (resumen, relevancia, slug),
        )
        if cur.rowcount:
            print(f"OK  {slug[:60]}")
            updated += cur.rowcount
        else:
            print(f"--- No encontrado: {slug[:60]}")
    conn.commit()
    conn.close()
    print(f"\nActualizadas: {updated} entradas")


if __name__ == "__main__":
    main()
