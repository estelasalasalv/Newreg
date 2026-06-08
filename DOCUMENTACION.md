# Monitor Regulatorio Energético — Documentación técnica

**Proyecto:** New_regulation  
**Repositorio:** https://github.com/estelasalasalv/Newreg  
**Web pública:** GitHub Pages (rama `gh-pages`, directorio `web/`)  
**Última actualización de este documento:** junio 2026

---

## Índice

1. [Propósito y arquitectura general](#1-propósito-y-arquitectura-general)
2. [Estructura de directorios](#2-estructura-de-directorios)
3. [Base de datos PostgreSQL](#3-base-de-datos-postgresql)
4. [Scrapers — fuentes y lógica](#4-scrapers--fuentes-y-lógica)
5. [Orquestador principal (main.py)](#5-orquestador-principal-mainpy)
6. [Clasificación y filtrado regulatorio](#6-clasificación-y-filtrado-regulatorio)
7. [Generación de resúmenes y análisis IA](#7-generación-de-resúmenes-y-análisis-ia)
8. [Utilidades y scripts puntuales](#8-utilidades-y-scripts-puntuales)
9. [Web estática (web/)](#9-web-estática-web)
10. [GitHub Actions — automatización](#10-github-actions--automatización)
11. [Variables de entorno y secretos](#11-variables-de-entorno-y-secretos)
12. [Procedimientos operativos](#12-procedimientos-operativos)

---

## 1. Propósito y arquitectura general

El Monitor Regulatorio Energético descarga, normaliza, almacena y publica información regulatoria del sector energético español y europeo. El sistema opera de forma completamente automatizada: los scrapers se ejecutan dos veces al día mediante GitHub Actions, almacenan los datos en PostgreSQL y generan un JSON estático que alimenta una web filtrable desplegada en GitHub Pages.

### Flujo end-to-end

```
Fuentes externas
  BOE (API + HTML)
  BOE-N (suplemento, PDF)
  CNMC (consultas, actuaciones, RSS)
  MITERD (participación pública)
  ACER (RSS + decisiones)
  EUR-Lex (SPARQL)
        │
        ▼
  Scrapers Python
  (filtrado, clasificación, normalización)
        │
        ▼
  PostgreSQL (Neon)
  (upsert idempotente, 8 tablas)
        │
        ▼
  export_to_json()
        │
        ▼
  web/data.json  ──►  GitHub Pages
                       web estática filtrable
```

### Principios de diseño

- **Idempotencia**: ejecutar dos veces el mismo día no duplica registros (upsert por `external_id`).
- **Resiliencia**: los scrapers no se interrumpen mutuamente ante fallos; CNMC tiene 3 reintentos intercalados.
- **Sin framework ORM**: psycopg2 directo; migraciones mediante `ALTER TABLE IF NOT EXISTS` dentro de `init_db()`.
- **Web sin backend**: `data.json` estático; toda la lógica de visualización es JavaScript puro en `web/index.html`.

---

## 2. Estructura de directorios

```
New_regulation/
├── main.py                     # Orquestador principal
├── requirements.txt
├── .env                        # Secretos locales (excluido del repo)
├── .env.example                # Plantilla de variables de entorno
├── CLAUDE.md                   # Instrucciones para Claude Code
├── DOCUMENTACION.md            # Este archivo
│
├── db/
│   ├── __init__.py
│   └── database.py             # Esquema, upserts y exportación JSON
│
├── scraper/
│   ├── __init__.py
│   ├── boe.py                  # BOE API JSON (Secciones I–IV)
│   ├── boe_anuncios.py         # BOE HTML Sección V (anuncios energéticos)
│   ├── boe_n.py                # BOE-N suplemento (Registros Propiedad + REE)
│   ├── cnmc.py                 # CNMC consultas públicas (paginado)
│   ├── cnmc_n.py               # CNMC actuaciones y noticias energéticas
│   ├── cnmc_rss.py             # CNMC RSS resoluciones y acuerdos
│   ├── eurlex.py               # EUR-Lex vía SPARQL (actos UE)
│   ├── miterd.py               # MITERD participación pública (4 áreas)
│   └── acer.py                 # ACER RSS + decisiones individuales
│
├── web/
│   ├── index.html              # Web estática (JavaScript + JSON)
│   └── data.json               # Exportación generada por export_to_json()
│
├── backfill_historico.py       # Carga histórica puntual (2025-2026)
├── generate_summaries.py       # Genera resúmenes con Claude API
├── generate_ree_analysis.py    # Análisis de impacto sobre funciones REE
├── import_summaries.py         # Importa resúmenes generados externamente
├── import_consultas_summaries.py
├── add_art64_badge.py          # Retroactivo: añade badge Art.64
├── add_nuevo_badge.py          # Retroactivo: añade badge NUEVO
├── seed_ree_funciones.py       # Carga inicial tabla ree_funciones
│
└── .github/
    └── workflows/
        └── scraper.yml         # GitHub Actions: ejecución + deploy
```

---

## 3. Base de datos PostgreSQL

**Driver:** psycopg2 (sin ORM).  
**Host:** Neon (PostgreSQL gestionado en cloud).  
**Conexión:** variable de entorno `DATABASE_URL`.  
**Constante crítica:** `YEAR_FILTER = 2026` — solo datos de 2026 se exportan a la web. Los históricos se conservan en BD.

### 3.1 Tablas

#### `boe_entries` — Entradas del BOE

| Campo | Tipo | Descripción |
|---|---|---|
| id | SERIAL PK | |
| external_id | TEXT UNIQUE | Identificador BOE (p.ej. `BOE-A-2026-12345`) |
| fecha | TEXT | Fecha de publicación (`YYYY-MM-DD`) |
| fuente | TEXT | Siempre `"BOE"` |
| seccion | TEXT | Sección BOE (I, II, III, IV, V) |
| tipo | TEXT | Tipo normativo (Ley, RD, Orden, Resolución…) |
| organismo | TEXT | Organismo emisor |
| subseccion | TEXT | Subsección o epígrafe |
| texto | TEXT | Título del documento |
| enlace | TEXT | URL oficial BOE |
| palabras_clave | TEXT | Keywords energéticas detectadas |
| resumen | TEXT | Resumen manual o extraído |
| importante | TEXT | `"Sí"` / `"No"` |
| acceso_conexion | TEXT | `"Sí"` / `"No"` / `"Acceso/Conexion"` / `"Transporte/Operador"` |
| tramitaciones | TEXT | `"Sí"` / `"No"` |
| publicable | TEXT | `"NO"` (campo heredado) |
| impacto_ree | TEXT | Clasificación impacto sobre REE |
| comprobado | TEXT | `"S"` / `"N"` |
| scraped_at | TIMESTAMP | Momento de captura |

#### `regulatory_entries` — CNMC, MITERD, ACER

| Campo | Tipo | Descripción |
|---|---|---|
| id | SERIAL PK | |
| source | TEXT | `"CNMC_C"` / `"CNMC_N"` / `"CNMC_S"` / `"CNMC_RSS"` / `"MITERD"` / `"ACER"` |
| external_id | TEXT UNIQUE | ID único por fuente |
| title | TEXT | Título del expediente o noticia |
| published_date | TEXT | Fecha de publicación (`YYYY-MM-DD`) |
| url | TEXT | URL oficial |
| section | TEXT | Sección o categoría dentro de la fuente |
| department | TEXT | Organismo o dirección |
| summary | TEXT | Resumen extraído o generado por IA |
| tipo | TEXT | `"consulta"` / `"regulacion"` / `"Decisión ACER"` / etc. |
| plazo | TEXT | Fecha límite de consulta pública |
| estado | TEXT | `"Abierta"` / `"Cerrada"` |
| sector | TEXT | `"electricidad"` / `"gas"` / `"otro"` |
| importante | TEXT | `"Sí"` / `"No"` |
| comprobado | TEXT | `"S"` / `"N"` |
| tramitaciones | TEXT | `"Sí"` / `"No"` |
| impacto_ree | TEXT | Análisis de impacto sobre REE |
| scraped_at | TIMESTAMP | Momento de captura |

#### `eurlex_entries` — Normativa europea (DOUE)

| Campo | Tipo | Descripción |
|---|---|---|
| id | SERIAL PK | |
| external_id | TEXT UNIQUE | ID derivado del cellar URI |
| fecha | TEXT | Fecha de publicación en DOUE |
| texto | TEXT | Título (preferentemente en español) |
| enlace | TEXT | URL EUR-Lex |
| tipo | TEXT | `"Reglamento (UE)"` / `"Directiva (UE)"` / `"Decisión (UE)"` / `"Acto Comité de las Regiones (UE)"` / etc. |
| fuente | TEXT | `"DOUE"` / `"NoticiaCE"` |
| importante | TEXT | `"Sí"` / `"No"` |
| comprobado | TEXT | `"S"` / `"N"` |
| resumen | TEXT | Resumen generado por IA |
| impacto_ree | TEXT | |
| scraped_at | TIMESTAMP | |

#### `boe_n_entries` — Anuncios BOE-N con Red Eléctrica confirmada

Anuncios de Registros de la Propiedad del Suplemento de Notificaciones del BOE en cuyo PDF se ha confirmado la mención a "Red Eléctrica" / "REE" / "Redeia". Se promueven a `boe_entries` si se confirma.

#### `boe_n_descarte` — Anuncios BOE-N sin REE

Anuncios procesados cuyo PDF no contiene referencia a Red Eléctrica. Se conservan para auditoría.

#### `boe_rechazos` — Normativa descartada

Entradas que fueron rechazadas por los filtros de clasificación. Permite auditar falsos negativos.

#### `ree_funciones` — Funciones de Red Eléctrica (tabla de referencia)

| Campo | Tipo |
|---|---|
| id | SERIAL PK |
| categoria | TEXT (`"transportista"` / `"operador"`) |
| actividad | TEXT |
| descripcion | TEXT |
| keywords | TEXT |

#### `ree_normativa_funciones` — Relación normativa ↔ funciones REE

Vincula cada entrada normativa (BOE, regulatory, EUR-Lex) con las funciones REE que afecta, generado por análisis con Claude API.

### 3.2 Funciones principales de `db/database.py`

| Función | Descripción |
|---|---|
| `init_db()` | Crea/valida esquema; añade columnas con `ALTER TABLE IF NOT EXISTS` |
| `purge_excluded()` | Elimina entradas spam ya identificadas |
| `backfill_sentencias()` | Enriquece títulos CNMC con nº de expediente |
| `upsert_boe(entries)` | Inserta entradas BOE; `ON CONFLICT DO NOTHING` |
| `upsert_entries(entries)` | Inserta CNMC/genéricas; actualiza `plazo` si cambia |
| `upsert_eurlex(entries)` | Inserta normativa UE |
| `upsert_boe_n_staging(entries)` | Inserta anuncios BOE-N en tabla temporal |
| `promote_boe_n(all_ids, ree_ids)` | Mueve BOE-N: con REE → `boe_entries`; sin REE → `boe_n_descarte` |
| `fetch_recent(limit)` | BOE + CNMC combinadas (pestaña "Todas") |
| `fetch_acceso_conexion()` | Entradas con acceso/conexión a la red |
| `fetch_eurlex(limit)` | Normativa europea filtrada por `YEAR_FILTER` |
| `fetch_cnmc_consultas()` | Consultas públicas abiertas (CNMC + MITERD) |
| `export_to_json(path, limit)` | Genera `web/data.json` con todas las pestañas |
| `backfill_pub_dates_from_rss()` | Actualiza fechas de publicación CNMC cruzando con RSS |

### 3.3 Secciones exportadas a `data.json`

| Clave JSON | Contenido | Filtro aplicado |
|---|---|---|
| `entries` | BOE + CNMC 2026 | `YEAR_FILTER` |
| `boe_trimestre` | BOE últimos 92 días | — |
| `reg_espanola` | BOE + CNMC Q1 2026 | — |
| `cnmc_consultas` | Consultas CNMC + MITERD | Abiertas y cerradas |
| `acceso_conexion` | Acceso/Conexión 2026 | `acceso_conexion = "Sí"` |
| `eurlex` | Normativa europea 2026 | `YEAR_FILTER` |
| `cnmc_all` / `cnmc_s` / `cnmc_n` | Actuaciones CNMC por fuente | — |
| `acer` | Publicaciones ACER | — |

---

## 4. Scrapers — fuentes y lógica

### 4.1 BOE — API oficial (`scraper/boe.py`)

**Fuente:** `https://www.boe.es/datosabiertos/api/boe/sumario/{YYYYMMDD}`  
**Secciones cubiertas:** I (Disposiciones generales), II (Autoridades y personal), III (Otras disposiciones), IV (Administración de Justicia)  
**Frecuencia:** Diaria (1 día; 3 días los lunes para capturar el sábado)

#### Flujo de filtrado

1. **Exclusión de subsecciones**: RTVE, seguros, telecomunicaciones.
2. **Exclusión de organismos**: AENA, universidades, Renfe, aeropuertos.
3. **Exclusión de títulos**: eficiencia servicio público, elecciones, estadística.
4. **Nombramientos/ceses**: solo se aceptan de Presidencia del Gobierno, MITERD, CNMC o Secretaría de Estado de Energía.
5. **Keywords energéticas** (`_find_keywords`): 157 términos compilados (Red Eléctrica, electricidad, energía, renovables, hidrógeno, gas natural, peajes, retribución transporte, etc.).
6. **Leyes y Decretos-ley**: se descarga el texto completo del BOE para verificar presencia de keywords o materias específicas (`_MATERIAS_RDL_RAW`) antes de incluir.
7. **Órdenes MITERD** (`TED`/`TEC`/`ITE`): incluidas automáticamente si el organismo es MITERD.

#### Clasificación de campos

| Campo | Función | Criterio |
|---|---|---|
| `tipo` | `_detect_tipo()` | Detecta Ley, RD-ley, RD, Orden, Resolución, Circular… por epígrafe y título |
| `importante` | `_is_importante()` | `"Sí"` para Ley, RD-ley, RD, Circular o infracción Art.64 |
| `acceso_conexion` | `_detect_acceso()` | `"Sí"` si menciona acceso, conexión, peaje o red de transporte |
| `tramitaciones` | `_detect_tramitaciones()` | `"Sí"` si es autorización previa (AAP), autorización de construcción (AAC), declaración de utilidad pública o sentencia |

---

### 4.2 BOE Sección V — Anuncios (`scraper/boe_anuncios.py`)

**Fuente:** `https://www.boe.es/boe/dias/{y}/{m}/{d}/index.php` (HTML)  
**Contenido:** Anuncios de información pública de la Sección V (Subdelegaciones del Gobierno, proyectos energéticos)

#### Estrategia de descarga en dos pasos

1. Se filtra primero con el **texto corto del índice** (sin petición adicional).
2. Solo si pasa el filtro (o es un Registro de la Propiedad), se descarga el **título completo** desde la página individual.

Esto evita miles de peticiones innecesarias.

#### Filtros aplicados (en orden)

1. `_EXCLUDED_RE`: Excluye pan, correos, carreteras, ferrocarril, sanidad, ADIF, Renfe.
2. `_EXCLUDED_CONTRATOS_RE`: Excluye licitaciones, adjudicaciones, formalizaciones de contratos.
3. `_EXCLUDED_TRABAJOS_RE`: Excluye autorizaciones de limpieza, dragado, trabajos puntuales en aprovechamientos (no aportan valor regulatorio aunque mencionen "hidroeléctrico").
4. `_ENERGY_SPECIFIC_RE`: Requiere al menos un término específicamente energético: línea eléctrica, subestación, parque solar/eólico, almacenamiento, notas de afección, peajes, liquidaciones sistema eléctrico, etc.

---

### 4.3 BOE-N — Suplemento de Notificaciones (`scraper/boe_n.py`)

**Fuente:** Suplemento de Notificaciones del BOE (índice HTML + PDFs individuales)  
**Objetivo:** Capturar anuncios de Registros de la Propiedad que afectan a instalaciones de Red Eléctrica de España / REE / Redeia.

#### Flujo de dos fases

**Fase 1 — Extracción (scrape)**  
Descarga el HTML del índice diario del suplemento BOE-N y extrae todos los anuncios de Registros de la Propiedad (y algunas Delegaciones del Gobierno con expropiación forzosa).

**Fase 2 — Verificación REE (filter_ree)**  
Lee los PDFs de cada anuncio en paralelo (hasta 8 workers concurrentes) buscando el patrón:
```python
_REE_RE = re.compile(r"red el[eé]ctrica|\bredeia\b|\bREE\b", re.IGNORECASE)
```
- Anuncios **con REE** → se promueven a `boe_entries` (visibles en la web).
- Anuncios **sin REE** → van a `boe_n_descarte` (guardados para auditoría, no visibles).

La fusión visual de anuncios con el mismo título/fecha/organismo (como "Registro de la Propiedad de Icod de los Vinos — 3 anuncios con distinto ID") se realiza en el frontend con `dedupeBoeNotif()`.

---

### 4.4 CNMC — Consultas públicas (`scraper/cnmc.py`)

**Fuente:** `https://www.cnmc.es/consultas-publicas/energia` (HTML, paginado)  
**Fuente tipo:** Consultas públicas abiertas/cerradas en materia energética

#### Filtrado

- **Incluye** si contiene alguno de 24 términos energéticos: energía, electricidad, gas, renovables, hidrógeno, red eléctrica, transporte, distribución, retribución, almacenamiento, nuclear, etc.
- **Excluye** audiovisual, telecomunicaciones, sector postal, ferroviario.

#### Enriquecimiento

- Para expedientes de sentencias (título genérico): navega a la página de detalle para obtener el número de expediente real.
- Para cada consulta: navega a su página individual para obtener el **plazo de remisión**.

**Campos:** `source="CNMC_C"`, `tipo="consulta"`, `estado` (Abierta/Cerrada), `sector`, `plazo`.

---

### 4.5 CNMC — Actuaciones y noticias (`scraper/cnmc_n.py`)

Tres subfuentes combinadas en un único módulo:

| Subfuente | Función | URL |
|---|---|---|
| **CNMC_N** | `_scrape_actuaciones()` | Portal de transparencia CNMC, ámbito energía (`idambito=9`) |
| **CNMC_S** | `scrape_cnmc_s()` | Todas las actuaciones energéticas CNMC (primeras N páginas) |
| Noticias | `scrape_noticias()` | Noticias energía CNMC recientes |

**Nota sobre fechas:** Las actuaciones CNMC tienen la fecha del acto, no la de publicación web. Los actos recientes pueden tardar semanas en aparecer en el portal, por eso se trabaja con los 50 más recientes sin filtro de fecha (el upsert por `external_id` evita duplicados).

El módulo `scrape_cnmc_s()` actúa como fuente principal de actuaciones; los datos de CNMC_N complementan con información de detalle de expediente.

---

### 4.6 CNMC — RSS (`scraper/cnmc_rss.py`)

**Fuente:** `https://www.cnmc.es/rss.xml`  
**Contenido:** Resoluciones y acuerdos del Consejo de la CNMC

#### Flujo

1. Descarga y parsea el feed RSS.
2. Filtra por palabras clave energéticas (`_is_energy_relevant`).
3. Para cada entrada, navega a la **página individual** para obtener:
   - Número de expediente (referencia formal).
   - Fecha real de publicación web.
   - Ámbito regulatorio.
   - Clasificación de riesgo Art.64 LSE.

#### Clasificación Art.64 LSE

Detecta si la resolución implica riesgo para el suministro eléctrico:

| Valor | Descripción |
|---|---|
| `"Sin riesgo GS"` | Infracción sin riesgo para el suministro |
| `"Con riesgo GS"` | Infracción con riesgo para el suministro |
| `"Con daño grave"` | Infracción con daño grave al suministro |
| `"Sin clasificar"` | No se pudo determinar |

Las entradas con Art.64 se marcan automáticamente como `importante = "Sí"`.

---

### 4.7 MITERD — Participación pública (`scraper/miterd.py`)

**Fuentes (4 áreas del Ministerio):**

| URL | Área |
|---|---|
| `.../es/energia/participacion.html` | Energía |
| `.../es/cambio-climatico/participacion-publica.html` | Cambio climático |
| `.../es/calidad-y-evaluacion-ambiental/participacion-publica.html` | Calidad ambiental |
| `.../es/costas/participacion-publica.html` | Costas |

Para cada consulta navega a la página individual para extraer el **plazo de remisión** (`_fetch_plazo`).

El estado (Abierta/Cerrada) se detecta por:
- Etiquetas `h2` ("Consultas abiertas" / "Consultas cerradas").
- Fecha de cierre del plazo (si ya venció → Cerrada).
- Para costas: el estado aparece en el propio título como `[DD/MM-DD/MM/YYYY]`.

---

### 4.8 ACER — Agencia de Reguladores de Energía (`scraper/acer.py`)

**Fuentes:**

| Subfuente | Función | URL |
|---|---|---|
| RSS | `scrape_rss()` | `https://www.acer.europa.eu/rss.xml` |
| Decisiones individuales | `scrape_decisions()` | `.../documents/official-documents/individual-decisions` |

#### Particularidades técnicas

El RSS de ACER tiene **formato no estándar**:
- El `<title>` contiene HTML embebido (`<a href="...">Texto</a>`) en lugar de texto plano.
- Las fechas usan el formato `"Thu, 04/30/2026 - 10:14"`.

El parser extrae el enlace y el texto del elemento `<a>` hijo, y normaliza la fecha con regex.

#### Traducción automática (inglés → español)

1. **Primera opción**: llama a la API gratuita de Google Translate (`translate.googleapis.com`).
2. **Fallback**: diccionario local con ~80 patrones de términos frecuentes de ACER (decisión, informe, electricidad, gas natural, interconexión, mercado mayorista, etc.).

#### Clasificación de tipo

`"Decisión ACER"` / `"Informe ACER"` / `"Opinión ACER"` / `"Consulta ACER"` / `"Publicación ACER"` según palabras clave del título.

---

### 4.9 EUR-Lex / DOUE (`scraper/eurlex.py`)

**Fuente:** SPARQL endpoint del Publications Office de la UE  
`https://publications.europa.eu/webapi/rdf/sparql`

#### Query SPARQL

Pide actos publicados en los últimos 7 días (margen para compensar el lag de indexación del endpoint SPARQL) que cumplan dos filtros simultáneos:

**Filtro de tipo de acto** — el título debe contener uno de:
- `(UE)`, `(EU)`, `(Euratom)` → actos legislativos UE
- `COMMISSION NOTICE` / `Commission notice` → avisos de la Comisión
- `Court of Auditors` / `Tribunal de Cuentas` → Tribunal de Cuentas UE
- `Committee of the Regions` / `Comité de las Regiones` → Comité de las Regiones
- `European Parliament` / `Parlamento Europeo` → Parlamento Europeo
- `Economic and Social Committee` → CESE

**Filtro temático** — el título debe contener alguna de ~35 palabras clave energéticas: `energ`, `electr`, `renew`, `renovable`, `hidrog`, `hydrogen`, `emission`, `climate`, `solar`, `wind`, `gas natural`, `carbon`, `decarboni`, `biofuel`, `biomass`, `net zero`, `storage`, `grid`, `power`, `nuclear`, `eficiencia`, `efficiency`, `greenhouse`, `fotovoltai`, `offshore`, `eolic`, `aerogen`, `taxonomy`, `remit`, `omnibus`…

#### Procesamiento de resultados (`_process_bindings`)

1. **Agrupa** los títulos por URI del documento (`work`).
2. **Selecciona el idioma** en orden de preferencia: español → inglés → cualquiera.
3. **Excluye** documentos no legislativos: informes de staff, comunicaciones, actas, minutos, agenda.
4. **Excluye** legislación nacional que se cuela en los resultados (decretos portugueses, italianos, franceses, alemanes…).
5. **Deduplica** por título normalizado.

#### Tipos de acto reconocidos

| Tipo | Etiqueta asignada |
|---|---|
| Reglamento de Ejecución (UE) | `Reglamento de Ejecución (UE)` |
| Reglamento Delegado (UE) | `Reglamento Delegado (UE)` |
| Reglamento (UE) | `Reglamento (UE)` |
| Directiva (UE) | `Directiva (UE)` |
| Decisión de Ejecución (UE) | `Decisión de Ejecución (UE)` |
| Decisión Delegada (UE) | `Decisión Delegada (UE)` |
| Decisión (UE) | `Decisión (UE)` |
| Recomendación (UE) | `Recomendación (UE)` |
| Aviso/Comunicación Comisión | `Aviso/Comunicación UE` |
| Dictamen Tribunal de Cuentas | `Dictamen Tribunal de Cuentas (UE)` |
| Acto Comité de las Regiones | `Acto Comité de las Regiones (UE)` |
| Acto Parlamento Europeo | `Acto Parlamento Europeo` |
| Dictamen CESE | `Dictamen CESE (UE)` |
| Otros | `Acto UE` |

**Nota sobre lag de indexación**: el SPARQL del Publications Office suele tardar entre 2 y 7 días en indexar un acto tras su publicación en el DOUE. Por eso el scraper pide siempre los últimos 7 días, no solo el día actual.

---

## 5. Orquestador principal (main.py)

El orquestador llama a los scrapers en un orden diseñado para maximizar la coherencia de datos y la resiliencia ante fallos.

```
1.  init_db()                      Crea/valida esquema
2.  purge_excluded()               Elimina spam ya identificado
3.  backfill_sentencias()          Enriquece títulos CNMC con nº expediente

4.  EUR-Lex scrape(days_back=7)    Captura actos UE con margen de 7 días
5.  BOE Anuncios scrape(days_back) Sección V (1 día; 3 días los lunes)
6.  BOE scrape(days_back)          API oficial secciones I–IV

7.  CNMC (intento 1/3)             Consultas públicas
8.  MITERD scrape()                Participación pública ministerio
9.  CNMC (intento 2/3)             Solo si el intento 1 falló
10. CNMC_N scrape(days_back=7)     Actuaciones energéticas
11. CNMC_S scrape_cnmc_s(pages=2)  Actuaciones (fuente secundaria)
12. CNMC RSS scrape()              Resoluciones y acuerdos
13. backfill_pub_dates_from_rss()  Actualiza fechas CNMC desde RSS
14. CNMC (intento 3/3)             Solo si los intentos 1 y 2 fallaron

15. BOE-N staging + filter_ree()   Anuncios Registro Propiedad + PDFs paralelos
16. ACER scrape(days_back=2)       RSS + decisiones

17. export_to_json()               Genera web/data.json
```

### Lógica del lunes

Los lunes, `_boe_days = 3` en lugar de 1, para capturar también los anuncios del sábado anterior que no se procesaron durante el fin de semana.

### Resiliencia de CNMC

CNMC.es tiene tiempos de respuesta variables y puede fallar por sobrecarga. Para mitigarlo, el scraper se ejecuta hasta 3 veces en momentos distintos del pipeline (antes de MITERD, después de MITERD, después de CNMC_N). Si los 3 intentos fallan, se registra el error y el pipeline continúa sin datos CNMC ese día.

---

## 6. Clasificación y filtrado regulatorio

### 6.1 Palabras clave energéticas (BOE)

El scraper BOE compila 157 expresiones regulares en `_KW_PATTERNS` que cubren todo el vocabulario del sector eléctrico y energético español:

**Infraestructuras:** Red Eléctrica de España, REE, Redeia, red de transporte, red de distribución, subestación, línea eléctrica, interconexión…

**Marco regulatorio:** Ley del Sector Eléctrico, peajes de acceso, cargos del sistema, retribución de la actividad de transporte, distribución, retribución de las energías renovables…

**Tecnologías:** fotovoltaico, eólico, hidráulico, almacenamiento de energía, hidrógeno renovable, biometano, cogeneración…

**Mercados:** mercado eléctrico, mercado ibérico, mercado mayorista, OMIE, precio de la energía…

**Organismos reguladores:** CNMC, MITERD, Secretaría de Estado de Energía, Comisión Nacional de Energía…

### 6.2 Filtro gas/hidróg./combustible (`esGasOnly`)

Implementado en `web/index.html`, determina si una entrada es **exclusivamente** de gas/hidrógeno/combustibles (para ocultarla cuando el usuario activa el filtro "Ocultar gas, hidrog, combust..."):

```javascript
function esGasOnly(e) {
  // 1. Debe contener términos de gas/_GAS_RE
  // 2. No debe contener términos eléctricos/_ELEC_RE
  // 3. No aplica a sistemas insulares/extrapeninsulares (_ISLAS_RE)
}
```

**`_GAS_RE`** incluye: gasoducto, biometano, GNL, regasificación, gas natural, sistema gasista, Enagas, hidrógeno, electrolizador, RFNBO, calefacción y refrigeración, materias primas minerales, carbón térmico, fuel-oil, gasoil, etc.

**`_ELEC_RE`** incluye: eléctric-, fotovolt-, aerogen-, parque solar/eólico, subestación, kV, línea de transporte.

### 6.3 Detección de acceso y conexión

`_detect_acceso()` en `boe.py` y el equivalente en `boe_anuncios.py` clasifica:

| Valor | Criterio |
|---|---|
| `"Acceso/Conexion"` | Menciona acceso, conexión, interconexión o peaje de acceso |
| `"Transporte/Operador"` | Menciona línea de transporte, subestación o red de transporte |
| `"No"` | No aplica |

### 6.4 Detección de tramitaciones

`_detect_tramitaciones()` identifica procedimientos de autorización:
- **AAP**: Autorización Administrativa Previa
- **AAC**: Autorización Administrativa de Construcción
- Declaración de utilidad pública
- Actas previas / levantamiento de actas (expropiación)
- Sentencias / resoluciones judiciales sobre instalaciones

---

## 7. Generación de resúmenes y análisis IA

### 7.1 `generate_summaries.py` — Resúmenes automáticos

Genera resúmenes de texto para entradas de todas las fuentes usando **Claude Haiku** (`claude-haiku-4-5-20251001`).

**Fuentes cubiertas:** BOE, CNMC_C, CNMC_RSS, CNMC_N, ACER, MITERD, EUR-Lex.

**Estructura de respuesta esperada (JSON):**
```json
{
  "resumen": "2-3 frases sobre QUÉ establece la normativa",
  "impacto_tso": "2-3 frases sobre afectación a Red Eléctrica como transportista/operador"
}
```

**Almacenamiento:**
- `boe_entries.resumen` + `boe_entries.impacto_ree`
- `regulatory_entries.summary` + `regulatory_entries.impacto_ree`
- `eurlex_entries.resumen` + `eurlex_entries.impacto_ree`

El script está **desactivado en el workflow** por defecto (requiere `ANTHROPIC_API_KEY`). Se activa descomentando el paso correspondiente en `scraper.yml`.

### 7.2 `generate_ree_analysis.py` — Análisis de impacto sobre funciones REE

Análisis más detallado que mapea cada entrada normativa a las **funciones específicas de Red Eléctrica** que afecta (transportista o gestor del sistema / operador del sistema).

**Modelo:** Claude Haiku. **Max tokens:** 1024.

**Estructura de respuesta:**
```json
{
  "resumen": "2-3 frases sobre qué regula",
  "funciones_afectadas": [
    {"funcion_id": 3, "justificacion": "Afecta al acceso de terceros a la red de transporte"},
    {"funcion_id": 7, "justificacion": "Modifica criterios de retribución del operador"}
  ]
}
```

Las relaciones se almacenan en `ree_normativa_funciones`. Los análisis fallidos van a la cola `ree_analisis_pendiente` (hasta 5 reintentos).

---

## 8. Utilidades y scripts puntuales

| Script | Propósito |
|---|---|
| `backfill_historico.py` | Carga histórica 2025-2026: recorre todos los días laborables y llama al scraper BOE para cada uno. Acepta `--desde`/`--hasta` para rangos específicos. |
| `add_art64_badge.py` | Añade retroactivamente la clasificación Art.64 a entradas ya existentes en BD. |
| `add_nuevo_badge.py` | Marca retroactivamente como `nuevo=true` entradas recientes. |
| `import_summaries.py` | Importa resúmenes generados externamente (fichero JSON → BD). |
| `import_consultas_summaries.py` | Ídem para consultas CNMC. |
| `seed_ree_funciones.py` | Carga inicial de la tabla `ree_funciones` con las funciones de Red Eléctrica como transportista y operador del sistema. |

---

## 9. Web estática (web/)

La web carga `data.json` y muestra los datos sin backend. Todo el procesamiento es JavaScript puro en `web/index.html` (~2300 líneas).

### 9.1 Pestañas y fuentes de datos

| Pestaña (panel) | Datos | Funciones de render |
|---|---|---|
| Todas | `entries` (BOE + CNMC 2026) | `renderTodas()` |
| BOE Trimestre | `boe_trimestre` | `renderEsp()` |
| Normativa española | `reg_espanola` | `renderEsp()` |
| Consultas CNMC | `cnmc_consultas` | `renderConsultas()` |
| Acceso / Conexión | `acceso_conexion` | `renderAcceso()` |
| Normativa Europea | `eurlex` | `renderEu()` |
| ACER | `acer` | `renderAcer()` |
| CNMC Actuaciones | `cnmc_all`/`cnmc_s`/`cnmc_n` | `renderCnmc()` / `renderCnmcS()` / `renderCnmcN()` |
| CNMC RSS | `cnmc_rss` | `renderCnmcRss()` |
| Resumen mensual | todas las anteriores | `renderResumen()` |

### 9.2 Filtros globales

Los siguientes filtros se aplican de forma consistente en **todas** las pestañas y en el Resumen mensual:

| Checkbox | Variable | Efecto |
|---|---|---|
| Solo importantes | `soloImportante` | Oculta entradas con `importante ≠ "Sí"` |
| Ocultar tramitaciones | `ocultarTramitaciones` | Oculta entradas con `tramitaciones = "Sí"` |
| Ocultar gas, hidrog, combust… | `ocultarGas` | Oculta entradas que `esGasOnly()` clasifica como exclusivamente gasistas/combustibles |

**`ocultarGas = true` por defecto** al cargar la web.

### 9.3 Funciones de agrupación visual

#### `dedupeBoeNotif(list, titleField, fechaField, orgField)`
Fusiona anuncios BOE-N (Suplemento de Notificaciones) que comparten **mismo título + misma fecha + mismo organismo** pero tienen distinto `external_id`, mostrándolos como una única tarjeta con el sufijo "(N anuncios con distinto ID)".

Aplicada en: `renderTodas`, `renderEsp` (Normativa española), `renderResumen` (sección nacional).

#### `groupCnmcByCode(list)` / `cnmcGroupCardHtml(g)` / `cnmcGroupCards(list)`
Agrupa entradas CNMC por número de expediente (extraído con regex de los títulos), fusionando las entradas `CNMC_S`, `CNMC_RSS` y `CNMC_N` del mismo expediente en una única tarjeta con chips de fuente clicables.

Aplicada en: `renderCnmc`, `renderEsp` (sección CNMC), `renderResumen` (sección nacional).

### 9.4 Lógica de período (selectores de días)

Las pestañas EU, ACER, CNMC y BOE Trimestre tienen un **selector de rango de días** con detección automática del lunes:

- Si hoy es lunes, `diasEfectivos = dias + 2` (para incluir también el fin de semana anterior).
- Se muestra una nota informativa al usuario cuando aplica esta ampliación.

El **Resumen mensual** tiene una lógica análoga para el período "Hoy": si es lunes, muestra también el sábado anterior y emite un aviso tipo toast explicándolo.

---

## 10. GitHub Actions — automatización

**Archivo:** `.github/workflows/scraper.yml`

### Programación

| Cron | Hora programada | Nota |
|---|---|---|
| `45 5 * * 1-6` | 07:45 CEST (lun–sáb) | Adelantada 10 min por retraso habitual de GitHub Actions |
| `50 9 * * 1-5` | 11:50 CEST (lun–vie) | Ídem |
| `workflow_dispatch` | Manual | Disponible en la interfaz web de GitHub |

**Nota sobre el retraso de GitHub Actions**: los triggers `schedule` en repositorios con poca actividad son retrasados por GitHub entre 2 y 6 horas respecto a la hora configurada. No es un error del workflow — es una limitación de la plataforma.

### Pasos del workflow

| Paso | Descripción | `continue-on-error` |
|---|---|---|
| Checkout | `actions/checkout@v4` | — |
| Python 3.12 | `actions/setup-python@v5` con caché pip | — |
| Install deps | `pip install -r requirements.txt` | — |
| **Scrape + export** | `python main.py` → PostgreSQL → `web/data.json` | `false` (fallo crítico) |
| Resúmenes IA | `python generate_summaries.py` (desactivado) | `true` |
| Análisis REE | `python generate_ree_analysis.py` (desactivado) | `true` |
| **Deploy** | `peaceiris/actions-gh-pages@v4` → rama `gh-pages` | — |

### Secrets requeridos

| Secret | Uso |
|---|---|
| `BBDD` | `DATABASE_URL` — cadena de conexión PostgreSQL (Neon) |
| `GITHUB_TOKEN` | Deploy a GitHub Pages (automático, no necesita configuración) |
| `ANTHROPIC_API_KEY` | Claude API para resúmenes IA (solo cuando se activen esos pasos) |

---

## 11. Variables de entorno y secretos

```env
# Obligatorio — cadena de conexión PostgreSQL
DATABASE_URL=postgresql://usuario:password@host:5432/base

# Opcional — solo para generate_summaries.py y generate_ree_analysis.py
ANTHROPIC_API_KEY=sk-ant-…
```

El fichero `.env` está excluido del repositorio por `.gitignore`. En producción (GitHub Actions) los valores se configuran como Secrets del repositorio.

---

## 12. Procedimientos operativos

### Ejecutar el pipeline completo en local

```bash
# Requiere DATABASE_URL en .env
python main.py
```

El proceso tarda entre 3 y 8 minutos dependiendo de la respuesta de CNMC. Al terminar genera `web/data.json` y loguea un resumen de entradas nuevas.

### Probar la web localmente

```bash
cd web
python -m http.server 8000
# Abrir http://localhost:8000
```

### Backfill histórico

```bash
# Cargar todo 2025 y 2026
python backfill_historico.py --desde 2025-01-01

# Solo un rango específico
python backfill_historico.py --desde 2026-01-01 --hasta 2026-03-31 --solo-boe
```

### Forzar ejecución del scraper en GitHub

Desde la interfaz web de GitHub: Actions → Regulatory Scraper → Run workflow.

### Añadir una nueva fuente

1. Crear `scraper/nueva_fuente.py` con función `scrape()` que devuelve `List[Dict]`.
2. Añadir el campo `external_id` único y `source` consistente.
3. Añadir llamada en `main.py` con `upsert_entries()` o `upsert_boe()`.
4. Si requiere nueva tabla: añadir `CREATE TABLE IF NOT EXISTS` y `upsert_*()` en `db/database.py`.
5. Añadir sección en `export_to_json()` si deben aparecer en la web.
6. Añadir pestaña y función de render en `web/index.html`.

### Modificar el esquema de la BD

Solo mediante `ALTER TABLE IF NOT EXISTS` dentro de `init_db()` en `db/database.py`. Nunca con `DROP`/`RENAME` sin copia de seguridad previa. Actualizar también `export_to_json()` y la web si el campo afecta a la visualización.

### Verificar duplicados

```sql
-- BOE
SELECT COUNT(*), COUNT(DISTINCT external_id) FROM boe_entries;

-- CNMC
SELECT source, COUNT(*) FROM regulatory_entries GROUP BY source ORDER BY source;

-- EUR-Lex
SELECT COUNT(*), COUNT(DISTINCT external_id) FROM eurlex_entries;
```

### Comprobar sintaxis Python

```bash
python -m compileall .
```

---

*Documento generado a partir del código fuente del proyecto en junio 2026.*

---

© 2026 Estela Salas Alvaro. Todos los derechos reservados.  
El código fuente, la documentación y los datos de este proyecto son propiedad de su autora.  
Queda prohibida su reproducción, distribución o uso sin autorización expresa.
