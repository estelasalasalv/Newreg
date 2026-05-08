# CLAUDE.md — Monitor Regulatorio Energético

## Propósito del proyecto

Este proyecto automatiza la descarga, normalización, almacenamiento y publicación web de información regulatoria procedente de:

- **BOE** — API oficial de datos abiertos (Sección I–IV) y scraping HTML (Sección V anuncios).
- **CNMC** — Scraping web de consultas públicas y feed RSS de resoluciones y acuerdos.
- **DOUE / EUR-Lex** — SPARQL endpoint de la Oficina de Publicaciones de la UE.
- **MITERD** — Scraping de la página de participación pública del Ministerio.

El resultado es una base de datos PostgreSQL con publicaciones jurídicas y regulatorias del sector energético, exportada como JSON estático y publicada en GitHub Pages como web filtrable.

---

## Estructura real del proyecto

```text
New_regulation/
├─ main.py                    # Orquestador principal
├─ requirements.txt
├─ .env                       # Secretos locales (nunca al repo)
├─ .env.example               # Plantilla de variables de entorno
├─ .gitignore
├─ README.md
├─ CLAUDE.md
├─ db/
│  ├─ __init__.py
│  └─ database.py             # Esquema PostgreSQL, upserts y exportación JSON
├─ scraper/
│  ├─ __init__.py
│  ├─ boe.py                  # BOE API JSON (Secciones I–IV)
│  ├─ boe_anuncios.py         # BOE HTML Sección V (anuncios energéticos)
│  ├─ cnmc.py                 # CNMC consultas públicas (paginado)
│  ├─ cnmc_rss.py             # CNMC RSS resoluciones y acuerdos
│  ├─ eurlex.py               # EUR-Lex vía SPARQL (actos UE)
│  └─ miterd.py               # MITERD participación pública
├─ web/
│  ├─ index.html              # Web estática (JavaScript + JSON)
│  └─ data.json               # Exportación generada por export_to_json()
├─ backfill_historico.py      # Carga histórica puntual
├─ generate_summaries.py      # Genera resúmenes con Claude API
├─ import_summaries.py        # Importa resúmenes generados
├─ add_art64_badge.py         # Retroactivo: añade badge Art.64
├─ add_nuevo_badge.py         # Retroactivo: añade badge NUEVO
└─ .github/
   └─ workflows/
      └─ scraper.yml          # GitHub Actions: ejecución + deploy GitHub Pages
```

No reorganizar esta estructura sin proponer un plan previo.

---

## Flujo funcional real (main.py)

```
init_db()                          # Crea/valida esquema PostgreSQL
purge_excluded()                   # Elimina entradas spam ya identificadas
backfill_expedientes_sentencias()  # Enriquece títulos CNMC con nº expediente

Scrapers (en orden):
  1. eurlex.py          → actos UE (7 días atrás, para cubrir lag SPARQL)
  2. boe_anuncios.py    → Sección V BOE (1 día, HTML)
  3. boe.py             → BOE API oficial (1 día, JSON)
  4. cnmc.py            → consultas públicas CNMC (5 páginas)
  5. miterd.py          → participación pública MITERD
  6. cnmc_rss.py        → RSS CNMC resoluciones y acuerdos

export_to_json()                   # Genera web/data.json
upsert_boe() / upsert_entries() / upsert_eurlex()  # Guarda en PostgreSQL
```

El proceso es idempotente: ejecutar dos veces el mismo día no duplica registros.

---

## Base de datos PostgreSQL

**Driver:** psycopg2 (sin ORM, sin Alembic). Las migraciones se hacen con `ALTER TABLE IF NOT EXISTS` dentro de `init_db()`.

### Tablas reales

#### `boe_entries`
| Campo | Tipo | Notas |
|---|---|---|
| id | SERIAL PK | |
| external_id | TEXT UNIQUE | Identificador BOE (BOE-A-2026-XXXXX) |
| fecha | TEXT | Fecha de publicación (YYYY-MM-DD) |
| titulo | TEXT | Título del documento |
| texto | TEXT | Resumen o texto breve |
| enlace | TEXT | URL oficial BOE |
| departamento | TEXT | Organismo emisor |
| rango | TEXT | Tipo normativo (Ley, RD, Orden…) |
| importante | TEXT | "Sí" / "No" |
| acceso_conexion | TEXT | "Sí" / "No" |
| tramitaciones | TEXT | "Sí" / "No" |
| impacto_ree | TEXT | Clasificación Art.64 LSE |
| comprobado | TEXT | "S" / "N" |
| es_anuncio | BOOLEAN | TRUE si procede de Sección V |
| nuevo | BOOLEAN | |
| resumen_ia | TEXT | Generado por Claude API |

#### `regulatory_entries`
| Campo | Tipo | Notas |
|---|---|---|
| id | SERIAL PK | |
| source | TEXT | "CNMC" / "MITERD" / "CNMC_RSS" |
| external_id | TEXT | ID único por fuente |
| title | TEXT | |
| url | TEXT | URL oficial |
| published_date | TEXT | |
| tipo | TEXT | "consulta" / "regulacion" |
| estado | TEXT | "Abierta" / "Cerrada" |
| plazo | TEXT | Fecha límite consulta |
| sector | TEXT | "electricidad" / "gas" / "otro" |
| importante | TEXT | "Sí" / "No" |
| comprobado | TEXT | "S" / "N" |
| expediente | TEXT | Nº de expediente CNMC |
| resumen | TEXT | Resumen extraído o generado |
| impacto_ree | TEXT | |
| resumen_ia | TEXT | Generado por Claude API |

#### `eurlex_entries`
| Campo | Tipo | Notas |
|---|---|---|
| id | SERIAL PK | |
| external_id | TEXT UNIQUE | CELEX o URL como ID |
| fecha | TEXT | Fecha publicación DOUE |
| titulo | TEXT | Preferentemente en español |
| texto | TEXT | |
| enlace | TEXT | URL EUR-Lex oficial |
| tipo | TEXT | "Reglamento (UE)" / "Directiva (UE)" / etc. |
| importante | TEXT | "Sí" para Reglamentos y Directivas |
| comprobado | TEXT | "S" / "N" |
| resumen_ia | TEXT | |

**Constante crítica en database.py:**
```python
YEAR_FILTER = 2026   # Solo datos de 2026 se muestran en la web
```

Cambiarla requiere actualizar también los filtros de la web y el JSON exportado.

### Reglas antes de modificar el esquema

1. Leer `db/database.py` completo antes de proponer cualquier cambio.
2. Añadir columnas solo con `ALTER TABLE IF NOT EXISTS` dentro de `init_db()`.
3. No eliminar columnas sin copia de seguridad previa.
4. No renombrar columnas usadas en `export_to_json()` sin actualizar también la web.
5. Mantener índices en: `external_id`, `fecha`, `source`, `estado`, `sector`, `importante`.

---

## Scrapers: reglas de modificación

Cada scraper tiene una fuente y responsabilidad clara. No mezclar scraping con persistencia en base de datos.

| Scraper | Fuente | Método | Frecuencia |
|---|---|---|---|
| `boe.py` | `boe.es/datosabiertos/api/boe/sumario/` | JSON API | Diario |
| `boe_anuncios.py` | `boe.es/boe/dias/…/index.php` | HTML scraping | Diario |
| `cnmc.py` | `cnmc.es/consultas-publicas/energia` | HTML scraping | Diario |
| `cnmc_rss.py` | `cnmc.es/rss.xml` | RSS + detail HTML | Diario |
| `eurlex.py` | SPARQL publications.europa.eu | SPARQL | Diario (7 días atrás) |
| `miterd.py` | `miteco.gob.es/…/participacion.html` | HTML scraping | Diario |

Al modificar un scraper:

1. Guardar siempre la URL oficial.
2. Manejar errores de red sin interrumpir los demás scrapers.
3. Si cambia la estructura HTML de la fuente, adaptar el parser con la menor superficie posible de cambio.
4. No depender de textos en idioma variable (usar selectores CSS o atributos estables).
5. Añadir logs útiles (`print()` o `logging`) para facilitar el diagnóstico en GitHub Actions.

---

## Clasificación y columnas de filtrado

La clasificación es explícita y trazable. Las reglas principales están en `boe.py`:

- **`_find_keywords()`** — 142+ palabras clave energéticas (acceso, conexión, peaje, red de transporte, CNMC, retribución…).
- **`_detect_tipo()`** — Rango normativo (Ley, RD-Ley, RD, Orden…).
- **`_is_importante()`** — "Sí" solo para actos de alto rango (Ley, RD, RDL) o infracciones Art.64.
- **`_detect_acceso()`** — "Sí" si menciona acceso, conexión, peaje o red de transporte.
- **`_detect_tramitaciones()`** — "Sí" si menciona autorización previa o de construcción.
- **`_clasificar_art64()`** — Riesgo para el suministro eléctrico (Art.64 LSE).
- **`_should_include()`** — Filtro de inclusión: excluye RTVE, universidades, telecomunicaciones; acepta solo nombramientos de Presidencia / MITERD / CNMC; requiere palabras clave energéticas.

Al añadir o modificar reglas de clasificación:

1. Mantener las reglas explícitas y revisables.
2. No marcar como importante todo por defecto.
3. Documentar en código la razón si una regla no es obvia.
4. Probar con datos reales antes de desplegar.

---

## Web estática (web/index.html + web/data.json)

La web carga `data.json` y muestra los datos sin backend. El esquema visual usa tonos navy/teal, inspirado en diseño institucional sobrio.

### Pestañas reales (según export_to_json)

| Pestaña | Fuente en data.json | Filtro |
|---|---|---|
| Todas | `entries` | BOE + CNMC 2026 |
| BOE Último Trimestre | `boe_trimestre` | Últimos 92 días |
| Regulación Española | `reg_espanola` | BOE + CNMC Q1 2026 |
| Consultas CNMC | `cnmc_consultas` | Abiertas y cerradas |
| Acceso / Conexión | `acceso_conexion` | acceso_conexion = "Sí" (2026) |
| Normativa Europea | `eurlex` | eurlex_entries 2026 |

### Reglas al modificar la web

1. No cambiar las claves de `data.json` sin actualizar también `export_to_json()` en `database.py`.
2. Probar en tres tamaños: escritorio (>1024px), tableta (768px) y móvil (<420px).
3. Comprobar que no aparece scroll horizontal en móvil.
4. Los enlaces deben apuntar siempre a la fuente oficial (BOE, CNMC, EUR-Lex).
5. No introducir dependencias JS pesadas: la web es estática y debe cargarse rápido.

---

## GitHub Actions (scraper.yml)

**Ejecución automática:**
- Lunes a sábado a las 08:00 CEST (`0 6 * * 1-6`).
- Lunes a viernes a las 12:00 CEST (`0 10 * * 1-5`).
- También manual (`workflow_dispatch`).

**Pasos del workflow:**
1. Checkout del código.
2. Python 3.12 con caché de pip.
3. `pip install -r requirements.txt`.
4. `python main.py` (requiere secreto `BBDD` → DATABASE_URL).
5. `python generate_summaries.py` (opcional, `continue-on-error: true`, requiere `ANTHROPIC_API_KEY`).
6. Deploy de `web/` a la rama `gh-pages` (GitHub Pages).

**Secretos requeridos en GitHub:**

| Secreto | Uso |
|---|---|
| `BBDD` | DATABASE_URL — cadena de conexión PostgreSQL |
| `ANTHROPIC_API_KEY` | Claude API para generación de resúmenes (opcional) |

No modificar el workflow sin confirmar que el cambio funciona primero en local.

---

## Variables de entorno

No guardar secretos en el repositorio. Usar `.env` en local y secretos de GitHub Actions en producción.

```env
DATABASE_URL=postgresql://usuario:password@host:5432/base
ANTHROPIC_API_KEY=sk-ant-…   # Opcional, solo para generate_summaries.py
```

El `.gitignore` ya excluye `.env`, `.venv/` y `__pycache__/`.

---

## Comandos de verificación

```bash
# Comprobar sintaxis
python -m compileall .

# Ejecutar pipeline completo (modifica BD y genera data.json)
python main.py

# Probar la web localmente
cd web
python -m http.server 8000
# Abrir http://localhost:8000
```

No existe suite de tests automatizados. Las comprobaciones son manuales.

---

## Criterio de aceptación de cambios

Un cambio solo se considera terminado si:

1. El código se ejecuta sin errores.
2. No aparecen errores en consola o logs de GitHub Actions.
3. No se duplican publicaciones (comprobar con `SELECT COUNT(*) FROM boe_entries`).
4. Los campos obligatorios (`external_id`, `fecha`, `titulo`, `enlace`) siguen rellenándose.
5. `export_to_json()` genera un `data.json` válido y completo.
6. Los filtros de la web funcionan correctamente.
7. La web se ve correctamente en escritorio, tableta y móvil.
8. Los enlaces apuntan a fuentes oficiales.
9. No se han expuesto secretos ni claves.

---

## Regla crítica: no eliminar funcionalidad existente sin avisar

Antes de sobrescribir, reescribir o reemplazar cualquier archivo que ya tenga funcionalidad implementada:

1. **Hacer `git diff HEAD -- <archivo>`** para identificar qué existe en el working tree pero aún no está commiteado.
2. **Hacer `git show HEAD:<archivo> | grep <feature>`** para verificar si la funcionalidad está en el último commit.
3. Si hay discrepancia (funcionalidad en working tree que no está en HEAD, o viceversa), **detener y preguntar al usuario** qué versión debe prevalecer.
4. **Nunca sobrescribir `web/index.html` completo** sin preservar explícitamente filtros, checkboxes, pestañas y badges ya implementados.
5. Si el cambio a realizar es en un archivo grande (>200 líneas), editar solo las secciones afectadas con la herramienta `Edit`, nunca reescribir el archivo entero con `Write`.

**Ejemplo de lo que NO hacer:** arreglar un bug en EUR-Lex y, al corregir `index.html`, entregar una versión que omite el checkbox "Ocultar tramitaciones" que el usuario había añadido previamente.

**Protocolo ante duda:** parar, mostrar al usuario qué cambios detectas en el working tree (`git diff HEAD -- archivo`), y preguntar si debe incluirse en el nuevo cambio.

---

## Instrucciones para Claude

1. Responder siempre en español.
2. Antes de editar, explicar qué se va a cambiar y por qué.
3. Leer el archivo completo relevante antes de proponer modificaciones.
4. **Antes de tocar `web/index.html`, ejecutar `git diff HEAD -- web/index.html` y revisar si hay cambios sin commitear que deban preservarse.**
5. Si el cambio afecta a varias capas (scraper → BD → web), hacerlo por fases.
6. Si detectas un posible problema de seguridad con `.env`, claves o base de datos, avisar antes de continuar.
7. Si una modificación puede afectar al despliegue en GitHub Actions, indicarlo expresamente.
8. Si no tienes claro el esquema real de la BD, inspeccionar `db/database.py` antes de proponer SQL.
9. No inventar datos normativos. Mostrar siempre enlaces a las fuentes oficiales.
10. Priorizar cambios pequeños, verificables y reversibles.
11. No sobrescribir datos existentes sin confirmar que existe lógica de deduplicación segura.
