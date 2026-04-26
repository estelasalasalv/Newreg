# Monitor Regulatorio Energético

Scraper automático de normativa energética del **BOE** (API oficial) y **CNMC**,
con persistencia en **PostgreSQL** y publicación en **GitHub Pages**.

## Arquitectura

```
GitHub Actions (2×/día)
    │
    ├─ scraper/boe.py   → API oficial BOE
    ├─ scraper/cnmc.py  → Scraping CNMC /consultas-publicas/energia
    │
    ├─ db/database.py   → PostgreSQL (Neon / Supabase / Railway)
    │
    └─ web/data.json    → Exportación JSON → GitHub Pages (web estática)
```

## Configuración rápida

### 1. Base de datos PostgreSQL gratuita

Crea una BD en **[Neon.tech](https://neon.tech)** (gratis, sin tarjeta):
- Crea un proyecto → copia la *Connection String* (formato `postgresql://...`)

### 2. Secretos de GitHub

En tu repo → **Settings → Secrets and variables → Actions**:

| Nombre | Tipo | Valor |
|--------|------|-------|
| `DATABASE_URL` | Secret | `postgresql://user:pass@host/db` |
| `BOE_KEYWORDS` | Variable (opcional) | `energía,eléctrico,gas natural,…` |

### 3. Activar GitHub Pages

En tu repo → **Settings → Pages**:
- Source: **Deploy from a branch**
- Branch: `gh-pages` / `/ (root)`

La URL de tu web será: `https://<usuario>.github.io/<repo>/`

### 4. Ejecución local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Configurar entorno
cp .env.example .env
# → edita .env con tu DATABASE_URL

# Ejecutar
python main.py
```

## Horario de ejecución

El workflow corre **lunes–viernes** en dos pasadas:
- `07:00 UTC` → 09:00 hora peninsular (CET)
- `19:00 UTC` → 21:00 hora peninsular (CET)

Para forzar una ejecución manual: **Actions → Regulatory Scraper → Run workflow**.

## Fuentes de datos

| Fuente | Método | URL |
|--------|--------|-----|
| BOE | API JSON oficial | `https://www.boe.es/datosabiertos/api/boe/sumario/YYYYMMDD` |
| CNMC | Web scraping | `https://www.cnmc.es/consultas-publicas/energia` |

**Sectores excluidos de CNMC:** Audiovisual · Telecomunicaciones · Postal · Ferroviario

## Despliegue alternativo: Cloudflare Pages

1. En [dash.cloudflare.com](https://dash.cloudflare.com) → Pages → Create project → Connect to Git
2. Build settings:
   - Framework: `None`
   - Build command: *(vacío)*
   - Build output directory: `web`
3. Añade la variable de entorno `DATABASE_URL` en *Settings → Environment variables*
4. El workflow de GitHub Actions sigue siendo el que ejecuta el scraper;
   Cloudflare Pages servirá la carpeta `web/` del branch `gh-pages`.
