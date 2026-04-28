with open('C:/Users/estel/New_regulation/web/index.html', encoding='utf-8') as f:
    c = f.read()

# CSS badges Art.64
art64_css = """
    /* ── Art.64 Infracciones ── */
    .badge-art64-gs  { background:#c62828;color:#fff;font-weight:700; }
    .badge-art64-dg  { background:#e65100;color:#fff;font-weight:700; }
    .badge-art64-nc  { background:#1565c0;color:#fff;font-weight:700; }
    .badge-art64-sin { background:#2e7d32;color:#fff;font-weight:700; }
"""
c = c.replace('    /* ── Badge NUEVO ── */', art64_css + '    /* ── Badge NUEVO ── */')

# Función JS
art64_js = """
  function art64Badge(v) {
    if (!v || !v.startsWith('Art.64')) return '';
    if (v.includes('Con riesgo GS'))   return '<span class="badge badge-art64-gs">⚡ ' + v + '</span>';
    if (v.includes('Con da\\u00f1o grave')) return '<span class="badge badge-art64-dg">\\u26a0\\ufe0f ' + v + '</span>';
    if (v.includes('Sin riesgo GS'))   return '<span class="badge badge-art64-sin">\\u2713 ' + v + '</span>';
    return '<span class="badge badge-art64-nc">\\U0001f4cb ' + v + '</span>';
  }

"""
c = c.replace('  function autoSummary(title) {', art64_js + '  function autoSummary(title) {')

# En normCardBOE: mostrar badge antes del resumen
old = "      ${e.resumen     || ''}"
new = "      ${art64Badge(e.impacto_ree)}\n        ${e.resumen || ''}"
c = c.replace(old, new)

with open('C:/Users/estel/New_regulation/web/index.html', 'w', encoding='utf-8') as f:
    f.write(c)
print('OK' if 'art64Badge' in c else 'FAIL')
