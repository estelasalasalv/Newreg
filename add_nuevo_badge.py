with open('C:/Users/estel/New_regulation/web/index.html', encoding='utf-8') as f:
    c = f.read()

# CSS badge NUEVO
c = c.replace(
    '    .badge-acc { background: rgba(0,184,176,.12);  color: var(--teal2); }',
    '    .badge-acc { background: rgba(0,184,176,.12);  color: var(--teal2); }\n    .badge-new { background: #e53935; color: #fff; animation: pulse-new 1.5s ease-in-out infinite; font-weight:700; }\n    @keyframes pulse-new { 0%,100%{opacity:1} 50%{opacity:.65} }'
)

# Badge en normCardBOE (dentro del norm-header junto a recencyLabel)
c = c.replace(
    "      ${recencyLabel(fecha)}",
    "      ${recencyLabel(fecha)}\n        ${e.es_nuevo ? '<span class=\"badge badge-new\" style=\"font-size:.7rem\">NUEVO</span>' : ''}"
)

# Badge en consultaCard (junto al badge de estado)
old_con = "          <span class=\"badge ${abierta ? 'badge-open' : 'badge-closed'}\">${abierta ? 'Abierta' : 'Finalizada'}</span>"
new_con  = old_con + "\n          ${e.es_nuevo ? '<span class=\"badge badge-new\" style=\"font-size:.7rem\">NUEVO</span>' : ''}"
c = c.replace(old_con, new_con)

# Badge en cnmcRegCard
old_cnmc = '      <div class="norm-card" style="border-left-color:var(--teal2)">'
new_cnmc = old_cnmc + "\n      ${e.es_nuevo ? '<span class=\"badge badge-new\" style=\"font-size:.7rem;margin-bottom:4px;display:inline-block\">NUEVO</span>' : ''}"
c = c.replace(old_cnmc, new_cnmc)

with open('C:/Users/estel/New_regulation/web/index.html', 'w', encoding='utf-8') as f:
    f.write(c)
print('OK' if 'badge-new' in c else 'FAIL')
print('recencyLabel patched:', "${e.es_nuevo" in c)
