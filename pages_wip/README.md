# Migración multipágina (trabajo en progreso)

Esta carpeta contiene la migración hacia la arquitectura **multipágina** de Streamlit
(`🏠_Home.py` + `pages/`), iniciada pero **incompleta**: 3 de 5 páginas (`Contexto Global`,
`Energía`, `Data Hub`) son stubs "en construcción".

**Decisión (auditoría 2026-06-24):** el entrypoint canónico de producción es el monolito
`streamlit_app.py`, que tiene el 100 % de la funcionalidad. Para evitar que Streamlit renderice
las páginas stub como navegación rota, este scaffold se aparcó aquí (Streamlit solo trata una
carpeta llamada `pages/` como navegación automática).

**Para retomar la migración:** completar las 3 páginas stub portando la lógica del monolito y,
cuando estén al 100 %, renombrar `pages_wip/` → `pages/`, fijar `🏠_Home.py` como Main file en
Streamlit Cloud y retirar `streamlit_app.py`. Las páginas 1 (Datos Macro) y 2 (Proyecciones) ya
están implementadas y sirven de plantilla.
