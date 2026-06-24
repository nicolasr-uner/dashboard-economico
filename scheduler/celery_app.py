import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()

redis_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "economic_brain",
    broker=redis_url,
    backend=redis_url,
    include=["scheduler.tasks"]
)

celery_app.conf.update(
    timezone="America/Bogota",
    enable_utc=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

# ── Schedules diferenciados por categoría de dato ─────────────────────────────
# Energía (XM, CENACE): precios horarios/diarios → actualizar cada hora en días hábiles
# Macro/Monetario (BanRep, Banxico, BCB): publicación diaria → 8am hora Bogotá
# Fiscal/Externo: publicación mensual → semanal es más que suficiente
# Global (FRED): mercados americanos → después del cierre USA (10pm Bogotá)

celery_app.conf.beat_schedule = {
    # Energía: lunes a viernes, cada hora entre 6am y 10pm
    'ingest-energy-variables': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=0, hour='6-22', day_of_week='1-5'),
        'kwargs': {'category': 'energy'},
    },
    # Macro/Monetario/Tasas: diario a las 8am (publicaciones mañaneras)
    'ingest-macro-morning': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=0, hour=8),
        'kwargs': {'category': 'macro'},
    },
    'ingest-rates-morning': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=30, hour=8),
        'kwargs': {'category': 'rates_monetary'},
    },
    # Tipo de cambio: diario a las 9am (apertura mercados)
    'ingest-fx-morning': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=0, hour=9),
        'kwargs': {'category': 'fx_rates'},
    },
    # Sector externo y precios: diario a las 10am
    'ingest-external-prices': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=0, hour=10),
        'kwargs': {'category': 'external'},
    },
    'ingest-prices-inflation': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=30, hour=10),
        'kwargs': {'category': 'prices_inflation'},
    },
    # PIB/Actividad y Fiscal: semanal (datos de baja frecuencia)
    'ingest-gdp-activity-weekly': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=0, hour=7, day_of_week=1),  # Lunes 7am
        'kwargs': {'category': 'gdp_activity'},
    },
    'ingest-fiscal-weekly': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=30, hour=7, day_of_week=1),  # Lunes 7:30am
        'kwargs': {'category': 'fiscal'},
    },
    # Global (FRED/commodities): después del cierre USA (22:30 Bogotá = 23:30 ET)
    'ingest-global-after-close': {
        'task': 'scheduler.tasks.ingest_variables_by_category',
        'schedule': crontab(minute=30, hour=22, day_of_week='1-5'),
        'kwargs': {'category': 'fx_rates'},  # Incluye S&P, VIX, commodities
    },
}
