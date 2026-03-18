import os
from celery import Celery
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

# Configurar el Calendario Inteligente Básico (Polling)
celery_app.conf.beat_schedule = {
    'ingest-daily-macro-variables': {
        'task': 'scheduler.tasks.ingest_all_active_variables',
        'schedule': 43200.0, # Cada 12 horas pollea las fuentes
    },
}
