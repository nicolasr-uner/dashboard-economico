from scheduler.celery_app import celery_app
from models.db import SessionLocal
from models.schema import MacroVariable
from data.agent import VariableAgent
import pandas as pd

@celery_app.task
def ingest_all_active_variables():
    """Itera sobre las variables activas y delega su extracción de forma asíncrona."""
    session = SessionLocal()
    try:
        variables = session.query(MacroVariable).filter_by(is_active=True).all()
        for var in variables:
            # Encola la tarea de extracción en Redis sin bloquear el proceso
            ingest_variable_task.delay(var.id)
        return f"Se despacharon {len(variables)} tareas de extracción."
    except Exception as e:
        print(f"Error despachando tareas: {e}")
        return str(e)
    finally:
        session.close()

@celery_app.task(bind=True, max_retries=3)
def ingest_variable_task(self, variable_id: int):
    """Extrae el valor actualizado de una variable y lo inserta en la serie de tiempo."""
    session = SessionLocal()
    try:
        var = session.query(MacroVariable).filter_by(id=variable_id).first()
        if not var:
            return f"Variable ID {variable_id} no encontrada."
            
        var_series = pd.Series({
            'id': var.id,
            'source_url': var.source_url,
            'css_selector': var.css_selector,
            'is_dynamic': var.is_dynamic,
            'name': var.name
        })
        
        result = VariableAgent.ingest_variable(var_series)
        
        if not result['success']:
            # Reintentar en 1 hora si la fuente falla (probablemente no han publicado en el Banco Central)
            raise self.retry(exc=Exception(result['error']), countdown=3600)
            
        return f"Ingesta exitosa para {var.name}: {result['value']}"
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=3600)
    finally:
        session.close()
