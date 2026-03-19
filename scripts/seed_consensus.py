"""
seed_consensus.py — Carga inicial de proyecciones de consenso desde analistas.
Ejecutar: python scripts/seed_consensus.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from data.consensus import save_consensus_forecast
from models.db import init_db, SessionLocal
from models.schema import MacroVariable

init_db()

def get_var_by_name_fragment(session, frag):
    vars = session.query(MacroVariable).all()
    for v in vars:
        if frag.lower() in v.name.lower():
            return v
    return None

def backfill_consensus():
    with SessionLocal() as session:
        # Colombia
        v_pib_co  = get_var_by_name_fragment(session, "PIB Trimestral CO")
        v_ipc_co  = get_var_by_name_fragment(session, "IPC")
        v_trm_co  = get_var_by_name_fragment(session, "TRM")
        v_tasa_co = get_var_by_name_fragment(session, "Tasa de Intervención")

        # Brasil
        v_ipca_br  = get_var_by_name_fragment(session, "IPCA")
        v_selic_br = get_var_by_name_fragment(session, "Selic")
        v_usd_br   = get_var_by_name_fragment(session, "USD/BRL")
        v_pib_br   = get_var_by_name_fragment(session, "PIB Trimestral BR")

        # Mexico
        v_ipc_mx  = get_var_by_name_fragment(session, "IPC Mex")
        v_tasa_mx = get_var_by_name_fragment(session, "Objetivo Banxico")
        v_usd_mx  = get_var_by_name_fragment(session, "USD/MXN")

        fdate = datetime.now()
        target_2026 = datetime(2026, 12, 31)

        projections = []

        # -- Colombia --
        if v_pib_co:
            projections.extend([
                (v_pib_co.id, "Bancolombia", v_pib_co.name, 2.9, 'base'),
                (v_pib_co.id, "BanRep", v_pib_co.name, 2.8, 'base'),
                (v_pib_co.id, "ANIF", v_pib_co.name, 3.0, 'base'),
                (v_pib_co.id, "BBVA Research", v_pib_co.name, 2.8, 'base'),
                (v_pib_co.id, "Corficolombiana", v_pib_co.name, 2.9, 'base')
            ])
        if v_ipc_co:
            projections.extend([
                (v_ipc_co.id, "Bancolombia", v_ipc_co.name, 5.0, 'base'),
                (v_ipc_co.id, "BanRep", v_ipc_co.name, 3.6, 'base'),
                (v_ipc_co.id, "ANIF", v_ipc_co.name, 4.5, 'base')
            ])
        if v_trm_co:
            projections.append((v_trm_co.id, "Bancolombia", v_trm_co.name, 3880.0, 'base'))
        if v_tasa_co:
            projections.append((v_tasa_co.id, "Bancolombia", v_tasa_co.name, 12.75, 'base'))
            
            # Additional targeted date (e.g., matching the request specifics like "2026-03-18")
            save_consensus_forecast(v_tasa_co.id, "BanRep (actual)", fdate, datetime(2026, 3, 18), 10.25, 'actual', "Tasa vigente reportada en Encuesta")

        # -- Brasil (Focus BCB) --
        if v_ipca_br:
            projections.append((v_ipca_br.id, "Focus BCB (mediana)", v_ipca_br.name, 4.5, 'base'))
        if v_selic_br:
            projections.append((v_selic_br.id, "Focus BCB (mediana)", v_selic_br.name, 15.0, 'base'))
        if v_usd_br:
            projections.append((v_usd_br.id, "Focus BCB (mediana)", v_usd_br.name, 5.90, 'base'))
        if v_pib_br:
            projections.append((v_pib_br.id, "Focus BCB (mediana)", v_pib_br.name, 1.5, 'base'))

        # -- Mexico (Encuesta Banxico) --
        if v_ipc_mx:
            projections.append((v_ipc_mx.id, "Encuesta Banxico", v_ipc_mx.name, 3.8, 'base'))
        if v_tasa_mx:
            projections.append((v_tasa_mx.id, "Encuesta Banxico", v_tasa_mx.name, 8.5, 'base'))
        if v_usd_mx:
            projections.append((v_usd_mx.id, "Encuesta Banxico", v_usd_mx.name, 20.5, 'base'))
        
        # Guardar todo
        print("Cargando proyecciones de consenso...")
        for vid, inst, varname, val, scen in projections:
            ok = save_consensus_forecast(
                variable_id=vid,
                source_institution=inst,
                forecast_date=fdate,
                target_date=target_2026,
                value=val,
                scenario=scen,
                notes="Generado por seed script inicial"
            )
            if ok:
                print(f" [OK] {inst} -> {varname} ({val})")
            else:
                print(f" [XX] Error en {inst} -> {varname}")

if __name__ == "__main__":
    backfill_consensus()
    print("\n✅ Carga de consenso completada.")
