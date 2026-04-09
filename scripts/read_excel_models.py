"""
Lee los modelos financieros de Exagon y Ruitoque para extraer variables macro.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openpyxl

BASE = r"c:\Users\Lenovo\OneDrive\Desktop\dashboard-economico"

def read_model_sheets(path, label):
    print(f"\n{'='*60}")
    print(f"MODELO: {label}")
    print(f"{'='*60}")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        print(f"Hojas: {wb.sheetnames}")
        wb.close()
        return wb.sheetnames
    except Exception as e:
        print(f"Error: {e}")
        return []

def read_sheet(path, sheet_name, max_rows=150):
    print(f"\n--- Hoja: [{sheet_name}] ---")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        if sheet_name not in wb.sheetnames:
            print(f"No encontrada. Disponibles: {wb.sheetnames}")
            wb.close()
            return
        ws = wb[sheet_name]
        row_count = 0
        for row in ws.iter_rows(min_row=1, max_row=max_rows, values_only=True):
            cells = row[:8]
            non_empty = [c for c in cells if c is not None]
            if non_empty:
                print(cells)
                row_count += 1
        wb.close()
        print(f"(Total filas con datos: {row_count})")
    except Exception as e:
        print(f"Error: {e}")

EXAGON = os.path.join(BASE, "Financial model - 24Mar2026 - Exagon - 13 Minifarms.xlsx")
RUITOQUE = os.path.join(BASE, "Financial model - Ruitoque Tax Partner.xlsx")

# Paso 1: listar hojas
sheets_ex = read_model_sheets(EXAGON, "Exagon 13 Minifarms")
sheets_ru = read_model_sheets(RUITOQUE, "Ruitoque Tax Partner")

# Paso 2: leer hoja Analysis / Assumptions / Macro de Ruitoque
for sn in sheets_ru:
    if any(k in sn.lower() for k in ['analysis', 'macro', 'assum', 'inputs', 'resumen', 'model']):
        read_sheet(RUITOQUE, sn, max_rows=200)

# Paso 3: leer hojas de supuestos macro de Exagon
for sn in sheets_ex:
    if any(k in sn.lower() for k in ['macro', 'assum', 'inputs', 'supuest', 'model', 'summary', 'resumen']):
        read_sheet(EXAGON, sn, max_rows=200)
