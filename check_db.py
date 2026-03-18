import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'db.sqlite3'

conn = sqlite3.connect(str(DB_PATH))
variables = pd.read_sql("SELECT v.id, v.name as variable, c.name as country, v.source_url, v.css_selector FROM dim_variable v JOIN dim_country c ON v.country_id = c.id", conn)
print("=== VARIABLES ACTUALES EN LA BD ===")
for _, row in variables.iterrows():
    print(f"ID: {row['id']} | {row['variable']} ({row['country']}) -> URL: {row['source_url']} -> Selector: '{row['css_selector']}'")
