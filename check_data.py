import sys
sys.path.insert(0, r'c:\PycharmProjects\quant_data_center')

from src.storage.parquet_store import ParquetStore
from src.storage.duckdb_store import DuckDBStore
from src.utils.config_mgr import ConfigManager
import pandas as pd

config = ConfigManager()
store = ParquetStore(root=config.root)

calendar_df = store.read_calendar()
if not calendar_df.empty and "calendar_date" in calendar_df.columns:
    max_cal = calendar_df["calendar_date"].max()
    print(f"Calendar max date: {max_cal}")
    print(f"Calendar rows: {len(calendar_df)}")
    print(f"Calendar columns: {list(calendar_df.columns)}")
    print(f"\nLast 10 calendar dates:")
    print(calendar_df["calendar_date"].sort_values().tail(10).tolist())
else:
    print("Calendar is empty or missing calendar_date column")

print("\n--- Checking hist data for sample codes ---")
sample_codes = ["000078", "000004", "000638", "001237", "000508"]
for code in sample_codes:
    for adjust in ["none", "qfq", "hfq"]:
        hist_df = store.read_stock_zh_a_hist(adjust, code)
        if hist_df.empty:
            print(f"code={code} adjust={adjust}: EMPTY")
            continue
        max_date = hist_df["date"].max() if "date" in hist_df.columns else "N/A"
        source_endpoints = hist_df["source_endpoint"].unique().tolist() if "source_endpoint" in hist_df.columns else []
        latest_row = hist_df.loc[hist_df["date"].idxmax()] if "date" in hist_df.columns else None
        latest_source = latest_row["source_endpoint"] if latest_row is not None and "source_endpoint" in latest_row.index else "N/A"
        print(f"code={code} adjust={adjust}: rows={len(hist_df)}, max_date={max_date}, latest_source_endpoint={latest_source}, all_endpoints={source_endpoints}")

store.close()

print("\n--- Checking DuckDB views ---")
duck_store = DuckDBStore(root=config.root)
duck_store.build_views()
with duck_store.connect() as conn:
    cal_max = conn.execute("SELECT max(calendar_date) FROM v_calendar").fetchone()
    print(f"DuckDB calendar max: {cal_max}")
    
    for code in sample_codes[:3]:
        for adjust in ["none"]:
            dataset = f"stock_zh_a_hist_{adjust}"
            view = f"v_{dataset}"
            try:
                row = conn.execute(
                    f"""
                    SELECT h.code, h.date AS latest_date, h.source_endpoint
                    FROM {view} AS h
                    WHERE h.code = ?
                    ORDER BY h.date DESC
                    LIMIT 1
                    """,
                    [code],
                ).fetchone()
                if row:
                    print(f"DuckDB: code={code} adjust={adjust}: latest_date={row[1]}, source_endpoint={row[2]}")
                else:
                    print(f"DuckDB: code={code} adjust={adjust}: NO DATA")
            except Exception as e:
                print(f"DuckDB: code={code} adjust={adjust}: ERROR: {e}")
