"""Reads a Delta table with DuckDB's delta_scan and prints every cell as text, as JSON rows.

Used by tests/sink/types.rs (R-12). Run: uv run --no-project --with duckdb python duckdb_read.py <table dir>
"""

import json
import sys

import duckdb

con = duckdb.connect()
scan = f"delta_scan('{sys.argv[1]}')"
cols = ", ".join(f'"{c}"::VARCHAR' for c in con.sql(f"SELECT * FROM {scan}").columns)
rows = con.sql(f"SELECT {cols} FROM {scan} ORDER BY id").fetchall()
print(json.dumps([list(r) for r in rows]))
