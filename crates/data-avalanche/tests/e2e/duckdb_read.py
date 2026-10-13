"""Reads a Delta table on S3 (RustFS) with DuckDB's delta_scan and prints its columns, their DuckDB
types and every cell as text, as JSON rows in `id` order.

Used by tests/e2e/types.rs. Run: uv run --no-project --with duckdb==1.5.6 python duckdb_read.py
<s3 table url>, with DUCKDB_S3_ENDPOINT (host:port), DUCKDB_S3_KEY_ID and DUCKDB_S3_SECRET set.
"""

import json
import os
import sys

import duckdb

con = duckdb.connect()


def quote(s):
    return "'" + s.replace("'", "''") + "'"


con.sql(
    "CREATE SECRET rustfs (TYPE s3, KEY_ID {}, SECRET {}, ENDPOINT {}, REGION 'us-east-1', "
    "URL_STYLE 'path', USE_SSL false)".format(
        quote(os.environ["DUCKDB_S3_KEY_ID"]),
        quote(os.environ["DUCKDB_S3_SECRET"]),
        quote(os.environ["DUCKDB_S3_ENDPOINT"]),
    )
)
# timestamptz cells print in UTC, whatever the host's zone.
con.sql("SET TimeZone = 'UTC'")
scan = f"delta_scan({quote(sys.argv[1])})"
table = con.sql(f"SELECT * FROM {scan}")
names = table.columns
types = [str(t) for t in table.types]
cols = ", ".join(f'"{c}"::VARCHAR' for c in names)
rows = con.sql(f"SELECT {cols} FROM {scan} ORDER BY id").fetchall()
print(json.dumps({"columns": names, "types": types, "rows": [list(r) for r in rows]}))
