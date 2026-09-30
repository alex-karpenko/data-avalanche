"""P10 independent cross-read: DuckDB's `delta` extension (delta-kernel-rs based) reads the table
written by tests/probe_types.rs and checks Hop-3b types and values.

    uv run --no-project --with duckdb --with pytz python scripts/p10_duckdb_crossread.py <table_dir>
"""
import sys

import duckdb

path = sys.argv[1]
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
print("duckdb", duckdb.__version__)

cols = con.execute(f"DESCRIBE SELECT * FROM delta_scan('{path}')").fetchall()
types = {c[0]: c[1] for c in cols}
print("types:", types)
rows = con.execute(f"SELECT * FROM delta_scan('{path}') ORDER BY id").fetchall()
names = [c[0] for c in cols]
for r in rows:
    print(dict(zip(names, r)))

expect_types = {
    "id": "BIGINT", "flag": "BOOLEAN", "tiny": "TINYINT", "small": "SMALLINT",
    "u64": "DECIMAL(20,0)", "dec": "DECIMAL(38,10)", "s": "VARCHAR", "js": "VARCHAR",
    "bin": "BLOB", "d": "DATE", "uuid": "VARCHAR", "t": "VARCHAR",
    "ts_tz": "TIMESTAMP WITH TIME ZONE", "ts_ntz": "TIMESTAMP", "tags": "VARCHAR[]",
}
bad = {k: (types.get(k), v) for k, v in expect_types.items() if types.get(k) != v}
assert not bad, f"type mismatch (got, want): {bad}"

by_id = {r[0]: dict(zip(names, r)) for r in rows}
assert len(by_id) == 3, by_id.keys()
r1, r2, r3 = by_id[1], by_id[2], by_id[3]
assert str(r2["u64"]) == "18446744073709551608", r2["u64"]
assert str(r1["dec"]) == "-1234567890123456789012345678.9012345671", r1["dec"]
assert r2["t"] == "23:59:59.999999" and r1["t"] == "00:00:00.000000" and r3["t"] == "12:34:56.789019"
assert r1["uuid"] == "0718293a-4b5c-6d7e-8fa0-b1c2d3e4f506", r1["uuid"]
assert bytes(r1["bin"]) == b"\x00\xff\x01" and bytes(r2["bin"]) == b"" and r3["bin"] is None
assert r1["s"] == "héllo ✓ 7" and r2["s"] == "" and r3["s"] is None
assert r1["js"] == '{"a":1,"b":[true,null]}'
assert r1["tags"] == ["t0", "x"] and r3["tags"] is None
assert r1["tiny"] == -128 and r2["small"] == 32767 and r3["flag"] is None
assert str(r2["d"]) == "1969-12-31"
# tz semantics: ts_tz is an instant (UTC), ts_ntz is wall-clock; same micros in the fixture
ts = con.execute(
    f"SELECT epoch_us(ts_tz), epoch_us(ts_ntz), strftime(ts_ntz, '%Y-%m-%d %H:%M:%S.%f') "
    f"FROM delta_scan('{path}') ORDER BY id").fetchall()
print("epoch_us(ts_tz), epoch_us(ts_ntz), ntz text:", ts)
assert ts[0][0] == 1_790_000_000_123_456 + 7 and ts[0][1] == 1_790_000_000_123_456 + 7
assert ts[1][0] == -1 and ts[1][2] == "1969-12-31 23:59:59.999999"
print("P10 DuckDB cross-read: OK")
