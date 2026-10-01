---
updated: 2026-10-01
domain: Data Types
tags: [data-types, mapping, arrow, iceberg, delta-lake, multi-sink, postgres, mysql, cdc]
resolves: G-18
sources: []
---

# Data Type Mapping: Sources → ColumnValue → Arrow → Sink (Iceberg | Delta)

## Overview

Type mapping in this CDC system is **not a single step** — it is three sequential hops, each
owned by a distinct pipeline layer. Understanding the chain is the prerequisite for designing
each hop correctly.

```
PostgreSQL wire (OID + binary)    ─┐
                                   ├─► [Decode] ──► ColumnValue ──► [Accumulate] ──► Arrow DataType ──► [Sink] ──► Iceberg Type
MySQL binlog (column type code)   ─┘                                   + logical tag             └──► Delta Type
                                        Phase 2          Phase 3                        Phase 4
                                     source-specific   source-agnostic             per-sink (sink.type):
                                     per-crate          Rust code                  iceberg-rust / delta-rs
```

**Hop 1 (Decode):** our pgoutput decoder (over the `pgwire-replication` transport) and the
`mysql_async` / `mysql_common` binlog row decoder (S-MY probes) decode the wire
binary into `ColumnValue` variants. All source-specific type knowledge lives here.

**Hop 2 (Accumulate):** `BatchAccumulator` maps each `ColumnValue` variant to an Arrow
`ArrayBuilder` and produces a typed `RecordBatch`. This hop must know the Arrow `DataType`
for each column — derived from the `ColumnValue` variant plus column metadata loaded during
schema resolution.

**Hop 3 (Sink):** the pipeline's sink (`sink.type`) translates the canonical Arrow `Schema` into
its table format's schema when creating or evolving a table — **3a** Iceberg via `iceberg-rust`,
**3b** Delta Lake via `delta-rs`. Where Arrow alone is ambiguous, the accumulator attaches a
**logical type tag** in field metadata (`data-avalanche.logical`, see "Logical type tags" below); the
accumulator never knows the target format, and any value conversion a format needs (e.g. uuid →
string for Delta) happens inside that sink. See `Multi_Sink_Architecture_and_Delta_Lake.md` §6.5.

---

## Required Extensions to the `ColumnValue` Enum

The current enum in `Data-Flow.md` has four gaps that block correct mapping:

```rust
// CURRENT (from Data-Flow.md) — has gaps:
ColumnValue = Null
            | Bool(bool)
            | Int16(i16) | Int32(i32) | Int64(i64)
            | Float32(f32) | Float64(f64)
            | Numeric(Decimal)
            | Text(String)
            | Bytes(Vec<u8>)
            | Timestamp(DateTime<Utc>)     // ⚠ Gap A: no tz-awareness flag
            | Date(NaiveDate)
            | Uuid(Uuid)
            | Json(serde_json::Value)
            | Array(Vec<ColumnValue>)      // ⚠ Gap B: no element type annotation

// PROPOSED — four additions:
ColumnValue = Null
            | Bool(bool)
            | Int16(i16) | Int32(i32) | Int64(i64)
            | UInt64(u64)                             // ➕ Gap C: MySQL UNSIGNED BIGINT
            | Float32(f32) | Float64(f64)
            | Numeric(Decimal)
            | Text(String)
            | Bytes(Vec<u8>)
            | Timestamp { value: DateTime<Utc>, with_tz: bool }  // ➕ Gap A fixed
            | Date(NaiveDate)
            | Time(NaiveTime)                         // ➕ Gap D: TIME columns (PG + MySQL); landed as Time(i64), Q-M3-3
            | Uuid(Uuid)
            | Json(serde_json::Value)
            | Array { element_type: ColumnValueType, values: Vec<ColumnValue> }  // ➕ Gap B fixed

// ColumnValueType mirrors the ColumnValue variants at the type level (no payload):
ColumnValueType = NullType | BoolType | Int16Type | Int32Type | Int64Type
                | Float32Type | Float64Type | NumericType(u8, i8)
                | TextType | BytesType | TimestampType | DateType | TimeType
                | UuidType | JsonType | ArrayType(Box<ColumnValueType>)
```

**Why each extension matters:**

| Gap | Impact if missing |
|-----|-------------------|
| A — `Timestamp` tz-awareness | PG `timestamp` and `timestamptz` both fold into `DateTime<Utc>`, losing the "no timezone" distinction; Iceberg gets `timestamptz` for both, which is semantically wrong for naive timestamps |
| B — `Array` element type | Accumulator cannot build a typed `arrow::array::ListBuilder<T>` without knowing `T`; would have to fall back to opaque `LargeUtf8`, defeating columnar analytics |
| C — `UInt64` | MySQL `BIGINT UNSIGNED` has a value range 0..2⁶⁴−1 which overflows `i64`; silently truncating is a correctness bug |
| D — `Time(NaiveTime)` | PG `time` and MySQL `TIME` have no date component; forcing into `Timestamp` requires an arbitrary epoch date and produces misleading data |

**Representation (v1, 2026-09-26):** `Timestamp` carries `micros: i64` and `Date` carries `i32` days since
the Unix epoch, exactly the ranges of Arrow `Timestamp(Microsecond)` and `Date32`. chrono's
`DateTime`/`NaiveDate` are narrower, so they couldn't hold the clamped edge values below.

---

## PostgreSQL Type Mapping

Column type information arrives via the pgoutput **Relation** message (OID per column). The
`pgwire-replication` crate exposes the OID; the decode layer must implement the table below.

### Core Types

| PostgreSQL Type | OID | ColumnValue Variant | Arrow DataType | Iceberg Type | Notes |
|----------------|-----|--------------------|--------------|-----------|----|
| `boolean` | 16 | `Bool(bool)` | `Boolean` | `boolean` | |
| `smallint` / `int2` | 21 | `Int16(i16)` | `Int16` | `int` | Iceberg `int` is 32-bit; widening is lossless |
| `integer` / `int4` | 23 | `Int32(i32)` | `Int32` | `int` | |
| `bigint` / `int8` | 20 | `Int64(i64)` | `Int64` | `long` | |
| `real` / `float4` | 700 | `Float32(f32)` | `Float32` | `float` | |
| `double precision` / `float8` | 701 | `Float64(f64)` | `Float64` | `double` | |
| `numeric(P,S)` / `decimal(P,S)` | 1700 | `Numeric(Decimal)` | `Decimal128(P, S)` | `decimal(P, S)` | ⚠ See precision cap note |
| `money` | 790 | `Numeric(Decimal)` | `Decimal128(19, 2)` | `decimal(19, 2)` | Fixed scale 2; locale-dependent symbol stripped |
| `char(n)` | 1042 | `Text(String)` | `Utf8` | `string` | Trailing spaces stripped before encoding |
| `varchar(n)` / `character varying` | 1043 | `Text(String)` | `Utf8` | `string` | |
| `text` | 25 | `Text(String)` | `Utf8` | `string` | |
| `"char"` (single-byte internal) | 18 | `Int16(i16)` | `Int8` | `int` | Internal PG type; rare in user tables |
| `name` | 19 | `Text(String)` | `Utf8` | `string` | Internal PG type; max 63 bytes |
| `bytea` | 17 | `Bytes(Vec<u8>)` | `Binary` | `binary` | |
| `oid` | 26 | `Int64(i64)` | `Int64` | `long` | Technically u32; widened to avoid sign issues |

### Date and Time Types

| PostgreSQL Type | OID | ColumnValue Variant | Arrow DataType | Iceberg Type | Notes |
|----------------|-----|--------------------|--------------|-----------|----|
| `date` | 1082 | `Date(i32)` | `Date32` | `date` | Days since Unix epoch; `±infinity` clamps (see Date/time edge values) |
| `time` | 1083 | `Time(i64)` | `Time64(Microsecond)` | `time` | Signed microseconds; `24:00:00` is 86 400 000 000 (see Date/time edge values) |
| `timetz` / `time with time zone` | 1266 | `Text(String)` | `Utf8` | `string` | ⚠ No Arrow/Iceberg equivalent; serialized as ISO-8601 |
| `timestamp` | 1114 | `Timestamp { value, with_tz: false }` | `Timestamp(Microsecond, None)` | `timestamp` | No timezone; microsecond precision |
| `timestamptz` / `timestamp with time zone` | 1184 | `Timestamp { value, with_tz: true }` | `Timestamp(Microsecond, Some("UTC"))` | `timestamptz` | PG always stores UTC internally |
| `interval` | 1186 | `Text(String)` | `Utf8` | `string` | ⚠ No Iceberg equivalent; ISO-8601 duration string (e.g. `P1Y2M3DT4H5M6S`) |

### Text-Serialized Types (No Direct Analogue in Iceberg)

These types have no Iceberg counterpart. They are decoded to their canonical text
representation and stored as `string`. This preserves all data and remains queryable
via SQL `LIKE` and `CAST` in engines like Spark or Trino.

| PostgreSQL Type | OID | ColumnValue | Arrow | Iceberg | Serialization format |
|----------------|-----|------------|-------|---------|---------------------|
| `inet` | 869 | `Text(String)` | `Utf8` | `string` | CIDR notation, e.g. `192.168.1.1` or `::1` |
| `cidr` | 650 | `Text(String)` | `Utf8` | `string` | Network address, e.g. `192.168.0.0/24` |
| `macaddr` | 829 | `Text(String)` | `Utf8` | `string` | `xx:xx:xx:xx:xx:xx` |
| `macaddr8` | 774 | `Text(String)` | `Utf8` | `string` | `xx:xx:xx:xx:xx:xx:xx:xx` |
| `point` | 600 | `Text(String)` | `Utf8` | `string` | `(x,y)` |
| `line` | 628 | `Text(String)` | `Utf8` | `string` | `{A,B,C}` |
| `lseg` | 601 | `Text(String)` | `Utf8` | `string` | `[(x1,y1),(x2,y2)]` |
| `box` | 603 | `Text(String)` | `Utf8` | `string` | `(x1,y1),(x2,y2)` |
| `path` | 602 | `Text(String)` | `Utf8` | `string` | `((x1,y1),...)` or `[(x1,y1),...]` |
| `polygon` | 604 | `Text(String)` | `Utf8` | `string` | `((x1,y1),(x2,y2),...)` |
| `circle` | 718 | `Text(String)` | `Utf8` | `string` | `<(x,y),r>` |
| `int4range` | 3904 | `Text(String)` | `Utf8` | `string` | `[lower,upper)` |
| `int8range` | 3926 | `Text(String)` | `Utf8` | `string` | `[lower,upper)` |
| `numrange` | 3906 | `Text(String)` | `Utf8` | `string` | `[lower,upper)` |
| `tsrange` | 3908 | `Text(String)` | `Utf8` | `string` | ISO-8601 bounds |
| `tstzrange` | 3910 | `Text(String)` | `Utf8` | `string` | ISO-8601 bounds with UTC offset |
| `daterange` | 3912 | `Text(String)` | `Utf8` | `string` | `[lower,upper)` |
| `tsvector` | 3614 | `Text(String)` | `Utf8` | `string` | PG text-search representation |
| `tsquery` | 3615 | `Text(String)` | `Utf8` | `string` | PG text-search query |
| `xml` | 142 | `Text(String)` | `Utf8` | `string` | Raw XML |

### Structured and Special Types

| PostgreSQL Type | OID | ColumnValue | Arrow | Iceberg | Notes |
|----------------|-----|------------|-------|---------|-------|
| `uuid` | 2950 | `Uuid(Uuid)` | `FixedSizeBinary(16)` | `uuid` | Stored as 16 raw bytes; iceberg-rust maps `FixedSizeBinary(16)` → `uuid` |
| `json` | 114 | `Json(serde_json::Value)` | `Utf8` | `string` | Serialized as compact JSON string; numbers keep every digit (serde_json `arbitrary_precision`, Q-M5-11) |
| `jsonb` | 3802 | `Json(serde_json::Value)` | `Utf8` | `string` | Semantically same as json after decode; its `numeric` numbers keep every digit (Q-M5-11) |
| `bit(n)` | 1560 | `Bytes(Vec<u8>)` | `Binary` | `fixed(⌈n/8⌉)` | MSB-first packed |
| `varbit(n)` | 1562 | `Bytes(Vec<u8>)` | `Binary` | `binary` | PostgreSQL's binary form: a 4-byte big-endian bit count, then the bits packed MSB-first, the last byte zero-padded (Q-M5-9) |
| User-defined `ENUM` | varies | `Text(String)` | `Utf8` | `string` | pgoutput sends the label string, not the OID index |
| `hstore` | 16935 (ext) | `Json(serde_json::Value)` | `Utf8` | `string` | Decoded as `{"key":"val"}` JSON object |
| `any[]` (array of type T) | varies | `Array { element_type, values }` | `List<T_arrow>` | `list<T_iceberg>` | Element type resolved from OID; nested arrays supported recursively |
| PostGIS `geometry` / `geography` | ext | `Bytes(Vec<u8>)` | `Binary` | `binary` | EWKB (WKB with the SRID): the hex text pgoutput sends, decoded to bytes (M5 D-M5.10) |
| Composite / user-defined | varies | `Text(String)` | `Utf8` | `string` | Serialized as `(f1,f2,...)` PG text; ⚠ consider `Bytes` for non-UTF data |

### PostgreSQL-Specific Decision Notes

**Numeric precision cap.** Iceberg `decimal` supports a maximum of 38 significant digits.
PostgreSQL `numeric` is unconstrained. Mapping rule (M5 D-M5.9, Q-M5-8):
- If `P ≤ 38`: map to `Decimal128(P, S)` as-is.
- A negative scale (PostgreSQL 15+, `numeric(P,-S)`) widens to `Decimal128(P+|S|, 0)`, since
  Delta and Iceberg require scale ≥ 0. A scale above the precision (`numeric(2,4)`) maps to
  `Decimal128(S, S)`.
- If `P > 38`: under `numeric_overflow_action: string`, store as `Text(String)` → `Utf8` →
  `string`, with a warning at schema resolution; under `error`, the table halts at resolution.
- If precision is unspecified (`numeric` without args): under `string`, text as above; under
  `error`, `Decimal128(38, 18)`, and a value that doesn't fit halts the table
  (`NUMERIC_OVERFLOW`). V-079 warns about this at resolution.
- `NaN` and `±Infinity` have no decimal form: a column holding one halts its table
  (`SCHEMA_MISMATCH`); `column_type_overrides: string` keeps them as text.

**TOAST columns.** When a large column (`text`, `bytea`, `jsonb`) is unchanged in an `UPDATE`
event, pgoutput emits an "unchanged TOAST" sentinel instead of the value. The pipeline must
carry forward the last known value from the previous event for that column, or issue a
`SELECT` for the current value via the control connection. The fallback strategy (carry-forward
vs re-query) must be a pipeline config option because re-query has latency cost.

**Array element type resolution.** PG array OIDs follow the pattern: the array OID for a
base type is `base_OID + 1` in most cases, but canonical resolution requires querying
`pg_type.typelem`. The decode layer must maintain a local `OID → base OID` cache, populated
from the Relation messages and supplemented by a startup `pg_type` query.

---

## MySQL Type Mapping

Column type information arrives via the **TABLE_MAP_EVENT** in the MySQL binlog. The
`mysql_async` binlog stream (`mysql_common` row decoding; it replaced `mysql-binlog-connector-rust`,
see `prototypes/source-capability/`) exposes the column type code and optional metadata.
`ENUM` and `SET` member lists, column signedness, and `TINYINT(1)` display widths require
supplementary schema data from `information_schema` — this is the primary reason
`schema_evolution.method: query` is the default for MySQL.

### Integer Types

MySQL integers exist in both signed and unsigned variants. Iceberg has no unsigned integer
types. The mapping policy is **widening without loss**: each unsigned integer is stored in
the next larger signed Iceberg type, except `UNSIGNED BIGINT` which requires `decimal(20, 0)`.

| MySQL Type | Signed range | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|-------------|------------|--------------|-----------|-------|
| `TINYINT` (signed) | −128..127 | `Int16(i16)` | `Int8` | `int` | Iceberg `int` is 32-bit; Int8 Arrow type still maps to `int` |
| `TINYINT(1)` (signed) | −128..127 | `Bool(bool)` | `Boolean` | `boolean` | ⚠ Requires `information_schema` to distinguish from plain TINYINT; see note |
| `TINYINT UNSIGNED` | 0..255 | `Int16(i16)` | `Int16` | `int` | Widened |
| `SMALLINT` (signed) | −32768..32767 | `Int16(i16)` | `Int16` | `int` | |
| `SMALLINT UNSIGNED` | 0..65535 | `Int32(i32)` | `Int32` | `int` | Widened |
| `MEDIUMINT` (signed) | −8M..8M | `Int32(i32)` | `Int32` | `int` | |
| `MEDIUMINT UNSIGNED` | 0..16M | `Int32(i32)` | `Int32` | `int` | Fits in i32 |
| `INT` / `INTEGER` (signed) | −2B..2B | `Int32(i32)` | `Int32` | `int` | |
| `INT UNSIGNED` | 0..4B | `Int64(i64)` | `Int64` | `long` | Widened |
| `BIGINT` (signed) | −2⁶³..2⁶³−1 | `Int64(i64)` | `Int64` | `long` | |
| `BIGINT UNSIGNED` | 0..2⁶⁴−1 | `UInt64(u64)` | `Decimal128(20, 0)` | `decimal(20, 0)` | ⚠ No signed 64-bit type can hold the full range; must use decimal |

**`TINYINT(1)` detection.** MySQL 8.0+ removes display width from the binlog TABLE_MAP_EVENT.
The boolean mapping (`TINYINT(1)` → `Bool`) requires a supplementary
`SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE ...` query during schema resolution.
A config toggle `source.mysql.tinyint1_as_bool: true` (default `true`) should control this.
When disabled, `TINYINT(1)` maps as `TINYINT SIGNED` → `Int16`.

### Floating Point and Decimal Types

| MySQL Type | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|------------|--------------|-----------|-------|
| `FLOAT` | `Float32(f32)` | `Float32` | `float` | Single precision |
| `DOUBLE` / `DOUBLE PRECISION` / `REAL` | `Float64(f64)` | `Float64` | `double` | |
| `DECIMAL(P,S)` / `NUMERIC(P,S)` | `Numeric(Decimal)` | `Decimal128(P, S)` | `decimal(P, S)` | MySQL max P=65; cap at 38, fall back to `string` if exceeded |

### String Types

| MySQL Type | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|------------|--------------|-----------|-------|
| `CHAR(n)` | `Text(String)` | `Utf8` | `string` | Right-padded spaces stripped |
| `VARCHAR(n)` | `Text(String)` | `Utf8` | `string` | |
| `TINYTEXT` | `Text(String)` | `Utf8` | `string` | Max 255 bytes |
| `TEXT` | `Text(String)` | `Utf8` | `string` | Max 65,535 bytes |
| `MEDIUMTEXT` | `Text(String)` | `Utf8` | `string` | Max 16 MiB |
| `LONGTEXT` | `Text(String)` | `Utf8` | `string` | Max 4 GiB at the server, but a replicated value is at most 1 GiB (`max_allowed_packet`); batches roll over instead of `LargeUtf8` (see Arrow widths at Hop 2) |

### Binary Types

| MySQL Type | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|------------|--------------|-----------|-------|
| `BINARY(n)` | `Bytes(Vec<u8>)` | `Binary` | `fixed(n)` | Right-padded with `\0` |
| `VARBINARY(n)` | `Bytes(Vec<u8>)` | `Binary` | `binary` | |
| `TINYBLOB` | `Bytes(Vec<u8>)` | `Binary` | `binary` | Max 255 bytes |
| `BLOB` | `Bytes(Vec<u8>)` | `Binary` | `binary` | |
| `MEDIUMBLOB` | `Bytes(Vec<u8>)` | `Binary` | `binary` | |
| `LONGBLOB` | `Bytes(Vec<u8>)` | `Binary` | `binary` | Max 4 GiB |

### Date and Time Types

| MySQL Type | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|------------|--------------|-----------|-------|
| `DATE` | `Date(i32)` | `Date32` | `date` | `0000-00-00` → NULL; zero-in-part / invalid dates halt the table (see Date/time edge values) |
| `TIME[(fsp)]` | `Time(i64)` | `Time64(Microsecond)` | `time` | `fsp` 0–6; an elapsed time, −838:59:59…838:59:59, as signed microseconds (see Date/time edge values) |
| `DATETIME[(fsp)]` | `Timestamp { value, with_tz: false }` | `Timestamp(Microsecond, None)` | `timestamp` | No timezone; MySQL stores as-is |
| `TIMESTAMP[(fsp)]` | `Timestamp { value, with_tz: true }` | `Timestamp(Microsecond, Some("UTC"))` | `timestamptz` | MySQL converts to UTC on write; returned as UTC |
| `YEAR` | `Int32(i32)` | `Int32` | `int` | 4-digit year integer (1901..2155); 0000 is also valid |

### Structured and Special Types

| MySQL Type | ColumnValue | Arrow DataType | Iceberg Type | Notes |
|-----------|------------|--------------|-----------|-------|
| `JSON` | `Json(serde_json::Value)` | `Utf8` | `string` | MySQL's binary JSON decoded to value, serialized as string |
| `ENUM('a','b',...)` | `Text(String)` | `Utf8` | `string` | Binlog TABLE_MAP carries member list; decode integer index to label |
| `SET('a','b',...)` | `Array { element_type: TextType, values }` | `List<Utf8>` | `list<string>` | Binlog carries bitmask; decode to list of active member labels |
| `BIT(N)` | `Bytes(Vec<u8>)` | `Binary` | `fixed(⌈N/8⌉)` | MSB-first packed bits; N ∈ 1..64 |
| `GEOMETRY` | `Bytes(Vec<u8>)` | `Binary` | `binary` | WKB format |
| `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | `Bytes(Vec<u8>)` | `Binary` | `binary` | WKB format |

---

## Date/time edge values (Q-M5-1, Q-M7-1 — decided 2026-09-26)

**PostgreSQL — clamp.** `infinity` / `-infinity` and values outside the Arrow range clamp to the
Arrow maximum/minimum of the column's Arrow type: `Date32` → `i32::MIN` / `i32::MAX` days;
`Timestamp(Microsecond, _)` → `i64::MIN` / `i64::MAX` µs.

| PostgreSQL value | Written as |
|---|---|
| `date` / `timestamp` / `timestamptz` `-infinity` | Arrow minimum |
| `date` / `timestamp` / `timestamptz` `infinity` | Arrow maximum |
| `timestamp[tz]` beyond the i64-µs range (294247–294276 AD) | Arrow maximum |

The clamped values are valid Arrow values, and Hop 2 carries them unchanged.

**Delta sink — clamp again to the Delta range (Q-M4-2, decided 2026-09-30).** The Delta protocol
covers 0001-01-01 … 9999-12-31 23:59:59.999999. delta-rs 1.0 panics writing a `Date32` of
`i32::MIN` (in its statistics), and readers fail beyond the range (DuckDB: "Date out of range in
timestamp conversion"). So Hop 3b clamps every `date`, `timestamp` and `timestamp_ntz` cell to
that range: PostgreSQL `±infinity` lands on `0001-01-01` / `9999-12-31` (and
`… 23:59:59.999999` for timestamps). A `timestamp_ntz` stored as `string` is not clamped; its
text form covers all of `i64`. The M4 round trip (Task 13) reads the bounds back through delta-rs
and DuckDB `delta_scan`.

**MySQL / MariaDB — NULL for "no date", halt for invalid dates.** Measured 2026-09-26: MySQL 8.4's
default `sql_mode` rejects all of the values below, but any session can relax it
(`sql_mode=''`); **MariaDB 11.8's default `sql_mode` accepts zero dates and zero-in-part dates**;
`ALLOW_INVALID_DATES` stores days like `2024-02-31` on both. The binlog carries what was stored.

| MySQL/MariaDB value | Written as |
|---|---|
| All-zero `DATE` `0000-00-00`, `DATETIME` / `TIMESTAMP` `0000-00-00 00:00:00` | **NULL**. It is the server's "no date" sentinel and carries no information. Non-key columns are always nullable in the sink (Multi_Sink §6). In an **identifier (key) column**, NULL is impossible → halt as below |
| Zero month or day (`2024-00-15`, `2024-05-00 10:00:00`; accepted without `NO_ZERO_IN_DATE`) | **Halt the table** `INVALID_DATE_VALUE` |
| Impossible day of month (`2024-02-31`; `ALLOW_INVALID_DATES`) | **Halt the table** `INVALID_DATE_VALUE` |

`INVALID_DATE_VALUE` halts only that table (other tables continue). The message names the table,
column, raw value and binlog position, and gives the remedy: set
`tables.column_type_overrides: {<column>: string}`, which replicates the server's raw text
(`2024-00-15`), then resume or re-snapshot the table; or correct the source rows. A partial date
such as `1980-00-00` usually means "year known, month/day unknown", so no silent conversion
(clamp or NULL) is applied.

**`time` — signed microseconds (Q-M3-3, decided 2026-09-29).** `ColumnValue::Time(i64)` holds
signed microseconds of elapsed time, so both engines' full ranges fit without clamping: PostgreSQL
`time` is `0…86 400 000 000` (`24:00:00` included), MySQL/MariaDB `TIME` is an interval of
−838:59:59…838:59:59. Arrow `Time64(Microsecond)` holds any `i64`. Delta has no `time` type: Hop 3b
writes `time_text`'s `[-]HH:MM:SS.ffffff` (`24:00:00.000000`, `838:59:59.000000`,
`-01:30:00.500000`), which keeps out-of-day values.

## Unsupported Type Policy

Any column type encountered that is not in the tables above follows this priority chain:

```
1. If a per-column override is configured (tables.column_type_overrides):
      → apply the override mapping
2. If the type is a PG array of an unsupported element type:
      → store as Text(String) → Utf8 → Iceberg string
         (PG text representation of the array, e.g. `{a,b,c}`)
3. If the type's wire encoding is textual (most PG types support text output):
      → Text(String) → Utf8 → Iceberg string
         log WARN: "column <name> type <type> has no native mapping; stored as string"
4. If the type's wire encoding is binary-only with no text fallback:
      → Bytes(Vec<u8>) → Binary → Iceberg binary
         log WARN: "column <name> type <type> stored as raw bytes"
5. If neither is possible (type decode error):
      → halt the column (treat as NULL for all events); log ERROR
         continue processing other columns in the table
```

**Override conversions (Q-M5-12, 2026-10-01).** Step 1 is applied by the source decoder at Hop 1,
since Hop 2 converts nothing (D-M3.6). An override equal to the column's mapped type changes
nothing. `string` keeps the source's text, and `decimal(P,S)` on a numeric/decimal column (or
`list<decimal(P,S)>` on an array of them) decodes as that decimal, halting `NUMERIC_OVERFLOW` per
value that doesn't fit. Otherwise the override must be one of the lossless conversions in
`data-avalanche-core` (`conversion`, `ColumnValue::convert`), which keep every value of the source
type:

| From | To |
|---|---|
| `int16` | `int32`, `int64`, `float64`, `decimal(P,S)` with `P − S ≥ 5` |
| `int32` | `int64`, `float64`, `decimal(P,S)` with `P − S ≥ 10` |
| `int64` | `decimal(P,S)` with `P − S ≥ 19` |
| `float32` | `float64`, through the shortest decimal text (`0.1` stays `0.1`) |
| `timestamp_ntz` | `timestamp_tz`: the wall clock read as UTC (as `timestamp_ntz_as: timestamp`) |
| `timestamp_tz` | `timestamp_ntz`: the UTC wall clock |
| `date` | `timestamp_ntz`, `timestamp_tz`: midnight; `±infinity` stays infinite; a date past the i64-µs range (294247 AD on) clamps to the maximum, as a timestamp does (Q-M5-1) |
| `list<T>` | `list<U>` when `T → U` is one of the above |

Any other override halts the table at resolution (`SCHEMA_MISMATCH`). That covers narrowing, a
float to a decimal, a decimal to a float, `int64 → float64`, `timestamp → date`, and text parsed as
`uuid` or `json`.

This policy ensures no data is silently lost. All fallbacks are logged at `WARN` or above so
operators know which columns require attention.

---

## Logical type tags (sink-neutral, Hop 2 → Hop 3 boundary)

The accumulator's Arrow field carries `data-avalanche.logical = <tag>` in field metadata whenever the
Arrow type does not determine the intended type on its own. The same vocabulary is the grammar of
`tables.column_type_overrides` (config §9.3, V-078/V-087):

`bool` · `int16` · `int32` · `int64` · `float32` · `float64` · `decimal(P,S)` · `string` · `binary` ·
`date` · `time` · `timestamp_tz` · `timestamp_ntz` · `uuid` · `json` · `list<T>`

Tags actually attached by Hop 2: `uuid` (FixedSizeBinary(16)), `time`, `json` (Utf8), `timestamp_tz` /
`timestamp_ntz` (explicit, because Iceberg `timestamp` = *without* tz while Delta `timestamp` = *with*
tz), `uint64` provenance on `Decimal128(20,0)` (informational), `enum` / `set` provenance on Utf8 /
List<Utf8> (informational; attached once the MySQL decoder lands, M7).

**Arrow widths at Hop 2 (D-M3.5, `doc/v1.0/M3-core-data-path.md`).** Every byte column is `Binary` and
every text column `Utf8`, never the `Large*` types; `FixedSizeBinary(16)` is used only for `uuid`.
A value is at most 1 GiB (the PostgreSQL and MySQL limits), and the accumulator rolls a batch over
before its rows reach 512 MiB, so a column stays under the `i32` offsets. Fixed-width source types
(`BINARY(n)`, `bit(n)`, `BIT(N)`) are `Binary` in Arrow; the Iceberg `fixed(n)` in the tables above is
chosen from the source type at Hop 3a (v2).

## Hop 3a: Arrow → Iceberg Type Mapping (via iceberg-rust)

`iceberg-rust` handles Arrow → Iceberg type conversion automatically for standard types.
Non-obvious mappings that need explicit annotation in the `SchemaBuilder`:

| Arrow DataType | Iceberg Type | Annotation required? |
|---------------|-------------|---------------------|
| `Boolean` | `boolean` | No |
| `Int8`, `Int16`, `Int32` | `int` | No |
| `Int64` | `long` | No |
| `Float32` | `float` | No |
| `Float64` | `double` | No |
| `Decimal128(P, S)` | `decimal(P, S)` | No |
| `Utf8`, `LargeUtf8` | `string` | No |
| `Binary`, `LargeBinary` | `binary` | No |
| `Date32` | `date` | No |
| `Time64(Microsecond)` | `time` | No |
| `Timestamp(Microsecond, None)` | `timestamp` | No |
| `Timestamp(Microsecond, Some("UTC"))` | `timestamptz` | No |
| `FixedSizeBinary(16)` | `uuid` | **Yes** — iceberg-rust needs a field-level metadata hint `{"iceberg.type":"uuid"}` or explicit `IcebergType::Uuid` in the schema builder call |
| `FixedSizeBinary(n)` where n ≠ 16 | `fixed(n)` | **Yes** — same mechanism; distinguish from uuid by absence of the hint |
| `List<T>` | `list<T>` | No (element type propagated automatically) |
| `Struct(fields)` | `struct(fields)` | No |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | **not directly supported** | Always pre-convert to signed or `Decimal128` before building the Arrow schema; never pass unsigned Arrow types to iceberg-rust |
| `Int8` (from PG `"char"`) | `int` | No — iceberg `int` is 32-bit, Int8 widens automatically |

**Capability gate + fallback (🧪 DA-TYPE-5, `CDC_Design_Gap_Analysis_v1.md` §24.5).** The
`FixedSizeBinary(16) → uuid` mapping is the one entry that needs an explicit annotation; verify the
field-level hint mechanism (`{"iceberg.type":"uuid"}` / `IcebergType::Uuid`) exists in the pinned
iceberg-rust version (fold into the `prototypes/iceberg-capability/` probe). **If the annotation is
unavailable**, fall back to `fixed(16)` (raw bytes, data intact, only the `uuid` logical type lost) or
canonical `string` (text uuid) — both lossless of data; the impact is cosmetic.

## Hop 3b: Arrow → Delta Lake Type Mapping (via delta-rs 1.0)

Delta's type system has no `uuid`, `time`, `fixed(n)` or unsigned integers; the Delta sink converts
those columns inside its own Hop 3 (the accumulator is unchanged).

| Arrow DataType (+ tag) | Delta type | Conversion in the sink? |
|---------------|-------------|---------------------|
| `Boolean` | `boolean` | No |
| `Int8` | `byte` | No |
| `Int16` | `short` | No |
| `Int32` | `integer` | No |
| `Int64` | `long` | No |
| `Float32` / `Float64` | `float` / `double` | No |
| `Decimal128(P, S)`, P ≤ 38 (incl. `uint64` → `decimal(20,0)`) | `decimal(P, S)` | No |
| `Utf8` (incl. `json`, `enum`, overflowed `numeric`) | `string` | No (Variant for `json` is a later option) |
| `Binary` | `binary` | No |
| `Date32` | `date` | No |
| `Time64(Microsecond)` (`time`) | `string` `HH:MM:SS.ffffff` | **Yes** — D-MS.8; override to `int64` for µs-since-midnight |
| `Timestamp(Microsecond, Some("UTC"))` (`timestamp_tz`) | `timestamp` | No |
| `Timestamp(Microsecond, None)` (`timestamp_ntz`) | `timestamp_ntz` (table feature `timestampNtz`; default) — or per `sink.delta.timestamp_ntz_as`: `timestamp` (wall clock as UTC instant) / `string` (`YYYY-MM-DD HH:MM:SS.ffffff`, any `i64`) | Only for `timestamp` / `string` (DA-MS-8) |
| `FixedSizeBinary(16)` (`uuid`) | `string` (canonical 36-char, lowercase) | **Yes** |
| `FixedSizeBinary(n)`, n ≠ 16 | `binary` | **Yes** (cast) |
| `List<T>` (incl. `set` → `list<string>`) | `array<T>` | No |
| `Struct(fields)` | `struct<…>` | No |
| unsigned Arrow types | — | Never reach Hop 3 (pre-converted in Hop 2, same as 3a) |

**Verified 2026-09-26 (probe P10, `prototypes/delta-capability/`):** every row above round-trips
exactly through the sink's append and MERGE paths and is read back identically by delta-rs and by
DuckDB's `delta_scan` (delta-kernel). **Interop note:** a `timestamp_ntz` column upgrades the table to
reader v3 / writer v7 (`timestampNtz` table feature); readers without table-feature support cannot
open it (gap DA-MS-8). **Decided (D-MS.16):** keep `timestamp_ntz` by default; `sink.delta.timestamp_ntz_as:
timestamp | string` is the explicit escape hatch for legacy-reader estates (config V-114).

**Decided for the v1 sink (M4, `doc/v1.0/M4-delta-sink.md` D-M4.1–D-M4.3, 2026-09-30):**
- Every column the sink creates carries its logical type in the Delta column metadata
  (`data-avalanche.type`), plus the Hop 2 tag. Reading a table's schema back therefore gives the Hop 2
  field exactly, although a Delta `string` may hold a uuid, a time or a `timestamp_ntz`.
- The conversion follows the table's column type, not the current `timestamp_ntz_as`: an existing
  column keeps its representation.
- Date and timestamp cells are clamped to the Delta range (§Date/time edge values, Q-M4-2).
- A negative decimal scale (`numeric(p,-s)`) is refused at Hop 3b: Delta requires scale ≥ 0. M5's
  Hop 1 widens such a column to scale 0 so it never reaches the sink.
- Hop 2 produces `FixedSizeBinary` only for uuid and no `Struct` or `Int8` (M3 D-M3.5), so those
  rows of the table above don't arise in v1.

---

## Schema Resolution Lifecycle

The Accumulator must build a stable Arrow schema for each table before processing the first
event. The resolution sequence is:

```
1. [Replication path] — on first Relation message for a table:
   a. Extract column OIDs (PG) or column type codes (MySQL)
   b. Resolve each column to a ColumnValueType using the tables above
   c. Compute the Arrow DataType for each ColumnValueType
   d. Build and cache the Arrow Schema

2. [Snapshot path] — at start of COPY / SELECT for a table:
   a. Query information_schema (or pg_attribute) for column type OIDs and names
   b. Same resolution as step 1b–d

3. On schema_evolution interval (if method = query):
   a. Re-query source for current column list
   b. Compare with cached schema
   c. On new nullable column: extend Arrow Schema + update Iceberg schema via catalog
   d. On type change or column drop: evaluate safe vs unsafe (see DDL Classification below)

4. [MySQL only] — after TABLE_MAP_EVENT:
   a. Update cached table_id → column_type mapping
   b. If column count differs from cached schema → trigger schema_evolution check immediately
```

---

## DDL Classification (Safe vs Unsafe — resolves G-19 partially)

Applies when a schema change is detected during `schema_evolution` detection:

| Change | Classification | Action |
|--------|---------------|--------|
| Add nullable column | **Safe** | Add column to Arrow schema + Iceberg schema; fill `null` for historical rows |
| Add column with DEFAULT | **Safe** | Same as above; emit WARN that historical rows will be NULL not the DEFAULT |
| Widen numeric type (e.g. `int` → `bigint`, `float` → `double`) | **Safe** | Update column type in Arrow schema + Iceberg schema |
| Increase varchar length | **Safe** | No schema change needed (string is string) |
| Drop column | **Unsafe** | Halt the affected table; log ERROR; continue others |
| Rename column | **Unsafe** | Halt; treat as drop + add (two-step process requiring config annotation) |
| Narrow numeric type | **Unsafe** | Halt; log ERROR |
| Change base type (e.g. `int` → `text`) | **Unsafe** | Halt; log ERROR |
| PG ENUM: add new label | **Safe** | No Iceberg schema change needed (stored as string) |
| PG ENUM: rename/drop label | **Unsafe** | Halt; log ERROR |
| MySQL TINYINT(1) to TINYINT | **Safe** | Toggle from `boolean` to `int`; requires manual config override to re-enable |

---

## Implied Config Additions

The following config fields are needed to support this mapping specification and do not
currently exist in `Configuration_file_model.md`:

```yaml
defaults:
  source:
    mysql:
      tinyint1_as_bool: true  # optional, default true.
                               # map TINYINT(1) columns to boolean.
                               # requires information_schema query during schema resolution.
      tinyint1_detection: information_schema  # optional, information_schema (default) or none.
                                              # 'none' disables the detection query; all TINYINT treated as int.
    toast_strategy: carry_forward  # postgres only. carry_forward (default) | re_query.
                                   # carry_forward: use last known value for unchanged TOAST columns.
                                   # re_query: issue a SELECT to fetch current value (adds latency).

  tables:
    column_type_overrides: {}  # optional. map of column name → sink-neutral logical type
                               # (grammar: see "Logical type tags"; never a format-specific name).
                               # e.g.:  public.orders.amount: "decimal(38,10)"
                               # e.g.:  public.orders.payload: "binary"
                               # overrides apply at schema resolution time.
    numeric_overflow_action: string  # optional, string (default) | error.
                                     # action when numeric(P,S) with P > 38 is encountered.
                                     # 'string': store as logical string with a WARN log.
                                     # 'error': halt the table.
```

---

## Validation Rules (to be merged into G-1f master table)

| Constraint | Severity | Message |
|-----------|----------|---------|
| `column_type_overrides` value is not a valid logical type (merged as V-078; per-sink mappability V-087) | error | "invalid logical type `<value>` in column_type_overrides for `<table>.<column>`" |
| `source.mysql.tinyint1_detection: information_schema` with `schema_evolution.method: events` | warning | "tinyint1_as_bool requires information_schema queries; schema_evolution.method:events cannot detect display width changes — set method:query" |
| `toast_strategy: re_query` with `snapshot_mode: always` | warning | "re_query TOAST strategy on high-volume snapshots may saturate the control connection pool" |
| `numeric_overflow_action: error` and any source table has unscaled `numeric` columns | warning (emitted at schema resolution) | "numeric column `<col>` has no precision declared; overflow action is 'error' which may halt the table unexpectedly — consider explicit column_type_override" |

---

## Sources

This document resolves **G-18** (type mapping table — previously 🔴 Blocker) and partially
resolves **G-19** (DDL classification table).

It depends on the following sources from the notebook index:

- **PostgreSQL: Documentation: 18: Chapter 29. Logical Replication** — pgoutput Relation
  message format, OID-based column type delivery
- **MySQL :: MySQL 8.4 Reference Manual :: 19 Replication** — TABLE_MAP_EVENT column type
  codes, RBR before/after image format
- **Rust — Apache Arrow** — Arrow DataType enum, array builder API
- **Apache Iceberg Rust 0.9.0 Release** — Arrow → Iceberg schema conversion API in iceberg-rust
- **Apache Iceberg Catalogs: Types & How to Choose** — Iceberg type system definition
- **rustcdc** — ColumnValue enum scope and integration contract
- **mysql-binlog-connector-rust** — MySQL column type decoding, ENUM/SET bitmask handling (original
  reference; the implementation uses `mysql_async` / `mysql_common`)
- **pgwire-replication** — PG OID-based column type delivery, TOAST sentinel handling
