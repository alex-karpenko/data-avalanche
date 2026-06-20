# CDC System — Cowork Project Instructions

---

## What this project is

A **Rust-based Change Data Capture (CDC) system** that replicates row-level changes from
PostgreSQL and MySQL into Apache Iceberg tables stored on cloud object storage (S3, GCS, Azure
Blob). Key subsystems: distributed worker cluster (consistent hashing + SWIM/memberlist), Apache
Arrow as the in-memory columnar format, and multiple Iceberg catalog backends (AWS Glue, HMS, REST).

Work style: **requirements-driven, iterative refinement**. Claude performs gap analysis, authors research documents, and maintains the living gap tracker — all as in-place file edits. Claude authors and maintains specifications.

---

## Repository layout

```
data-avalanche/
├── .claude/CLAUDE.md                         ← this file
|-- doc
    ├── specs/
    │   ├── System-Overview.md                    authoritative top-level requirements
    │   ├── Data-Flow.md                          end-to-end data-flow spec
    │   ├── Configuration_Specification.md        full YAML config data model
    │   └── Schema_Change_Detection_and_Evolution.md   DDL detection & schema evolution
    ├── research/
    │   ├── Type_Mapping.md                       three-hop type mapping (DB → ColumnValue → Arrow → Iceberg)
    │   ├── Catalog_Auth_Research.md              catalog authentication protocols
    │   └── Clustering_Architecture_Research.md   clustering design proposal
    ├── analysis/
    │   └── CDC_Gap_Analysis_Report.md            living gap tracker (current: v9)
    └── sources/
        └── Sources_Index_v3.md                   50-source external knowledge index (read-only reference)
```

---

## Document authority and edit rules

| Directory | Authority | Claude may edit? |
|---|---|---|
| `doc/specs/` | Source of truth | Propose patches, don't edit without approve |
| `doc/research/` | Claude authors; Oleksii reviews | Yes — in-place edits/extensions on request |
| `doc/analysis/` | Claude maintains | Yes — in-place updates every gap-analysis cycle |
| `doc/sources/` | Static reference | Never edit |

When Oleksii says **"update the gap report"** or **"re-run gap analysis"**, edit
`doc/analysis/CDC_Gap_Analysis_Report.md` in place and increment the revision number.

When Oleksii says **"write a research doc on X"**, create `doc/research/X.md` following the
frontmatter convention below.

When Oleksii says **"apply this fix to the spec"**, make the surgical `str_replace`-style edit to
the relevant `doc/specs/*.md` file and summarise what changed.

---

## Frontmatter convention

Every document must carry a YAML frontmatter block:

```yaml
---
updated: YYYY-MM-DD
domain: <one of: Architecture | Data Types | Configuration | Data Flow | Auth | Clustering | Schema Evolution>
tags: [comma, separated, tags]
resolves: [G-xx, G-yy]          # gap IDs closed or advanced by this doc (omit if none)
sources: [source name, ...]     # from Sources_Index_v3.md (omit if none)
---
```

---

## Gap analysis conventions

### Severity codes
- 🔴 **Blocker** — implementation cannot start without resolving this
- 🟡 **High** — wrong answer causes correctness bugs or data loss
- 🟢 **Medium** — wrong answer degrades performance or operability

### Resolution markers
- ✅ **Resolved** — spec or research doc fully closes this gap
- 🔄 **Partially resolved** — progress made; sub-questions remain open
- 🆕 **New gap** — surfaced by the latest revision cycle

### Gap ID system
- `G-1` … `G-40` — original gap set
- `G-NEW-1` … `G-NEW-22` — gaps surfaced during revision cycles
- `D-1` … `D-N` — design decisions (open questions within a research doc)
- `OQ-*` — open questions (e.g. `OQ-CL1`, `OQ-OB2`)

When closing a gap, mark it ✅ and add a one-line rationale pointing at the doc/section that
closes it. **Do not delete resolved gaps** — they are the audit trail.

---

## Core domain concepts (quick reference)

### Sources
- **PostgreSQL:** logical replication via `pgoutput`, WAL/LSN, replication slots, publications
- **MySQL:** binlog / GTID-set, statement vs. row-based replication

### In-memory format
- **Apache Arrow** `RecordBatch` — columnar, zero-copy
- **`ColumnValue` enum** — source-specific decoded value; four extensions in flight:
  timezone flag on `Timestamp`, element type on `Array`, new `Time(NaiveTime)` variant,
  new `UInt64(u64)` for MySQL `BIGINT UNSIGNED`

### Sink / catalog
- **Apache Iceberg** via `iceberg-rust` crate; MOR (equality deletes) or COW write modes
- **Catalogs:** AWS Glue, Hive Metastore (Thrift: NOSASL/SASL PLAIN/GSSAPI), REST (bearer/oauth2/sigv4)
- **Auth note:** Iceberg REST's built-in `/v1/oauth/tokens` is deprecated (Java 1.6.0),
  removed in 2.0 — `oauth2` must point to external IdP

### Type mapping chain
Three hops, each owned by a distinct layer:
```
DB wire format → ColumnValue (source decode) → Arrow DataType (accumulate) → Iceberg schema (sink)
```
Non-trivial cases: `TINYINT(1)` boolean detection requires `information_schema` query;
`UNSIGNED BIGINT` → `decimal(20,0)`.

### Cluster coordination
- **AP plane:** `memberlist`/SWIM for membership + `alive/suspect/dead/left` state machine;
  hash ring is *derived* from membership (not gossiped)
- **CP plane:** per-pipeline lease object in `globals.state`, acquired via **conditional writes
  (compare-and-swap)**; clock-skew-safe acquire/renew/takeover protocol
- **Single-node** is a one-member cluster with the lease active — same code path

### State storage
- Pipeline state and leases stored in `globals.state` (cloud object storage)
- All state files carry a `version` field; updates are conditional writes
- GCS caveat: uses *generation* preconditions, not ETag-on-write (verify via `opendal`)

---

## Session startup checklist

Before responding to any gap-analysis or spec-review request:

1. Read `doc/analysis/*.md` to load current gaps state.
2. Read the specific `doc/specs/` or `doc/research/` files relevant to the request.
3. Cross-reference with `doc/sources/Sources_Index_v3.md` if the topic maps to a known source.
4. Only then produce analysis or edits.

For a **full re-evaluation cycle** (all files changed), read all nine documents before writing.

---

## Commit message conventions

```
spec: <file> — <what changed>          # spec doc edit
research: <topic> — <what this covers> # new or updated research doc
gap: v<N> — <what resolved/surfaced>   # gap report update
infra: <topic>                         # CLAUDE.md or repo structure changes
```
