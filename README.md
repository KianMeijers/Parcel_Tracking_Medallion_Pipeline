# Parcel Tracking Medallion Pipeline

An end-to-end data pipeline that turns raw, messy carrier tracking data from three
different carrier integrations into a clean, validated Apache Iceberg
dataset, orchestrated by Apache Airflow and modeled to answer
business questions about parcel transit time and on-time delivery.

## Architecture

```
data/raw/                     Airflow DAG: medallion_pipeline
├── carrier_a/*.json.gz   ─┐
├── carrier_b/*.jsonl.gz  ─┼─▶  BRONZE  ─▶ [quality gate] ─▶  SILVER  ─▶ [quality gate] ─▶  GOLD  ─▶ [quality gate]
├── carrier_c/*.json.gz   ─┤   (raw, as-is,     |                (unified,   |               (business-
├── parcel_events/*.gz    ─┘    quarantined      |                 deduped,   |                model, joined,
└── reference/*.json      ──▶  reference data)   |                 quarantined)|               transit time /
                                                  |                            |                 on-time SLA)
                                            fails the task,                fails the task,
                                            blocks downstream            blocks downstream
```

- **Bronze** (`src/bronze/`): one ingestion module per raw source (`carrier_a`, `carrier_b`,
  `carrier_c`, `parcel_events`). Each file is parsed into an Iceberg table
  in its original shape. Lines that
  aren't valid JSON are quarantined into a sibling `*_quarantine` table rather than failing the
  load. Each load is scoped to and idempotent per source file (`source_file` partition,
  atomic overwrite), and already-ingested files are skipped on rerun.
- **Silver** (`src/silver/`): consolidates the three carriers' different vocabularies,
  status codes, units (grams vs. kg), and timestamp formats (ISO-8601, epoch millis, naive
  strings) into one unified `silver.tracking_events` table, and normalizes the internal
  `silver.parcel_events` lifecycle stream. Webhook retries are deduped on
  `(carrier_code, tracking_number, status, event_time)`. Semantically-bad rows (missing IDs,
  unrecognized status codes, unparseable timestamps) are quarantined with a reason. Every run
  fully recomputes both tables from Bronze, so a case completed by tomorrow's file is picked up     correctly without merge/upsert logic.
- **Gold** (`src/gold/`): `gold.dim_carriers` / `gold.dim_organisations` (loaded from
  `data/raw/reference/`) plus the central fact table `gold.shipments` — one row per parcel,
  joining the internal lifecycle with each carrier's own scan history and SLA, and deriving
  `transit_hours` (carrier possession → delivery) and `is_on_time` against the carrier's SLA.
- **Quality gates** (`src/quality.py`): run as their own Airflow tasks after each layer.
  Bronze/Silver gates fail if a table's quarantine rate exceeds 5%; the Gold gate fails on an
  empty table, duplicate `parcel_id`s, or negative transit times. A gate failure fails the task
  and blocks every downstream task, so a bad batch never propagates.

See the module docstrings in `src/bronze/*.py`, `src/silver/*.py`, and `src/gold/*.py` for the
specific data quirks each one handles (status vocabularies, timezone assumptions, duplicate
webhook retries, etc.) — they're documented in detail right next to the code that deals with them.

## Repository layout

```
dags/medallion_pipeline.py   Airflow DAG (sequencing/retries only;
                              tasks read/write Iceberg tables directly)
src/
├── common/
│   ├── catalog.py           Local SQLite-backed Iceberg catalog (data/catalog.db, warehouse at data/)
│   └── ingestion.py         Shared read/parse/quarantine/overwrite loop used by all bronze sources
├── bronze/                  One ingestion module per raw source
├── silver/                  Unification + normalization
├── gold/                    Business-facing dimensional model
├── quality.py                Data-quality gates
├── extract.py                CLI entrypoint for bronze ingestion (python -m src.extract <source>)
└── transform.py               CLI entrypoint for silver/gold transforms (python -m src.transform <table>)
data/raw/                    Sample input dataset (checked into the repo, see below)
tests/                       Unit tests, mirroring the src/ layout
agent/                       MCP server exposing the Gold layer to Claude Code (see AI Agent below)
├── server.py                 MCP stdio entrypoint: query_gold tool + gold://schema resource
├── sql_guard.py               Read-only SQL validation (sqlglot-based)
├── duckdb_views.py            Registers gold Iceberg tables as DuckDB views
├── query_service.py           Executes a validated query, caps returned rows
├── schema_resource.py         Loads the static schema doc below
└── resources/gold_schema.md   Column docs + semantic caveats for the gold layer
```

## Prerequisites

- **Python 3.12+**, in a **POSIX environment** — Linux, macOS, or **WSL2 if you're on Windows**.
  Everything in this repo (the manual pipeline, Airflow, the tests, and the AI agent) is built
  and documented against that environment. Airflow's scheduler/webserver specifically require
  POSIX and won't run on native Windows; the local Iceberg catalog also records absolute file
  paths at write time, so reading `data/` from a different OS than the one that built it breaks
  table loads. Do everything below from inside WSL2 (or Linux/macOS) — don't mix in a native
  Windows Python for any of it.

## Setup

```bash
git clone <this-repo>
cd Sendcloud_Trial_Case

python3 -m venv wsl-venv
source wsl-venv/bin/activate

pip install -r requirements.txt
```

> `requirements.txt` pins `apache-airflow==3.3.1`. If plain `pip install` struggles to resolve
> Airflow's dependency set, install it with the upstream constraints file first, then the rest:
> `pip install "apache-airflow==3.3.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"`
> followed by `pip install -r requirements.txt`.

The sample raw dataset (`data/raw/`) needs to be loaded into the repository manually. Everything the pipeline writes (`data/bronze/`, `data/silver/`, `data/gold/`,
`data/catalog.db`) is generated locally, so you're always working from a clean
slate on a fresh clone.

## Running the pipeline

### Option A — manual CLI run (fastest way to see it work)

Runs the exact same functions the Airflow DAG calls, without needing Airflow installed/running:

```bash
# Bronze: ingest every raw source
python -m src.extract carrier_a
python -m src.extract carrier_b
python -m src.extract carrier_c
python -m src.extract parcel_events

# Silver: consolidate
python -m src.transform tracking_events
python -m src.transform parcel_events

# Gold: build dimensions, then the shipments fact table
python -m src.transform dimensions
python -m src.transform shipments
```

Each command prints rows loaded/quarantined. Re-running any of these commands is safe —
bronze skips files it has already ingested (`--force` to reprocess), and silver/gold fully
and deterministically recompute their output each run.

### Option B — through Airflow (shows the actual orchestration)

```bash
# from the repo root, with the venv active
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone
```

`airflow standalone` initializes a local metadata DB, creates an admin user (credentials are
printed to the console on first run), and starts the webserver + scheduler. Open
[http://localhost:8080](http://localhost:8080), find the `medallion_pipeline` DAG, and trigger it
manually. The DAG runs `bronze → quality gate → silver → quality gate → gold → quality
gate` in that order; a gate failure blocks everything downstream of it.

## Running the tests

```bash
pytest
```

Tests live in `tests/`, mirroring the `src/` layout, and cover: per-carrier bronze parsing and
quarantine behavior, bronze skip-already-ingested logic, silver normalization/dedup/quarantine
for each carrier's quirks, gold dimension and shipment-fact construction, the data-quality
gate thresholds, and the agent's SQL guard/query execution (`tests/agent/`). Each test uses an
isolated `tmp_path` Iceberg warehouse, so tests never touch the real `data/`.

## AI Agent (`agent/`)

Claude Code itself is the agent, the Gold layer is
exposed over [MCP](https://modelcontextprotocol.io/) as one read-only SQL tool, and Claude Code
running in this repo is the terminal interface.

- **`query_gold(sql)`** — runs a single read-only `SELECT` against three DuckDB views backed by
  the live `gold.shipments` / `gold.dim_carriers` / `gold.dim_organisations` Iceberg tables
  (loaded via `pyiceberg`'s own `DataScan.to_duckdb()`), open-ended enough to answer exploratory
  questions like "why did performance drop in June," not just fixed lookups.
- **`gold://schema`** — a static resource (`agent/resources/gold_schema.md`) documenting columns,
  join keys, and the semantic caveats that matter for correct answers — in particular that
  `handed_over_at` ("shipped") and `accepted_at` ("accepted") are different moments, and that
  `transit_hours`/`is_on_time` are legitimately `NULL` for parcels never scanned by their
  carrier (still real shipments, just unresolved for on-time analysis).

### Running it

The agent uses the same `wsl-venv` from [Setup](#setup) — nothing extra to install.

`.mcp.json` registers the server with Claude Code, launching it as
`wsl.exe --cd <repo-path> -- wsl-venv/bin/python -m agent.server`. If you're running Claude Code
on Windows, that `wsl.exe --cd ...` wrapper is what lets it reach into WSL2 for you — you never
open an Ubuntu terminal yourself; Claude Code (wherever you normally launch it from) spawns the
agent process into WSL2 automatically. **The repo path in `.mcp.json` is machine-specific** —
update its `args` to your own `/mnt/c/...` path if you're on a different machine or username. If
you're on Linux/macOS, drop the `wsl.exe`/`--cd` wrapping and point `command` straight at
`wsl-venv/bin/python`.

Then just run `claude` from the repo root — the `gold-query` MCP server connects automatically.
Now answerable:

- How many parcels did organization 4471 ship in July 2026, broken down by carrier?
- Which carrier has the worst on-time delivery rate for parcels over 5 kg?
- Why did delivery performance drop in June?

## Inspecting the output

The Gold layer is queryable directly with `pyiceberg`, without Airflow running:

```python
from src.common.catalog import get_catalog

catalog = get_catalog()
table = catalog.load_table("gold.shipments")
rows = table.scan().to_arrow().to_pylist()
print(len(rows), "shipments")
print(rows[0])
```

Or scan any intermediate table the same way, e.g. `bronze.carrier_a_quarantine` to see quarantined
rows, or `silver.tracking_events` to see the unified carrier feed.

## Data quirks handled

Per the case's known data issues — each is handled at the layer noted:

| Quirk | Where it's handled |
|---|---|
| Filenames only indicate dump time, not event time | Bronze treats filenames purely as a lineage/idempotency key; timestamps come from record fields |
| Carrier webhook retries create duplicate events | Bronze intentionally keeps duplicates; Silver dedupes on `(carrier, tracking_number, status, event_time)` |
| Mismatched units, time zones, vocabularies across carriers | Silver normalizes all three carriers' status codes, units (g→kg), and timestamp formats into one schema |
| Bad/unparseable records | Quarantined (not dropped, not fatal) at both Bronze (structural JSON errors) and Silver (semantic errors: bad status codes, missing IDs, unparseable timestamps) |
| Incomplete cases completed by later files | Silver and Gold fully recompute from their inputs each run, so a case a later file completes is picked up automatically |
| Idempotency on pipeline reruns | Bronze: per-file atomic overwrite + skip-if-already-ingested. Silver/Gold: full deterministic overwrite each run. |


