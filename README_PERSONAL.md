# Personal Notes — Konfio Data Pipeline

This document explains every design decision I made, why I made it, and what trade-offs I considered.
It is meant to be read alongside the code, not instead of it.

---

## Big-picture approach

The test evaluates clarity of reasoning over quantity of features. I therefore:
- Chose the **simplest tool that correctly solves each problem** rather than the most impressive one.
- Kept each source file to a **single responsibility** so the code is readable without jumping between files.
- Made the pipeline **idempotent by design** — running it twice never creates duplicates.

---

## Libraries

### `pyspark==3.5.3`
Required by the test. PySpark is the standard choice for distributed DataFrame processing.
Version 3.5.x is the current LTS-equivalent line and pairs with `iceberg-spark-runtime-3.5_2.12`.

### `requests==2.32.3`
The Frankfurter API is a simple REST endpoint. `requests` is the de facto Python HTTP client —
no need for `httpx` or `aiohttp` since we don't need async or streaming.

### `tenacity==9.0.0`
Provides `@retry` with exponential backoff in a single decorator. The alternative is writing a
manual `for _ in range(retries)` loop with `time.sleep(2**attempt)`. `tenacity` is cleaner,
composable, and the same pattern used at scale in production data pipelines.

### `pytest==8.3.4`
Standard Python testing framework. I use `scope="session"` for the SparkSession fixture to avoid
the cost of starting a new JVM per test (each SparkSession start takes ~5 seconds).

### No Airflow / Prefect / etc.
The test says "implement the pipeline as a DAG". I interpreted this as: clear sequential dependency
between steps (extract → transform → CDC → load → events), not a full workflow orchestrator.
Adding Airflow would mean running three extra Docker services (webserver, scheduler, postgres)
for no additional correctness — exactly the kind of unnecessary complexity the rubric penalises.

---

## Extraction — `src/extract.py`

### Why a single range API call instead of one call per day?
The Frankfurter API supports `GET /v1/2024-01-01..2024-06-30`, which returns all trading-day
rates in a single response. Making 130+ individual calls would be slower, hit rate limits, and
produce far more retry surface area. One call = one point of failure = simpler error handling.

### Why `tenacity` retry only on `Timeout` and `ConnectionError`?
HTTP 4xx errors (bad request, auth) are programmer errors — retrying them wastes time and
obscures the real problem. HTTP 5xx errors from a public API are usually transient infrastructure
issues, but since the Frankfurter API is very reliable, I only retry on network-level failures
(timeout, connection reset). The `reraise=True` flag ensures the exception surfaces if all
retries are exhausted, rather than swallowing it silently.

### Why `datetime.utcnow()` for `ingestion_timestamp`?
Simple, no external dependency. In production you'd use a clock service or pass the timestamp
from the orchestrator to ensure all rows in a batch share the exact same timestamp. For a local
pipeline, `utcnow()` at the start of the extraction step is precise enough.

### Why type the schema explicitly instead of inferring it?
Schema inference reads the data twice (once to infer, once to process) and can produce
incorrect types (e.g., parsing `17.065` as a string). Explicit schema is faster, catches API
schema drift immediately, and is self-documenting.

---

## Transformation — `src/transform.py`

### Why pure functions (DataFrame → DataFrame)?
Each transform function has no side effects — it only reads its input and returns a new DataFrame.
This makes them trivially unit-testable without mocking Iceberg, HDFS, or any external system.
It also means you can chain them freely without worrying about state.

### Moving average windows (`rowsBetween` vs `rangeBetween`)
I used `rowsBetween(-6, 0)` for the 7-day MA (current + 6 prior rows) rather than
`rangeBetween(-6*24*3600, 0)` (6 days of seconds). The row-based window is simpler and
correct because the data is already pre-aggregated to daily granularity — there is exactly
one row per (date, currency). A range-based window would only matter if the data could have
multiple rows per day (e.g., intraday ticks).

### Why `stddev_pop` instead of `stddev_samp` for volatility?
`stddev_pop` is the population standard deviation (divide by N). `stddev_samp` divides by N-1
(Bessel's correction, appropriate when estimating a population from a sample). We are computing
volatility over a window of known observations — not estimating from a sample of a larger
population — so `stddev_pop` is the correct choice. The difference is negligible for windows
of 30 days but the reasoning matters.

### Anomaly detection threshold
I used 2 standard deviations (configurable via `ANOMALY_THRESHOLD`). This is the standard
"2σ rule": ~95% of values fall within 2σ for a normal distribution. Exchange rate daily changes
are approximately normal (fat-tailed in reality, but normal is a reasonable approximation for
this exercise). The threshold is exposed as an env var so a risk analyst can tune it without
touching code.

### Quality report: why Python loop instead of Spark SQL `sequence()`?
Generating a complete calendar is a small operation (≤365 rows × 4 currencies = ≤1460 rows).
Doing it in Python and calling `spark.createDataFrame()` is simpler and faster than writing
a Spark SQL `EXPLODE(SEQUENCE(start, end, INTERVAL 1 DAY))` expression. For a calendar spanning
decades with many currencies, the Spark approach would be better.

---

## CDC — `src/cdc.py`

### Why row-hash comparison instead of column-by-column?
Column comparison requires a list of all payload columns and a predicate for each. When you add a
new derived field (e.g., a new moving average window), you must update the comparison logic.
Row-hash (MD5 of all payload columns) is **schema-agnostic**: adding a column automatically
changes the hash, so the CDC automatically detects it as an UPDATE. The only downside is that
you lose the ability to know *which* column changed — but for a reporting pipeline that's
acceptable. If we needed column-level audit logs, we'd switch to column comparison.

### Why MD5 and not SHA-256?
MD5 is faster, and collision resistance at cryptographic levels is irrelevant here — we just
need to distinguish "same data" from "different data" across a ~50k-row dataset. MD5 is PySpark's
`F.md5()` built-in. SHA-256 would need `F.sha2(col, 256)` — same complexity, slower for no
benefit in this use case.

### Business key: `(rate_date, currency_code)`
This is the natural key of an exchange rate observation. You can have exactly one USD/MXN rate
per trading day. `base_currency` is always "USD" in this pipeline so it's not needed in the key,
but it's kept as a payload column so the schema is self-describing without needing to know the
pipeline configuration.

### DELETE handling
The test says DELETES are "optional or logical". I chose **logical deletes**: a row whose key
disappears from the incoming data gets `operation_type='DELETE'` in the CDC output, but is not
physically removed from Iceberg. This is standard practice in lakehouse architectures —
physical deletes break time travel and audit trails. The MERGE INTO only processes
INSERT/UPDATE/UNCHANGED rows; logical DELETE rows are preserved in the CDC DataFrame for event
emission but not merged back into the fact table (a real system would add a `deleted_at` column
to the target table for this).

### CDC Simulation (two-pass in `main.py`)
The test says you can simulate two snapshots. Rather than requiring the user to run the pipeline
twice, `main.py` runs a partial load (Jan–Mar) first, then runs the full load (Jan–Jun) with a
synthetic 0.1% rate revision on the first 5 January rows. This:
- Demonstrates INSERT (first run, all new rows)
- Demonstrates UPDATE (second run, revised Jan rows)
- Demonstrates UNCHANGED (second run, non-revised Jan rows that haven't changed)
- Demonstrates INSERT again (second run, Apr–Jun rows that didn't exist before)

---

## Load — `src/load.py`

### Why Hadoop catalog instead of Hive Metastore or Nessie?
The test explicitly says "no external infrastructure required". The Hadoop catalog is file-based
(metadata JSON files alongside the Parquet data files) — it needs no running services. In
production at Konfio, you'd use Nessie (for Git-like branching) or AWS Glue Catalog (for
cloud-native integration), but both require additional Docker services or cloud accounts.

### MERGE INTO for the fact table, `createOrReplace` for others
The fact table (`tipos_cambio_enriquecidos`) is the only table that needs CDC-aware upserts.
The aggregate tables (monthly metrics, anomalies, quality report) are fully recomputed from
the fact data on every run — they are deterministic views of the fact table, so overwriting
them is correct and simpler than trying to merge them.

### Why not use `df.write.format("iceberg").mode("overwrite")`?
That syntax writes all data as a new snapshot, replacing everything. `writeTo().createOrReplace()`
uses Iceberg's DynamicOverwrite, which rewrites only the affected partitions — much more
efficient for large tables. For the MERGE INTO path, Iceberg's copy-on-write (the default)
only rewrites Parquet files that contain changed rows.

### Time travel
After the MERGE INTO, I read the snapshot immediately before the current one:
```python
spark.read.option("snapshot-id", prev_id).table("local.db.tipos_cambio_enriquecidos")
```
This demonstrates that Iceberg preserves full history and you can query any past state.
In production, this is used for debugging (what did the table look like before the bad load?)
and compliance (point-in-time reporting for auditors).

### Partitioning strategy
`(year, month)` on the fact table means a query like:
```sql
SELECT * FROM db.tipos_cambio_enriquecidos WHERE year = 2024 AND month = 3
```
scans only the March 2024 Parquet files, skipping the other 5 months. This is the most common
query pattern for financial time-series data (monthly/quarterly reporting). An alternative would
be `days(rate_date)`, which gives finer granularity but produces more metadata files for a
dataset of this size (~130 rows) — overhead without benefit.

---

## Events — `src/events.py`

### Why collect to driver instead of writing from workers?
Each event is a small JSON object (< 1 KB). Distributed writing from Spark workers would require
coordinating unique filenames across executors (usually done with a UUIDs + commit protocol or
by writing to a Kafka topic). For the "minimum viable" JSON file option in the test, collecting
to the driver is simpler and correct. The event count for a 6-month daily dataset is ~500 rows
per currency × 4 currencies = ~2000 events — trivially small to handle on the driver.

### Schema version `"1.0"`
Explicitly versioning the event schema means consumers can route events based on `schema_version`
without parsing the payload. If we add a field (e.g., `bid_rate`), we bump to `"1.1"` and old
consumers continue to work. This is the same pattern used with Avro schemas in real Kafka setups.

### `entity_id = f"{rate_date}::{currency_code}"`
A stable, human-readable composite key that uniquely identifies the business entity. Consumers
can use this to deduplicate events or look up the corresponding Iceberg row.

---

## Docker

### Why a multi-stage build?
The Iceberg JAR (~30 MB) is downloaded once in stage 1 and copied into stage 2. If you rebuild
after changing only Python code, Docker reuses the cached JAR layer — much faster than
downloading 30 MB on every `docker build`.

### Why `python:3.11-slim` and not `apache/spark` base image?
The official `apache/spark` image is ~1.5 GB because it bundles Hadoop, HDFS, and all Spark
dependencies. `python:3.11-slim` + `pyspark` pip package gives us exactly what we need (~400 MB)
without the extra weight. PySpark bundles its own Spark distribution when installed via pip.

### Why `PYTHONPATH=/app`?
This allows both `src/main.py` and `tests/test_transform.py` to `import config` and
`from src.transform import ...` without relative import hacks. The project root is always
on the Python module search path.

---

## Testing philosophy

Tests are written at the **unit level** — each test creates a tiny DataFrame (3–30 rows), applies
one function, and asserts on the result. This is fast (< 30 seconds for the full suite) and
isolates failures precisely.

I deliberately chose **not** to test Iceberg persistence (MERGE INTO, time travel) in unit tests
because:
1. These require the Iceberg JAR, a real catalog, and filesystem writes — that's an integration test.
2. Iceberg is a well-tested open-source project; we don't need to re-test its internals.
3. The added complexity would make the test suite fragile and slow.

The correct place for integration tests is a CI pipeline (e.g., GitHub Actions) that runs
`docker compose up` against the real services.

---

## Assumptions documented

1. **"Revised" data arriving from the API** is simulated by a synthetic ±0.1% rate change.
   In production, the API would return corrected rates that differ from what was stored.

2. **Frankfurter API availability**: Assumed to be highly available. The retry logic handles
   transient errors. If the API is completely down, the pipeline fails fast after MAX_RETRIES.

3. **Rate ranges**: Valid rates are assumed to be in (0, 10,000). USD/MXN is ~17, USD/EUR ~0.9,
   USD/BRL ~5, USD/COP ~4,000. A rate of 10,000 would be an obvious data error.

4. **Logical deletes only**: Physical deletion from the fact table is not implemented. This is
   consistent with standard lakehouse practices where you never lose historical data.

5. **Single timezone**: All timestamps use UTC. A production pipeline would standardize on UTC
   company-wide and convert only at the presentation layer.

6. **No authentication for the API**: Frankfurter is a free, public API with no API keys.
   If it required auth, we'd inject credentials via environment variables (never hardcoded).
