# Konfio Data Pipeline

Pipeline ETL de tipos de cambio usando PySpark + Apache Iceberg.

Extrae tipos de cambio históricos USD desde la [Frankfurter API](https://api.frankfurter.dev),
aplica transformaciones analíticas, implementa Change Data Capture (CDC),
y persiste los resultados en un lakehouse con Iceberg.

---

Execution order:
  1. Extract   — fetch historical rates from Frankfurter API
  2. Transform  — clean, enrich, aggregate, detect anomalies, quality report
  3. CDC        — compare new data against existing Iceberg snapshot
  4. Load       — persist to Iceberg (MERGE INTO + overwrites)
  5. Events     — emit CDC changes as JSON event files

The pipeline is idempotent: running it multiple times with the same date range
produces no duplicates or inconsistencies in the Iceberg tables.

CDC Simulation:
  To demonstrate CDC without requiring two separate pipeline runs, we execute
  two passes in sequence:
    Pass 1 — Jan–Mar 2024 (partial load → all INSERTs)
    Pass 2 — Jan–Jun 2024 with some synthetic rate modifications
              (Apr–Jun → INSERTs, modified Jan–Mar rows → UPDATEs)
  This faithfully exercises the INSERT, UPDATE, and UNCHANGED classifications.

## Comparación con dbt + Snowflake

Esta tabla mapea cada archivo del proyecto a su equivalente en dbt/Snowflake:

```
Tu stack (dbt/Snowflake)          Este proyecto (PySpark/Iceberg)
─────────────────────────         ──────────────────────────────
Source / Raw table                src/extract.py
  (JSON ya en Snowflake)            (llama la API y crea el DataFrame "raw")

stg_ (staging models)             src/transform.py → clean()
  (limpiar nulls, dedup,            (quita nulls, rates <= 0, dedup por date+currency)
   cast de tipos)

trf_ / int_ (intermediate)        src/transform.py → enrich()
  (joins, window functions,         (daily_change_pct, moving averages 7d/30d,
   cálculos derivados)               volatility_30d, year, month)

fct_ (fact tables)                src/transform.py → aggregate()
  (métricas agregadas,              (resumen mensual: avg/min/max rate, volatility,
   grain definido)                   observation_count — grain: year+month+currency)

Test custom / alertas             src/transform.py → detect_anomalies()
                                    (z-score > 2σ del rolling 30d)

dbt source freshness /            src/transform.py → quality_report()
  elementary                        (calendario: available / weekend / no_data)

dbt snapshot                      src/cdc.py → compute_cdc()
  (strategy: check,                 (strategy: row hash con MD5
   unique_key: [rate_date,           unique_key: [rate_date, currency_code]
    currency_code])                  clasifica: INSERT / UPDATE / DELETE / UNCHANGED)

Materialización final             src/load.py → load_all()
  (tabla en Snowflake)              (MERGE INTO + createOrReplace para dims/aggs)

Post-hook / notificaciones        src/events.py → emit_events()
  (Slack, downstream triggers)      (JSON por cada cambio CDC — simula Kafka)

dbt_project.yml / profiles.yml    config.py
  (configuración)                   (fechas, monedas, rutas, umbrales)

dbt run                           src/main.py → run_pipeline()
  (ejecuta modelos en orden)        (Extract → Transform → CDC → Load → Events)
```

### DAG equivalente en dbt

```
source('frankfurter_api')
    │
    ▼
stg_exchange_rates          ← extract.py + clean()
    │
    ▼
trf_exchange_rates_enriched ← enrich()
    │
    ├──► fct_monthly_metrics    ← aggregate()
    ├──► fct_anomalies          ← detect_anomalies()
    ├──► audit_data_quality     ← quality_report()
    │
    ▼
snp_exchange_rates          ← cdc.py (snapshot / CDC)
    │
    ▼
fct_exchange_rates          ← load.py (MERGE INTO)
dim_currency                ← load.py
    │
    ▼
events / notifications      ← events.py (post-hook)
```

---

## Paso a paso: qué hace cada función

### 1. Extract (`src/extract.py`) — Source table

En Snowflake ya tienes la tabla raw. Aquí no hay tabla raw — el extract
llama a la API de Frankfurter y crea el DataFrame en memoria.

Extract layer — fetches historical exchange rates from the Frankfurter API.

Uses a single range call (start..end) instead of one call per day to minimize
round-trips. The API skips weekends and holidays automatically, so we'll always
have gaps in the date sequence — those get classified in quality_report().

One quirk I found: if start_date falls on a holiday (e.g. Jan 1), the API
returns the last trading day before it (e.g. Dec 29). The clean() step
downstream doesn't filter by date range, so those extra rows pass through.
Not a bug per se, just something to be aware of.

Retry with exponential backoff (tenacity) handles transient API failures.
""" 
API: https://api.frankfurter.dev/v1/2024-01-01..2024-06-30
Monedas: MXN, EUR, BRL, COP (base: USD)
```

Schema resultante (tu source table):

| Columna              | Tipo      | Ejemplo             |
|----------------------|-----------|---------------------|
| rate_date            | DATE      | 2024-01-02          |
| base_currency        | STRING    | USD                 |
| currency_code        | STRING    | MXN                 |
| rate                 | DOUBLE    | 17.065              |
| ingestion_timestamp  | TIMESTAMP | 2024-01-02 00:00:00 |

### 2. Clean (`transform.py → clean()`) — stg_

En dbt sería:

```sql
SELECT *
FROM {{ source('frankfurter', 'raw_exchange_rates') }}
WHERE rate IS NOT NULL
  AND rate > 0
  AND rate < 10000
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rate_date, currency_code
    ORDER BY ingestion_timestamp
) = 1
```

### 3. Enrich (`transform.py → enrich()`) — trf_ 
"""
Transform layer — all PySpark transformations on raw exchange rate data.

Each function is a pure transformation (DataFrame in → DataFrame out) with no
side effects, which makes them straightforward to unit-test.

Layers:
  1. clean()          — nulls, dedup, type validation, range checks
  2. enrich()         — daily % change, 7/30-day MA, rolling volatility
  3. aggregate()      — monthly summary per currency
  4. detect_anomalies() — days > 2σ from 30-day rolling mean
  5. quality_report() — missing-day analysis and dataset statistics
"""

    Remove invalid rows and enforce data quality constraints.

    Rules:
    - Drop rows where rate is null (API anomaly or missing symbol).
    - Drop rows where rate <= 0 (nonsensical exchange rate).
    - Deduplicate on (rate_date, currency_code) — keep first occurrence.
    - Ensure rate_date is actually a date column.
En dbt:
    """
    Add derived analytical columns to the cleaned dataset.

    - delta_daily : % change from previous trading day for same currency.
    - avg_lag_7d            : 7-day simple moving average of the rate.
    - avg_lag_30d           : 30-day simple moving average of the rate.
    - volatility_30d   : rolling 30-day population standard deviation (proxy for volatility).
    - year / month     : partition columns for Iceberg.
```sql
SELECT *,
    (rate - LAG(rate) OVER w) / LAG(rate) OVER w * 100 AS daily_change_pct,
    AVG(rate) OVER (w ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7d,
    AVG(rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30d,
    STDDEV_POP(rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS volatility_30d,
    YEAR(rate_date) AS year,
    MONTH(rate_date) AS month
FROM {{ ref('stg_exchange_rates') }}
WINDOW w AS (PARTITION BY currency_code ORDER BY rate_date)
```

Agrega: `daily_change_pct`, `ma_7d`, `ma_30d`, `volatility_30d`, `year`, `month`.

### 4. Aggregate (`transform.py → aggregate()`) — fct_

En dbt:

```sql
SELECT
    year, month, currency_code, base_currency,
    AVG(rate), MIN(rate), MAX(rate),
    STDDEV_POP(rate), COUNT(rate), AVG(daily_change_pct)
FROM {{ ref('trf_exchange_rates_enriched') }}
GROUP BY 1, 2, 3, 4
```

Grain: una fila por (year, month, currency_code).

### 5. Anomalies (`transform.py → detect_anomalies()`) — Test custom

En dbt:

```sql
SELECT rate_date, currency_code, rate, daily_change_pct,
       ma_30d, volatility_30d,
       ABS(daily_change_pct) / volatility_30d AS z_score
FROM {{ ref('trf_exchange_rates_enriched') }}
WHERE daily_change_pct IS NOT NULL
  AND volatility_30d > 0
  AND ABS(daily_change_pct) / volatility_30d > 2.0
```

### 6. Quality Report (`transform.py → quality_report()`) — Source freshness

Genera calendario de todos los días del rango y clasifica cada uno:
- `available` — hay dato
- `weekend` — sábado/domingo (esperado)
- `no_data` — día hábil sin dato (gap o feriado)

### 7. CDC (`cdc.py`) — dbt snapshot
CDC (Change Data Capture) layer.

Similar concept to dbt snapshots (strategy: check), but implemented manually
with a row-hash approach since PySpark doesn't have built-in snapshot support.

Strategy: row-hash comparison.
  - Business key  : (rate_date, currency_code)
  - Change signal : MD5 hash of all payload columns (rate, avg_lag_7d, avg_lag_30d, etc.)

I went with hash instead of column-by-column comparison because it's simpler
to maintain — if I add a new column later, the hash picks it up automatically
without touching the comparison logic. Trade-off: MD5 collisions are
theoretically possible, but with this data volume it's a non-issue.

Flow:
  1. Compute a row_hash for the incoming (new) data.
  2. Load the existing snapshot from Iceberg (if the table exists).
  3. Full outer join on the business key.
  4. Classify each row:
       INSERT    — key exists in new but not in existing.
       UPDATE    — key exists in both, hashes differ.
       DELETE    — key exists in existing but not in new.
       UNCHANGED — hashes match; carried forward for MERGE INTO idempotency.

Note: if we wanted append-only (no DELETEs), we'd change the full_outer join
to a left join from new data — rows missing from the new extract simply
wouldn't appear and Iceberg would keep them untouched.

The pipeline is idempotent: running it twice with the same input produces no
net changes (all rows classify as UNCHANGED on the second run).
"""

    """
    Compare new_df against the current Iceberg snapshot and return a CDC DataFrame.

    Columns added:
      operation_type     : INSERT | UPDATE | DELETE | UNCHANGED
      ingestion_timestamp: timestamp of this pipeline run
      updated_at         : same as ingestion_timestamp (when the row was last changed)
      row_hash           : MD5 of payload columns

    The UNCHANGED rows are included so the MERGE INTO in load.py can safely
    upsert without duplicating data — Iceberg's MERGE INTO handles idempotency.
    """
Exactamente como `dbt snapshot` con `strategy: check`:

```yaml
{% snapshot snp_exchange_rates %}
  {{ config(
      strategy='check',
      unique_key=['rate_date', 'currency_code'],
      check_cols='all'
  ) }}
{% endsnapshot %}
```

Usa MD5 hash de todas las columnas payload. Clasifica cada fila como
INSERT, UPDATE, DELETE o UNCHANGED.

### 8. Load (`load.py`) — Materialización
Load layer — persists DataFrames as Apache Iceberg tables.

This is roughly equivalent to dbt's materialization layer, but done manually
because PySpark doesn't have a built-in equivalent of `dbt run`.

Iceberg features used:
  1. MERGE INTO   — idempotent upserts driven by CDC (like dbt incremental + merge).
  2. Time travel  — reads back the previous snapshot after each write for auditing.
  3. Partitioning — (year, month) so time-range queries skip irrelevant Parquet files.
  4. Schema-first DDL — explicit CREATE TABLE, not inferred from data.

The fact table uses MERGE INTO because it needs incremental logic (CDC-driven).
Derived tables (metrics, anomalies, quality) use createOrReplace because they
get fully recalculated each run — no point in incremental logic there.

Usa `MERGE INTO` (igual que Snowflake):

```sql
MERGE INTO tipos_cambio_enriquecidos AS target
USING cdc_source AS source
ON target.rate_date = source.rate_date
   AND target.currency_code = source.currency_code
WHEN MATCHED AND target.row_hash != source.row_hash
    THEN UPDATE SET ...
WHEN NOT MATCHED
    THEN INSERT ...
```

Tablas Iceberg que crea:

| Tabla Iceberg                  | Equivalente dbt          | Tipo           |
|--------------------------------|--------------------------|----------------|
| `tipos_cambio_enriquecidos`    | `fct_exchange_rates`     | MERGE INTO     |
| `metricas_mensuales`           | `fct_monthly_metrics`    | Full refresh   |
| `anomalias`                    | `fct_anomalies`          | Full refresh   |
| `reporte_calidad`              | `audit_data_quality`     | Full refresh   |
| `dim_currency`                 | `dim_currency`           | Full refresh   |

### 9. Events (`events.py`) — Post-hook

Por cada INSERT/UPDATE/DELETE del CDC, genera un archivo JSON.
En producción sería un Kafka producer. Como un `post-hook` de dbt
que dispara un downstream pipeline.

Each CDC change (INSERT, UPDATE, DELETE) produces one JSON event file in
the /events/ directory. File names include a timestamp and a UUID to ensure
uniqueness across pipeline runs.

Event schema (v1):
  {
    "schema_version": "1.0",
    "event_type":     "INSERT" | "UPDATE" | "DELETE",
    "event_timestamp": "2024-01-02T00:00:00Z",
    "entity":         "exchange_rate",
    "entity_id":      "2024-01-02::MXN",
    "payload": {
      "rate_date":         "2024-01-02",
      "currency_code":     "MXN",
      "base_currency":     "USD",
      "rate":              17.065,
      "delta_daily":  null,
      "avg_lag_7d":             17.065,
      "avg_lag_30d":            17.065,
      "volatility_30d":    0.0,
      "row_hash":          "abc123..."
    },
    "metadata": {
      "pipeline_run_id": "...",
      "ingestion_timestamp": "..."
    }
  }

In production this would be a Kafka producer with Avro + Schema Registry, but
for this exercise JSON files are more transparent and don't require standing
up a Kafka cluster in Docker (which would add ~1GB of images for no real value).

UNCHANGED rows don't generate events — only actual changes get emitted.
"""
---

## Iceberg vs Snowflake

| Concepto Snowflake     | Equivalente Iceberg                              |
|------------------------|--------------------------------------------------|
| Database + Schema      | Catalog + Namespace (`local.db`)                 |
| Table storage          | Archivos Parquet + metadata JSON en `warehouse/` |
| Time Travel            | `SELECT * FROM table VERSION AS OF snapshot_id`  |
| Clustering             | Partitioning por `(year, month)`                 |
| MERGE INTO             | Mismo SQL, misma semántica                       |
| Information Schema     | `.snapshots`, `.history`, `.files` metadata       |

---

## Correr localmente (sin Docker)

### Requisitos
- Python 3.11+ (`py --version`)
- Java 11 (instalado en `C:\Program Files\Eclipse Adoptium\jdk-11.0.20.101-hotspot`)

### Instalar dependencias
```bash
py -m pip install pyspark==3.5.5 requests tenacity pytest
```

### Correr tests
```bash
py -m pytest tests/test_transform.py -v
```

### Correr el pipeline completo
```bash
py src/main.py
```

### Ver resultados
- `warehouse/` — Tablas Iceberg (Parquet + metadata)
- `events/` — JSON de eventos CDC

---

## Docker (para GitHub)

```bash
docker compose up --build
```

`config.py` usa `os.getenv()` con defaults locales. Docker sobreescribe
las rutas via variables de entorno en `docker-compose.yml`.

---

## Configuracion

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `START_DATE` | `2024-01-01` | Inicio de extraccion |
| `END_DATE` | `2024-06-30` | Fin de extraccion |
| `BASE_CURRENCY` | `USD` | Moneda base |
| `TARGET_CURRENCIES` | `MXN,EUR,BRL,COP` | Monedas objetivo |
| `MAX_RETRIES` | `5` | Reintentos API |
| `REQUEST_TIMEOUT` | `30` | Timeout HTTP (seg) |
| `WAREHOUSE_PATH` | `./warehouse` | Ruta Iceberg |
| `EVENTS_PATH` | `./events` | Ruta eventos JSON |
| `ANOMALY_THRESHOLD` | `2.0` | Umbral anomalias (σ) |

---

## Estructura del proyecto

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── config.py               ← configuracion centralizada (env vars)
├── src/
│   ├── main.py             ← entry point (orquesta el DAG)
│   ├── spark_session.py    ← SparkSession con Iceberg
│   ├── extract.py          ← cliente API Frankfurter (retry/backoff)
│   ├── transform.py        ← transformaciones PySpark (clean/enrich/agg/anomaly/quality)
│   ├── cdc.py              ← Change Data Capture (row-hash)
│   ├── load.py             ← persistencia Iceberg (MERGE INTO, time travel)
│   └── events.py           ← simulacion Kafka (JSON events)
└── tests/
    └── test_transform.py   ← unit tests para transform
```

---

## Data Model

| Table | Grain |
|-------|-------|
| `tipos_cambio_enriquecidos` | Una fila por (trading date, currency) |
| `metricas_mensuales` | Una fila por (year, month, currency) |
| `anomalias` | Una fila por (anomalous date, currency) |
| `reporte_calidad` | Una fila por (calendar date, currency) |
| `dim_currency` | Una fila por currency code |

CDC Business Key: `(rate_date, currency_code)`
