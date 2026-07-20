"""
Load layer — persists DataFrames as Iceberg tables.

Fact table uses MERGE INTO (CDC-driven). Derived tables use createOrReplace.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config

logger = logging.getLogger(__name__)

CATALOG = config.CATALOG_NAME
DB = config.DATABASE_NAME

_CURRENCY_NAMES = {
    "MXN": "Mexican Peso",
    "EUR": "Euro",
    "BRL": "Brazilian Real",
    "COP": "Colombian Peso",
}


def _full(name: str) -> str:
    return f"{CATALOG}.{DB}.{name}"


def _ensure_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")


# ─── DDL ──────────────────────────────────────────────────────────────────────

def _ddl_enriched(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_full('tipos_cambio_enriquecidos')} (
            rate_date            DATE         NOT NULL,
            currency_code        STRING       NOT NULL,
            base_currency        STRING,
            rate                 DOUBLE,
            delta_daily     DOUBLE,
            avg_lag_7d                DOUBLE,
            avg_lag_30d               DOUBLE,
            volatility_30d       DOUBLE,
            year                 INT,
            month                INT,
            ingestion_timestamp  TIMESTAMP,
            updated_at           TIMESTAMP,
            operation_type       STRING,
            row_hash             STRING
        )
        USING iceberg
        PARTITIONED BY (year, month)
    """)


def _ddl_monthly(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_full('metricas_mensuales')} (
            year                  INT,
            month                 INT,
            currency_code         STRING,
            base_currency         STRING,
            avg_rate              DOUBLE,
            min_rate              DOUBLE,
            max_rate              DOUBLE,
            monthly_volatility    DOUBLE,
            observation_count     LONG,
            avg_delta_daily  DOUBLE
        )
        USING iceberg
        PARTITIONED BY (year, month)
    """)


def _ddl_anomalies(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_full('anomalias')} (
            rate_date          DATE,
            currency_code      STRING,
            rate               DOUBLE,
            delta_daily   DOUBLE,
            avg_lag_30d             DOUBLE,
            volatility_30d     DOUBLE,
            z_score            DOUBLE
        )
        USING iceberg
        PARTITIONED BY (months(rate_date))
    """)


def _ddl_quality(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_full('reporte_calidad')} (
            calendar_date  DATE,
            currency_code  STRING,
            is_weekend     BOOLEAN,
            status         STRING
        )
        USING iceberg
        PARTITIONED BY (months(calendar_date))
    """)


def _ddl_dim_currency(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_full('dim_currency')} (
            currency_code  STRING NOT NULL,
            base_currency  STRING,
            currency_name  STRING
        )
        USING iceberg
    """)


# ─── Writers ──────────────────────────────────────────────────────────────────

def _merge_enriched(spark: SparkSession, cdc_df: DataFrame) -> None:
    """MERGE INTO the fact table using CDC results."""
    table = _full("tipos_cambio_enriquecidos")
    cdc_df.createOrReplaceTempView("cdc_source")

    spark.sql(f"""
        MERGE INTO {table} AS target
        USING (
            SELECT * FROM cdc_source
            WHERE operation_type IN ('INSERT', 'UPDATE', 'UNCHANGED')
        ) AS source
        ON target.rate_date    = source.rate_date
       AND target.currency_code = source.currency_code
        WHEN MATCHED AND target.row_hash != source.row_hash THEN
            UPDATE SET
                rate                = source.rate,
                delta_daily    = source.delta_daily,
                avg_lag_7d               = source.avg_lag_7d,
                avg_lag_30d              = source.avg_lag_30d,
                volatility_30d      = source.volatility_30d,
                updated_at          = source.updated_at,
                operation_type      = source.operation_type,
                row_hash            = source.row_hash
        WHEN NOT MATCHED THEN
            INSERT (
                rate_date, currency_code, base_currency, rate,
                delta_daily, avg_lag_7d, avg_lag_30d, volatility_30d,
                year, month, ingestion_timestamp, updated_at,
                operation_type, row_hash
            )
            VALUES (
                source.rate_date, source.currency_code, source.base_currency, source.rate,
                source.delta_daily, source.avg_lag_7d, source.avg_lag_30d, source.volatility_30d,
                source.year, source.month, source.ingestion_timestamp, source.updated_at,
                source.operation_type, source.row_hash
            )
    """)
    logger.info("MERGE INTO %s completed", table)


def _write_dim_currency(spark: SparkSession, cdc_df: DataFrame) -> None:
    # chained WHEN instead of UDF to avoid Python worker issues on Windows
    currency_name_expr = F.col("currency_code")
    for code, name in _CURRENCY_NAMES.items():
        currency_name_expr = F.when(F.col("currency_code") == code, F.lit(name)).otherwise(currency_name_expr)
    dim = (
        cdc_df.select("currency_code", "base_currency").distinct()
              .withColumn("currency_name", currency_name_expr)
    )
    dim.writeTo(_full("dim_currency")).createOrReplace()


# ─── Time travel ──────────────────────────────────────────────────────────────

def read_previous_snapshot(spark: SparkSession) -> DataFrame | None:
    """Iceberg time travel — return the previous snapshot, or None if only one exists."""
    table = _full("tipos_cambio_enriquecidos")
    try:
        snapshots = (
            spark.sql(f"SELECT * FROM {table}.snapshots ORDER BY committed_at")
                 .collect()
        )
        if len(snapshots) < 2:
            logger.info("Time travel: only one snapshot exists — skipping")
            return None

        prev_id = snapshots[-2]["snapshot_id"]
        logger.info("Time travel: reading snapshot_id=%d", prev_id)
        return spark.read.option("snapshot-id", prev_id).table(table)
    except Exception as exc:
        logger.warning("Time travel query failed: %s", exc)
        return None


# ─── Public entry point ───────────────────────────────────────────────────────

def load_all(
    spark: SparkSession,
    cdc_df: DataFrame,
    monthly_df: DataFrame,
    anomalies_df: DataFrame,
    quality_df: DataFrame,
) -> None:
    """Persist all layers to their respective Iceberg tables."""
    _ensure_database(spark)

    # Fact table — the only table that uses MERGE INTO (incremental, CDC-driven)
    _ddl_enriched(spark)
    _merge_enriched(spark, cdc_df)

    # Time travel: read the snapshot that existed before this run
    prev = read_previous_snapshot(spark)
    if prev is not None:
        logger.info("Time travel: previous snapshot row count = %d", prev.count())

    # Dimension
    _ddl_dim_currency(spark)
    _write_dim_currency(spark, cdc_df)
    logger.info("Wrote dim_currency")

    # These 3 tables are fully recalculated each run (createOrReplace, not MERGE)
    # because they're derived views — no point in incremental logic here
    _ddl_monthly(spark)
    monthly_df.writeTo(_full("metricas_mensuales")).createOrReplace()
    logger.info("Wrote metricas_mensuales")

    _ddl_anomalies(spark)
    anomalies_df.writeTo(_full("anomalias")).createOrReplace()
    logger.info("Wrote anomalias")

    _ddl_quality(spark)
    quality_df.writeTo(_full("reporte_calidad")).createOrReplace()
    logger.info("Wrote reporte_calidad")
