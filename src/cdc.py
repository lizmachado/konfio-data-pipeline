"""
CDC layer — compares new data vs existing Iceberg snapshot using MD5 row hashes.

Classifies each row as INSERT, UPDATE, DELETE, or UNCHANGED.
Similar to dbt snapshots (strategy: check) but done manually.
"""

import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

import config

logger = logging.getLogger(__name__)

# Composite business key — same grain as the fact table
BUSINESS_KEY = ["rate_date", "currency_code"]

# Any change in these columns triggers an UPDATE classification
PAYLOAD_COLUMNS = [
    "base_currency",
    "rate",
    "delta_daily",
    "avg_lag_7d",
    "avg_lag_30d",
    "volatility_30d",
    "year",
    "month",
]


def _compute_row_hash(df: DataFrame) -> DataFrame:
    """Append a deterministic MD5 hash of all payload columns."""
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast(StringType()), F.lit("__null__")) for c in PAYLOAD_COLUMNS],
    )
    return df.withColumn("row_hash", F.md5(concat_expr))


def _table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
        return True
    except Exception:
        return False


def compute_cdc(
    spark: SparkSession,
    new_df: DataFrame,
    target_table: str,
) -> DataFrame:
    """Compare new data vs Iceberg snapshot — returns DataFrame with operation_type column."""
    now = datetime.utcnow()

    new_hashed = _compute_row_hash(new_df).withColumn(
        "ingestion_timestamp", F.lit(now).cast(TimestampType())
    ).withColumn("updated_at", F.lit(now).cast(TimestampType()))

    if not _table_exists(spark, target_table):
        logger.info("Target table %s does not exist — all rows classified as INSERT", target_table)
        return new_hashed.withColumn("operation_type", F.lit("INSERT"))

    logger.info("Loading existing snapshot from %s for CDC comparison", target_table)
    existing_df = spark.table(target_table).select(
        *BUSINESS_KEY, "row_hash"
    ).withColumnRenamed("row_hash", "existing_hash")

    # full_outer catches all 3 operations: INSERT (new only), DELETE (existing only), UPDATE/UNCHANGED (both)
    # switching to how="left" here would give us append-only behavior (no DELETEs)
    joined = new_hashed.alias("new").join(
        existing_df.alias("existing"),
        on=BUSINESS_KEY,
        how="full_outer",
    )

    cdc = joined.withColumn(
        "operation_type",
        F.when(F.col("existing.existing_hash").isNull(), "INSERT")
         .when(F.col("new.row_hash").isNull(), "DELETE")
         .when(F.col("new.row_hash") != F.col("existing.existing_hash"), "UPDATE")
         .otherwise("UNCHANGED"),
    )

    # For DELETE rows, carry over the business key from the existing side
    for col_name in BUSINESS_KEY:
        cdc = cdc.withColumn(
            col_name,
            F.coalesce(F.col(f"new.{col_name}"), F.col(f"existing.{col_name}")),
        )

    cdc = cdc.select(
        *BUSINESS_KEY,
        *[c for c in new_hashed.columns if c not in BUSINESS_KEY],
        "operation_type",
    )

    # Log CDC summary
    summary = cdc.groupBy("operation_type").count().collect()
    for row in summary:
        logger.info("CDC | operation=%-10s count=%d", row["operation_type"], row["count"])

    return cdc
