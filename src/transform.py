import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

import config

logger = logging.getLogger(__name__)


def _windows():
    """Window specs for rolling calculations — built lazily (needs active SparkContext)."""
    base = Window.partitionBy("currency_code").orderBy(F.col("rate_date").cast("long"))
    return base, base.rowsBetween(-6, 0), base.rowsBetween(-29, 0)


# ─── 1. Clean ─────────────────────────────────────────────────────────────────

def clean(df: DataFrame) -> DataFrame:
    """Remove nulls, zeros, negatives, and duplicates."""
    logger.info("Cleaning raw data")

    initial_count = df.count()

    cleaned = (
        df.dropDuplicates(["rate_date", "currency_code"])
          .filter(F.col("rate").isNotNull())
          .filter(F.col("rate") > 0)
          .withColumn("rate_date", F.col("rate_date").cast(DateType()))
    )

    final_count = cleaned.count()
    dropped = initial_count - final_count
    if dropped:
        logger.warning("Cleaning dropped %d rows (nulls, dupes, out-of-range rates)", dropped)

    return cleaned


# ─── 2. Enrich ────────────────────────────────────────────────────────────────

def enrich(df: DataFrame) -> DataFrame:
    """Add derived columns: delta_daily, avg_lag_7d/30d, volatility_30d, year, month."""
    logger.info("Enriching data with derived fields")

    w_base, w_7d, w_30d = _windows()

    enriched = (
        df
        .withColumn("prev_rate", F.lag("rate", 1).over(w_base))
        .withColumn(
            "delta_daily",
            F.when(
                F.col("prev_rate").isNotNull() & (F.col("prev_rate") != 0),
                (F.col("rate") - F.col("prev_rate")) / F.col("prev_rate") * 100,
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("prev_rate")
        .withColumn("avg_lag_7d", F.avg("rate").over(w_7d))
        .withColumn("avg_lag_30d", F.avg("rate").over(w_30d))
        .withColumn("volatility_30d", F.stddev_pop("rate").over(w_30d))
        .withColumn("year", F.year("rate_date"))
        .withColumn("month", F.month("rate_date"))
    )

    return enriched


# ─── 3. Aggregate ─────────────────────────────────────────────────────────────

def aggregate(df: DataFrame) -> DataFrame:
    logger.info("Building monthly aggregations")

    return (
        df.groupBy("year", "month", "currency_code", "base_currency")
          .agg(
              F.avg("rate").alias("avg_rate"),
              F.min("rate").alias("min_rate"),
              F.max("rate").alias("max_rate"),
              F.stddev_pop("rate").alias("monthly_volatility"),
              F.count("rate").alias("observation_count"),
              F.avg("delta_daily").alias("avg_delta_daily"),
          )
          .orderBy("year", "month", "currency_code")
    )


# ─── 4. Detect anomalies ──────────────────────────────────────────────────────

def detect_anomalies(df: DataFrame) -> DataFrame:
    logger.info("Detecting anomalies (threshold: %.1f σ)", config.ANOMALY_THRESHOLD)

    w_order = Window.partitionBy("currency_code").orderBy("rate_date")

    # skip first 30 rows — rolling window isn't full yet, inflates z-scores
    return (
        df.withColumn("_row_num", F.row_number().over(w_order))
          .filter(F.col("_row_num") > 30)
          .filter(F.col("delta_daily").isNotNull())
          .filter(F.col("volatility_30d").isNotNull() & (F.col("volatility_30d") > 0))
          .withColumn(
              "z_score",
              F.abs(F.col("delta_daily")) / F.col("volatility_30d"),
          )
          .filter(F.col("z_score") > config.ANOMALY_THRESHOLD)
          .select(
              "rate_date",
              "currency_code",
              "rate",
              "delta_daily",
              "avg_lag_30d",
              "volatility_30d",
              "z_score",
          )
          .orderBy("rate_date", "currency_code")
    )


# ─── 5. Quality report ────────────────────────────────────────────────────────

def quality_report(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    logger.info("Generating data quality report")

    # spark.range() runs in the JVM — avoids Python worker issues on Windows
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    num_days = (end - start).days + 1

    spark: SparkSession = df.sparkSession
    currencies_df = df.select("currency_code").distinct()

    calendar_df = (
        spark.range(num_days)
        .withColumn(
            "calendar_date",
            F.date_add(F.lit(start_date), F.col("id").cast("int")).cast(DateType()),
        )
        .drop("id")
        .crossJoin(currencies_df)
        # Spark dayofweek: 1=Sunday, 7=Saturday (opposite of Python isoweekday)
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
    )

    available_dates = df.select(
        F.col("rate_date").alias("calendar_date"),
        F.col("currency_code"),
        F.lit(True).alias("has_data"),
    )

    report = (
        calendar_df
        .join(available_dates, on=["calendar_date", "currency_code"], how="left")
        .withColumn(
            "status",
            F.when(F.col("has_data").isNotNull(), "available")
             .when(F.col("is_weekend"), "weekend")
             .otherwise("no_data"),
        )
        .select("calendar_date", "currency_code", "is_weekend", "status")
        .orderBy("calendar_date", "currency_code")
    )

    # Log summary
    summary = report.groupBy("status").count().collect()
    for row in summary:
        logger.info("Quality report | status=%-10s count=%d", row["status"], row["count"])

    return report
