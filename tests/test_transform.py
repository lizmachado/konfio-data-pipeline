"""Unit tests for the Transform layer."""

import json
import os
import sys
import tempfile
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from src.transform import clean, enrich, aggregate, detect_anomalies, quality_report


def _setup_windows_env():
    java11 = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.20.101-hotspot"
    if os.path.isdir(java11):
        os.environ["JAVA_HOME"] = java11
        os.environ["PATH"] = os.path.join(java11, "bin") + os.pathsep + os.environ.get("PATH", "")

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        hadoop_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hadoop")
        if os.path.isdir(hadoop_dir):
            os.environ["HADOOP_HOME"] = hadoop_dir
            os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    _setup_windows_env()
    tmp_dir = tmp_path_factory.mktemp("spark_data")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("KonfioTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session._test_tmp_dir = tmp_dir  # stash so _make_df can use it
    yield session
    session.stop()


# Schema the pipeline uses for raw exchange-rate rows.
RAW_SCHEMA = StructType([
    StructField("rate_date", DateType(), nullable=False),
    StructField("base_currency", StringType(), nullable=False),
    StructField("currency_code", StringType(), nullable=False),
    StructField("rate", DoubleType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

_TS = datetime(2024, 1, 1, 0, 0, 0)

# JSON schema mirrors RAW_SCHEMA with string types so Spark can read without workers.
_JSON_SCHEMA = StructType([
    StructField("rate_date", StringType()),
    StructField("base_currency", StringType()),
    StructField("currency_code", StringType()),
    StructField("rate", DoubleType(), nullable=True),
    StructField("ingestion_timestamp", StringType()),
])


_make_df_counter = 0


def _make_df(spark, rows):
    """Create a test DataFrame from a list of tuples via NDJSON (avoids Python workers)."""
    global _make_df_counter
    _make_df_counter += 1

    tmp_dir = getattr(spark, "_test_tmp_dir", tempfile.gettempdir())
    tmp_path = os.path.join(str(tmp_dir), f"raw_{_make_df_counter}.json")

    records = []
    for rate_date, base_currency, currency_code, rate, ingestion_timestamp in rows:
        records.append({
            "rate_date": rate_date.isoformat() if rate_date is not None else None,
            "base_currency": base_currency,
            "currency_code": currency_code,
            "rate": rate,  # becomes JSON null when None
            "ingestion_timestamp": (
                ingestion_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                if ingestion_timestamp is not None else None
            ),
        })

    with open(tmp_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    return (
        spark.read.schema(_JSON_SCHEMA).json(tmp_path)
        .withColumn("rate_date", F.to_date("rate_date", "yyyy-MM-dd").cast(DateType()))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
        .select("rate_date", "base_currency", "currency_code", "rate", "ingestion_timestamp")
    )


# ─── clean() tests ────────────────────────────────────────────────────────────

class TestClean:
    def test_removes_null_rates(self, spark):
        df = _make_df(spark, [
            (date(2024, 1, 2), "USD", "MXN", None, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.0, _TS),
        ])
        result = clean(df)
        assert result.count() == 1
        assert result.collect()[0]["rate"] == 17.0

    def test_removes_zero_rates(self, spark):
        df = _make_df(spark, [
            (date(2024, 1, 2), "USD", "MXN", 0.0, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.0, _TS),
        ])
        result = clean(df)
        assert result.count() == 1

    def test_removes_negative_rates(self, spark):
        df = _make_df(spark, [
            (date(2024, 1, 2), "USD", "MXN", -5.0, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.0, _TS),
        ])
        result = clean(df)
        assert result.count() == 1

    def test_deduplicates_on_date_currency(self, spark):
        df = _make_df(spark, [
            (date(2024, 1, 2), "USD", "MXN", 17.0, _TS),
            (date(2024, 1, 2), "USD", "MXN", 17.5, _TS),  # duplicate key
        ])
        result = clean(df)
        assert result.count() == 1

    def test_keeps_valid_rows(self, spark):
        df = _make_df(spark, [
            (date(2024, 1, 2), "USD", "MXN", 17.065, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.005, _TS),
            (date(2024, 1, 2), "USD", "EUR", 0.9155, _TS),
        ])
        result = clean(df)
        assert result.count() == 3


# ─── enrich() tests ───────────────────────────────────────────────────────────

class TestEnrich:
    def _base_df(self, spark):
        rows = [
            (date(2024, 1, 2), "USD", "MXN", 17.0, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.5, _TS),
            (date(2024, 1, 4), "USD", "MXN", 17.2, _TS),
        ]
        return clean(_make_df(spark, rows))

    def test_adds_year_month_columns(self, spark):
        df = enrich(self._base_df(spark))
        cols = set(df.columns)
        assert "year" in cols
        assert "month" in cols

    def test_year_month_values(self, spark):
        df = enrich(self._base_df(spark))
        row = df.filter("rate_date = '2024-01-02'").collect()[0]
        assert row["year"] == 2024
        assert row["month"] == 1

    def test_first_row_daily_change_is_null(self, spark):
        df = enrich(self._base_df(spark))
        row = df.orderBy("rate_date").first()
        assert row["delta_daily"] is None

    def test_delta_daily_calculation(self, spark):
        df = enrich(self._base_df(spark))
        rows = {r["rate_date"].isoformat(): r for r in df.collect()}
        # Jan 3: (17.5 - 17.0) / 17.0 * 100 ≈ 2.941%
        expected = (17.5 - 17.0) / 17.0 * 100
        assert abs(rows["2024-01-03"]["delta_daily"] - expected) < 1e-6

    def test_moving_averages_present(self, spark):
        df = enrich(self._base_df(spark))
        cols = set(df.columns)
        assert "avg_lag_7d" in cols
        assert "avg_lag_30d" in cols

    def test_avg_lag_7d_equal_rate_for_single_row_window(self, spark):
        # First row has only itself in the window, so avg_lag_7d == rate
        df = enrich(self._base_df(spark))
        first = df.orderBy("rate_date").first()
        assert abs(first["avg_lag_7d"] - first["rate"]) < 1e-10

    def test_volatility_column_present(self, spark):
        df = enrich(self._base_df(spark))
        assert "volatility_30d" in df.columns


# ─── aggregate() tests ────────────────────────────────────────────────────────

class TestAggregate:
    def _enriched(self, spark):
        rows = [
            (date(2024, 1, 2), "USD", "MXN", 17.0, _TS),
            (date(2024, 1, 3), "USD", "MXN", 17.5, _TS),
            (date(2024, 1, 4), "USD", "MXN", 17.2, _TS),
            (date(2024, 2, 1), "USD", "MXN", 16.8, _TS),
        ]
        return enrich(clean(_make_df(spark, rows)))

    def test_one_row_per_month(self, spark):
        df = aggregate(self._enriched(spark))
        assert df.count() == 2  # January + February

    def test_january_observation_count(self, spark):
        df = aggregate(self._enriched(spark))
        jan = df.filter("month = 1").collect()[0]
        assert jan["observation_count"] == 3

    def test_january_min_max(self, spark):
        df = aggregate(self._enriched(spark))
        jan = df.filter("month = 1").collect()[0]
        assert jan["min_rate"] == pytest.approx(17.0)
        assert jan["max_rate"] == pytest.approx(17.5)


# ─── detect_anomalies() tests ─────────────────────────────────────────────────

class TestDetectAnomalies:
    def _enriched_with_spike(self, spark):
        # 60 stable days (warmup) + 1 massive spike on day 61
        rows = []
        for i in range(60):
            d = date(2024, 1, 1)
            from datetime import timedelta
            d = date(2024, 1, 1) + timedelta(days=i)
            rows.append((d, "USD", "MXN", 17.0, _TS))
        # Day 61: spike
        from datetime import timedelta
        rows.append((date(2024, 1, 1) + timedelta(days=60), "USD", "MXN", 25.0, _TS))
        return enrich(clean(_make_df(spark, rows)))

    def test_detects_spike_as_anomaly(self, spark):
        df = detect_anomalies(self._enriched_with_spike(spark))
        anomaly_dates = [r["rate_date"].isoformat() for r in df.collect()]
        assert "2024-03-01" in anomaly_dates

    def test_stable_days_not_flagged(self, spark):
        df = detect_anomalies(self._enriched_with_spike(spark))
        for row in df.collect():
            assert row["rate_date"].isoformat() != "2024-01-02"


# ─── quality_report() tests ───────────────────────────────────────────────────

class TestQualityReport:
    def test_weekends_classified_correctly(self, spark):
        # 2024-01-06 is a Saturday
        rows = [(date(2024, 1, 2), "USD", "MXN", 17.0, _TS)]
        df = clean(_make_df(spark, rows))
        report = quality_report(df, "2024-01-05", "2024-01-07")
        rows_out = {r["calendar_date"].isoformat(): r for r in report.collect()}
        assert rows_out["2024-01-06"]["status"] == "weekend"
        assert rows_out["2024-01-07"]["status"] == "weekend"

    def test_available_days_classified_correctly(self, spark):
        rows = [(date(2024, 1, 2), "USD", "MXN", 17.0, _TS)]
        df = clean(_make_df(spark, rows))
        report = quality_report(df, "2024-01-02", "2024-01-02")
        row = report.collect()[0]
        assert row["status"] == "available"

    def test_missing_weekday_classified_as_no_data(self, spark):
        # Provide data only for Jan 2; Jan 3 is a weekday with no data
        rows = [(date(2024, 1, 2), "USD", "MXN", 17.0, _TS)]
        df = clean(_make_df(spark, rows))
        report = quality_report(df, "2024-01-02", "2024-01-03")
        rows_out = {r["calendar_date"].isoformat(): r for r in report.collect()}
        assert rows_out["2024-01-03"]["status"] == "no_data"
