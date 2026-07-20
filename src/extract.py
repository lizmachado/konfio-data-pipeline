"""Extract layer — fetches exchange rates from the Frankfurter API."""

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any

import requests
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
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

import config

logger = logging.getLogger(__name__)

# Typed schema for raw exchange rate rows
RAW_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("currency_code", StringType(), nullable=False),
        StructField("rate", DoubleType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
    ]
)


@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(config.MAX_RETRIES),
    wait=wait_exponential(
        multiplier=1,
        min=config.RETRY_WAIT_MIN,
        max=config.RETRY_WAIT_MAX,
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _fetch_range(start: str, end: str, currencies: list[str]) -> dict[str, Any]:
    """Call Frankfurter range endpoint with retry/backoff."""
    symbols = ",".join(currencies)
    url = f"{config.API_BASE_URL}/v1/{start}..{end}"
    params = {"base": config.BASE_CURRENCY, "symbols": symbols}

    logger.debug("GET %s params=%s", url, params)
    response = requests.get(url, params=params, timeout=config.REQUEST_TIMEOUT)

    if response.status_code == 422:
        # API returns 422 when date range has no trading days (e.g. single weekend)
        logger.warning("API returned 422 for range %s..%s — no trading data", start, end)
        return {}

    response.raise_for_status()
    return response.json()


def fetch_exchange_rates(
    spark: SparkSession,
    start_date: str = config.START_DATE,
    end_date: str = config.END_DATE,
    currencies: list[str] = config.TARGET_CURRENCIES,
):
    """Fetch rates for the given date range and return a typed DataFrame."""
    logger.info(
        "Extracting exchange rates | %s → %s | currencies: %s",
        start_date,
        end_date,
        currencies,
    )

    ingestion_ts = datetime.utcnow()

    try:
        payload = _fetch_range(start_date, end_date, currencies)
    except requests.HTTPError as exc:
        logger.error("HTTP error fetching rates: %s", exc)
        raise

    rates_by_date: dict[str, dict] = payload.get("rates", {})

    if not rates_by_date:
        logger.warning("API returned no rates for range %s..%s", start_date, end_date)

    rows: list[dict] = []
    for date_str, currency_map in rates_by_date.items():
        for currency, rate in currency_map.items():
            rows.append(
                {
                    "rate_date": date_str,
                    "base_currency": payload.get("base", config.BASE_CURRENCY),
                    "currency_code": currency,
                    "rate": float(rate) if rate is not None else None,
                    "ingestion_timestamp": ingestion_ts.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    logger.info("Extracted %d raw rate records (%d trading days)", len(rows), len(rates_by_date))

    # write to temp file and read with spark.read.json() to avoid Python worker issues
    tmp_path = os.path.join(tempfile.gettempdir(), "konfio_extract.json")
    with open(tmp_path, "w", encoding="utf-8") as f:
        for rec in rows:
            f.write(json.dumps(rec) + "\n")

    json_schema = StructType([
        StructField("rate_date", StringType()),
        StructField("base_currency", StringType()),
        StructField("currency_code", StringType()),
        StructField("rate", DoubleType(), nullable=True),
        StructField("ingestion_timestamp", StringType()),
    ])

    return (
        spark.read.schema(json_schema).json(tmp_path)
        .withColumn("rate_date", F.to_date("rate_date", "yyyy-MM-dd").cast(DateType()))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
        .select("rate_date", "base_currency", "currency_code", "rate", "ingestion_timestamp")
    )
