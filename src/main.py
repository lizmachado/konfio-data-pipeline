"""
Pipeline entry point — runs the full ETL in order.
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from src.spark_session import create_spark_session
from src.extract import fetch_exchange_rates
from src.transform import clean, enrich, aggregate, detect_anomalies, quality_report
from src.cdc import compute_cdc
from src.load import load_all
from src.events import emit_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def run_pipeline(
    start_date: str = config.START_DATE,
    end_date: str = config.END_DATE,
) -> None:
    """Run the full ETL: extract → transform → CDC → load → events."""
    spark = create_spark_session()

    # Extract
    logger.info("Extracting rates %s → %s", start_date, end_date)
    raw = fetch_exchange_rates(spark, start_date, end_date)

    # Transform
    cleaned = clean(raw)
    enriched = enrich(cleaned)
    monthly = aggregate(enriched)
    anomalies = detect_anomalies(enriched)
    quality = quality_report(cleaned, start_date, end_date)

    # CDC
    target_table = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.tipos_cambio_enriquecidos"
    cdc = compute_cdc(spark, enriched, target_table)

    # Load to Iceberg
    load_all(spark, cdc, monthly, anomalies, quality)

    # Events (Kafka simulation)
    total_events = emit_events(cdc)

    # Summary
    logger.info("=" * 60)
    logger.info("Pipeline complete")
    logger.info("  Records processed : %d", enriched.count())
    logger.info("  Anomalies detected: %d", anomalies.count())
    logger.info("  Events emitted    : %d", total_events)
    logger.info("  Iceberg warehouse : %s", config.WAREHOUSE_PATH)
    logger.info("  Events directory  : %s", config.EVENTS_PATH)
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
