import json
import logging
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame

import config

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"
ENTITY_NAME = "exchange_rate"

# Only these operation types produce outbound events
_EMITTABLE_OPERATIONS = {"INSERT", "UPDATE", "DELETE"}


def _safe_get(row, col):
    """PySpark Row doesn't have .get() like a dict — this avoids AttributeError."""
    try:
        return row[col]
    except (ValueError, IndexError):
        return None


def _row_to_event(row, run_id: str) -> dict:
    """Convert a CDC DataFrame row into a structured event dict."""
    rate_date = row["rate_date"].isoformat() if row["rate_date"] else None
    currency = row["currency_code"]

    ts = _safe_get(row, "ingestion_timestamp")

    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": row["operation_type"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "entity": ENTITY_NAME,
        "entity_id": f"{rate_date}::{currency}",
        "payload": {
            "rate_date": rate_date,
            "currency_code": currency,
            "base_currency": _safe_get(row, "base_currency"),
            "rate": _safe_get(row, "rate"),
            "delta_daily": _safe_get(row, "delta_daily"),
            "avg_lag_7d": _safe_get(row, "avg_lag_7d"),
            "avg_lag_30d": _safe_get(row, "avg_lag_30d"),
            "volatility_30d": _safe_get(row, "volatility_30d"),
            "row_hash": _safe_get(row, "row_hash"),
        },
        "metadata": {
            "pipeline_run_id": run_id,
            "ingestion_timestamp": ts.isoformat() if ts else None,
        },
    }


def emit_events(cdc_df: DataFrame, events_dir: str = config.EVENTS_PATH) -> int:
    """Write one JSON event file per CDC change. Returns the count of events written."""
    os.makedirs(events_dir, exist_ok=True)

    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    emittable = (
        cdc_df.filter(
            cdc_df["operation_type"].isin(list(_EMITTABLE_OPERATIONS))
        )
        .collect()
    )

    count = 0
    for row in emittable:
        event = _row_to_event(row, run_id)
        op = event["event_type"]
        entity_id = event["entity_id"].replace("::", "_").replace("-", "")
        filename = f"{run_ts}_{op}_{entity_id}_{count:06d}.json"
        filepath = os.path.join(events_dir, filename)

        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(event, fh, indent=2, default=str)

        count += 1

    logger.info("Emitted %d events to %s (run_id=%s)", count, events_dir, run_id)
    return count
