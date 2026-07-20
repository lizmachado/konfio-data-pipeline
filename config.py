import os

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Date range for historical data extraction
START_DATE = os.getenv("START_DATE", "2024-01-01")
END_DATE = os.getenv("END_DATE", "2024-06-30")

# Currency configuration
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "USD")
TARGET_CURRENCIES = os.getenv("TARGET_CURRENCIES", "MXN,EUR,BRL,COP").split(",")

# API settings
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.frankfurter.dev")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
RETRY_WAIT_MIN = float(os.getenv("RETRY_WAIT_MIN", "1"))
RETRY_WAIT_MAX = float(os.getenv("RETRY_WAIT_MAX", "10"))

# Storage paths — defaults work locally; Docker overrides via env vars
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", os.path.join(_PROJECT_ROOT, "warehouse"))
EVENTS_PATH = os.getenv("EVENTS_PATH", os.path.join(_PROJECT_ROOT, "events"))

# Iceberg catalog and database
CATALOG_NAME = "local"
DATABASE_NAME = "db"

# Iceberg JAR — local default; Docker overrides via env var
ICEBERG_JAR = os.getenv(
    "ICEBERG_JAR",
    os.path.join(_PROJECT_ROOT, "jars", "iceberg-spark-runtime-3.5_2.12-1.7.1.jar"),
)

# Anomaly detection threshold (standard deviations)
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "2.0"))

# Moving average windows
MA_SHORT_WINDOW = 7
MA_LONG_WINDOW = 30
