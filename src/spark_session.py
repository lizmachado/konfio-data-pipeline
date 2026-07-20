import logging
import os
import sys
from pyspark.sql import SparkSession
import config

logger = logging.getLogger(__name__)


def _setup_windows_env():
    # PySpark 3.5 needs Java 11 on Windows
    java11 = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.20.101-hotspot"
    if os.path.isdir(java11):
        os.environ["JAVA_HOME"] = java11
        os.environ["PATH"] = os.path.join(java11, "bin") + os.pathsep + os.environ.get("PATH", "")

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        hadoop_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hadoop")
        if os.path.isdir(hadoop_dir):
            os.environ["HADOOP_HOME"] = hadoop_dir
            os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")


def create_spark_session(app_name: str = "KonfioPipeline") -> SparkSession:
    """Build a SparkSession with the Iceberg Hadoop catalog (file-based, no external deps)."""
    _setup_windows_env()
    logger.info("Initialising Spark session with Iceberg support")

    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{config.CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{config.CATALOG_NAME}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{config.CATALOG_NAME}.warehouse",
            config.WAREHOUSE_PATH,
        )
        .config("spark.jars", config.ICEBERG_JAR)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session ready (version %s)", spark.version)
    return spark
