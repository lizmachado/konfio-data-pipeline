# ── Stage 1: download Iceberg JAR (separate layer for better caching) ──────────
FROM python:3.11-slim AS jar-downloader

RUN apt-get update && apt-get install -y curl --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Pin a specific Iceberg release that matches Spark 3.5 + Scala 2.12
ARG ICEBERG_VERSION=1.7.1
RUN curl -fsSL -o /iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"


# ── Stage 2: runtime image ─────────────────────────────────────────────────────
FROM python:3.11-slim

# Java is required to run the JVM (Spark/Iceberg)
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy the pre-downloaded Iceberg JAR from the first stage
COPY --from=jar-downloader /iceberg-spark-runtime.jar /opt/iceberg-spark-runtime.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create output directories (also mounted as volumes in docker-compose)
RUN mkdir -p /app/warehouse /app/events

# PYTHONPATH lets Python find the top-level modules (config, src/*)
ENV PYTHONPATH=/app

CMD ["python", "src/main.py"]
#you can also run debug_pipeline.py for more details of the steps 
