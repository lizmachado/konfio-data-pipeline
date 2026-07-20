"""Debug script — runs each step with .show() so you can inspect the data at every stage."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import config
from src.spark_session import create_spark_session
from src.extract import fetch_exchange_rates
from src.transform import clean, enrich, aggregate, detect_anomalies, quality_report
from src.cdc import compute_cdc
from src.load import load_all
from src.events import emit_events


def pause(msg="Presiona ENTER para continuar al siguiente paso..."):
    print(f"\n{'─'*70}")
    input(msg)
    print()


def main():
    spark = create_spark_session()

    # ── 1. EXTRACT ───────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 1: EXTRACT — Datos crudos de la API Frankfurter")
    print("=" * 70)
    print("Llamando a la API para el rango 2024-03-01 → 2024-05-31")
    print("  (Marzo ya existe en Iceberg → UNCHANGED, Abril+Mayo son nuevos → INSERT)")
    print()

    raw = fetch_exchange_rates(spark, start_date="2024-03-01", end_date="2024-05-31")

    print(f"Filas extraídas: {raw.count()}")
    print(f"Columnas: {raw.columns}")
    print()
    print("Schema (tipos de dato):")
    raw.printSchema()
    print("Primeras 20 filas:")
    raw.show(20, truncate=False)

    pause()

    # ── 2. CLEAN ─────────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 2: CLEAN — Limpieza (nulls, duplicados, rangos)")
    print("  En dbt esto sería tu modelo stg_exchange_rates")
    print("=" * 70)

    cleaned = clean(raw)

    print(f"Filas antes: {raw.count()} → después: {cleaned.count()}")
    print()
    print("Primeras 20 filas limpias:")
    cleaned.show(20, truncate=False)

    pause()

    # ── 3. ENRICH ────────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 3: ENRICH — Campos derivados (window functions)")
    print("  En dbt esto sería tu modelo trf_exchange_rates o int_exchange_rates")
    print("  Agrega: delta_daily, avg_lag_7d, avg_lag_30d, volatility_30d, year, month")
    print("=" * 70)

    enriched = enrich(cleaned)

    print(f"Columnas nuevas: {[c for c in enriched.columns if c not in cleaned.columns]}")
    print(f"Filas: {enriched.count()}")
    print()
    print("Primeras 10 filas de MXN (para ver las window functions):")
    enriched.filter("currency_code = 'MXN'").orderBy("rate_date").show(10, truncate=False)

    print("Primeras 10 filas de EUR:")
    enriched.filter("currency_code = 'EUR'").orderBy("rate_date").show(10, truncate=False)

    pause()

    # ── 4. AGGREGATE ─────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 4: AGGREGATE — Resumen mensual por moneda")
    print("  En dbt esto sería tu modelo fct_monthly_rates")
    print("  Grano: 1 fila por (year, month, currency_code)")
    print("=" * 70)

    monthly = aggregate(enriched)

    print(f"Filas: {monthly.count()}")
    print()
    print("Tabla completa:")
    monthly.show(50, truncate=False)

    pause()

    # ── 5. DETECT ANOMALIES ──────────────────────────────────────────────
    print("=" * 70)
    print("PASO 5: DETECT ANOMALIES — Días con movimiento > 2 sigmas")
    print("  En dbt sería un test custom o un modelo de alertas")
    print("=" * 70)

    anomalies = detect_anomalies(enriched)

    count = anomalies.count()
    print(f"Anomalías detectadas: {count}")
    print()
    if count > 0:
        print("Todas las anomalías:")
        anomalies.show(anomalies.count(), truncate=False)
    else:
        print("(Ninguna anomalía en este rango — los mercados estuvieron estables)")

    pause()

    # ── 6. QUALITY REPORT ────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 6: QUALITY REPORT — Cobertura de datos por día")
    print("  En dbt sería como dbt source freshness + elementary")
    print("  Clasifica cada día: available, weekend, no_data")
    print("=" * 70)

    quality = quality_report(cleaned, "2024-03-01", "2024-05-31")

    print(f"Filas totales: {quality.count()}")
    print()
    print("Resumen por status:")
    quality.groupBy("status").count().show()

    print("Primeras 2 semanas (para ver weekends y gaps):")
    quality.filter("currency_code = 'MXN'").orderBy("calendar_date").show(14, truncate=False)

    pause()

    # ── 7. CDC ────────────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 7: CDC — Change Data Capture")
    print("  En dbt esto sería dbt snapshot (strategy: check)")
    print("  Primera ejecución: todo es INSERT")
    print("=" * 70)

    target_table = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.tipos_cambio_enriquecidos"
    cdc = compute_cdc(spark, enriched, target_table)

    print(f"Filas CDC: {cdc.count()}")
    print()
    print("Conteo por tipo de operación:")
    cdc.groupBy("operation_type").count().show()

    print("Primeras 10 filas CDC (nota las columnas operation_type, row_hash, updated_at):")
    cdc.select(
        "rate_date", "currency_code", "rate", "operation_type", "row_hash", "updated_at"
    ).orderBy("rate_date", "currency_code").show(10, truncate=False)

    pause()

    # ── 8. LOAD ──────────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 8: LOAD — Persistir en Iceberg")
    print("  MERGE INTO para la fact table, createOrReplace para las demás")
    print("=" * 70)

    load_all(spark, cdc, monthly, anomalies, quality)

    print("Tablas en el catálogo Iceberg:")
    spark.sql(f"SHOW TABLES IN {config.CATALOG_NAME}.{config.DATABASE_NAME}").show(truncate=False)

    print("Leyendo fact table desde Iceberg (primeras 10 filas):")
    spark.table(target_table).orderBy("rate_date", "currency_code").show(10, truncate=False)

    print("dim_currency:")
    spark.table(f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.dim_currency").show(truncate=False)

    pause()

    # ── 9. EVENTS ────────────────────────────────────────────────────────
    print("=" * 70)
    print("PASO 9: EVENTS — Emisión de eventos JSON (simulación Kafka)")
    print("  Un archivo JSON por cada INSERT/UPDATE/DELETE")
    print("=" * 70)

    total_events = emit_events(cdc)

    print(f"Eventos emitidos: {total_events}")
    print(f"Directorio: {config.EVENTS_PATH}")
    print()

    event_files = os.listdir(config.EVENTS_PATH)
    print(f"Archivos generados: {len(event_files)}")
    if event_files:
        print()
        print("Ejemplo — contenido del primer evento:")
        import json
        with open(os.path.join(config.EVENTS_PATH, sorted(event_files)[0])) as f:
            print(json.dumps(json.load(f), indent=2))

    # ── RESUMEN ──────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("PIPELINE COMPLETO")
    print("=" * 70)
    print(f"  Registros procesados  : {enriched.count()}")
    print(f"  Anomalías detectadas  : {anomalies.count()}")
    print(f"  Eventos emitidos      : {total_events}")
    print(f"  Iceberg warehouse     : {config.WAREHOUSE_PATH}")
    print(f"  Events directory      : {config.EVENTS_PATH}")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
