# Konfio Data Engineering — Exchange Rate Pipeline

Pipeline batch de tipos de cambio construido con **PySpark 3.5** y **Apache Iceberg**, con lógica incremental CDC y emisión de eventos (simulación Kafka).

## Ejecución rápida

```bash
docker compose up --build
```

El pipeline corre de principio a fin sin intervención manual. Al terminar, los resultados quedan en:
- `warehouse/` — tablas Iceberg (Parquet + metadata)
- `events/` — archivos JSON (un evento por cambio CDC)

Los logs muestran el progreso de cada paso del ETL en tiempo real.

## Arquitectura del Pipeline (DAG)

```
Extract ──→ Clean ──→ Enrich ──→ Aggregate ──→ CDC ──→ Load (Iceberg) ──→ Events (JSON)
  │            │         │           │            │          │                  │
  │            │         │           │            │          ├─ MERGE INTO      └─ 1 archivo
  │            │         │           │            │          ├─ Time travel        por cambio
  │            │         │           │            │          └─ Partitioned
  │            │         │           └─ metricas  │
  │            │         │              mensuales  └─ INSERT/UPDATE/DELETE
  │            │         │
  │            │         ├─ detect_anomalies ──→ anomalias
  │            │         └─ quality_report ────→ reporte_calidad
  │            │
  API         Dedup, nulls, rangos
Frankfurter
```

Cada paso es una función pura (DataFrame in → DataFrame out), sin side effects, lo que facilita testing unitario y debugging.

## Estructura del repositorio

```
├── README.md
├── Dockerfile                  # Multi-stage build (descarga JAR + runtime)
├── docker-compose.yml          # Un solo comando: docker compose up
├── requirements.txt            # pyspark, requests, tenacity, pytest
├── config.py                   # Configuración externalizada (env vars)
├── src/
│   ├── main.py                 # Punto de entrada — orquesta el DAG completo
│   ├── spark_session.py        # SparkSession + catálogo Iceberg Hadoop
│   ├── extract.py              # Extracción desde Frankfurter API
│   ├── transform.py            # Limpieza, enriquecimiento, agregación, anomalías, calidad
│   ├── cdc.py                  # Change Data Capture (hash de fila)
│   ├── load.py                 # Persistencia en Iceberg (MERGE INTO, time travel)
│   └── events.py               # Emisión de eventos JSON (simulación Kafka)
├── tests/
│   └── test_transform.py       # 20 tests unitarios para la capa de transformación
├── warehouse/                  # Generado por el pipeline (catálogo Iceberg)
└── events/                     # Generado por el pipeline (eventos JSON)
```

## Capas del pipeline

### 1. Extracción (`src/extract.py`)

**Fuente:** Frankfurter API — endpoint de rango de fechas.

| Decisión | Justificación |
|----------|---------------|
| Una sola llamada con rango `2024-01-01..2024-06-30` | Minimiza round-trips y respeta rate limits. La API devuelve todos los trading days en una respuesta |
| Retry con backoff exponencial (`tenacity`) | Maneja timeouts y errores HTTP transitorios. Configurable: 5 intentos, espera 1–10s |
| Schema tipado (`RAW_SCHEMA`) | `rate_date: DATE`, `rate: DOUBLE`, `ingestion_timestamp: TIMESTAMP`. Tipos forzados al leer, no inferidos |
| Monedas: MXN, EUR, BRL, COP | MXN y EUR requeridas + dos adicionales relevantes para Konfio (Brasil y Colombia) |

**Manejo de gaps:** La API no devuelve fines de semana ni festivos. Esto se clasifica downstream en `quality_report()` (no se inventan datos).

### 2. Transformación (`src/transform.py`)

Cinco funciones puras, cada una un paso del pipeline:

#### 2.1 Limpieza (`clean`)
- Elimina filas con `rate IS NULL`, `rate <= 0`
- Deduplicación por `(rate_date, currency_code)` — mantiene primera ocurrencia
- Loguea cuántas filas se descartaron y por qué

#### 2.2 Enriquecimiento (`enrich`)
Campos derivados con window functions:

| Campo | Cálculo |
|-------|---------|
| `delta_daily` | `(rate - LAG(rate)) / LAG(rate) * 100` — variación % vs. día anterior |
| `avg_lag_7d` | Promedio móvil 7 días (`AVG OVER ROWS BETWEEN -6 AND CURRENT`) |
| `avg_lag_30d` | Promedio móvil 30 días |
| `volatility_30d` | Desviación estándar poblacional en ventana móvil de 30 días |
| `year`, `month` | Extraídos de `rate_date` — columnas de partición para Iceberg |

Window spec: `PARTITION BY currency_code ORDER BY rate_date`.

#### 2.3 Agregación (`aggregate`)
Tabla resumen mensual. **Grano: una fila por (year, month, currency_code).**

Métricas: `avg_rate`, `min_rate`, `max_rate`, `monthly_volatility`, `observation_count`, `avg_delta_daily`.

#### 2.4 Detección de anomalías (`detect_anomalies`)
Identifica días donde `|delta_daily| > 2σ` respecto al promedio móvil de 30 días (z-score).

El umbral es configurable via `ANOMALY_THRESHOLD` (default: 2.0).

#### 2.5 Reporte de calidad (`quality_report`)
Genera un calendario completo del rango solicitado y clasifica cada día:

| Status | Significado |
|--------|-------------|
| `available` | Dato presente en el dataset |
| `weekend` | Sábado o domingo (gap esperado) |
| `no_data` | Día hábil sin dato (posible festivo o falla de API) |

Grano: una fila por `(calendar_date, currency_code)`.

### 3. CDC — Change Data Capture (`src/cdc.py`)

**Estrategia:** Comparación por hash de fila (MD5).

| Elemento | Valor |
|----------|-------|
| **Llave de negocio** | `(rate_date, currency_code)` |
| **Señal de cambio** | MD5 de columnas payload: `rate, delta_daily, avg_lag_7d, avg_lag_30d, volatility_30d, ...` |
| **Operaciones detectadas** | `INSERT`, `UPDATE`, `DELETE`, `UNCHANGED` |

**Trade-off:** Hash vs. comparación columna a columna. Elegí hash porque es extensible — agregar una columna al schema no requiere modificar la lógica de comparación.

**Flujo:**
1. Calcular `row_hash` (MD5) para los datos nuevos
2. Cargar snapshot actual de Iceberg (si la tabla existe)
3. `FULL OUTER JOIN` sobre la llave de negocio
4. Clasificar cada fila según presencia y hash

**Campos de auditoría:**
- `ingestion_timestamp` — cuándo se ingirieron los datos
- `updated_at` — cuándo se detectó el último cambio
- `operation_type` — tipo de operación CDC

**Idempotencia:** Ejecutar el pipeline dos veces con los mismos datos produce 100% filas `UNCHANGED` — sin duplicados ni inconsistencias.

**Demostración CDC:** El script `debug_pipeline.py` corre el pipeline dos veces con rangos superpuestos para demostrar los 3 tipos de operación (INSERT, DELETE, UNCHANGED). También se puede correr `main.py` dos veces cambiando el rango de fechas.

### 4. Modelado de datos

#### Tablas de hechos

| Tabla | Grano | Partición | Descripción |
|-------|-------|-----------|-------------|
| `fact_exchange_rates` (`tipos_cambio_enriquecidos`) | 1 fila por (date, currency) | `year, month` | Tipo de cambio diario enriquecido con métricas derivadas |
| `metricas_mensuales` | 1 fila por (year, month, currency) | `year, month` | Resumen mensual por moneda |
| `anomalias` | 1 fila por (date, currency) anómala | `months(rate_date)` | Días con movimientos atípicos (z-score > 2σ) |
| `reporte_calidad` | 1 fila por (date, currency) del calendario | `months(calendar_date)` | Log de calidad: available, weekend, no_data |

#### Tabla de dimensiones

| Tabla | Grano | Descripción |
|-------|-------|-------------|
| `dim_currency` | 1 fila por currency_code | Código ISO, nombre del currency, moneda base |

**Llaves:** La fact table usa `(rate_date, currency_code)` como llave compuesta natural. `dim_currency.currency_code` es la llave de la dimensión, referenciada desde todas las fact tables.

**Decisión de modelado:** Se usa un esquema estrella simple (star schema). La dimensión `dim_currency` se separa de los hechos para normalizar metadata que no cambia (nombre del currency) y permitir JOINs analíticos limpios.

### 5. Carga — Apache Iceberg (`src/load.py`)

**Catálogo:** Hadoop (file-based, sin metastore externo). Las tablas se crean con DDL explícito (`CREATE TABLE IF NOT EXISTS ... USING iceberg`).

| Feature Iceberg | Implementación | Archivo |
|-----------------|----------------|---------|
| **MERGE INTO** | Upsert idempotente: INSERT nuevas filas, UPDATE si hash cambió, no-op si hash igual | `load.py:141-172` |
| **Time travel** | Después de cada escritura, lee el snapshot anterior para comparar row counts | `load.py:189-209` |
| **Partitioning** | `tipos_cambio_enriquecidos` y `metricas_mensuales` por `(year, month)`; `anomalias` y `reporte_calidad` por `months(date)` | DDL en `load.py` |
| **Schema-first DDL** | Todas las tablas definidas con tipos explícitos, no inferidos | `load.py:44-123` |

**Idempotencia:** El `MERGE INTO` usa la llave de negocio `(rate_date, currency_code)`. Si se ejecuta múltiples veces con los mismos datos, no genera duplicados — las filas `UNCHANGED` no modifican datos en Iceberg.

### 6. Emisión de eventos — simulación Kafka (`src/events.py`)

Cada cambio CDC (INSERT, UPDATE, DELETE) produce un archivo JSON en `/events/`. Las filas `UNCHANGED` no generan eventos.

**Schema del evento (v1):**
```json
{
  "schema_version": "1.0",
  "event_type": "INSERT",
  "event_timestamp": "2024-01-02T00:00:00Z",
  "entity": "exchange_rate",
  "entity_id": "2024-01-02::MXN",
  "payload": {
    "rate_date": "2024-01-02",
    "currency_code": "MXN",
    "base_currency": "USD",
    "rate": 17.065,
    "delta_daily": null,
    "avg_lag_7d": 17.065,
    "avg_lag_30d": 17.065,
    "volatility_30d": 0.0,
    "row_hash": "abc123..."
  },
  "metadata": {
    "pipeline_run_id": "uuid-v4",
    "ingestion_timestamp": "2024-07-20T12:00:00Z"
  }
}
```

| Decisión | Justificación |
|----------|---------------|
| Schema versionado (`schema_version: "1.0"`) | Permite evolución del schema sin romper consumidores downstream |
| `entity_id` como `{date}::{currency}` | Identificador único legible que permite deduplicación en el consumidor |
| Payload completo (no solo delta) | Un consumidor puede reconstruir el estado completo de la entidad con un solo evento |
| JSON (no Avro/Protobuf) | Transparencia y simplicidad para esta prueba. En producción se usaría Avro con Schema Registry |

**Consistencia:** Los eventos reflejan exactamente los cambios detectados por CDC. El `event_type` corresponde 1:1 con el `operation_type` del CDC, y el payload contiene los mismos datos que se persistieron en Iceberg.

## Testing

20 tests unitarios en `tests/test_transform.py`:

```bash
# Dentro del contenedor
docker compose run pipeline pytest tests/test_transform.py -v

# O localmente (requiere Java 11 y pyspark)
pytest tests/test_transform.py -v
```

| Suite | Tests | Qué valida |
|-------|-------|------------|
| `TestClean` | 5 | Nulls, zeros, negativos, dedup, filas válidas |
| `TestEnrich` | 7 | Columnas year/month, delta_daily, moving averages, volatility |
| `TestAggregate` | 3 | Una fila por mes, observation_count, min/max correctos |
| `TestDetectAnomalies` | 2 | Spike detectado como anomalía, días estables no flaggeados |
| `TestQualityReport` | 3 | Weekends, datos disponibles, weekdays sin datos |

Los tests crean DataFrames pequeños via NDJSON (sin Python workers) y validan cada transformación de forma aislada.

## Configuración

Toda la configuración es externalizable via variables de entorno (ver `config.py`):

| Variable | Default | Descripción |
|----------|---------|-------------|
| `START_DATE` | `2024-01-01` | Inicio del rango de extracción |
| `END_DATE` | `2024-06-30` | Fin del rango de extracción |
| `BASE_CURRENCY` | `USD` | Moneda base |
| `TARGET_CURRENCIES` | `MXN,EUR,BRL,COP` | Monedas destino (comma-separated) |
| `WAREHOUSE_PATH` | `./warehouse` | Ruta del catálogo Iceberg |
| `EVENTS_PATH` | `./events` | Ruta de los eventos JSON |
| `ICEBERG_JAR` | `./jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar` | JAR de Iceberg |
| `ANOMALY_THRESHOLD` | `2.0` | Umbral de z-score para detección de anomalías |
| `MAX_RETRIES` | `5` | Intentos de retry para la API |

Docker Compose sobreescribe estas variables para las rutas del contenedor (`/app/warehouse`, `/opt/iceberg-spark-runtime.jar`, etc.).

## Decisiones técnicas y trade-offs

| Decisión | Alternativa considerada | Por qué esta opción |
|----------|------------------------|---------------------|
| Catálogo Hadoop (file-based) | Hive Metastore, SQLite catalog | Sin dependencias externas. Se ejecuta con un solo `docker compose up`. Suficiente para el volumen de datos de esta prueba |
| CDC por hash MD5 | Comparación columna por columna | Extensible: agregar columnas al schema no requiere cambiar la lógica de comparación. Trade-off: hash collisions teóricamente posibles pero estadísticamente negligibles con MD5 |
| Simulación de Kafka con JSON files | Kafka real en Docker | Cumple el requerimiento mínimo sin complejidad de infraestructura. Un Kafka real agregaría ~1GB de imágenes Docker sin aportar al core del pipeline |
| `MERGE INTO` para fact table, `createOrReplace` para tablas derivadas | Append-only | Las tablas derivadas (métricas, anomalías, calidad) se recalculan completas en cada ejecución — no necesitan incrementalidad. La fact table sí la necesita |
| Retry con backoff exponencial (tenacity) | Retry lineal, sin retry | Backoff exponencial es el estándar para APIs externas. Evita saturar el endpoint durante fallos transitorios |

## Supuestos

1. **Frankfurter API disponible:** El pipeline requiere conectividad a internet para la extracción. Si la API no responde después de 5 reintentos, el pipeline falla con un error claro.
2. **Rates positivos:** Se asume que los tipos de cambio son siempre positivos. Rates nulos o ≤ 0 se descartan en la limpieza.
3. **Sin DELETES naturales:** La API no elimina datos históricos. Los DELETE en CDC se detectarían si una fila existente en Iceberg no aparece en la nueva extracción, pero en la práctica esto no ocurre con esta fuente.
4. **Ejecución diaria:** El pipeline está diseñado para ejecutarse una vez al día (o bajo demanda). No es un pipeline streaming.
5. **Single node:** El pipeline corre en `local[*]` mode (un solo nodo). Para producción se configuraría un cluster Spark, pero el código es el mismo.

## Cómo testear el pipeline

### Opción 1: Docker 

Solo necesitas Docker instalado. No importa tu sistema operativo (macOS, Linux, Windows con WSL).

```bash
git clone https://github.com/lizmachado/konfio-data-pipeline.git
cd konfio-data-pipeline
docker compose up --build
```

Docker se encarga de instalar Java, Python, PySpark y el JAR de Iceberg automáticamente. Al terminar verás el resumen del pipeline en la terminal.

### Opción 2: GitHub Codespaces (sin instalar nada) -- testeado personalmente

1. Ve al repositorio en GitHub y haz clic en **Code → Codespaces → Create codespace on main**
2. Espera a que el contenedor se construya (~2 min). El `devcontainer.json` instala Python 3.11, Java 17 y Docker automáticamente
3. En la terminal del Codespace:

```bash
docker compose up --build
```

> **Nota:** El Codespace usa Ubuntu. La configuración de `devcontainer.json` ya incluye todas las dependencias necesarias.

### Opción 3: Ejecución local (sin Docker)

**Requisitos:**
- Python 3.11 (recomendado) o 3.12
- Java 11+ (OpenJDK)
- Conexión a internet (para la API de Frankfurter)

**Pasos:**

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Descargar el JAR de Iceberg (~30 MB)
mkdir -p jars
curl -fsSL -o jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

# 3. Ejecutar el pipeline
python src/main.py

# 4. (Opcional) Ejecutar con debug detallado
python debug_pipeline.py

# 5. Ejecutar tests unitarios
pytest tests/test_transform.py -v
```

> **Nota Windows:** PySpark puede mostrar warnings sobre archivos temporales (`Unable to delete file`) al final. Estos  no afectan los resultados.

> **Nota:** Python 3.13 no es compatible con PySpark 3.5. Usa Python 3.11 o 3.12.

### Verificar resultados

Después de ejecutar el pipeline, verifica que se generaron los archivos:

```
warehouse/          ← Tablas Iceberg (archivos Parquet + metadata JSON)
├── db/
│   ├── tipos_cambio_enriquecidos/
│   ├── metricas_mensuales/
│   ├── anomalias/
│   ├── reporte_calidad/
│   └── dim_currency/
events/             ← Eventos CDC en formato JSON
├── INSERT_2024-01-02_MXN_*.json
├── ...
```

