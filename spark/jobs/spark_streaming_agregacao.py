"""
================================================================================
Forest Risk Monitoring System — Spark Structured Streaming (3 streams + join)
================================================================================

OBJETIVO (Fase 3 — Data Engineer):
    Ler 3 streams do Kafka em simultaneo, fazer join por grid_id e janela
    temporal, e calcular um indice de risco composto por zona.

    Cumpre o requisito do documento:
    "Join entre os tres streams (sensores + satelite + meteorologia)
     com watermark de 2 minutos"

STREAMS:
    sensor-events      -> temperatura, humidade, vento, risk_score (producer-sensores)
    satellite-hotspots -> hotspots NASA FIRMS em tempo real       (producer-apis)
    weather-data       -> dados IPMA/ERA5 em tempo real           (producer-apis)

ARQUITETURA DO JOB:
    Stream 1 (sensores)    ─┐
    Stream 2 (satelite)    ─┼─ join por grid_id + janela 10 min ─> risco composto
    Stream 3 (meteo)       ─┘

ESCREVE EM DOIS DESTINOS:
    A) Console → tabelas em tempo real (demonstracao)
    B) S3      → Parquet no data lake (arquivo)

COMO CORRER:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      /home/jovyan/spark-jobs/spark_streaming_agregacao.py
================================================================================
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, coalesce, count, from_json,
    lit, max as spark_max, sum as spark_sum,
    to_timestamp, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# ── Configuracao ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

S3_BUCKET     = "forest-risk-datalake"
S3_PATH       = f"s3a://{S3_BUCKET}/agregados_streaming/"
S3_CHECKPOINT = f"s3a://{S3_BUCKET}/checkpoints/agregados_join/"

# ══════════════════════════════════════════════════════════════════════════════
# 1. SESSAO SPARK
# ══════════════════════════════════════════════════════════════════════════════

spark = (
    SparkSession.builder
    .appName("ForestRisk-StreamingJoin3")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Configura S3
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", AWS_ENDPOINT_URL)
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

print(f"Sessao Spark criada. Kafka: {KAFKA_BOOTSTRAP}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. SCHEMAS DOS 3 TOPICS
# ══════════════════════════════════════════════════════════════════════════════

# Schema do topic sensor-events (producer_sensores.py + producer_apis_reais.py)
schema_sensores = StructType([
    StructField("grid_id",       StringType()),
    StructField("regiao",        StringType()),
    StructField("latitude",      DoubleType()),
    StructField("longitude",     DoubleType()),
    StructField("timestamp",     StringType()),
    StructField("temp_celsius",  DoubleType()),
    StructField("humidity_pct",  DoubleType()),
    StructField("wind_kmh",      DoubleType()),
    StructField("hotspot_count", IntegerType()),
    StructField("risk_score",    DoubleType()),
    StructField("source",        StringType()),
])

# Schema do topic satellite-hotspots (producer_apis_reais.py — NASA FIRMS)
schema_satelite = StructType([
    StructField("grid_id",    StringType()),
    StructField("latitude",   DoubleType()),
    StructField("longitude",  DoubleType()),
    StructField("timestamp",  StringType()),
    StructField("brightness", DoubleType()),   # bright_ti4
    StructField("frp",        DoubleType()),   # Fire Radiative Power (MW)
    StructField("confidence", StringType()),   # h/n/l
    StructField("daynight",   StringType()),
    StructField("source",     StringType()),
])

# Schema do topic weather-data (producer_apis_reais.py — IPMA)
schema_meteo = StructType([
    StructField("grid_id",          StringType()),
    StructField("latitude",         DoubleType()),
    StructField("longitude",        DoubleType()),
    StructField("timestamp",        StringType()),
    StructField("temp_max",         DoubleType()),
    StructField("temp_min",         DoubleType()),
    StructField("humidity_avg",     DoubleType()),
    StructField("wind_max_kmh",     DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("source",           StringType()),
])

# ══════════════════════════════════════════════════════════════════════════════
# 3. LER OS 3 STREAMS DO KAFKA
# ══════════════════════════════════════════════════════════════════════════════

def ler_topic(topic, schema, timestamp_col="timestamp"):
    """Le um topic do Kafka, parseia o JSON e converte o timestamp."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col(timestamp_col)))
    )

stream_sensores = ler_topic("sensor-events",      schema_sensores)
stream_satelite = ler_topic("satellite-hotspots", schema_satelite)
stream_meteo    = ler_topic("weather-data",        schema_meteo)

# ══════════════════════════════════════════════════════════════════════════════
# 4. AGREGACAO POR JANELA E ZONA (com watermark de 2 min)
# ══════════════════════════════════════════════════════════════════════════════
# Cada stream e agregado independentemente numa janela de 10 min / slide 5 min.
# O watermark de 2 min tolera eventos atrasados.
# Depois os 3 agregados sao joined por (janela, grid_id).

# ── Agregacao dos sensores IoT ────────────────────────────────────────────────
agg_sensores = (
    stream_sensores
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("grid_id")
    )
    .agg(
        count("*").alias("n_leituras_sensor"),
        avg("risk_score").alias("risk_medio_sensor"),
        spark_max("risk_score").alias("risk_maximo_sensor"),
        avg("temp_celsius").alias("temp_media"),
        avg("humidity_pct").alias("humidade_media"),
        avg("wind_kmh").alias("vento_medio"),
    )
)

# ── Agregacao dos hotspots de satelite ───────────────────────────────────────
agg_satelite = (
    stream_satelite
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("grid_id")
    )
    .agg(
        count("*").alias("n_hotspots"),
        avg("frp").alias("frp_medio"),
        spark_max("frp").alias("frp_maximo"),
        avg("brightness").alias("brightness_media"),
    )
)

# ── Agregacao meteorologica ───────────────────────────────────────────────────
agg_meteo = (
    stream_meteo
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("grid_id")
    )
    .agg(
        avg("temp_max").alias("temp_max_media"),
        avg("humidity_avg").alias("humidade_ipma"),
        avg("wind_max_kmh").alias("vento_max_ipma"),
        avg("precipitation_mm").alias("precipitacao_media"),
    )
)

# ══════════════════════════════════════════════════════════════════════════════
# 5. JOIN DOS 3 STREAMS
# ══════════════════════════════════════════════════════════════════════════════
# outer join: garante que aparece resultado mesmo que um stream nao tenha dados
# numa determinada janela/zona (ex: sem hotspots de satelite numa zona)

joined = (
    agg_sensores
    .join(agg_satelite,
          on=["window", "grid_id"],
          how="left")
    .join(agg_meteo,
          on=["window", "grid_id"],
          how="left")
)

# ══════════════════════════════════════════════════════════════════════════════
# 6. INDICE DE RISCO COMPOSTO (combina os 3 streams)
# ══════════════════════════════════════════════════════════════════════════════
# Ponderacao:
#   60% risco dos sensores IoT (temperatura, humidade, vento)
#   25% intensidade dos hotspots NASA (FRP)
#   15% condicoes meteorologicas IPMA

df_resultado = joined.select(
    col("window.start").alias("janela_inicio"),
    col("window.end").alias("janela_fim"),
    col("grid_id"),

    # Sensores IoT
    col("n_leituras_sensor"),
    col("risk_medio_sensor"),
    col("risk_maximo_sensor"),
    col("temp_media"),
    col("humidade_media"),
    col("vento_medio"),

    # Satelite NASA
    coalesce(col("n_hotspots"),     lit(0)).alias("n_hotspots"),
    coalesce(col("frp_medio"),      lit(0.0)).alias("frp_medio"),
    coalesce(col("frp_maximo"),     lit(0.0)).alias("frp_maximo"),

    # Meteorologia IPMA
    col("temp_max_media"),
    col("humidade_ipma"),
    col("vento_max_ipma"),
    col("precipitacao_media"),

    # Indice de risco composto (0-100):
    # 60% sensor + 25% FRP normalizado (max=200MW) + 15% vento IPMA
    (
        coalesce(col("risk_medio_sensor"), lit(0.0)) * 0.60 +
        (coalesce(col("frp_medio"), lit(0.0)) / 200.0 * 100.0) * 0.25 +
        (coalesce(col("vento_max_ipma"), lit(0.0)) / 150.0 * 100.0) * 0.15
    ).alias("risco_composto"),
)

# ══════════════════════════════════════════════════════════════════════════════
# 7. ESCREVER — CONSOLE + S3
# ══════════════════════════════════════════════════════════════════════════════

# Destino A — Console (demonstracao ao vivo)
query_console = (
    df_resultado.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 15)
    .trigger(processingTime="30 seconds")
    .start()
)

# Destino B — S3 Parquet (arquivo data lake)
query_s3 = (
    df_resultado.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", S3_PATH)
    .option("checkpointLocation", S3_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming iniciado! Join de 3 streams por grid_id e janela de 10 min.")
print("  Stream 1: sensor-events      (IoT simulado + APIs reais)")
print("  Stream 2: satellite-hotspots (NASA FIRMS em tempo real)")
print("  Stream 3: weather-data       (IPMA meteorologia)")
print(f"  Console  : tabelas ao vivo")
print(f"  S3       : {S3_PATH}")
print("Pressiona Ctrl+C para parar.\n")

spark.streams.awaitAnyTermination()
