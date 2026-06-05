"""
================================================================================
Forest Risk Monitoring System — Spark Structured Streaming
================================================================================

OBJETIVO (Fase 3 — Data Engineer):
    Ler eventos de sensores do Kafka em tempo real e calcular, para cada zona
    geográfica (grid_id), a MÉDIA de risco, temperatura, humidade e vento numa
    JANELA DESLIZANTE (sliding window) de 10 minutos, recalculada a cada 5 min.

DIFERENÇA PARA O consumer_kafka_cassandra.py:
    - O consumer Python guarda CADA leitura individual no Cassandra (1-para-1).
    - Este job Spark AGREGA várias leituras por zona/janela (1-para-muitos → 1).
    Os dois leem do mesmo Kafka mas fazem trabalhos complementares.

TECNOLOGIA: Apache Spark Structured Streaming (UC Big Data Tools II)
    - readStream do Kafka
    - sliding window de 10 min / slide de 5 min
    - watermark de 2 min (tolera eventos atrasados)
    - agregação por grid_id

ESCREVE EM DOIS DESTINOS:
    A) Console → tabelas em tempo real (demonstração)
    B) S3      → Parquet no data lake (arquivo de longo prazo)

COMO CORRER (dentro do Jupyter, que já tem Spark 3.5):
    Abre um terminal no Jupyter Lab e corre.
    NOTA: agora são precisos DOIS packages (Kafka + S3/hadoop-aws):
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
          /home/jovyan/spark-jobs/spark_streaming_agregacao.py
================================================================================
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configuração (lê das variáveis de ambiente do Jupyter) ────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "sensor-events"

# ══════════════════════════════════════════════════════════════════════════════
# 1. CRIAR A SESSÃO SPARK
# ══════════════════════════════════════════════════════════════════════════════
# O package spark-sql-kafka permite ao Spark ler diretamente do Kafka.
# Em modo local[*] usa todos os cores disponíveis do container.

spark = (
    SparkSession.builder
    .appName("ForestRisk-StreamingAgregacao")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .config("spark.sql.shuffle.partitions", "4")  # menos partições = mais leve em local
    .master("local[*]")
    .getOrCreate()
)

# Reduz o ruído dos logs — só mostra avisos e erros
spark.sparkContext.setLogLevel("WARN")

print("Sessao Spark criada. A ligar ao Kafka em", KAFKA_BOOTSTRAP)

# ══════════════════════════════════════════════════════════════════════════════
# 2. DEFINIR O SCHEMA DOS EVENTOS
# ══════════════════════════════════════════════════════════════════════════════
# O Kafka entrega bytes. Temos de dizer ao Spark qual a estrutura do JSON
# que o producer_sensores.py envia, para o conseguir parsear.

schema_evento = StructType([
    StructField("grid_id",       StringType()),
    StructField("regiao",        StringType()),
    StructField("latitude",      DoubleType()),
    StructField("longitude",     DoubleType()),
    StructField("timestamp",     StringType()),   # ISO string → convertido abaixo
    StructField("temp_celsius",  DoubleType()),
    StructField("humidity_pct",  DoubleType()),
    StructField("wind_kmh",      DoubleType()),
    StructField("hotspot_count", IntegerType()),
    StructField("risk_score",    DoubleType()),
    StructField("source",        StringType()),
])

# ══════════════════════════════════════════════════════════════════════════════
# 3. LER O STREAM DO KAFKA (readStream)
# ══════════════════════════════════════════════════════════════════════════════
# startingOffsets=latest → só lê eventos novos a partir de agora
# (usa "earliest" se quiseres reprocessar tudo o que já está no topic)

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ══════════════════════════════════════════════════════════════════════════════
# 4. PARSEAR O JSON E PREPARAR AS COLUNAS
# ══════════════════════════════════════════════════════════════════════════════
# O valor do Kafka vem em bytes na coluna "value" → cast para string → from_json

df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema_evento).alias("dados"))
    .select("dados.*")
    # converte o timestamp ISO (texto) para tipo timestamp real do Spark
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

# ══════════════════════════════════════════════════════════════════════════════
# 5. SLIDING WINDOW + AGREGAÇÃO POR ZONA   ← O CORAÇÃO DO JOB
# ══════════════════════════════════════════════════════════════════════════════
# - withWatermark("event_time", "2 minutes"):
#     diz ao Spark para tolerar eventos que chegam até 2 min atrasados.
#     Eventos mais atrasados que isso são ignorados (já fechámos a janela).
#
# - window("event_time", "10 minutes", "5 minutes"):
#     janela de 10 minutos que desliza a cada 5 minutos (sliding window).
#     Cada evento pode pertencer a duas janelas sobrepostas.
#
# - groupBy(janela, grid_id):
#     agrupa por janela temporal E por zona geográfica.

df_agregado = (
    df_parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("grid_id"),
    )
    .agg(
        count("*").alias("n_leituras"),
        avg("risk_score").alias("risk_medio"),
        spark_max("risk_score").alias("risk_maximo"),
        avg("temp_celsius").alias("temp_media"),
        avg("humidity_pct").alias("humidade_media"),
        avg("wind_kmh").alias("vento_medio"),
    )
)

# Reorganiza as colunas para uma saída legível
df_saida = df_agregado.select(
    col("window.start").alias("janela_inicio"),
    col("window.end").alias("janela_fim"),
    col("grid_id"),
    col("n_leituras"),
    col("risk_medio"),
    col("risk_maximo"),
    col("temp_media"),
    col("humidade_media"),
    col("vento_medio"),
)

# ══════════════════════════════════════════════════════════════════════════════
# 6. ESCREVER O RESULTADO — DOIS DESTINOS EM SIMULTÂNEO
# ══════════════════════════════════════════════════════════════════════════════
# O mesmo stream agregado é escrito para dois sítios ao mesmo tempo:
#   A) Console  → para demonstração ao vivo (vês as tabelas no terminal)
#   B) S3       → para arquivo no data lake (Parquet, análise posterior)

# ── Configuração S3 (endpoint configurável: LocalStack ou AWS real) ───────────
S3_BUCKET = "forest-risk-datalake"
S3_PATH = f"s3a://{S3_BUCKET}/agregados_streaming/"
S3_CHECKPOINT = f"s3a://{S3_BUCKET}/checkpoints/agregados/"

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")  # None = real AWS
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Configura o Spark para falar com o S3 (via protocolo s3a)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
if AWS_ENDPOINT_URL:
    # LocalStack ou outro endpoint local
    hadoop_conf.set("fs.s3a.endpoint", AWS_ENDPOINT_URL)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ── Destino A: Console (demonstração) ─────────────────────────────────────────
# outputMode="update" → mostra só as janelas que mudaram
query_console = (
    df_saida.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="30 seconds")
    .start()
)

# ── Destino B: S3 em Parquet (arquivo) ────────────────────────────────────────
# outputMode="append" → grava janelas já fechadas (obrigatório para ficheiros)
# O checkpoint guarda o progresso para não reprocessar/duplicar em reinícios.
query_s3 = (
    df_saida.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", S3_PATH)
    .option("checkpointLocation", S3_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming iniciado! A agregar risco por zona em janelas de 10 min.")
print(f"  Destino A: console (demonstracao)")
print(f"  Destino B: {S3_PATH} (arquivo Parquet)")
print("Pressiona Ctrl+C para parar.\n")

# Aguarda ambos os streams
spark.streams.awaitAnyTermination()
