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

COMO CORRER (dentro do Jupyter, que já tem Spark 3.5):
    Abre um terminal no Jupyter Lab e corre:
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
          /home/jovyan/work/spark_streaming_agregacao.py

    Ou copia o código para uma célula de notebook e corre.
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
# 6. ESCREVER O RESULTADO (writeStream)
# ══════════════════════════════════════════════════════════════════════════════
# outputMode="update" → mostra apenas as janelas que mudaram desde o último batch
# format="console"    → imprime no terminal (ideal para demonstração)
# trigger 30s         → processa um micro-batch a cada 30 segundos

query = (
    df_saida.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="30 seconds")
    .start()
)

print("Streaming iniciado! A agregar risco por zona em janelas de 10 min.")
print("Pressiona Ctrl+C para parar.\n")

query.awaitTermination()
