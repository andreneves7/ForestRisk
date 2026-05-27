"""
================================================================================
Forest Risk Monitoring System — Consumer Kafka → Cassandra
================================================================================

O QUE FAZ ESTE FICHEIRO:
    Este script é o "meio" do pipeline. O producer envia dados para o Kafka,
    e este consumer lê esses dados e guarda-os no Cassandra.

    Fluxo completo:
        producer_sensores.py  →  Kafka  →  consumer_kafka_cassandra.py  →  Cassandra

COMO CORRER (terminal do Jupyter):
    python work/consumer_kafka_cassandra.py

    Deixa correr em paralelo com o producer — enquanto um envia, este guarda.
    Para parar: Ctrl+C

O QUE APARECE NO ECRÃ:
    ✅ Ligado ao Kafka e Cassandra
    📡 A receber eventos dos sensores
    🔥 ALERTA quando risk_score >= 60 (HIGH ou CRITICAL)
    ✅ Contador a cada 10 eventos processados
================================================================================
"""

import json
import logging
import os
from datetime import datetime, timezone
from threading import Thread

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from influxdb_client import InfluxDBClient, Point  # ← NOVO
from kafka import KafkaConsumer

# ── Configuração ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE        = "forest_risk"

# ── Configuração InfluxDB ─────────────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUXDB_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN",  "forest-risk-influx-token-2024")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG",    "forest-risk-org")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "metrics")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s — %(message)s"
)
log = logging.getLogger(__name__)


# ── FUNÇÃO: Ligar ao InfluxDB ─────────────────────────────────────────────────
def connect_influx():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api()
    log.info("✅ InfluxDB ligado!")
    return client, write_api


# ── FUNÇÃO: Enviar latência para o InfluxDB ───────────────────────────────────
def send_latency(write_api, latency_ms: float, topic: str, grid_id: str):
    """
    Envia a latência do pipeline para o InfluxDB.
    latency_ms = tempo entre o timestamp do evento e o momento em que foi guardado no Cassandra.
    """
    point = (
        Point("pipeline_latency")
        .tag("topic", topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", latency_ms)
        .time(datetime.now(timezone.utc))
    )
    write_api.write(bucket=INFLUX_BUCKET, record=point)


# ── FUNÇÃO: Ligar ao Cassandra ────────────────────────────────────────────────
def connect_cassandra():
    log.info(f"A ligar ao Cassandra em {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=4
    )
    session = cluster.connect(KEYSPACE)
    log.info("✅ Cassandra ligado!")
    return cluster, session


# ── FUNÇÃO: Preparar os INSERTs ───────────────────────────────────────────────
def prepare_statements(session):

    insert_sensor = session.prepare("""
        INSERT INTO sensor_readings
            (grid_id, hour_bucket, event_time, source,
             temp_celsius, humidity_pct, wind_kmh,
             hotspot_count, risk_score, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    insert_alert = session.prepare("""
        INSERT INTO fire_alerts
            (alert_id, grid_id, alert_time, risk_score, risk_level,
             trigger_temp, trigger_humidity, trigger_wind, hotspot_count)
        VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    return insert_sensor, insert_alert


# ── FUNÇÃO: Classificar o nível de risco ─────────────────────────────────────
def risk_level(score: float) -> str:
    if score >= 80:
        return "CRITICAL"
    elif score >= 60:
        return "HIGH"
    elif score >= 30:
        return "MEDIUM"
    return "LOW"


# ── FUNÇÃO: Consumer do topic sensor-events ───────────────────────────────────
def consume_sensor_events(session, insert_sensor, insert_alert, write_api):
    consumer = KafkaConsumer(
        "sensor-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-sensor-writer",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1
    )

    log.info("📡 Consumer sensor-events iniciado")
    contagem = 0

    for msg in consumer:
        try:
            ev = msg.value

            # Timestamp do evento (criado pelo producer)
            ts = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00"))
            hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

            # Guardar no Cassandra e medir latência
            t_antes = datetime.now(timezone.utc)
            session.execute(insert_sensor, (
                ev["grid_id"],
                hour_bucket,
                ts,
                ev.get("source", "iot_simulator_v1"),
                float(ev["temp_celsius"]),
                float(ev["humidity_pct"]),
                float(ev["wind_kmh"]),
                int(ev.get("hotspot_count", 0)),
                float(ev["risk_score"]),
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))
            t_depois = datetime.now(timezone.utc)

            # Latência total: desde que o evento foi criado até ser guardado
            latency_ms = (t_depois - ts).total_seconds() * 1000
            send_latency(write_api, latency_ms, "sensor-events", ev["grid_id"])

            contagem += 1

            score = float(ev["risk_score"])
            nivel = risk_level(score)

            if nivel in ("HIGH", "CRITICAL"):
                session.execute(insert_alert, (
                    ev["grid_id"],
                    ts,
                    score,
                    nivel,
                    float(ev["temp_celsius"]),
                    float(ev["humidity_pct"]),
                    float(ev["wind_kmh"]),
                    int(ev.get("hotspot_count", 0)),
                ))
                log.warning(
                    f"🔥 ALERTA {nivel} — {ev['grid_id']} "
                    f"Risk={score} Temp={ev['temp_celsius']}°C "
                    f"Latência={latency_ms:.1f}ms"
                )

            if contagem % 10 == 0:
                log.info(f"✅ {contagem} eventos processados | última latência: {latency_ms:.1f}ms")

        except Exception as e:
            log.error(f"Erro ao processar mensagem: {e} | dados: {msg.value}")


# ── FUNÇÃO: Consumer do topic satellite-hotspots ──────────────────────────────
def consume_satellite_hotspots(session, insert_sensor, write_api):
    consumer = KafkaConsumer(
        "satellite-hotspots",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-hotspot-writer",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1
    )

    log.info("🛰️  Consumer satellite-hotspots iniciado")

    for msg in consumer:
        try:
            ev = msg.value
            ts_str = ev.get("timestamp", datetime.now(timezone.utc).isoformat())
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

            session.execute(insert_sensor, (
                ev.get("grid_id", "PT-UNKNOWN"),
                hour_bucket,
                ts,
                "nasa_firms",
                float(ev.get("brightness", 0.0)),
                0.0,
                0.0,
                1,
                float(ev.get("frp", 0.0)),
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))

            # Latência do hotspot
            latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
            send_latency(write_api, latency_ms, "satellite-hotspots", ev.get("grid_id", "PT-UNKNOWN"))

            log.info(f"🛰️  Hotspot registado — {ev.get('grid_id')} FRP={ev.get('frp')} Latência={latency_ms:.1f}ms")

        except Exception as e:
            log.error(f"Erro hotspot: {e} | dados: {msg.value}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    cluster, session = connect_cassandra()
    insert_sensor, insert_alert = prepare_statements(session)

    # Ligar ao InfluxDB para enviar métricas de latência
    influx_client, write_api = connect_influx()

    log.info("🚀 A iniciar consumers em threads paralelas...")
    log.info("   Ctrl+C para parar\n")

    t_hotspots = Thread(
        target=consume_satellite_hotspots,
        args=(session, insert_sensor, write_api),
        name="hotspot-consumer",
        daemon=True
    )
    t_hotspots.start()

    try:
        consume_sensor_events(session, insert_sensor, insert_alert, write_api)
    except KeyboardInterrupt:
        log.info("\nConsumer parado pelo utilizador.")
    finally:
        cluster.shutdown()
        influx_client.close()
        log.info("Ligações fechadas.")


if __name__ == "__main__":
    main()
