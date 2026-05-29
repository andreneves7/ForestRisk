"""
================================================================================
Forest Risk Monitoring System — Consumer Kafka → Validação → Cassandra
================================================================================

PIPELINE:
    Kafka  →  data_quality.py (micro-batch GE)
                ├── ✅ válidos   → Cassandra + InfluxDB (latência)
                └── ❌ inválidos → topic: data-quality-metrics (quarentena)
                                 → InfluxDB (métricas de rejeição)

COMO CORRER:
    python work/consumer_kafka_cassandra.py
    Ctrl+C para parar.
================================================================================
"""

import json
import logging
import os
from datetime import datetime, timezone
from threading import Thread

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer, KafkaProducer

from data_quality import (
    send_latency,
    send_quality_metrics,
    send_rejected_metrics,
)
from data_quality_validation import (
    build_ge_context,
    build_rejected_record,
    run_ge_validation,
    split_valid_invalid,
)
from s3_writer import write_parquet_to_s3

# ── Configuração ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE        = "forest_risk"

INFLUX_URL    = os.getenv("INFLUXDB_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN",  "forest-risk-influx-token-2024")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG",    "forest-risk")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "metrics")

BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "3"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s — %(message)s"
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# LIGAÇÕES
# ══════════════════════════════════════════════════════════════════════════════

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


def connect_influx():
    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    log.info("✅ InfluxDB ligado!")
    return client, write_api


def connect_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    log.info("✅ Kafka producer (quarentena) ligado!")
    return producer


# ══════════════════════════════════════════════════════════════════════════════
# CASSANDRA — statements e persistência
# ══════════════════════════════════════════════════════════════════════════════

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


def risk_level(score: float) -> str:
    if score >= 80:   return "CRITICAL"
    elif score >= 60: return "HIGH"
    elif score >= 30: return "MEDIUM"
    return "LOW"


def persist_valid_event(session, insert_sensor, insert_alert, write_api, ev: dict):
    """Guarda um evento válido no Cassandra e envia latência para InfluxDB."""
    ts          = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00"))
    hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

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

    latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
    send_latency(write_api, INFLUX_BUCKET, INFLUX_ORG,
                 latency_ms, "sensor-events", ev["grid_id"])

    score = float(ev["risk_score"])
    nivel = risk_level(score)
    if nivel in ("HIGH", "CRITICAL"):
        session.execute(insert_alert, (
            ev["grid_id"], ts, score, nivel,
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


# ══════════════════════════════════════════════════════════════════════════════
# PROCESSAR MICRO-BATCH
# ══════════════════════════════════════════════════════════════════════════════

def process_batch(batch: list[dict], ge_context,
                  session, insert_sensor, insert_alert,
                  write_api, dq_producer, topic: str):
    """
    1. Validação GE agregada  → métricas InfluxDB / Grafana
    2. Split linha a linha     → válidos vs inválidos
    3. Válidos   → Cassandra + latência InfluxDB
    4. Inválidos → topic data-quality-metrics + métricas InfluxDB
    """

    # 1. Great Expectations sobre o batch completo
    df = pd.DataFrame(batch)
    try:
        success_pct, n_success, n_failed = run_ge_validation(ge_context, df)
    except Exception as e:
        log.error(f"Erro GE: {e}")
        success_pct, n_success, n_failed = 0.0, 0, len(batch)

    send_quality_metrics(
        write_api, INFLUX_BUCKET, INFLUX_ORG,
        "sensor_readings", success_pct, n_success, n_failed, len(batch)
    )
    log.info(
        f"📊 GE [{topic}] batch={len(batch)} | "
        f"qualidade={success_pct:.1f}% | falhas={n_failed}"
    )

    # 2. Split linha a linha
    valid_evs, invalid_evs = split_valid_invalid(batch)

    # 3. Válidos → Cassandra
    for ev in valid_evs:
        try:
            persist_valid_event(session, insert_sensor, insert_alert, write_api, ev)
        except Exception as e:
            log.error(f"Erro Cassandra: {e} | grid={ev.get('grid_id')}")

    # 3b. Válidos → S3 Parquet (histórico)
    if valid_evs:
        write_parquet_to_s3(valid_evs, topic=topic)

    # 4. Inválidos → quarentena
    for ev in invalid_evs:
        rejected = build_rejected_record(ev)
        reasons  = rejected["rejection_reasons"]

        try:
            dq_producer.send("data-quality-metrics", value=rejected)
        except Exception as e:
            log.error(f"Erro ao publicar em data-quality-metrics: {e}")

        # write summary and detailed rejected-event point for Grafana
        from data_quality import send_rejected_event_detail, send_rejected_metrics

        send_rejected_metrics(
            write_api, INFLUX_BUCKET, INFLUX_ORG,
            rejected["grid_id"], reasons
        )
        try:
            send_rejected_event_detail(write_api, INFLUX_BUCKET, INFLUX_ORG, rejected)
        except Exception as e:
            log.error(f"Erro ao escrever detalhe do evento rejeitado: {e}")
        log.warning(
            f"❌ REJEITADO — {rejected['grid_id']} | "
            f"motivos: {', '.join(reasons)}"
        )

    log.info(
        f"   ✅ Válidos→Cassandra: {len(valid_evs)} | "
        f"❌ Rejeitados→quarentena: {len(invalid_evs)}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# CONSUMERS
# ══════════════════════════════════════════════════════════════════════════════

def consume_sensor_events(session, insert_sensor, insert_alert, write_api, dq_producer):
    consumer = KafkaConsumer(
        "sensor-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-sensor-writer",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=BATCH_TIMEOUT * 1000  # liberta o loop ao fim de 15 min sem mensagens
    )

    ge_context = build_ge_context()
    log.info("📡 Consumer sensor-events iniciado")

    batch: list[dict] = []
    total = 0
    last_flush = datetime.now(timezone.utc)

    while True:
        try:
            for msg in consumer:
                try:
                    batch.append(msg.value)

                    agora = datetime.now(timezone.utc)
                    elapsed = (agora - last_flush).total_seconds()

                    # Processa se atingiu o tamanho OU passou o timeout
                    if len(batch) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
                        motivo = "tamanho" if len(batch) >= BATCH_SIZE else "timeout 2min"
                        log.info(f"⏱️  A processar batch por {motivo} ({len(batch)} eventos)")
                        process_batch(
                            batch, ge_context,
                            session, insert_sensor, insert_alert,
                            write_api, dq_producer, "sensor-events"
                        )
                        total += len(batch)
                        log.info(f"✅ Total acumulado: {total} eventos processados")
                        batch = []
                        last_flush = datetime.now(timezone.utc)

                except Exception as e:
                    log.error(f"Erro ao processar mensagem: {e} | dados: {msg.value}")

        except StopIteration:
            # consumer_timeout_ms expirou — processa o que tiver no batch
            if batch:
                log.info(f"⏱️  Timeout 2min — a processar batch com {len(batch)} eventos")
                process_batch(
                    batch, ge_context,
                    session, insert_sensor, insert_alert,
                    write_api, dq_producer, "sensor-events"
                )
                total += len(batch)
                log.info(f"✅ Total acumulado: {total} eventos processados")
                batch = []
            last_flush = datetime.now(timezone.utc)
            log.info("⏳ A aguardar novos eventos...")


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
    hotspot_buffer: list[dict] = []
    HOTSPOT_BATCH = 10

    for msg in consumer:
        try:
            ev          = msg.value
            ts_str      = ev.get("timestamp", datetime.now(timezone.utc).isoformat())
            ts          = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

            session.execute(insert_sensor, (
                ev.get("grid_id", "PT-UNKNOWN"),
                hour_bucket, ts, "nasa_firms",
                float(ev.get("brightness", 0.0)),
                0.0, 0.0, 1,
                float(ev.get("frp", 0.0)),
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))

            latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
            send_latency(write_api, INFLUX_BUCKET, INFLUX_ORG,
                         latency_ms, "satellite-hotspots", ev.get("grid_id", "PT-UNKNOWN"))
            log.info(
                f"🛰️  Hotspot registado — {ev.get('grid_id')} "
                f"FRP={ev.get('frp')} Latência={latency_ms:.1f}ms"
            )

            hotspot_buffer.append(ev)
            if len(hotspot_buffer) >= HOTSPOT_BATCH:
                write_parquet_to_s3(hotspot_buffer, topic="satellite-hotspots")
                hotspot_buffer.clear()

        except Exception as e:
            log.error(f"Erro hotspot: {e} | dados: {msg.value}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    cluster, session            = connect_cassandra()
    insert_sensor, insert_alert = prepare_statements(session)
    influx_client, write_api    = connect_influx()
    dq_producer                 = connect_kafka_producer()

    log.info("🚀 A iniciar consumers em threads paralelas...")
    log.info(f"   Micro-batch GE: {BATCH_SIZE} eventos ou {BATCH_TIMEOUT}s, o que vier primeiro")
    log.info("   Ctrl+C para parar\n")

    t_hotspots = Thread(
        target=consume_satellite_hotspots,
        args=(session, insert_sensor, write_api),
        name="hotspot-consumer",
        daemon=True
    )
    t_hotspots.start()

    try:
        consume_sensor_events(
            session, insert_sensor, insert_alert,
            write_api, dq_producer
        )
    except KeyboardInterrupt:
        log.info("\nConsumer parado pelo utilizador.")
    finally:
        dq_producer.flush()
        cluster.shutdown()
        influx_client.close()
        log.info("Ligações fechadas.")


if __name__ == "__main__":
    main()
