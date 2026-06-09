"""
================================================================================
Forest Risk Monitoring System — Consumer Kafka → Validação → Cassandra
================================================================================

PAPEL NA PIPELINE:
    Lê eventos do Kafka, valida a qualidade com Great Expectations,
    persiste os válidos no Cassandra e envia métricas para o InfluxDB.
    É o componente central da ingestão de dados.

QUANDO CORRE:
    Arranca automaticamente com `docker compose up` (container consumer).
    Corre dois threads em paralelo indefinidamente:
    - Thread principal: consume sensor-events (com validação GE + micro-batch)
    - Thread daemon:    consume satellite-hotspots (sem validação, directo)

FLUXO COMPLETO:
    Kafka (sensor-events)
           │
           ▼
      Acumula batch (3 eventos ou 30s, o que vier primeiro)
           │
           ▼
      run_ge_validation()  →  métricas InfluxDB (% qualidade)
           │
           ▼
      split_valid_invalid()
           │
           ├──► válidos   → persist_valid_event() → Cassandra (sensor_readings)
           │                                      → Cassandra (fire_alerts) se risk >= 60
           │                                      → InfluxDB (latência)
           │
           └──► inválidos → Kafka (data-quality-metrics)
                          → InfluxDB (rejected_events + rejected_event_detail)

TABELAS CASSANDRA POPULADAS:
    sensor_readings → toda leitura válida (sensores IoT + hotspots NASA)
    fire_alerts     → só quando risk_score >= 60 (HIGH ou CRITICAL)

COMO CORRER MANUALMENTE:
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
    build_rejected_record_nasa,
    run_ge_validation,
    split_valid_invalid,
    split_valid_invalid_nasa,
)

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
    """
    Estabelece ligação ao Cassandra e conecta directamente ao keyspace forest_risk.
    RoundRobinPolicy distribui queries uniformemente entre nós do cluster.
    protocol_version=4 é compatível com Cassandra 4.x.
    Devolve (cluster, session) — cluster é necessário para shutdown limpo.
    """
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
    """
    Cria cliente InfluxDB com escrita SÍNCRONA.
    SYNCHRONOUS = aguarda confirmação do servidor antes de devolver.
    Mais lento que assíncrono mas garante que as métricas chegam ao InfluxDB.
    """
    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    log.info("✅ InfluxDB ligado!")
    return client, write_api


def connect_kafka_producer():
    """
    Cria producer Kafka para dois propósitos:
    1. data-quality-metrics → publica eventos inválidos (quarentena)
    2. fire-alerts          → publica alertas quando condições críticas
                              (temp > 35°C E hum < 20% E vento > 30 km/h)
    O consumer também é producer — lê de sensor-events e escreve nos
    dois topics acima conforme necessário.
    """
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
    """
    Pré-compila os statements CQL uma vez ao arrancar.
    Vantagem de performance: o Cassandra compila e optimiza a query uma vez.
    Nas execuções seguintes só envia os parâmetros (?), não o texto completo.
    Para milhares de inserts/hora, a diferença é significativa.

    hour_bucket: campo derivado do timestamp que agrupa eventos por hora.
    Ex: evento das 19:37 → hour_bucket = "2026-06-09T19:00:00"
    Permite queries Cassandra eficientes do tipo "todas as leituras da hora X".
    """
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
    """
    Converte o risk_score numérico (0-100) em categoria textual.
    HIGH e CRITICAL disparam a criação de registo em fire_alerts.
    LOW e MEDIUM são apenas persistidos em sensor_readings.

    Escala:
        0-29   LOW      → verde  → monitorização normal
        30-59  MEDIUM   → amarelo → atenção
        60-79  HIGH     → laranja → alerta criado
        80-100 CRITICAL → vermelho → alerta urgente criado
    """
    if score >= 80:   return "CRITICAL"
    elif score >= 60: return "HIGH"
    elif score >= 30: return "MEDIUM"
    return "LOW"


def persist_valid_event(session, insert_sensor, insert_alert, write_api, fire_alert_producer, ev: dict):
    """
    Persiste um evento válido individual no Cassandra e regista a latência.

    Passos:
    1. Parseia o timestamp ISO 8601 e calcula o hour_bucket
    2. Insere em sensor_readings (sempre, para todos os eventos válidos)
    3. Calcula latência (agora - timestamp_evento) e envia para InfluxDB
    4. Se risco HIGH ou CRITICAL → insere em fire_alerts (Cassandra)
    5. Se condições críticas do documento (temp>35 E hum<20 E vento>30)
       → publica no topic fire-alerts (Kafka) para a Pessoa C consumir

    Regra de alerta do documento (secção 3.2):
        temperatura > 35°C E humidade < 20% E vento > 30 km/h
    Esta regra é independente do risk_score — um evento pode ter risco
    MEDIUM mas ainda assim cumprir os 3 critérios meteorológicos.

    Nota sobre .replace("Z", "+00:00"):
    Python < 3.11 não aceita o sufixo "Z" no fromisoformat().
    A substituição converte "2026-06-09T19:00:00Z" para
    "2026-06-09T19:00:00+00:00" que é aceite em todas as versões.
    """
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

    # ── Publicação no topic fire-alerts (requisito do documento secção 3.2) ──
    # Regra definida no documento: temp > 35°C E humidade < 20% E vento > 30 km/h
    # Esta verificação é independente do risk_score — avalia directamente
    # as condições meteorológicas brutas definidas pelo documento.
    # A Pessoa C (BI & DevOps) lê este topic e persiste em Cassandra/S3
    # e actualiza o mapa Power BI a cada 5 minutos.
    temp_ev  = float(ev.get("temp_celsius", 0))
    hum_ev   = float(ev.get("humidity_pct", 100))
    vento_ev = float(ev.get("wind_kmh", 0))

    if temp_ev > 35 and hum_ev < 20 and vento_ev > 30:
        alerta_kafka = {
            "grid_id":       ev["grid_id"],
            "timestamp":     ev["timestamp"],
            "risk_score":    score,
            "risk_level":    nivel,
            "temp_celsius":  temp_ev,
            "humidity_pct":  hum_ev,
            "wind_kmh":      vento_ev,
            "hotspot_count": int(ev.get("hotspot_count", 0)),
            "source":        ev.get("source", "unknown"),
            "trigger":       f"temp>{temp_ev}C hum<{hum_ev}% vento>{vento_ev}kmh"
        }
        try:
            fire_alert_producer.send(
                "fire-alerts",
                key=ev["grid_id"].encode("utf-8"),
                value=json.dumps(alerta_kafka).encode("utf-8")
            )
            log.warning(
                f"🚨 FIRE-ALERT publicado — {ev['grid_id']} "
                f"Temp={temp_ev}°C Hum={hum_ev}% Vento={vento_ev}km/h"
            )
        except Exception as e:
            log.error(f"Erro ao publicar fire-alert: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PROCESSAR MICRO-BATCH
# ══════════════════════════════════════════════════════════════════════════════

def process_batch(batch: list[dict], ge_context,
                  session, insert_sensor, insert_alert,
                  write_api, dq_producer, fire_alert_producer, topic: str):
    """
    Processa um micro-batch completo em 4 passos sequenciais:

    PASSO 1 — Great Expectations (batch completo)
        Corre validação GE sobre o DataFrame do batch inteiro.
        Resultado: percentagem de qualidade → InfluxDB → Grafana.
        NÃO decide quem é válido individualmente.

    PASSO 2 — Split linha a linha
        split_valid_invalid() aplica as regras evento a evento.
        Resultado: lista de válidos e lista de inválidos com motivos.

    PASSO 3 — Válidos → Cassandra
        persist_valid_event() para cada evento válido.
        Grava em sensor_readings + fire_alerts (se risco alto).

    PASSO 4 — Inválidos → quarentena
        Publica no topic data-quality-metrics (para análise posterior).
        Envia métricas resumidas e detalhadas para o InfluxDB.
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
            persist_valid_event(session, insert_sensor, insert_alert, write_api, fire_alert_producer, ev)
        except Exception as e:
            log.error(f"Erro Cassandra: {e} | grid={ev.get('grid_id')}")

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

def consume_sensor_events(session, insert_sensor, insert_alert, write_api, dq_producer, fire_alert_producer):
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
                            write_api, dq_producer, fire_alert_producer, "sensor-events"
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
                    write_api, dq_producer, fire_alert_producer, "sensor-events"
                )
                total += len(batch)
                log.info(f"✅ Total acumulado: {total} eventos processados")
                batch = []
            last_flush = datetime.now(timezone.utc)
            log.info("⏳ A aguardar novos eventos...")


def consume_satellite_hotspots(session, insert_sensor, write_api, dq_producer):
    """
    Consome hotspots NASA do topic satellite-hotspots.
    Agora com validação de qualidade específica para dados NASA:
    - FRP dentro de intervalo físico (0-5000 MW)
    - Brightness em Kelvin (200-500 K)
    - Coordenadas dentro de Portugal Continental
    - grid_id não pode ser PT-UNKNOWN

    Eventos inválidos vão para quarentena (data-quality-metrics)
    em vez de serem gravados silenciosamente no Cassandra.
    """
    consumer = KafkaConsumer(
        "satellite-hotspots",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-hotspot-writer",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1
    )

    log.info("Consumer satellite-hotspots iniciado (com validação NASA)")
    total_validos  = 0
    total_invalidos = 0

    for msg in consumer:
        try:
            ev = msg.value

            # ── Validação de qualidade específica NASA ────────────────────
            # Testa FRP, brightness, coordenadas e grid_id
            valid_evs, invalid_evs = split_valid_invalid_nasa([ev])

            # ── Eventos inválidos → quarentena ────────────────────────────
            for inv in invalid_evs:
                total_invalidos += 1
                rejected = build_rejected_record_nasa(inv)
                reasons  = rejected["rejection_reasons"]
                try:
                    dq_producer.send("data-quality-metrics", value=rejected)
                except Exception as e:
                    log.error(f"Erro ao publicar hotspot rejeitado: {e}")
                log.warning(
                    f"Hotspot NASA rejeitado — {inv.get('grid_id')} "
                    f"motivos: {', '.join(reasons)}"
                )
                # Envia também métrica para InfluxDB (visível no Grafana)
                from data_quality import send_rejected_metrics
                send_rejected_metrics(
                    write_api, INFLUX_BUCKET, INFLUX_ORG,
                    rejected["grid_id"], reasons
                )

            # ── Eventos válidos → Cassandra ───────────────────────────────
            for ev in valid_evs:
                total_validos += 1
                ts_str      = ev.get("timestamp", datetime.now(timezone.utc).isoformat())
                ts          = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

                # Aviso se FRP=0 (hotspot de baixíssima intensidade — válido mas suspeito)
                frp = float(ev.get("frp_mw", ev.get("frp", 0.0)))
                if frp == 0.0:
                    log.warning(
                        f"Hotspot com FRP=0 — {ev.get('grid_id')} "
                        f"(baixa intensidade ou possível falso positivo)"
                    )

                session.execute(insert_sensor, (
                    ev.get("grid_id"),
                    hour_bucket,
                    ts,
                    "nasa_firms",
                    float(ev.get("brightness", 0.0)),
                    0.0, 0.0, 1,              # humidity=0, wind=0, hotspot_count=1
                    frp,
                    float(ev.get("latitude", 0.0)),
                    float(ev.get("longitude", 0.0)),
                ))

                latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
                send_latency(
                    write_api, INFLUX_BUCKET, INFLUX_ORG,
                    latency_ms, "satellite-hotspots", ev.get("grid_id")
                )
                log.info(
                    f"Hotspot registado — {ev.get('grid_id')} "
                    f"FRP={frp}MW Latência={latency_ms:.1f}ms"
                )

        except Exception as e:
            log.error(f"Erro hotspot: {e} | dados: {msg.value}")

    log.info(f"Consumer satellite-hotspots: {total_validos} válidos | {total_invalidos} rejeitados")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    cluster, session            = connect_cassandra()
    insert_sensor, insert_alert = prepare_statements(session)
    influx_client, write_api    = connect_influx()

    # Um producer para quarentena (data-quality-metrics)
    # e outro para alertas (fire-alerts) — separados para clareza,
    # mas poderiam ser o mesmo producer
    dq_producer         = connect_kafka_producer()
    fire_alert_producer = connect_kafka_producer()

    log.info("🚀 A iniciar consumers em threads paralelas...")
    log.info(f"   Micro-batch GE: {BATCH_SIZE} eventos ou {BATCH_TIMEOUT}s, o que vier primeiro")
    log.info("   Ctrl+C para parar\n")

    t_hotspots = Thread(
        target=consume_satellite_hotspots,
        args=(session, insert_sensor, write_api, dq_producer),
        name="hotspot-consumer",
        daemon=True
    )
    t_hotspots.start()

    try:
        consume_sensor_events(
            session, insert_sensor, insert_alert,
            write_api, dq_producer, fire_alert_producer
        )
    except KeyboardInterrupt:
        log.info("\nConsumer parado pelo utilizador.")
    finally:
        dq_producer.flush()
        fire_alert_producer.flush()
        cluster.shutdown()
        influx_client.close()
        log.info("Ligações fechadas.")


if __name__ == "__main__":
    main()
