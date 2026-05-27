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
from kafka import KafkaConsumer

# ── Configuração ──────────────────────────────────────────────────────────────
# Lê as variáveis de ambiente definidas no docker-compose.yml
# Se não existirem, usa os valores padrão (que funcionam dentro do Docker)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE        = "forest_risk"  # keyspace criado pelo cassandra/init.cql

# Configuração dos logs — mostra data, nível (INFO/WARNING/ERROR) e mensagem
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s — %(message)s"
)
log = logging.getLogger(__name__)


# ── FUNÇÃO: Ligar ao Cassandra ────────────────────────────────────────────────
# Abre a ligação ao Cassandra e seleciona o keyspace forest_risk
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
# "Prepared statements" são queries CQL pré-compiladas pelo Cassandra.
# São mais rápidas do que enviar a query completa em texto a cada INSERT.
# Os "?" são os valores que vão ser preenchidos a cada chamada.
def prepare_statements(session):

    # INSERT na tabela sensor_readings (leituras de sensores IoT)
    insert_sensor = session.prepare("""
        INSERT INTO sensor_readings
            (grid_id, hour_bucket, event_time, source,
             temp_celsius, humidity_pct, wind_kmh,
             hotspot_count, risk_score, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    # INSERT na tabela fire_alerts (alertas de risco alto/crítico)
    # uuid() é gerado automaticamente pelo Cassandra para o alert_id
    insert_alert = session.prepare("""
        INSERT INTO fire_alerts
            (alert_id, grid_id, alert_time, risk_score, risk_level,
             trigger_temp, trigger_humidity, trigger_wind, hotspot_count)
        VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    return insert_sensor, insert_alert


# ── FUNÇÃO: Classificar o nível de risco ─────────────────────────────────────
# Converte o risk_score numérico (0-100) numa etiqueta textual.
# Estes limiares estão alinhados com o documento do projeto.
def risk_level(score: float) -> str:
    if score >= 80:
        return "CRITICAL"   # situação de emergência
    elif score >= 60:
        return "HIGH"       # risco elevado — gera alerta
    elif score >= 30:
        return "MEDIUM"     # risco moderado — só regista
    return "LOW"            # risco baixo — só regista


# ── FUNÇÃO: Consumer do topic sensor-events ───────────────────────────────────
# Fica à escuta do topic "sensor-events" no Kafka.
# Para cada mensagem recebida:
#   1. Guarda em sensor_readings (sempre)
#   2. Se risk_score >= 60, guarda também em fire_alerts
def consume_sensor_events(session, insert_sensor, insert_alert):
    consumer = KafkaConsumer(
        "sensor-events",                              # topic a consumir
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-sensor-writer",           # nome do grupo de consumers
        auto_offset_reset="latest",                   # começa nas mensagens novas (não lê histórico)
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),  # converte bytes → dict Python
        consumer_timeout_ms=-1                        # -1 = loop infinito (nunca para por timeout)
    )

    log.info("📡 Consumer sensor-events iniciado")
    contagem = 0  # contador para o log periódico

    for msg in consumer:  # bloqueia aqui e processa cada mensagem quando chega
        try:
            ev = msg.value  # dicionário com os dados do sensor (vem do producer)

            # Calcular o hour_bucket: agrupa eventos pela hora em que aconteceram
            # Ex: "2026-05-26T22:00:00" — todos os eventos dessa hora ficam juntos
            # Isto é a chave de partição secundária da tabela Cassandra
            ts = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00"))
            hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

            # Guardar a leitura do sensor no Cassandra
            session.execute(insert_sensor, (
                ev["grid_id"],                        # ex: "PT-NORTE-01"
                hour_bucket,                          # ex: "2026-05-26T22:00:00"
                ts,                                   # timestamp exato do evento
                ev.get("source", "iot_simulator_v1"), # origem dos dados
                float(ev["temp_celsius"]),            # temperatura em °C
                float(ev["humidity_pct"]),            # humidade em %
                float(ev["wind_kmh"]),                # velocidade do vento em km/h
                int(ev.get("hotspot_count", 0)),      # nº de hotspots satelite próximos
                float(ev["risk_score"]),              # índice de risco (0-100)
                float(ev.get("latitude", 0.0)),       # coordenadas GPS
                float(ev.get("longitude", 0.0)),
            ))

            contagem += 1

            # Verificar se o risco é alto ou crítico
            score = float(ev["risk_score"])
            nivel = risk_level(score)

            # Se HIGH ou CRITICAL → guardar também em fire_alerts
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
                    f"Risk={score} Temp={ev['temp_celsius']}°C"
                )

            # Log a cada 10 eventos para confirmar que está vivo
            if contagem % 10 == 0:
                log.info(f"✅ {contagem} eventos processados e guardados no Cassandra")

        except Exception as e:
            # Se uma mensagem falhar, regista o erro mas não para — continua a processar
            log.error(f"Erro ao processar mensagem: {e} | dados: {msg.value}")


# ── FUNÇÃO: Consumer do topic satellite-hotspots ──────────────────────────────
# Fica à escuta do topic "satellite-hotspots".
# Dados de satélite NASA FIRMS — deteções de calor em tempo real.
# Guarda em sensor_readings com source="nasa_firms".
def consume_satellite_hotspots(session, insert_sensor):
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
                "nasa_firms",                        # identifica a origem como satélite
                float(ev.get("brightness", 0.0)),    # temperatura de brilho do pixel (Kelvin → usado como proxy)
                0.0,                                  # satélite não mede humidade
                0.0,                                  # satélite não mede vento
                1,                                    # cada mensagem = 1 hotspot detetado
                float(ev.get("frp", 0.0)),            # Fire Radiative Power: intensidade do foco de calor
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))

            log.info(f"🛰️  Hotspot registado — {ev.get('grid_id')} FRP={ev.get('frp')}")

        except Exception as e:
            log.error(f"Erro hotspot: {e} | dados: {msg.value}")


# ── MAIN: ponto de entrada do script ─────────────────────────────────────────
# Quando corres "python consumer_kafka_cassandra.py", começa aqui.
def main():
    # 1. Ligar ao Cassandra
    cluster, session = connect_cassandra()

    # 2. Preparar os INSERTs (compilar as queries uma vez só)
    insert_sensor, insert_alert = prepare_statements(session)

    log.info("🚀 A iniciar consumers em threads paralelas...")
    log.info("   Ctrl+C para parar\n")

    # 3. Lançar o consumer de satellite-hotspots numa thread separada
    #    (daemon=True significa que esta thread para automaticamente quando o programa principal para)
    t_hotspots = Thread(
        target=consume_satellite_hotspots,
        args=(session, insert_sensor),
        name="hotspot-consumer",
        daemon=True
    )
    t_hotspots.start()

    # 4. Correr o consumer de sensor-events na thread principal
    #    (fica bloqueado aqui até Ctrl+C)
    try:
        consume_sensor_events(session, insert_sensor, insert_alert)
    except KeyboardInterrupt:
        log.info("\nConsumer parado pelo utilizador.")
    finally:
        cluster.shutdown()  # fechar a ligação ao Cassandra de forma limpa
        log.info("Ligação Cassandra fechada.")


if __name__ == "__main__":
    main()
