"""
Forest Risk Monitoring System
Producer de sensores IoT simulados — envia dados para Kafka em tempo real

Como correr (no teu PC, fora do Docker):
    pip install kafka-python
    python producer_sensores.py

Ou dentro do Jupyter (já tem kafka-python instalado):
    Copia o código para uma célula e corre
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Configuração ──────────────────────────────────────────────────────────────
#KAFKA_BOOTSTRAP = "localhost:29092"   # fora do Docker
KAFKA_BOOTSTRAP = "kafka:9092"      # dentro do Jupyter/Docker

#INTERVALO_SEGUNDOS = 2                # envia 1 evento a cada 2 segundos
INTERVALO_SEGUNDOS = 900   # 15 minutos = 15 × 60
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Grelha de zonas de Portugal (grid_id → coordenadas aproximadas) ───────────
ZONAS_PORTUGAL = {
    "PT-NORTE-01":    {"lat": 41.55, "lon": -8.42,  "regiao": "Braga"},
    "PT-NORTE-02":    {"lat": 41.69, "lon": -7.91,  "regiao": "Braganca"},
    "PT-CENTRO-01":   {"lat": 40.20, "lon": -8.41,  "regiao": "Coimbra"},
    "PT-CENTRO-02":   {"lat": 39.82, "lon": -7.49,  "regiao": "Castelo Branco"},
    "PT-CENTRO-03":   {"lat": 40.64, "lon": -8.65,  "regiao": "Aveiro"},
    "PT-LVT-01":      {"lat": 39.35, "lon": -8.13,  "regiao": "Santarem"},
    "PT-LVT-02":      {"lat": 38.71, "lon": -9.14,  "regiao": "Lisboa"},
    "PT-ALENTEJO-01": {"lat": 38.57, "lon": -7.91,  "regiao": "Evora"},
    "PT-ALENTEJO-02": {"lat": 37.80, "lon": -7.49,  "regiao": "Beja"},
    "PT-ALGARVE-01":  {"lat": 37.10, "lon": -8.67,  "regiao": "Faro"},
}

# ── Perfis de risco por época do ano ─────────────────────────────────────────
def get_perfil_climatico():
    """Simula condições climáticas realistas para Portugal."""
    mes = datetime.now().month
    # Verão: risco alto. Inverno: risco baixo.
    if mes in [6, 7, 8, 9]:       # verão
        return {"temp_base": 32, "temp_var": 8, "hum_base": 25, "hum_var": 15, "vento_base": 20, "vento_var": 25}
    elif mes in [3, 4, 5, 10]:    # primavera/outono
        return {"temp_base": 22, "temp_var": 6, "hum_base": 50, "hum_var": 20, "vento_base": 15, "vento_var": 20}
    else:                          # inverno
        return {"temp_base": 12, "temp_var": 5, "hum_base": 75, "hum_var": 15, "vento_base": 10, "vento_var": 15}

def calcular_risk_score(temp, humidade, vento, hotspots):
    """
    Índice de risco composto (0-100).
    Fórmula simplificada baseada no Canadian Fire Weather Index.
    """
    # Temperatura contribui positivamente para o risco
    score_temp = min(40, max(0, (temp - 15) * 1.5))
    # Humidade baixa aumenta o risco
    score_hum  = min(30, max(0, (100 - humidade) * 0.4))
    # Vento forte aumenta o risco
    score_vento = min(20, max(0, vento * 0.5))
    # Hotspots próximos aumentam muito o risco
    score_spots = min(10, hotspots * 3)

    total = score_temp + score_hum + score_vento + score_spots
    return round(min(100, total), 1)

def gerar_evento_sensor(grid_id, zona):
    """Gera uma leitura de sensor realista para uma zona."""
    perfil = get_perfil_climatico()

    # Valores base com variação aleatória
    temp     = round(perfil["temp_base"] + random.gauss(0, perfil["temp_var"] / 3), 1)
    humidade = round(max(5, min(100, perfil["hum_base"] + random.gauss(0, perfil["hum_var"] / 3))), 1)
    vento    = round(max(0, perfil["vento_base"] + random.gauss(0, perfil["vento_var"] / 3)), 1)
    hotspots = max(0, int(random.expovariate(0.8)))  # distribuição exponencial (maioria = 0)

    # 5% de probabilidade de evento extremo (simulação de situação crítica)
    if random.random() < 0.05:
        temp     = round(random.uniform(38, 45), 1)
        humidade = round(random.uniform(5, 18), 1)
        vento    = round(random.uniform(35, 60), 1)
        hotspots = random.randint(3, 8)
        log.warning(f"🔥 EVENTO EXTREMO gerado para {grid_id} ({zona['regiao']})")

    risk_score = calcular_risk_score(temp, humidade, vento, hotspots)

    return {
        "grid_id":       grid_id,
        "regiao":        zona["regiao"],
        "latitude":      round(zona["lat"] + random.uniform(-0.05, 0.05), 4),
        "longitude":     round(zona["lon"] + random.uniform(-0.05, 0.05), 4),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "temp_celsius":  temp,
        "humidity_pct":  humidade,
        "wind_kmh":      vento,
        "hotspot_count": hotspots,
        "risk_score":    risk_score,
        "source":        "iot_simulator_v1"
    }

def gerar_evento_meteorologia(grid_id, zona):
    """Gera dados meteorológicos (simula IPMA)."""
    perfil = get_perfil_climatico()
    return {
        "grid_id":       grid_id,
        "regiao":        zona["regiao"],
        "latitude":      zona["lat"],
        "longitude":     zona["lon"],
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "temp_max":      round(perfil["temp_base"] + random.uniform(0, 6), 1),
        "temp_min":      round(perfil["temp_base"] - random.uniform(4, 10), 1),
        "humidity_avg":  round(perfil["hum_base"] + random.gauss(0, 5), 1),
        "wind_max_kmh":  round(perfil["vento_base"] + random.uniform(0, 15), 1),
        "precipitation_mm": round(max(0, random.gauss(2, 5)), 1),
        "source":        "ipma_simulator_v1"
    }

# ── Producer principal ────────────────────────────────────────────────────────
def main():
    log.info(f"A ligar ao Kafka em {KAFKA_BOOTSTRAP}...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all"
    )

    log.info("✅ Ligado ao Kafka! A enviar dados...")
    log.info(f"   Topics: sensor-events, weather-data")
    log.info(f"   Zonas:  {len(ZONAS_PORTUGAL)} zonas de Portugal")
    log.info(f"   Ritmo:  1 ciclo a cada {INTERVALO_SEGUNDOS}s")
    log.info("   Ctrl+C para parar\n")

    ciclo = 0
    try:
        while True:
            ciclo += 1
            zonas_ciclo = random.sample(list(ZONAS_PORTUGAL.items()), k=3)  # 3 zonas aleatórias por ciclo

            for grid_id, zona in zonas_ciclo:
                # Evento de sensor IoT
                evento_sensor = gerar_evento_sensor(grid_id, zona)
                producer.send(
                    topic="sensor-events",
                    key=grid_id,
                    value=evento_sensor
                )

                # A cada 5 ciclos envia também dados meteorológicos
                if ciclo % 5 == 0:
                    evento_meteo = gerar_evento_meteorologia(grid_id, zona)
                    producer.send(
                        topic="weather-data",
                        key=grid_id,
                        value=evento_meteo
                    )

                # Log visual do risco
                risk = evento_sensor["risk_score"]
                emoji = "🟢" if risk < 30 else "🟡" if risk < 60 else "🟠" if risk < 80 else "🔴"
                log.info(
                    f"{emoji} [{grid_id:20s}] "
                    f"Temp={evento_sensor['temp_celsius']:5.1f}°C  "
                    f"Hum={evento_sensor['humidity_pct']:5.1f}%  "
                    f"Vento={evento_sensor['wind_kmh']:5.1f}km/h  "
                    f"Hotspots={evento_sensor['hotspot_count']}  "
                    f"Risk={risk:5.1f}"
                )

            producer.flush()
            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        log.info("\nProducer parado pelo utilizador.")
    finally:
        producer.close()
        log.info("Ligação Kafka fechada.")

if __name__ == "__main__":
    main()
