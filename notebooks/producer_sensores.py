"""
================================================================================
Forest Risk Monitoring System — Producer de Sensores IoT Simulados
================================================================================

PAPEL NA PIPELINE:
    Este script é a FONTE de dados simulados do sistema. Imita sensores IoT
    físicos distribuídos por 10 zonas de Portugal Continental, enviando
    leituras de temperatura, humidade, vento e risco para o Kafka.

    É intencionalmente simulado (não usa APIs reais) para garantir que a
    pipeline tem dados contínuos mesmo sem conectividade externa ou sem a
    NASA FIRMS key configurada.

QUANDO CORRE:
    Arranca automaticamente com `docker compose up` (container producer-sensores).
    Corre em loop infinito até o container parar.

TOPICS QUE ALIMENTA:
    - sensor-events  → leitura de sensor IoT a cada 2 segundos (3 zonas por ciclo)
    - weather-data   → dados meteorológicos simulados a cada 10 segundos

FLUXO SIMPLIFICADO:
    Ciclo a cada 2s:
        1. Sorteia 3 zonas de 10
        2. Gera leitura realista para cada zona (com perfil sazonal)
        3. Envia para Kafka (sensor-events)
        4. A cada 5 ciclos, envia também dados meteorológicos (weather-data)

COMO CORRER MANUALMENTE (fora do Docker):
    pip install kafka-python
    python producer_sensores.py

COMO CORRER DENTRO DO JUPYTER:
    Copia o código para uma célula e corre.
    Muda KAFKA_BOOTSTRAP para "kafka:9092" se estiveres dentro da rede Docker.
================================================================================
"""

import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

# Endereço do broker Kafka.
# "kafka:9092"      → dentro da rede Docker (container-to-container)
# "localhost:29092" → fora do Docker (PC local, porta mapeada no docker-compose)
KAFKA_BOOTSTRAP = "kafka:9092"

# Pausa entre ciclos de envio.
# 2s   → modo demo (gera ~90.000 eventos/dia, útil para testes)
# 30s  → modo produção (menos carga, mais realista)
INTERVALO_SEGUNDOS = 2

LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# GRELHA DE ZONAS DE PORTUGAL
# ══════════════════════════════════════════════════════════════════════════════
# 10 zonas geográficas que representam as principais regiões de Portugal
# Continental com maior histórico de incêndios florestais.
#
# O grid_id é a chave que liga todos os dados da pipeline:
#   - sensor-events usa grid_id como chave Kafka
#   - consumer grava no Cassandra com grid_id como partition key
#   - Spark agrega por grid_id nas janelas temporais
#   - S3 particionado por grid_id para queries eficientes

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


# ══════════════════════════════════════════════════════════════════════════════
# PERFIL CLIMÁTICO SAZONAL
# ══════════════════════════════════════════════════════════════════════════════

def get_perfil_climatico():
    """
    Devolve os parâmetros base para geração de dados consoante a época do ano.

    Simula a sazonalidade real de Portugal:
    - Verão (Jun-Set):      temperaturas altas, humidade baixa → risco máximo
    - Primavera/Outono:     valores intermédios → risco moderado
    - Inverno (Dez-Fev):    temperaturas baixas, humidade alta → risco mínimo

    Cada chave representa:
    - temp_base/hum_base/vento_base → valor médio para a época
    - temp_var/hum_var/vento_var    → variância (quanto os valores oscilam)
    """
    mes = datetime.now().month

    if mes in [6, 7, 8, 9]:       # verão — época crítica de incêndios
        return {
            "temp_base": 32, "temp_var": 8,
            "hum_base":  25, "hum_var":  15,
            "vento_base": 20, "vento_var": 25,
        }
    elif mes in [3, 4, 5, 10]:    # primavera/outono — risco moderado
        return {
            "temp_base": 22, "temp_var": 6,
            "hum_base":  50, "hum_var":  20,
            "vento_base": 15, "vento_var": 20,
        }
    else:                          # inverno — risco baixo
        return {
            "temp_base": 12, "temp_var": 5,
            "hum_base":  75, "hum_var":  15,
            "vento_base": 10, "vento_var": 15,
        }


# ══════════════════════════════════════════════════════════════════════════════
# CÁLCULO DO ÍNDICE DE RISCO
# ══════════════════════════════════════════════════════════════════════════════

def calcular_risk_score(temp, humidade, vento, hotspots):
    """
    Calcula um índice de risco de incêndio composto (0-100).
    Fórmula simplificada baseada no Canadian Fire Weather Index (FWI).

    Componentes e pesos máximos:
    ┌─────────────────┬────────────────────────────────────┬──────────┐
    │ Componente      │ Fórmula                            │ Máx pts  │
    ├─────────────────┼────────────────────────────────────┼──────────┤
    │ Temperatura     │ (temp - 15) × 1.5                  │ 40 pts   │
    │ Humidade baixa  │ (100 - humidade) × 0.4             │ 30 pts   │
    │ Vento           │ vento × 0.5                        │ 20 pts   │
    │ Hotspots        │ hotspots × 3                       │ 10 pts   │
    └─────────────────┴────────────────────────────────────┴──────────┘

    Exemplo crítico: temp=40, hum=10, vento=40, hotspots=3
      score_temp  = min(40, (40-15)*1.5) = 37.5
      score_hum   = min(30, (100-10)*0.4) = 30.0 (cap atingido)
      score_vento = min(20, 40*0.5) = 20.0 (cap atingido)
      score_spots = min(10, 3*3) = 9.0
      TOTAL = 96.5 → CRITICAL
    """
    # Temperatura: relevante acima de 15°C, contribui até 40 pontos
    score_temp = min(40, max(0, (temp - 15) * 1.5))

    # Humidade: quanto mais seco, maior o risco (invertida)
    score_hum = min(30, max(0, (100 - humidade) * 0.4))

    # Vento: propaga e intensifica incêndios
    score_vento = min(20, max(0, vento * 0.5))

    # Hotspots activos na zona: indicador directo de incêndio em curso
    score_spots = min(10, hotspots * 3)

    total = score_temp + score_hum + score_vento + score_spots
    return round(min(100, total), 1)


# ══════════════════════════════════════════════════════════════════════════════
# GERAÇÃO DE EVENTOS
# ══════════════════════════════════════════════════════════════════════════════

def gerar_evento_sensor(grid_id, zona):
    """
    Gera uma leitura de sensor IoT realista para uma zona geográfica.

    Processo:
    1. Obtém o perfil climático da época do ano actual
    2. Gera valores com distribuição gaussiana em torno dos valores base
       (simula a variabilidade natural de sensores físicos)
    3. Com 5% de probabilidade, força um evento extremo para testar alertas
    4. Calcula o risk_score com base nos 4 factores
    5. Devolve dicionário JSON pronto para enviar ao Kafka

    Nota sobre coordenadas:
    As coordenadas têm uma pequena variação aleatória (±0.05°) em torno
    do centroide da zona — simula sensores espalhados pela área, não todos
    no mesmo ponto geográfico exacto.
    """
    perfil = get_perfil_climatico()

    # Variação gaussiana: random.gauss(média=0, desvio=var/3)
    # Dividir por 3 garante que ~99.7% dos valores ficam dentro de ±var
    temp     = round(perfil["temp_base"] + random.gauss(0, perfil["temp_var"] / 3), 1)
    humidade = round(max(5, min(100, perfil["hum_base"] + random.gauss(0, perfil["hum_var"] / 3))), 1)
    vento    = round(max(0, perfil["vento_base"] + random.gauss(0, perfil["vento_var"] / 3)), 1)

    # Distribuição exponencial para hotspots: na maioria dos casos é 0,
    # raramente 1-2, muito raramente 3+. Parâmetro 0.8 → média ~1.25
    hotspots = max(0, int(random.expovariate(0.8)))

    # Evento extremo (5% de probabilidade): simula situação de crise
    # para testar o sistema de alertas CRITICAL no consumer
    if random.random() < 0.05:
        temp     = round(random.uniform(38, 45), 1)
        humidade = round(random.uniform(5, 18), 1)
        vento    = round(random.uniform(35, 60), 1)
        hotspots = random.randint(3, 8)
        log.warning(f"EVENTO EXTREMO gerado para {grid_id} ({zona['regiao']})")

    risk_score = calcular_risk_score(temp, humidade, vento, hotspots)

    return {
        "grid_id":       grid_id,                                           # chave da zona
        "regiao":        zona["regiao"],                                     # nome legível
        "latitude":      round(zona["lat"] + random.uniform(-0.05, 0.05), 4), # GPS com jitter
        "longitude":     round(zona["lon"] + random.uniform(-0.05, 0.05), 4),
        "timestamp":     datetime.now(timezone.utc).isoformat(),            # ISO 8601 UTC
        "temp_celsius":  temp,
        "humidity_pct":  humidade,
        "wind_kmh":      vento,
        "hotspot_count": hotspots,
        "risk_score":    risk_score,
        "source":        "iot_simulator_v1"                                  # identifica a origem
    }


def gerar_evento_meteorologia(grid_id, zona):
    """
    Gera dados meteorológicos simulados no formato da API IPMA.

    Estes dados vão para o topic weather-data, que o Spark Streaming
    usa como um dos 3 streams no join para calcular o risco composto.
    Enviado a cada 5 ciclos (~10 segundos) porque dados meteorológicos
    mudam mais devagar que leituras de sensores individuais.

    Campos gerados:
    - temp_max / temp_min   → intervalo de temperatura do dia
    - humidity_avg          → humidade média (gaussiana para maior realismo)
    - wind_max_kmh          → vento máximo (sempre maior que o base)
    - precipitation_mm      → precipitação (maioria = 0 em Portugal no verão)
    """
    perfil = get_perfil_climatico()
    return {
        "grid_id":          grid_id,
        "regiao":           zona["regiao"],
        "latitude":         zona["lat"],
        "longitude":        zona["lon"],
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "temp_max":         round(perfil["temp_base"] + random.uniform(0, 6), 1),
        "temp_min":         round(perfil["temp_base"] - random.uniform(4, 10), 1),
        "humidity_avg":     round(perfil["hum_base"] + random.gauss(0, 5), 1),
        "wind_max_kmh":     round(perfil["vento_base"] + random.uniform(0, 15), 1),
        "precipitation_mm": round(max(0, random.gauss(2, 5)), 1),   # nunca negativo
        "source":           "ipma_simulator_v1"
    }


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCER PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Ciclo principal do producer. Corre indefinidamente até Ctrl+C.

    Estrutura do ciclo:
    ┌─────────────────────────────────────────────────────────────┐
    │  A cada 2 segundos (INTERVALO_SEGUNDOS):                    │
    │    1. Sorteia 3 zonas de 10 aleatoriamente                  │
    │    2. Para cada zona:                                       │
    │       a. Gera evento sensor → envia para sensor-events     │
    │       b. Se ciclo múltiplo de 5:                           │
    │          Gera evento meteo → envia para weather-data       │
    │    3. flush() confirma envio ao broker                     │
    │    4. sleep(2s)                                             │
    └─────────────────────────────────────────────────────────────┘

    Decisões de design:
    - KafkaProducer com acks="all": aguarda confirmação de todas as réplicas
      antes de considerar a mensagem enviada. Garante zero perda de dados.
    - retries=5: re-tenta automaticamente em caso de falha temporária do broker
    - key=grid_id: mensagens da mesma zona vão para a mesma partição Kafka,
      preservando a ordem cronológica por zona
    - flush() após cada ciclo: garante que mensagens saem do buffer imediatamente
    - KeyboardInterrupt capturado: fecha a ligação Kafka de forma limpa
    """
    log.info(f"A ligar ao Kafka em {KAFKA_BOOTSTRAP}...")

    # KafkaProducer com serialização JSON automática
    # value_serializer: converte o dicionário Python em bytes JSON UTF-8
    # key_serializer: converte o grid_id string em bytes
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,    # re-tenta até 5 vezes em caso de falha temporária
        acks="all"    # confirmação de todas as réplicas (mais seguro que acks=1)
    )

    log.info("Ligado ao Kafka! A enviar dados...")
    log.info(f"   Topics: sensor-events, weather-data")
    log.info(f"   Zonas:  {len(ZONAS_PORTUGAL)} zonas de Portugal")
    log.info(f"   Ritmo:  1 ciclo a cada {INTERVALO_SEGUNDOS}s")
    log.info("   Ctrl+C para parar\n")

    ciclo = 0
    try:
        while True:
            ciclo += 1

            # Sorteia 3 zonas aleatórias de 10 por ciclo.
            # Simula que nem todos os sensores transmitem ao mesmo tempo
            # (cobertura de rede, bateria, etc.)
            zonas_ciclo = random.sample(list(ZONAS_PORTUGAL.items()), k=3)

            for grid_id, zona in zonas_ciclo:

                # ── Evento de sensor IoT (enviado sempre) ──────────────────
                evento_sensor = gerar_evento_sensor(grid_id, zona)
                producer.send(
                    topic="sensor-events",
                    key=grid_id,        # chave → garante ordem por zona na partição
                    value=evento_sensor
                )

                # ── Evento meteorológico (enviado a cada 5 ciclos = 10s) ───
                # Frequência menor porque dados meteo mudam mais devagar
                if ciclo % 5 == 0:
                    evento_meteo = gerar_evento_meteorologia(grid_id, zona)
                    producer.send(
                        topic="weather-data",
                        key=grid_id,
                        value=evento_meteo
                    )

                # ── Log colorido do nível de risco ─────────────────────────
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

            # Garante que todas as mensagens saíram do buffer interno
            # (sem flush, podem ficar em memória e perder-se num crash)
            producer.flush()
            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        log.info("\nProducer parado pelo utilizador.")
    finally:
        # Fecha a ligação de forma limpa, mesmo em caso de erro
        producer.close()
        log.info("Ligação Kafka fechada.")


if __name__ == "__main__":
    main()
