# Forest Risk Monitoring System — Documentação Passo a Passo

**Projeto:** ISEP Pós-Graduação Big Data & Data Science 2024/2025  
**Foco:** O que acontece em cada linha/passo de cada ficheiro

---

## Índice

1. [producer_sensores.py](#1-producer_sensorespy)
2. [producer_apis_reais.py](#2-producer_apis_reaispy)
3. [consumer_kafka_cassandra.py](#3-consumer_kafka_cassandrapy)
4. [data_quality_validation.py](#4-data_quality_validationpy)
5. [data_quality.py](#5-data_qualitypy)
6. [spark_streaming_agregacao.py](#6-spark_streaming_agregacaopy)
7. [carga_historico_s3.py](#7-carga_historico_s3py)
8. [check_and_load.sh](#8-check_and_loadsh)

---

## 1. producer_sensores.py

**Papel:** Gera leituras de sensores IoT simulados e envia para o Kafka continuamente.

---

### Passo 1 — Imports e configuração (linhas 13–32)

```python
import json, time, random, logging
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP    = "kafka:9092"
INTERVALO_SEGUNDOS = 2
```

Carrega as bibliotecas necessárias e define duas variáveis globais:
- `KAFKA_BOOTSTRAP` — endereço do broker Kafka dentro da rede Docker
- `INTERVALO_SEGUNDOS` — pausa entre cada ciclo de envio (2 segundos = ~90.000 eventos/dia)

---

### Passo 2 — Definição das 10 zonas de Portugal (linhas 35–46)

```python
ZONAS_PORTUGAL = {
    "PT-NORTE-01": {"lat": 41.55, "lon": -8.42, "regiao": "Braga"},
    ...
}
```

Dicionário com as 10 zonas geográficas que o sistema monitoriza. Cada zona tem:
- `lat` / `lon` — coordenadas GPS do centroide da zona
- `regiao` — nome da região para logs legíveis

Estas zonas são usadas em toda a pipeline como chave `grid_id`.

---

### Passo 3 — Perfil climático sazonal: `get_perfil_climatico()` (linhas 49–58)

```python
def get_perfil_climatico():
    mes = datetime.now().month
    if mes in [6, 7, 8, 9]:    # verão
        return {"temp_base": 32, "hum_base": 25, ...}
    elif mes in [3, 4, 5, 10]: # primavera/outono
        return {"temp_base": 22, "hum_base": 50, ...}
    else:                       # inverno
        return {"temp_base": 12, "hum_base": 75, ...}
```

Verifica o mês actual e devolve os parâmetros base para geração de dados realistas. Em Junho (verão), os sensores vão gerar temperaturas altas e humidade baixa — condições reais de risco de incêndio em Portugal.

---

### Passo 4 — Cálculo do risco: `calcular_risk_score()` (linhas 60–75)

```python
def calcular_risk_score(temp, humidade, vento, hotspots):
    score_temp  = min(40, max(0, (temp - 15) * 1.5))      # máx 40 pontos
    score_hum   = min(30, max(0, (100 - humidade) * 0.4)) # máx 30 pontos
    score_vento = min(20, max(0, vento * 0.5))             # máx 20 pontos
    score_spots = min(10, hotspots * 3)                    # máx 10 pontos
    return round(min(100, score_temp + score_hum + score_vento + score_spots), 1)
```

Calcula um índice de risco 0–100 baseado em 4 factores, inspirado no Canadian Fire Weather Index:

| Factor | Fórmula | Máximo | Lógica |
|---|---|---|---|
| Temperatura | `(temp - 15) × 1.5` | 40 pts | Acima de 15°C começa a ser relevante |
| Humidade | `(100 - hum) × 0.4` | 30 pts | Quanto mais seco, maior o risco |
| Vento | `vento × 0.5` | 20 pts | Vento propaga incêndios |
| Hotspots | `hotspots × 3` | 10 pts | Incêndios activos na zona |

Exemplo: Temp=38°C, Hum=15%, Vento=30km/h, Hotspots=0 → Risk=`34.5 + 34 + 15 + 0 = 83.5` (CRITICAL)

---

### Passo 5 — Geração de evento de sensor: `gerar_evento_sensor()` (linhas 77–109)

```python
def gerar_evento_sensor(grid_id, zona):
    perfil = get_perfil_climatico()

    # Variação gaussiana em torno dos valores base
    temp     = perfil["temp_base"] + random.gauss(0, perfil["temp_var"] / 3)
    humidade = max(5, min(100, perfil["hum_base"] + random.gauss(0, ...)))
    vento    = max(0, perfil["vento_base"] + random.gauss(0, ...))
    hotspots = max(0, int(random.expovariate(0.8)))  # maioria = 0

    # 5% de probabilidade de evento extremo
    if random.random() < 0.05:
        temp=42, humidade=10, vento=50, hotspots=5  # crise simulada
```

Sub-passos dentro desta função:

1. **Obtém o perfil** da época do ano (verão/inverno/etc.)
2. **Gera valores** com distribuição gaussiana em torno dos valores base — simula a variabilidade natural dos sensores
3. **`hotspots` usa distribuição exponencial** — na maioria dos casos é 0, raramente é 3 ou mais (realista)
4. **5% de chance de evento extremo** — força valores críticos para testar o sistema de alertas
5. **Calcula o `risk_score`** com a função anterior
6. **Constrói o dicionário JSON** com todos os campos incluindo coordenadas GPS com pequena variação (simula sensores dispersos pela zona)

---

### Passo 6 — Geração de evento meteorológico: `gerar_evento_meteorologia()` (linhas 111–126)

```python
def gerar_evento_meteorologia(grid_id, zona):
    return {
        "temp_max": perfil["temp_base"] + random.uniform(0, 6),
        "wind_max_kmh": perfil["vento_base"] + random.uniform(0, 15),
        ...
        "source": "ipma_simulator_v1"
    }
```

Gera dados no formato da API IPMA (temperaturas máx/mín, humidade média, vento máximo, precipitação). Enviados para o topic `weather-data` a cada 5 ciclos (ou seja, a cada 10 segundos).

---

### Passo 7 — Ciclo principal: `main()` (linhas 129–189)

```python
def main():
    # 7a. Cria ligação ao Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all"
    )

    ciclo = 0
    while True:
        ciclo += 1

        # 7b. Selecciona 3 zonas aleatórias (de 10) por ciclo
        zonas_ciclo = random.sample(list(ZONAS_PORTUGAL.items()), k=3)

        for grid_id, zona in zonas_ciclo:
            # 7c. Gera e envia evento de sensor
            evento = gerar_evento_sensor(grid_id, zona)
            producer.send("sensor-events", key=grid_id, value=evento)

            # 7d. A cada 5 ciclos envia também dados meteorológicos
            if ciclo % 5 == 0:
                meteo = gerar_evento_meteorologia(grid_id, zona)
                producer.send("weather-data", key=grid_id, value=meteo)

        # 7e. Confirma envio e aguarda
        producer.flush()
        time.sleep(INTERVALO_SEGUNDOS)
```

**Sub-passos do ciclo:**

- **7a — KafkaProducer:** `acks="all"` garante que o broker confirmou a recepção antes de avançar. `retries=5` re-tenta em caso de falha temporária.
- **7b — 3 zonas aleatórias:** Em cada ciclo, apenas 3 das 10 zonas são amostradas — simula que nem todos os sensores enviam ao mesmo tempo.
- **7c — Envio com chave:** A chave `grid_id` garante que eventos da mesma zona vão sempre para a mesma partição Kafka, preservando a ordem.
- **7d — Meteorologia periódica:** Enviada a cada 5 ciclos (10 segundos) porque dados meteorológicos mudam mais devagar que leituras de sensores.
- **7e — flush + sleep:** `flush()` garante que os eventos saíram do buffer. `sleep()` impõe o ritmo de 2 segundos por ciclo.

---

## 2. producer_apis_reais.py

**Papel:** Consulta APIs externas reais (NASA FIRMS, IPMA, ICNF) e publica dados no Kafka.

---

### Passo 1 — Imports e configuração (linhas 36–73)

```python
import os, json, time, logging, requests
import pandas as pd
from kafka import KafkaProducer

NASA_FIRMS_KEY = os.getenv("NASA_FIRMS_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
NASA_SATELLITE = "VIIRS_SNPP_NRT"
NASA_DAYS = 1
INTERVALO_NASA_SEGUNDOS  = 3600   # 1 hora
INTERVALO_IPMA_SEGUNDOS  = 1800   # 30 minutos
INTERVALO_ICNF_SEGUNDOS  = 86400  # 1 dia
```

Três decisões importantes nesta configuração:
- **`NASA_FIRMS_KEY` vem do ambiente** (ficheiro `.env`) — nunca hardcoded por segurança
- **`NASA_DAYS = 1`** — pede só hotspots das últimas 24h (dados mais recentes e relevantes)
- **Intervalos realistas** — a NASA actualiza a cada ~3h, o IPMA a cada hora — consultar mais vezes seria desnecessário e poderia atingir limites de taxa

---

### Passo 2 — Estações IPMA (linhas 76–100)

```python
ESTACOES_IPMA = {
    1200535: {"regiao": "Lisboa", "grid_id": "PT-LVT-02", "lat": 38.77, "lon": -9.13},
    1210702: {"regiao": "Porto",  "grid_id": "PT-NORTE-01", ...},
    ...  # 15 estações no total
}
```

Mapeamento entre os IDs internos da API IPMA e as zonas do sistema. Este mapeamento é necessário porque a IPMA usa IDs numéricos mas o sistema usa `grid_id`.

---

### Passo 3 — Fetch NASA FIRMS: `fetch_nasa_firms()` (linhas 105–174)

```python
def fetch_nasa_firms():
    # 3a. Verifica se a key existe
    if not NASA_FIRMS_KEY:
        log.warning("NASA key não configurada — a saltar")
        return []

    # 3b. Constrói URL da API NASA
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv"
           f"/{NASA_FIRMS_KEY}/{NASA_SATELLITE}/{PORTUGAL_BBOX}/{NASA_DAYS}"

    # 3c. Faz o pedido HTTP
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # 3d. Parseia a resposta CSV com pandas
    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty:
        return []

    # 3e. Converte cada linha num evento JSON para o Kafka
    eventos = []
    for _, row in df.iterrows():
        grid_id = coords_para_grid(row["latitude"], row["longitude"])
        eventos.append({
            "grid_id":    grid_id,
            "latitude":   float(row["latitude"]),
            "longitude":  float(row["longitude"]),
            "timestamp":  f"{row['acq_date']}T{str(row['acq_time']).zfill(4)[:2]}:00:00Z",
            "brightness": float(row.get("bright_ti4", 0)),
            "frp":        float(row.get("frp", 0)),
            "confidence": str(row.get("confidence", "n")),
            "daynight":   str(row.get("daynight", "D")),
            "source":     "nasa_firms_viirs_snpp"
        })
    return eventos
```

**Sub-passos:**
- **3a — Guard clause:** Se a key NASA não está configurada, retorna lista vazia em vez de falhar
- **3b — URL da NASA:** A bounding box de Portugal (`PORTUGAL_BBOX = "-9.5,36.9,-6.2,42.2"`) limita os resultados a Portugal Continental
- **3c — Pedido HTTP com timeout:** `timeout=30` evita que o producer fique bloqueado indefinidamente se a NASA não responder
- **3d — A NASA devolve CSV:** Ao contrário de JSON, a API NASA usa CSV — o pandas trata da conversão
- **3e — Enriquece com grid_id:** As coordenadas GPS são mapeadas para o `grid_id` mais próximo usando distância euclidiana

---

### Passo 4 — Fetch IPMA: `fetch_ipma_observacoes()` (linhas 180–263)

```python
def fetch_ipma_observacoes():
    # 4a. Pedido à API IPMA (formato GeoJSON)
    url = "https://api.ipma.pt/open-data/observation/meteorology/stations/obs-surface.geojson"
    resp = requests.get(url, timeout=15)
    dados = resp.json()

    # 4b. Itera sobre as features GeoJSON (uma por estação)
    features = dados.get("features", [])
    for feature in features:
        props      = feature.get("properties", {})
        estacao_id = props.get("idEstacao")

        # 4c. Só processa as 10 estações mapeadas em ESTACOES_IPMA
        if estacao_id not in ESTACOES_IPMA:
            continue

        # 4d. Extrai coordenadas — GeoJSON usa [lon, lat] (invertido!)
        coords = feature.get("geometry", {}).get("coordinates", [0, 0])

        # 4e. Campos opcionais tratados com default 0.0 (não descarta a estação)
        # Descarta só se temperatura ou humidade forem None (campos críticos)
        temp     = props.get("temperatura")
        humidade = props.get("humidade")
        if temp is None or humidade is None:
            continue

        eventos.append({
            "grid_id":          info["grid_id"],
            "latitude":         coords[1],   # índice 1 = latitude (GeoJSON inverte)
            "longitude":        coords[0],   # índice 0 = longitude
            "temp_celsius":     float(temp),
            "humidity_pct":     float(humidade),
            "wind_kmh":         float(props.get("intensidadeVento") or 0.0),
            "precipitation_mm": float(props.get("precAcum") or 0.0),
            "source":           "ipma_real"
        })
```

**Sub-passos:**
- **4a — GeoJSON:** A API IPMA usa formato GeoJSON (FeatureCollection), não JSON simples — cada Feature tem `geometry` (coordenadas) e `properties` (valores meteorológicos)
- **4b — Uma Feature por estação:** Ao contrário de algumas APIs que agrupam por tempo, o IPMA devolve uma Feature por estação com os dados mais recentes
- **4c — Filtro por estações configuradas:** Das ~100 estações do país, só são processadas as 10 mapeadas em `ESTACOES_IPMA` (uma por zona do sistema)
- **4d — Coordenadas invertidas:** GeoJSON usa `[longitude, latitude]` — ao contrário do GPS padrão que usa `[latitude, longitude]`. Por isso `coords[1]` é lat e `coords[0]` é lon
- **4e — Dois níveis de tratamento de nulos:** Temperatura e humidade são obrigatórios (sem eles não há dados úteis) — a estação é descartada. Os outros campos (vento, precipitação) usam `0.0` como default

---

### Passo 5 — Fetch ICNF: `fetch_icnf_vegetacao()` (linhas 270–338)

```python
def fetch_icnf_vegetacao():
    # 5a. Pedido ao serviço WFS ArcGIS do ICNF
    url = (
        "https://sig.icnf.pt/arcgis/rest/services/ICNF/COS2018/MapServer/0/query"
        "?where=1%3D1"                          # WHERE 1=1 → todos os registos
        "&outFields=DESCRICAO,AREA_HA"           # só os campos necessários
        "&geometry=-9.5%2C36.9%2C-6.2%2C42.2"  # bbox Portugal Continental
        "&returnGeometry=false"                  # não precisa das geometrias
        "&resultRecordCount=100"                 # limita a 100 polígonos
        "&f=json"
    )
    resp = requests.get(url, timeout=30)
    dados = resp.json()

    # 5b. Itera sobre os polígonos de vegetação
    features = dados.get("features", [])
    for feature in features:
        attrs     = feature.get("attributes", {})
        descricao = attrs.get("DESCRICAO", "Desconhecido")
        area_ha   = attrs.get("AREA_HA", 0)

        # 5c. Classifica o risco de propagação por tipo de vegetação
        risco_veg = _risco_por_vegetacao(descricao)

        eventos.append({
            "tipo_vegetacao":  descricao,
            "area_ha":         float(area_ha),
            "risco_vegetacao": risco_veg,   # "LOW", "MEDIUM" ou "HIGH"
            "source":          "icnf_cos2018"
        })
```

**O que é o ICNF e porque é diferente da NASA e IPMA:**

O ICNF fornece a **Carta de Ocupação do Solo 2018 (COS2018)** — um mapa estático que classifica o território por tipo de vegetação. Não diz se há fogo (NASA), nem que tempo faz (IPMA) — diz o quão perigosa é a zona se houver fogo:

```
Eucaliptal / Pinheiro bravo  → HIGH   (óleos essenciais, arde muito rápido)
Sobreiro / Carvalho          → MEDIUM (folha larga, mais resistente)
Área urbana / Agrícola       → LOW    (não propaga)
```

**Sub-passos:**
- **5a — WFS ArcGIS:** Web Feature Service — protocolo OGC para dados geográficos vectoriais. `where=1%3D1` é `WHERE 1=1` codificado em URL (devolve todos os registos). `resultRecordCount=100` limita para evitar timeout por excesso de dados
- **5b — Polígonos de área:** Ao contrário dos hotspots (pontos) e estações meteo (pontos), os dados ICNF são polígonos — áreas de terreno. Por isso os eventos ICNF não têm `grid_id` próprio (key="ICNF" no Kafka)
- **5c — Classificação de risco:** `_risco_por_vegetacao()` usa correspondência de texto na descrição do COS2018 para atribuir nível de risco. É uma simplificação — para maior rigor científico usaria índices de inflamabilidade da literatura

**Nota sobre disponibilidade:** O serviço WFS do ICNF tem disponibilidade irregular — devolve 404 frequentemente por manutenção. O producer trata o erro silenciosamente com `except Exception` e tenta novamente no dia seguinte. Não é um erro do código.

**Limitação actual:** Os dados ICNF chegam ao topic `sensor-events` mas o Spark não os usa no `risco_composto`. São persistidos no Cassandra mas não influenciam o índice em tempo real. Uma integração futura usaria o tipo de vegetação como multiplicador do risco por zona.

---

### Passo 6 — Ciclo principal: `main()` (linhas 396–472)

```python
def main():
    # 6a. Liga ao Kafka
    producer = KafkaProducer(acks="all", retries=5, ...)

    # datetime.min → força fetch imediato na 1ª iteração
    ultimo_nasa = datetime.min
    ultimo_ipma = datetime.min
    ultimo_icnf = datetime.min

    while True:
        agora = datetime.now()

        # 6b. NASA FIRMS — hotspots (a cada hora)
        if (agora - ultimo_nasa).total_seconds() >= INTERVALO_NASA_SEGUNDOS:
            eventos = fetch_nasa_firms()
            for ev in eventos:
                producer.send("satellite-hotspots", key=ev["grid_id"], value=ev)
            if eventos: producer.flush()
            ultimo_nasa = agora

        # 6c. IPMA — meteorologia (a cada 30 min)
        if (agora - ultimo_ipma).total_seconds() >= INTERVALO_IPMA_SEGUNDOS:
            eventos = fetch_ipma_observacoes()
            for ev in eventos:
                producer.send("weather-data",  key=ev["grid_id"], value=ev)
                producer.send("sensor-events", key=ev["grid_id"], value=ev)
            if eventos: producer.flush()
            ultimo_ipma = agora

        # 6d. ICNF — vegetação (1 vez por dia)
        if (agora - ultimo_icnf).total_seconds() >= INTERVALO_ICNF_SEGUNDOS:
            eventos = fetch_icnf_vegetacao()
            for ev in eventos:
                producer.send("sensor-events", key="ICNF", value=ev)
            if eventos: producer.flush()
            ultimo_icnf = agora

        # 6e. Pausa de 60s entre verificações
        time.sleep(60)
```

**Sub-passos:**
- **6a — `datetime.min`:** Força o primeiro fetch imediatamente ao arrancar. Sem isto, o primeiro NASA só aconteceria após 1 hora de espera
- **6b — NASA só para `satellite-hotspots`:** Topic próprio — o consumer e o Spark tratam-nos de forma diferente dos sensores IoT
- **6c — IPMA para dois topics:** `weather-data` para o Spark (join dos 3 streams); `sensor-events` para o consumer/Cassandra (dados reais misturados com os simulados)
- **6d — ICNF com `key="ICNF"`:** Os dados de vegetação não têm `grid_id` próprio (são polígonos, não pontos) — a chave "ICNF" identifica a origem nos logs
- **6e — Sleep de 60s:** O loop verifica a cada minuto se chegou a hora de actualizar cada API — granularidade que equilibra responsividade e CPU

---

## 3. consumer_kafka_cassandra.py

**Papel:** Lê do Kafka, valida dados, persiste no Cassandra e envia métricas para o InfluxDB.

---

### Passo 1 — Imports e configuração

```python
from data_quality import send_latency, send_quality_metrics, send_rejected_metrics
from data_quality_validation import (
    build_ge_context, build_rejected_record, build_rejected_record_nasa,
    run_ge_validation, split_valid_invalid, split_valid_invalid_nasa,
)

BATCH_SIZE    = 3    # eventos por batch antes de processar
BATCH_TIMEOUT = 30   # segundos máximos sem processar
```

Importa os dois módulos auxiliares de qualidade. Agora inclui as funções NASA:
- `split_valid_invalid_nasa` — valida hotspots com regras físicas específicas
- `build_rejected_record_nasa` — formata hotspots inválidos para quarentena

O `BATCH_SIZE=3` é pequeno (adequado para demo) — em produção usaria 100-1000.

---

### Passo 2 — Ligações: `connect_cassandra()`, `connect_influx()`, `connect_kafka_producer()` (linhas 68–94)

```python
def connect_cassandra():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                      load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
    session = cluster.connect(KEYSPACE)   # liga directamente ao keyspace forest_risk
    return cluster, session
```

Três ligações são estabelecidas ao arrancar:
- **Cassandra** — para persistir os dados (tabelas `sensor_readings` e `fire_alerts`)
- **InfluxDB** — para enviar métricas de qualidade e latência
- **Kafka Producer** — para publicar eventos inválidos na quarentena (`data-quality-metrics`)

O `RoundRobinPolicy` distribui as queries pelo cluster Cassandra de forma equilibrada.

---

### Passo 3 — Prepared Statements: `prepare_statements()` (linhas 101–115)

```python
def prepare_statements(session):
    insert_sensor = session.prepare("""
        INSERT INTO sensor_readings
            (grid_id, hour_bucket, event_time, source,
             temp_celsius, humidity_pct, wind_kmh,
             hotspot_count, risk_score, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_alert = session.prepare("""
        INSERT INTO fire_alerts (alert_id, grid_id, alert_time, ...)
        VALUES (uuid(), ?, ?, ?, ...)
    """)
    return insert_sensor, insert_alert
```

Prepara os statements CQL uma vez ao arrancar. A razão é performance: o Cassandra compila e optimiza a query uma vez — nas execuções seguintes só envia os parâmetros `(?)`, não o texto completo da query. Para milhares de inserts por hora, a diferença é significativa.

O campo `hour_bucket` (ex: `"2026-06-09T19:00:00"`) é uma técnica de Cassandra — agrupa eventos por hora para que as queries de "todas as leituras de uma hora" sejam eficientes.

---

### Passo 4 — Classificação de risco: `risk_level()` (linhas 118–122)

```python
def risk_level(score: float) -> str:
    if score >= 80:   return "CRITICAL"
    elif score >= 60: return "HIGH"
    elif score >= 30: return "MEDIUM"
    return "LOW"
```

Converte o `risk_score` numérico (0–100) em categoria textual. As categorias HIGH e CRITICAL disparam a criação de registos na tabela `fire_alerts`.

---

### Passo 5 — Persistir evento válido: `persist_valid_event()` (linhas 125–162)

```python
def persist_valid_event(session, insert_sensor, insert_alert, write_api, fire_alert_producer, ev):
    # 5a. Parseia o timestamp e calcula o hour_bucket
    ts          = datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00"))
    hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

    # 5b. Insere em sensor_readings (sempre)
    session.execute(insert_sensor, (ev["grid_id"], hour_bucket, ts, ...))

    # 5c. Calcula latência e envia para InfluxDB
    latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
    send_latency(write_api, ..., latency_ms, "sensor-events", ev["grid_id"])

    # 5d. Se risco alto/crítico, cria alerta no Cassandra
    nivel = risk_level(float(ev["risk_score"]))
    if nivel in ("HIGH", "CRITICAL"):
        session.execute(insert_alert, (ev["grid_id"], ts, score, nivel, ...))
        log.warning(f"🔥 ALERTA {nivel} — ...")

    # 5e. Publica no topic fire-alerts se condições meteorológicas críticas
    temp_ev  = float(ev.get("temp_celsius", 0))
    hum_ev   = float(ev.get("humidity_pct", 100))
    vento_ev = float(ev.get("wind_kmh", 0))

    if temp_ev > 35 and hum_ev < 20 and vento_ev > 30:
        alerta = {
            "grid_id":    ev["grid_id"],
            "risk_level": nivel,
            "temp_celsius": temp_ev,
            "humidity_pct": hum_ev,
            "wind_kmh":   vento_ev,
            "trigger":    f"temp>{temp_ev}C hum<{hum_ev}% vento>{vento_ev}kmh"
        }
        fire_alert_producer.send("fire-alerts", key=ev["grid_id"], value=alerta)
```

**Sub-passos:**
- **5a — `replace("Z", "+00:00")`:** Conversão necessária porque o Python < 3.11 não aceita o sufixo `Z` directamente no `fromisoformat`
- **5b — Sempre grava em `sensor_readings`:** Todas as leituras válidas ficam persistidas, independentemente do nível de risco
- **5c — Latência:** Mede o tempo entre o `timestamp` do evento (quando foi criado pelo producer) e o momento actual (quando está a ser gravado). Latências altas indicam problemas de throughput
- **5d — Alertas por risk_score:** Só cria registo em `fire_alerts` (Cassandra) quando o risco é HIGH (≥60) ou CRITICAL (≥80)
- **5e — Alertas por condições brutas:** Publicação no topic `fire-alerts` (Kafka) independente do `risk_score` — verifica directamente os valores meteorológicos. A regra `temp>35 E hum<20 E vento>30` está definida no documento do projecto (secção 3.2). O campo `trigger` regista os valores exactos que dispararam o alerta.

---

### Passo 6 — Processar batch: `process_batch()` (linhas 169–235)

```python
def process_batch(batch, ge_context, session, insert_sensor, insert_alert, write_api, dq_producer, topic):

    # 6a. Validação Great Expectations sobre o batch completo
    df = pd.DataFrame(batch)
    success_pct, n_success, n_failed = run_ge_validation(ge_context, df)
    send_quality_metrics(write_api, ..., success_pct, n_success, n_failed, len(batch))

    # 6b. Split linha a linha em válidos/inválidos
    valid_evs, invalid_evs = split_valid_invalid(batch)

    # 6c. Válidos → Cassandra
    for ev in valid_evs:
        persist_valid_event(session, insert_sensor, insert_alert, write_api, ev)

    # 6d. Inválidos → quarentena + InfluxDB
    for ev in invalid_evs:
        rejected = build_rejected_record(ev)
        dq_producer.send("data-quality-metrics", value=rejected)
        send_rejected_metrics(write_api, ..., rejected["grid_id"], reasons)
```

**Sub-passos:**
- **6a — GE no batch completo:** Primeiro corre Great Expectations em todo o batch para obter estatísticas globais (% de qualidade). Este número vai para o Grafana.
- **6b — Split linha a linha:** Depois faz uma segunda validação evento-a-evento para separar os válidos dos inválidos. Os dois métodos são complementares: GE dá estatísticas, `split_valid_invalid` dá a decisão por evento.
- **6c — Válidos para Cassandra:** Cada evento válido é persistido individualmente
- **6d — Inválidos para quarentena:** Os eventos inválidos não são descartados — ficam no topic `data-quality-metrics` para análise posterior, e os detalhes vão para o InfluxDB

---

### Passo 7 — Consumer de sensores: `consume_sensor_events()` (linhas 242–299)

```python
def consume_sensor_events(...):
    consumer = KafkaConsumer("sensor-events",
                             group_id="cassandra-sensor-writer",
                             auto_offset_reset="latest",
                             consumer_timeout_ms=BATCH_TIMEOUT * 1000)
    batch = []
    last_flush = datetime.now(timezone.utc)

    while True:
        for msg in consumer:
            batch.append(msg.value)

            elapsed = (now - last_flush).total_seconds()
            if len(batch) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
                process_batch(batch, ...)
                batch = []
                last_flush = now

        # StopIteration = consumer_timeout_ms expirou
        if batch:
            process_batch(batch, ...)
```

**Sub-passos:**
- **`group_id`:** Identifica este consumer group — se houver dois consumers com o mesmo group_id, o Kafka distribui as partições entre eles (escalabilidade horizontal)
- **`auto_offset_reset="latest"`:** Ao arrancar, começa a ler do ponto actual, não do início do topic
- **`consumer_timeout_ms`:** Quando não chegam mensagens por 30 segundos, o loop `for msg in consumer` levanta `StopIteration`, permitindo processar o batch parcial
- **Dois gatilhos de flush:** tamanho (`BATCH_SIZE`) ou tempo (`BATCH_TIMEOUT`) — o que vier primeiro

---

### Passo 8 — Consumer de hotspots: `consume_satellite_hotspots()` (com validação NASA)

```python
def consume_satellite_hotspots(session, insert_sensor, write_api, dq_producer):
    consumer = KafkaConsumer("satellite-hotspots",
                             group_id="cassandra-hotspot-writer",
                             consumer_timeout_ms=-1)  # nunca expira

    for msg in consumer:
        ev = msg.value

        # 8a. Validação específica NASA antes de gravar
        valid_evs, invalid_evs = split_valid_invalid_nasa([ev])

        # 8b. Inválidos → quarentena (data-quality-metrics)
        for inv in invalid_evs:
            rejected = build_rejected_record_nasa(inv)
            dq_producer.send("data-quality-metrics", value=rejected)
            send_rejected_metrics(write_api, ..., rejected["grid_id"], reasons)

        # 8c. Válidos → Cassandra
        for ev in valid_evs:
            frp = float(ev.get("frp_mw", 0.0))
            if frp == 0.0:
                log.warning("Hotspot com FRP=0 (baixa intensidade ou falso positivo)")
            session.execute(insert_sensor, (
                ev.get("grid_id"),
                hour_bucket, ts, "nasa_firms",
                float(ev.get("brightness", 0.0)),
                0.0, 0.0, 1,       # humidity=0, wind=0, hotspot_count=1
                frp,
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))
```

Diferenças em relação ao consumer de sensores:

- **Com validação NASA específica (correcção de gap):** Os dados NASA não são validados com GE (campos diferentes dos IoT) mas têm regras físicas próprias:

| Campo | Intervalo | Porquê |
|---|---|---|
| `frp_mw` | 0 – 5000 MW | Maior incêndio histórico ~3000 MW — acima é erro de sensor |
| `brightness` | 200 – 500 K | Temperatura de brilho VIIRS — fora disto não é fogo real |
| `latitude` | 36.9 – 42.2 | Dentro de Portugal Continental |
| `longitude` | -9.5 – -6.2 | Dentro de Portugal Continental |
| `grid_id` | ≠ PT-UNKNOWN | PT-UNKNOWN indica falha de mapeamento de coordenadas |

- **Hotspots inválidos vão para quarentena:** Antes eram gravados silenciosamente no Cassandra com `PT-UNKNOWN` ou `frp=0`. Agora vão para `data-quality-metrics` e as métricas aparecem no Grafana
- **Aviso para FRP=0:** Tecnicamente válido (hotspot de baixíssima intensidade) mas suspeito — registado como warning nos logs
- **Sem batching:** Processa cada hotspot individualmente — chegam em pequenas quantidades (dezenas por hora, não milhares)
- **`consumer_timeout_ms=-1`:** Nunca expira — espera indefinidamente por hotspots (que chegam só de hora a hora)
- **Usa a mesma tabela `sensor_readings`:** Adapta os campos NASA (`brightness` → `temp_celsius`, `frp_mw` → `risk_score`) para o schema existente

---

### Passo 9 — Main: arranque paralelo

```python
def main():
    # 9a. Estabelece todas as ligações
    cluster, session            = connect_cassandra()
    insert_sensor, insert_alert = prepare_statements(session)
    influx_client, write_api    = connect_influx()
    dq_producer                 = connect_kafka_producer()  # quarentena
    fire_alert_producer         = connect_kafka_producer()  # fire-alerts

    # 9b. Inicia consumer de hotspots numa thread separada (daemon)
    t_hotspots = Thread(
        target=consume_satellite_hotspots,
        args=(session, insert_sensor, write_api, dq_producer),  # passa dq_producer
        daemon=True
    )
    t_hotspots.start()

    # 9c. Consumer de sensores corre na thread principal
    consume_sensor_events(session, insert_sensor, insert_alert,
                          write_api, dq_producer, fire_alert_producer)
```

**Sub-passos:**
- **9a — Dois producers Kafka:** `dq_producer` para quarentena (`data-quality-metrics`), `fire_alert_producer` para alertas (`fire-alerts`). Poderiam ser o mesmo mas separados por clareza
- **9b — `dq_producer` passado ao hotspot consumer:** Necessário para publicar hotspots inválidos na quarentena (correcção do gap de validação)
- **9c — `fire_alert_producer` no consumer de sensores:** Publica no topic `fire-alerts` quando `temp > 35°C E hum < 20% E vento > 30 km/h` (requisito do documento)
- **Thread daemon para hotspots:** `daemon=True` — quando a thread principal termina (Ctrl+C), esta termina automaticamente — evita processos zombie

---

## 4. data_quality_validation.py

**Papel:** Módulo de validação de qualidade. Importado pelo consumer.

---

### Passo 1 — Regras de validação (linhas 13–17)

```python
RULES = {
    "temp_celsius": (-10,  60),
    "humidity_pct": (  0, 100),
    "wind_kmh":     (  0, 150),
    "risk_score":   (  0, 100),
}
NOT_NULL = ["grid_id", "risk_score"]
```

Define dois tipos de regras:
- **`RULES`** — intervalos válidos para campos numéricos
- **`NOT_NULL`** — campos que não podem ser nulos (seria um evento incompleto)

---

### Passo 2 — Validação Great Expectations: `run_ge_validation()` (linhas 27–65)

```python
def run_ge_validation(context, df):
    # 2a. Cria contexto efémero (sem persistência)
    context = gx.get_context(mode="ephemeral")

    # 2b. Regista o DataFrame como datasource
    datasource = context.sources.add_pandas("sensor_data")
    asset = datasource.add_dataframe_asset("readings")
    batch = asset.build_batch_request(dataframe=df)

    # 2c. Define as expectativas (regras de qualidade)
    validator.expect_column_values_to_be_between("temp_celsius", min_value=-10, max_value=60)
    validator.expect_column_values_to_not_be_null("grid_id")
    ...

    # 2d. Corre o checkpoint e extrai estatísticas
    results = context.run_checkpoint(checkpoint_name="quality_check")
    stats = results.get_statistics()
    return success_pct, n_success, n_failed
```

**Sub-passos:**
- **2a — `mode="ephemeral"`:** Não guarda nada em disco — execução em memória, mais rápida
- **2b — DataFrame como datasource:** GE trabalha com o conceito de "batch" — aqui é o micro-batch do consumer
- **2c — Expectativas:** São as mesmas regras de `RULES` mas no vocabulário do Great Expectations
- **2d — Estatísticas agregadas:** Devolve percentagem de sucesso e contagens, não os detalhes de cada linha

---

### Passo 3 — Validação linha a linha: `split_valid_invalid()` (linhas 89–116)

```python
def split_valid_invalid(batch):
    valid, invalid = [], []

    for ev in batch:
        reasons = []

        # 3a. Verifica cada regra numérica
        for col, (min_val, max_val) in RULES.items():
            val = ev.get(col)
            if val is None:
                reasons.append(f"{col}_null")
            elif not (min_val <= float(val) <= max_val):
                reasons.append(f"{col}_out_of_range({val})")

        # 3b. Verifica campos obrigatórios
        for col in NOT_NULL:
            if col not in RULES and ev.get(col) is None:
                reasons.append(f"{col}_null")

        # 3c. Classifica o evento
        if reasons:
            ev["_rejection_reasons"] = reasons
            invalid.append(ev)
        else:
            valid.append(ev)

    return valid, invalid
```

**Sub-passos:**
- **3a — Trata `None` e `out_of_range` separadamente:** Um valor nulo e um valor fora de intervalo são problemas diferentes (sensor desligado vs. sensor avariado)
- **3b — `NOT_NULL` sem regra numérica:** `grid_id` é string — não tem intervalo, só verifica se existe
- **3c — Motivos detalhados:** `reasons` é uma lista com o nome do campo e o problema — ex: `["temp_celsius_out_of_range(999.9)"]`. Crucial para debugging no Grafana.

---

### Passo 4 — Construir registo de rejeição: `build_rejected_record()` (linhas 119–130)

```python
def build_rejected_record(ev):
    return {
        "grid_id":           ev.get("grid_id", "UNKNOWN"),
        "rejected_at":       datetime.now(timezone.utc).isoformat(),
        "original_timestamp": ev.get("timestamp"),
        "temp_celsius":      ev.get("temp_celsius"),
        "rejection_reasons": ev.get("_rejection_reasons", []),
        ...
    }
```

Constrói o registo de quarentena — inclui todos os dados originais mais o timestamp de rejeição e os motivos. Vai para o topic `data-quality-metrics` e para o InfluxDB.

---

## 5. data_quality.py

**Papel:** Escreve métricas no InfluxDB. Importado pelo consumer.

---

### Passo 1 — Métricas de qualidade: `send_quality_metrics()` (linhas 11–22)

```python
def send_quality_metrics(write_api, bucket, org, source, success_pct, n_success, n_failed, total_rows):
    point = (
        Point("data_quality")              # nome da "tabela" no InfluxDB
        .tag("source", source)             # tag = indexado, para filtrar no Grafana
        .field("success_percent", float(success_pct))
        .field("successful_expectations", int(n_success))
        .field("failed_expectations",     int(n_failed))
        .field("total_rows",              int(total_rows))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)
```

O InfluxDB usa o modelo **tag + field**: tags são indexadas (boas para filtrar/agrupar), fields são os valores numéricos. Aqui `source` é tag porque o Grafana vai querer filtrar por fonte ("sensor_readings" vs "satellite-hotspots").

---

### Passo 2 — Latência da pipeline: `send_latency()` (linhas 57–67)

```python
def send_latency(write_api, bucket, org, latency_ms, topic, grid_id):
    point = (
        Point("pipeline_latency")
        .tag("topic",   topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", float(latency_ms))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)
```

Guarda a latência individual de cada evento processado. Com duas tags (`topic` e `grid_id`), o Grafana pode mostrar a latência média por zona ou por topic — útil para detectar zonas com sensores lentos.

---

### Passo 3 — Métricas de rejeição: `send_rejected_metrics()` e `send_rejected_event_detail()` (linhas 24–55)

```python
def send_rejected_metrics(write_api, bucket, org, grid_id, reasons):
    point = Point("rejected_events")
        .tag("grid_id", grid_id)
        .tag("reason",  ",".join(reasons))
        .field("count", 1)    # sempre 1 — cada ponto é uma rejeição

def send_rejected_event_detail(write_api, bucket, org, rejected):
    detail = Point("rejected_event_detail")
        .field("temp_celsius",      _safe_float(rejected.get("temp_celsius")))
        .field("rejection_reasons", ",".join(reasons))
        ...
```

Dois níveis de detalhe:
- **`rejected_events`** — contagem por zona e motivo (para dashboards de resumo)
- **`rejected_event_detail`** — todos os campos do evento rejeitado (para debugging de eventos específicos)

`_safe_float()` trata o caso em que o valor é `None` (o campo que falhou pode não ter valor).

---

## 6. spark_streaming_agregacao.py

**Papel:** Job Spark que faz join dos 3 streams e calcula risco composto por zona.

---

### Passo 1 — Configuração (linhas 45–57)

```python
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
S3_PATH       = "s3a://forest-risk-datalake/agregados_streaming/"
S3_CHECKPOINT = "s3a://forest-risk-datalake/checkpoints/agregados_join/"
```

O `S3_CHECKPOINT` é fundamental — o Spark grava aqui o progresso do streaming. Se o job reiniciar, retoma do ponto onde ficou sem reprocessar eventos já tratados.

---

### Passo 2 — Sessão Spark (linhas 60–88)

```python
spark = (SparkSession.builder
    .appName("ForestRisk-StreamingJoin3")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .master("local[*]")
    .getOrCreate())

# Configura S3
hadoop_conf.set("fs.s3a.endpoint", AWS_ENDPOINT_URL)
hadoop_conf.set("fs.s3a.path.style.access", "true")   # obrigatório para LocalStack
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")  # LocalStack usa HTTP
```

**Decisões de configuração:**
- **`shuffle.partitions=4`** — em vez do default 200, usa 4 partições (adequado para máquina local)
- **`statefulOperator.checkCorrectness=false`** — desactiva uma verificação que impede left joins em streams com watermark (comportamento por design neste caso)
- **`path.style.access=true`** — o LocalStack requer que o path inclua o bucket name (diferente da AWS real que usa subdomain)

---

### Passo 3 — Schemas dos 3 topics (linhas 91–130)

```python
schema_sensores = StructType([
    StructField("grid_id",      StringType()),
    StructField("temp_celsius", DoubleType()),
    StructField("risk_score",   DoubleType()),
    ...
])
schema_satelite = StructType([
    StructField("frp",          DoubleType()),
    StructField("brightness",   DoubleType()),
    ...
])
schema_meteo = StructType([
    StructField("temp_max",     DoubleType()),
    StructField("wind_max_kmh", DoubleType()),
    ...
])
```

O Kafka entrega bytes — o Spark precisa de saber a estrutura do JSON para o parsear correctamente. Cada topic tem um schema diferente porque cada producer envia campos diferentes. Campos não declarados no schema são ignorados.

---

### Passo 4 — Leitura dos streams: `ler_topic()` (linhas 133–148)

```python
def ler_topic(topic, schema, timestamp_col="timestamp"):
    return (
        spark.readStream
        .format("kafka")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")   # só eventos novos
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")  # bytes → string
        .select(from_json("json_str", schema).alias("d")) # string → struct
        .select("d.*")                                     # struct → colunas
        .withColumn("event_time", to_timestamp("timestamp")) # string → timestamp
    )

stream_sensores = ler_topic("sensor-events",      schema_sensores)
stream_satelite = ler_topic("satellite-hotspots", schema_satelite)
stream_meteo    = ler_topic("weather-data",        schema_meteo)
```

Cada chamada cria um stream independente. A transformação `CAST(value AS STRING)` → `from_json()` → `select("d.*")` é o padrão padrão para ler JSON do Kafka no Spark.

---

### Passo 5 — Agregação com sliding window (linhas 151–211)

```python
# Para cada um dos 3 streams:
agg_sensores = (
    stream_sensores
    .withWatermark("event_time", "2 minutes")   # tolera atrasos até 2 min
    .groupBy(
        window("event_time", "10 minutes", "5 minutes"),  # janela 10min, slide 5min
        col("grid_id")
    )
    .agg(
        count("*").alias("n_leituras_sensor"),
        avg("risk_score").alias("risk_medio_sensor"),
        spark_max("risk_score").alias("risk_maximo_sensor"),
        avg("temp_celsius").alias("temp_media"),
        ...
    )
)
```

**O conceito em números concretos:**
- Uma janela `19:00–19:10` cobre todos os eventos com `event_time` entre 19:00 e 19:10
- A próxima janela é `19:05–19:15` (avança 5 min)
- Um evento às 19:07 pertence a **duas janelas**: `19:00–19:10` e `19:05–19:15`

---

### Passo 6 — Join dos 3 streams (linhas 214–231)

```python
joined = (
    agg_sensores
    .join(agg_satelite, on=["window", "grid_id"], how="left")
    .join(agg_meteo,    on=["window", "grid_id"], how="left")
)
```

Junta os 3 DataFrames aggregados pela combinação `(janela de tempo, grid_id)`. `left join` garante que linhas de `agg_sensores` sem correspondência em `agg_satelite` ou `agg_meteo` aparecem na mesma na mesma linha com valores `null` (depois tratados com `coalesce`).

---

### Passo 7 — Índice de risco composto (linhas 234–282)

```python
df_resultado = joined.select(
    col("window.start").alias("janela_inicio"),
    col("window.end").alias("janela_fim"),
    col("grid_id"),
    ...
    coalesce(col("n_hotspots"), lit(0)).alias("n_hotspots"),     # null → 0
    coalesce(col("frp_medio"),  lit(0.0)).alias("frp_medio"),

    # Índice composto 0-100
    (
        coalesce(col("risk_medio_sensor"), lit(0.0)) * 0.60 +
        (coalesce(col("frp_medio"), lit(0.0)) / 200.0 * 100.0) * 0.25 +
        (coalesce(col("vento_max_ipma"), lit(0.0)) / 150.0 * 100.0) * 0.15
    ).alias("risco_composto"),
)
```

`coalesce(col, lit(0))` transforma `null` em `0` — essencial porque o `left join` pode deixar campos `null` quando um stream não tinha dados para aquela janela/zona. Sem isto, `null + qualquer_coisa = null` e o `risco_composto` ficaria nulo.

---

### Passo 8 — Escrita em dois destinos (linhas 285–310)

```python
# Destino A — Console (update: mostra o que mudou)
query_console = (
    df_resultado.writeStream
    .outputMode("update")    # mostra janelas que mudaram
    .format("console")
    .trigger(processingTime="30 seconds")
    .start()
)

# Destino B — S3 Parquet (append: só janelas fechadas)
query_s3 = (
    df_resultado.writeStream
    .outputMode("append")    # só janelas completamente fechadas
    .format("parquet")
    .option("path", S3_PATH)
    .option("checkpointLocation", S3_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

spark.streams.awaitAnyTermination()  # espera que qualquer stream termine
```

**Dois modos de output diferentes:**
- **Console usa `update`** — mostra todas as janelas que tiveram novos dados, incluindo janelas abertas. Ideal para demo.
- **S3 usa `append`** — só grava janelas que já fecharam definitivamente (passaram o watermark). Evita reescrever/duplicar dados parciais no S3.

---

## 7. carga_historico_s3.py

**Papel:** Carrega dados históricos das EDAs para o S3 em Parquet particionado. Só é chamado pelo `check_and_load.sh` quando os Parquet das EDAs existem e são mais recentes que os dados no S3.

---

### Passo 1 — Configuração de caminhos (linhas 33–55)

```python
BASE_DIR       = Path(os.getenv("BASE_DIR", "/home/jovyan/work"))
PASTA_EDA_NASA = BASE_DIR / "Filtragem_Parquet"   # gerado pela EDA_NASA.py
PASTA_EDA_ERA5 = BASE_DIR / "ERA5_Parquet"         # gerado pela EDA_ERA5.py

# Bbox Portugal Continental
LAT_MIN, LAT_MAX = 36.9, 42.2
LON_MIN, LON_MAX = -9.5, -6.2
```

Define as pastas onde as EDAs da Pessoa B depositam os Parquet limpos. A bounding box é usada internamente pela função `coords_para_grid` para verificar coordenadas.

Nota: a variável `PASTA_CSV_NASA` ainda existe no código por compatibilidade mas já não é chamada automaticamente — o `check_and_load.sh` garante que este script só é invocado quando há Parquet das EDAs disponíveis.

---

### Passo 2 — Leitura de Parquet EDA: `ler_parquet_eda()` (linhas 59–86)

```python
def ler_parquet_eda(pasta, prefixo, nome_eda):
    pasta = Path(pasta)
    if not pasta.exists():
        return None    # pasta não existe → devolve None

    # Tenta ficheiro combinado primeiro (mais rápido)
    f_todos = pasta / f"{prefixo}_todos.parquet"
    if f_todos.exists():
        df = pd.read_parquet(f_todos)
        return df

    # Fallback: lê ficheiros por ano e concatena
    ficheiros = sorted([f for f in pasta.glob(f"{prefixo}_*.parquet")
                        if "todos" not in f.name])
    if not ficheiros:
        return None

    return pd.concat([pd.read_parquet(f) for f in ficheiros], ignore_index=True)
```

**Sub-passos:**
- Retorna `None` (não levanta excepção) se a pasta não existir — o `main()` trata o `None` e avisa
- Prefere o ficheiro `_todos.parquet` (mais eficiente que ler N ficheiros individuais)
- Fallback para ficheiros por ano quando o `_todos` não existe (algumas EDAs geram um ficheiro por ano)

---

### Passo 3 — Leitura CSV (uso manual): `ler_csv_como_eda()` (linhas 89–142)

```python
def ler_csv_como_eda(pasta):
    for f in sorted(pasta.glob("*.csv")):
        # 3a. Detecta o satélite pelo nome do ficheiro
        if 'snpp' in f.name.lower():
            satelite = 'VIIRS S-NPP'
        elif 'jpss1' in f.name.lower():
            satelite = 'VIIRS NOAA-20'

        df = pd.read_csv(f, low_memory=False)
        df['satelite'] = satelite

        # 3b. Converte datas
        df['acq_date'] = pd.to_datetime(df['acq_date'])
        df['ano'] = df['acq_date'].dt.year
        df['mes'] = df['acq_date'].dt.month
        df['dia'] = df['acq_date'].dt.day

    combinado = pd.concat(frames)

    # 3c. Filtra para Portugal (bbox)
    df_pt = combinado[
        (combinado['latitude']  >= LAT_MIN) & (combinado['latitude']  <= LAT_MAX) &
        (combinado['longitude'] >= LON_MIN) & (combinado['longitude'] <= LON_MAX)
    ]

    # 3d. Selecciona as mesmas colunas que a EDA produziria
    COLUNAS_EDA = ['latitude','longitude','bright_ti4','bright_ti5',
                   'frp','acq_date','acq_time','confidence','daynight',
                   'satellite','satelite','ano','mes','dia']
    cols = [c for c in COLUNAS_EDA if c in df_pt.columns]
    return df_pt[cols]
```

**Nota importante:** Esta função existe no código mas **não é chamada automaticamente** pelo `check_and_load.sh`. O `check_and_load.sh` garante que este script só é invocado quando há Parquet das EDAs disponíveis. A função `ler_csv_como_eda` só é usada se alguém correr o `carga_historico_s3.py` manualmente e não existirem Parquet das EDAs.

**Sub-passos:**
- **3a — Detecção por nome:** `viirs-snpp_2020.csv` → `VIIRS S-NPP`; `viirs-jpss1_2020.csv` → `VIIRS NOAA-20`
- **3b — Colunas de partição:** `ano`, `mes`, `dia` são criadas a partir de `acq_date` para uso posterior no particionamento S3
- **3c — Bbox de Portugal:** Alinhado exactamente com o que a `EDA_NASA.py` faz — garante consistência entre os dois modos
- **3d — Selecção de colunas:** `[c for c in COLUNAS_EDA if c in df_pt.columns]` é seguro — não falha se alguma coluna não existir num ficheiro mais antigo

---

### Passo 4 — Adicionar grid_id: `adicionar_grid_id()` (linhas 145–155)

```python
def adicionar_grid_id(df):
    if "grid_id" not in df.columns:
        df = df.copy()
        df["grid_id"] = df.apply(
            lambda r: coords_para_grid(r["latitude"], r["longitude"]), axis=1
        )
    return df
```

`df.copy()` antes de modificar — boa prática para não alterar o DataFrame original (evita `SettingWithCopyWarning` do pandas). O `apply` com `coords_para_grid` calcula a zona mais próxima por distância euclidiana para cada linha.

---

### Passo 5 — Opções S3: `storage_options()` (linhas 170–183)

```python
def storage_options():
    opts = {
        "key":    os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "client_kwargs": {"region_name": AWS_REGION},
    }
    if AWS_ENDPOINT_URL:
        opts["client_kwargs"]["endpoint_url"] = AWS_ENDPOINT_URL  # LocalStack
    return opts
```

A ausência de `endpoint_url` faz o `s3fs` ir para a AWS real — é o mecanismo de "endpoint configurável". LocalStack em dev, AWS real em produção, sem alterar código.

---

### Passo 6 — Gravar em Parquet particionado: `gravar_s3()` (linhas 186–194)

```python
def gravar_s3(df, prefixo, partition_cols):
    caminho = f"s3://{BUCKET}/{prefixo}/"
    df.to_parquet(
        caminho,
        engine="pyarrow",
        partition_cols=partition_cols,   # ["ano", "mes", "grid_id"]
        storage_options=storage_options(),
        index=False
    )
```

`partition_cols=["ano","mes","grid_id"]` diz ao pandas/pyarrow para **não** incluir estas colunas dentro do ficheiro Parquet — em vez disso, cria sub-pastas com o nome `ano=2020`, `mes=8`, `grid_id=PT-NORTE-01`. Quando se lê a pasta completa, estas colunas são reconstruídas automaticamente.

---

### Passo 7 — Main: carrega Parquet das EDAs (linhas 206–265)

```python
def main():
    # NASA FIRMS — lê Parquet da EDA_NASA.py
    df_nasa = ler_parquet_eda(PASTA_EDA_NASA, "firms_portugal_limpo", "EDA_NASA")

    if df_nasa is not None:
        df_nasa = adicionar_grid_id(df_nasa)
        gravar_s3(df_nasa, PREFIXO_NASA, ["ano", "mes", "grid_id"])
        verificar_s3(PREFIXO_NASA)
    else:
        print("SALTADO: Filtragem_Parquet/ não encontrada ou vazia")

    # ERA5 — lê Parquet da EDA_ERA5.py (só se EDA correu)
    df_era5 = ler_parquet_eda(PASTA_EDA_ERA5, "era5_portugal", "EDA_ERA5")
    if df_era5 is not None:
        df_era5 = preparar_era5(df_era5)
        gravar_s3(df_era5, PREFIXO_ERA5, ["ano", "mes"])
    else:
        print("SALTADO: ERA5_Parquet/ não encontrada — EDA_ERA5.py ainda não correu")
```

**Sub-passos:**
- **NASA:** lê `Filtragem_Parquet/` gerado pela `EDA_NASA.py`. Se não existir, salta e avisa — não há fallback automático para CSV
- **ERA5:** lê `ERA5_Parquet/` gerado pela `EDA_ERA5.py`. Também salta se não existir — é esperado durante o desenvolvimento
- A assimetria é intencional: ambos dependem exclusivamente dos Parquet das EDAs. O `check_and_load.sh` garante que este script só é chamado quando pelo menos uma EDA correu

---

## 8. check_and_load.sh

**Papel:** Script bash chamado pelo container `carga-historico` ao arrancar. Decide se os dados históricos precisam de ser carregados para o S3 comparando as datas dos Parquet das EDAs com os dados já no S3.

---

### Passo 1 — Array de EDAs registadas

```bash
EDAS=(
    "EDA_NASA.py|/home/jovyan/work/Filtragem_Parquet|Hotspots NASA FIRMS (satélite)"
    "EDA_ERA5.py|/home/jovyan/work/ERA5_Parquet|Meteorologia ERA5 (Copernicus)"
    "EDA_ICNF.py|/home/jovyan/work/ICNF_Parquet|Cartografia florestal ICNF (COS2018)"
)
```

Array central do script — regista todas as EDAs esperadas com o formato `NOME|PASTA|DESCRIÇÃO`. Para adicionar uma EDA nova basta acrescentar uma linha. O script itera sobre este array em todos os passos seguintes.

---

### Passo 2 — Verifica quais EDAs já correram

```bash
for entry in "${EDAS[@]}"; do
    NOME=$(echo "$entry" | cut -d'|' -f1)
    PASTA=$(echo "$entry" | cut -d'|' -f2)

    N=$(find "$PASTA" -name "*.parquet" 2>/dev/null | wc -l)

    if [ "$N" -gt 0 ]; then
        EDAS_PRONTAS+=("$entry")   # tem Parquet → pronta
        ALGUMA_DISPONIVEL=1
    else
        EDAS_EM_FALTA+=("$NOME|$DESC")  # sem Parquet → avisa
    fi
done
```

Itera sobre todas as EDAs e conta os ficheiros `.parquet` na pasta de cada uma. Popula dois arrays: `EDAS_PRONTAS` (têm Parquet, podem ser carregadas) e `EDAS_EM_FALTA` (ainda não correram). `2>/dev/null` suprime o erro se a pasta não existe.

---

### Passo 3 — Se nenhuma EDA correu, avisa e para

```bash
if [ $ALGUMA_DISPONIVEL -eq 0 ]; then
    echo "Para ser possível popular o data lake, o responsável pelos"
    echo "EDAs tem de correr e validar primeiro os respectivos:"
    for entry in "${EDAS_EM_FALTA[@]}"; do
        echo "  → python /home/jovyan/work/$NOME   ($DESC)"
    done
    exit 0   # saída limpa — não é erro, é estado esperado
fi
```

Se zero EDAs têm Parquet, lista todas com o comando exacto para as correr e termina sem tocar no S3. `exit 0` (não `exit 1`) porque não é um erro — é o estado esperado durante o desenvolvimento antes das EDAs correrem.

---

### Passo 4 — Avisa EDAs em falta sem bloquear as disponíveis

```bash
if [ ${#EDAS_EM_FALTA[@]} -gt 0 ]; then
    echo "⏳ As seguintes EDAs ainda não correram (dados parciais no S3):"
    for entry in "${EDAS_EM_FALTA[@]}"; do
        echo "  → python /home/jovyan/work/$NOME"
    done
fi
```

Se algumas EDAs correram e outras não, avisa sobre as que faltam mas **não bloqueia** — é preferível ter dados parciais no S3 a não ter nada. O script continua com as EDAs disponíveis.

---

### Passo 5 — Compara datas Parquet vs S3 (Python interno)

```python
# Data do Parquet mais recente entre todas as pastas EDA disponíveis
data_parquet = max(f.stat().st_mtime for f in todos_parquets)

# Data do objecto S3 mais recente em hotspots/
resp = s3.list_objects_v2(Bucket='forest-risk-datalake', Prefix='hotspots/')
data_s3 = max(o['LastModified'] for o in objectos)

if data_parquet_dt > data_s3:
    sys.exit(1)   # Parquet mais recente → recarregar
else:
    sys.exit(0)   # S3 actualizado → saltar
```

Usa Python (embutido no bash via heredoc) para comparar datas porque o boto3 facilita a ligação ao S3. `st_mtime` é o timestamp Unix da última modificação do ficheiro. O `LastModified` do S3 já vem com timezone UTC. Códigos de saída: `0` = saltar, `1` = carregar, `2` = sem Parquet.

---

### Passo 6 — Decisão final e carga

```bash
if [ $STATUS -eq 0 ]; then
    echo "✅ S3 está actualizado — carga saltada."
    exit 0
fi

# STATUS=1: S3 vazio ou desactualizado → carrega
python3 /home/jovyan/work/carga_historico_s3.py
```

Só chega aqui se o S3 está vazio ou os Parquet das EDAs são mais recentes. Chama o `carga_historico_s3.py` que lê os Parquet e grava no S3.

**Resumo dos 4 cenários possíveis:**

| Cenário | O que acontece |
|---|---|
| Sem Parquet EDAs | Avisa com instruções e para |
| S3 vazio + Parquet EDAs | Carrega para o S3 |
| Parquet EDAs mais recentes que S3 | Recarrega o S3 |
| S3 actualizado | Salta sem fazer nada |

---

*Documentação gerada para o projecto Forest Risk Monitoring System — ISEP 2024/2025*
