# Forest Risk Monitoring System — Documentação Técnica da Pipeline

**Projeto:** ISEP Pós-Graduação Big Data & Data Science 2024/2025  
**Papel documentado:** Data Engineer (Pessoa A)  
**Última actualização:** Junho 2026

---

## Índice

1. [Visão geral da pipeline](#1-visão-geral-da-pipeline)
2. [producer_sensores.py](#2-producer_sensorespy)
3. [producer_apis_reais.py](#3-producer_apis_reaispy)
4. [consumer_kafka_cassandra.py](#4-consumer_kafka_cassandrapy)
5. [data_quality_validation.py](#5-data_quality_validationpy)
6. [data_quality.py](#6-data_qualitypy)
7. [spark_streaming_agregacao.py](#7-spark_streaming_agregacaopy)
8. [carga_historico_s3.py](#8-carga_historico_s3py)
9. [Sequência de arranque](#9-sequência-de-arranque)
10. [Dependências entre ficheiros](#10-dependências-entre-ficheiros)

---

## 1. Visão geral da pipeline

A pipeline processa dados de incêndio florestal em tempo real, desde a recolha até ao armazenamento e análise. Há dois fluxos distintos:

```
FLUXO 1 — TEMPO REAL (contínuo, arranca com docker compose up)
─────────────────────────────────────────────────────────────
producer_sensores.py  ─┐
                       ├──► Kafka ──► consumer_kafka_cassandra.py ──► Cassandra (sensor_readings)
producer_apis_reais.py ─┘             ├──► Cassandra (fire_alerts) se risk >= 60
                                      ├──► Kafka (fire-alerts) se temp>35 E hum<20 E vento>30
                                      ├──► Kafka (data-quality-metrics) eventos inválidos
                                      └──► InfluxDB (métricas de qualidade e latência)
                         Kafka ──► spark_streaming_agregacao.py ──► S3 + console

FLUXO 2 — BATCH HISTÓRICO (gerido pelo check_and_load.sh)
─────────────────────────────────────────────────────────────────
check_and_load.sh ──► verifica Parquet EDAs vs S3
                       ├── Parquet EDA mais recente → carga_historico_s3.py ──► S3
                       └── Sem Parquet EDA → avisa e não carrega nada
```

**Cada ficheiro tem uma responsabilidade única e bem definida.**

---

## 2. producer_sensores.py

### O que é
Script Python que **simula sensores IoT** distribuídos por 10 zonas de Portugal Continental e envia leituras para o Kafka continuamente.

### Quando é usado
Arranca automaticamente como container Docker (`producer-sensores`) quando se executa `docker compose up`. Corre em loop infinito até o container parar.

### O que produz
Envia eventos JSON para dois topics do Kafka:

| Topic | Frequência | Conteúdo |
|---|---|---|
| `sensor-events` | A cada 2 segundos | Leitura de sensor IoT |
| `weather-data` | A cada 10 segundos (1 em 5 ciclos) | Dados meteorológicos simulados |

### Estrutura de um evento `sensor-events`
```json
{
  "grid_id":       "PT-CENTRO-01",
  "regiao":        "Coimbra",
  "latitude":      40.1987,
  "longitude":     -8.4213,
  "timestamp":     "2026-06-09T19:03:54Z",
  "temp_celsius":  34.2,
  "humidity_pct":  18.5,
  "wind_kmh":      22.1,
  "hotspot_count": 0,
  "risk_score":    67.3,
  "source":        "iot_simulator_v1"
}
```

### Lógica de risco simulado
O `risk_score` (0-100) é calculado com base em 4 factores:
- **Temperatura** — acima de 15°C contribui para o risco (máx 40 pontos)
- **Humidade baixa** — quanto mais seco, maior o risco (máx 30 pontos)
- **Vento forte** — propaga incêndios (máx 20 pontos)
- **Hotspots próximos** — incêndios activos na zona (máx 10 pontos)

Adicionalmente, **5% dos eventos são extremos** (temperatura alta, humidade baixa, vento forte) para simular situações de crise.

### Perfil sazonal
O producer ajusta os valores base consoante o mês do ano:
- **Verão (Jun-Set):** temperatura alta, humidade baixa → risco elevado
- **Inverno (Dez-Fev):** temperatura baixa, humidade alta → risco baixo
- **Primavera/Outono:** valores intermédios

### Configuração
```python
KAFKA_BOOTSTRAP    = "kafka:9092"   # endereço do broker Kafka
INTERVALO_SEGUNDOS = 2              # pausa entre ciclos de envio
```

### Como corre no Docker
```yaml
producer-sensores:
  command: python notebooks/producer_sensores.py
  environment:
    - KAFKA_BOOTSTRAP=kafka:9092
```

---

## 3. producer_apis_reais.py

### O que é
Script Python que **consulta APIs externas reais** (NASA FIRMS, IPMA, ICNF) e publica os dados no Kafka. É a fonte de dados reais do sistema, em contraste com o producer de sensores simulados.

### Quando é usado
Arranca automaticamente como container Docker (`producer-apis`) com `docker compose up`. Consulta as APIs em intervalos regulares (não em tempo real contínuo — as APIs têm limites de taxa).

### Fontes de dados e intervalos

| Fonte | O que fornece | Intervalo | Topic destino | Registo necessário |
|---|---|---|---|---|
| **NASA FIRMS** | Hotspots de incêndio detectados por satélite | 1 hora | `satellite-hotspots` | Sim (gratuito) |
| **IPMA** | Observações meteorológicas em tempo real | 30 minutos | `weather-data` + `sensor-events` | Não |
| **ICNF** | Cartografia de vegetação (dados estáticos) | 1 vez/dia | `sensor-events` | Não |

### NASA FIRMS — como funciona
A NASA FIRMS (Fire Information for Resource Management System) devolve hotspots térmicos detectados pelo satélite VIIRS S-NPP sobre Portugal. Para cada hotspot activo, o producer envia:

```json
{
  "grid_id":    "PT-NORTE-02",
  "latitude":   41.6523,
  "longitude":  -7.8901,
  "timestamp":  "2026-06-09T19:03:00Z",
  "brightness": 312.4,
  "frp":        45.2,
  "confidence": "h",
  "daynight":   "D",
  "source":     "nasa_firms_viirs_snpp"
}
```

O campo `frp` (Fire Radiative Power, em MW) é o mais importante — quantifica a intensidade do incêndio detectado pelo satélite. Valores acima de 100 MW indicam incêndios de grande dimensão.

### IPMA — como funciona
Consulta a API pública do IPMA para obter observações das estações meteorológicas em Portugal Continental. Estão configuradas 15 estações (Lisboa, Porto, Coimbra, Faro, etc.).

```json
{
  "grid_id":          "PT-LVT-02",
  "timestamp":        "2026-06-09T19:00:00Z",
  "temp_max":         36.2,
  "temp_min":         22.1,
  "humidity_avg":     28.5,
  "wind_max_kmh":     18.0,
  "precipitation_mm": 0.0,
  "source":           "ipma_api"
}
```

### Configuração obrigatória
```python
NASA_FIRMS_KEY = os.getenv("NASA_FIRMS_KEY", "")  # lida do .env
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
```

A NASA key deve estar no ficheiro `.env`:
```
NASA_FIRMS_KEY=579b22bcae291064c381d64a3375f069
```

Obtém uma key gratuita em: https://firms.modaps.eosdis.nasa.gov/api/area/

### ICNF — como funciona

O ICNF (Instituto da Conservação da Natureza e das Florestas) disponibiliza a **Carta de Ocupação do Solo 2018 (COS2018)** através de um serviço WFS (Web Feature Service). Classifica o território por tipo de uso/vegetação.

**O que é diferente do NASA e IPMA:**

| | NASA FIRMS | IPMA | ICNF |
|---|---|---|---|
| Tipo de dado | Evento activo (fogo agora) | Observação meteorológica (clima agora) | Característica estática (tipo de solo) |
| Frequência de mudança | Contínua | Contínua | Anos |
| Responde a | "Há fogo aqui?" | "Que tempo faz?" | "Que vegetação existe aqui?" |
| Consultado a cada | 1 hora | 30 minutos | 1 vez por dia |

**Porquê é relevante para o risco de incêndio:**

O tipo de vegetação determina a **velocidade de propagação** do fogo — não diz se há fogo, mas diz o quão perigosa é a zona se houver fogo:

```
Eucaliptal / Pinheiro bravo  → HIGH   (óleos essenciais, arde muito rapidamente)
Sobreiro / Carvalho          → MEDIUM (folha larga, mais resistente ao fogo)
Área urbana / Agrícola       → LOW    (não propaga ou propaga muito lentamente)
```

**Evento publicado no topic `sensor-events`:**
```json
{
  "timestamp":       "2026-06-09T19:00:00Z",
  "tipo_vegetacao":  "Eucaliptal",
  "area_ha":         1250.5,
  "risco_vegetacao": "HIGH",
  "source":          "icnf_cos2018"
}
```

**Nota sobre disponibilidade — o ICNF frequentemente devolve 404:**

O serviço WFS do ICNF (`sig.icnf.pt`) tem disponibilidade irregular — entra em manutenção sem aviso. O producer trata o erro silenciosamente e tenta novamente no dia seguinte. Não é um problema do código — é uma limitação conhecida da infraestrutura do ICNF.

**Limitação actual — dados ICNF não integrados no risco composto:**

Os dados ICNF chegam ao topic `sensor-events` mas o job Spark não os usa no cálculo do `risco_composto`. São persistidos no Cassandra mas não influenciam o índice de risco em tempo real. Uma melhoria futura seria o Spark usar o tipo de vegetação como multiplicador do risco por zona — uma zona com eucaliptal teria o risco amplificado face a uma zona com sobreiro nas mesmas condições meteorológicas.

### Diferença para o producer_sensores.py

| Aspecto | producer_sensores | producer_apis_reais |
|---|---|---|
| Dados | Simulados | Reais |
| Frequência | Contínua (2s) | Periódica (30min-1h) |
| Dependências externas | Nenhuma | NASA key, internet |
| Topics alimentados | sensor-events, weather-data | satellite-hotspots, weather-data, sensor-events |

---

## 4. consumer_kafka_cassandra.py

### O que é
O **coração da ingestão** — lê mensagens dos topics Kafka, valida a qualidade dos dados, e persiste no Cassandra e InfluxDB.

### Quando é usado
Arranca automaticamente como container Docker (`consumer`) com `docker compose up`. Corre em paralelo com os producers — enquanto os producers enviam, o consumer lê e processa.

### O que faz — fluxo detalhado

```
Kafka (sensor-events)
       │
       ▼
  Acumula batch
  (3 eventos ou 30s)
       │
       ▼
  Validação de qualidade
  (data_quality_validation.py)
       │
       ├──► Eventos VÁLIDOS ──► Cassandra (sensor_readings)
       │                    ──► Cassandra (fire_alerts) se risk_score >= 60
       │                    ──► Kafka (fire-alerts) se temp>35 E hum<20 E vento>30
       │                    ──► InfluxDB (latência)
       │
       └──► Eventos INVÁLIDOS ──► Kafka (data-quality-metrics)
                              ──► InfluxDB (métricas de rejeição)
```

### As tabelas Cassandra que popula

**`sensor_readings`** — guarda cada leitura válida individual:
```
grid_id, timestamp, temp_celsius, humidity_pct, wind_kmh,
risk_score, hotspot_count, source
```

**`fire_alerts`** — só quando `risk_score >= 60` (risco ALTO ou CRÍTICO):
```
grid_id, timestamp, risk_score, risk_level, temp_celsius,
humidity_pct, wind_kmh, source
```

### Sistema de micro-batches
Em vez de processar evento a evento (ineficiente), o consumer acumula eventos num batch e processa-os juntos. O batch fecha quando:
- Atingiu **3 eventos** (`BATCH_SIZE=3`), ou
- Passaram **30 segundos** sem novos eventos (`BATCH_TIMEOUT=30`)

Isto equilibra eficiência (menos operações Cassandra) e latência (não espera demasiado).

### Alertas no topic `fire-alerts`
Para além de gravar em Cassandra quando `risk_score >= 60`, o consumer publica também no topic `fire-alerts` quando as condições meteorológicas brutas atingem os limiares definidos no documento:

```
temperatura > 35°C  E  humidade < 20%  E  vento > 30 km/h
```

Esta verificação é **independente do `risk_score`** — avalia directamente os valores medidos. O evento publicado inclui um campo `trigger` com os valores exactos que dispararam o alerta, facilitando o debugging pela Pessoa C.

### Dois consumers em paralelo
O ficheiro corre **dois threads** em simultâneo:

1. **`consume_sensor_events`** — lê `sensor-events`, valida com GE, grava em `sensor_readings` e `fire_alerts`, publica em `fire-alerts` quando condições críticas
2. **`consume_satellite_hotspots`** — lê `satellite-hotspots` (NASA), valida com regras específicas NASA, grava em `sensor_readings` ou quarentena

### Validação dos dados NASA (correcção de gap)

Os dados NASA não passam pela validação GE dos sensores IoT porque têm campos diferentes. Foi implementada validação específica em `data_quality_validation.py` com as seguintes regras:

| Campo | Intervalo válido | Justificação |
|---|---|---|
| `frp_mw` | 0 – 5000 MW | Maior incêndio registado historicamente ~3000 MW |
| `brightness` | 200 – 500 K | Temperatura de brilho VIIRS para fogo activo |
| `latitude` | 36.9 – 42.2 | Bounding box Portugal Continental |
| `longitude` | -9.5 – -6.2 | Bounding box Portugal Continental |
| `grid_id` | não PT-UNKNOWN | PT-UNKNOWN indica falha de mapeamento de coordenadas |

Hotspots que falham estas regras vão para quarentena (`data-quality-metrics`) em vez de serem gravados silenciosamente no Cassandra. A métrica de rejeição é visível no Grafana tal como para os sensores IoT.

### Classificação de risco
```python
risk_score < 30  → "LOW"      (verde)
risk_score < 60  → "MEDIUM"   (amarelo)
risk_score < 80  → "HIGH"     (laranja)  → gera alerta Cassandra
risk_score >= 80 → "CRITICAL" (vermelho) → gera alerta Cassandra
```

### Configuração
```python
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",  "cassandra")
INFLUX_URL      = os.getenv("INFLUXDB_URL",    "http://influxdb:8086")
INFLUX_TOKEN    = os.getenv("INFLUXDB_TOKEN",  "...")
BATCH_SIZE      = 3    # eventos por batch
BATCH_TIMEOUT   = 30   # segundos máximos por batch
```

---

## 5. data_quality_validation.py

### O que é
Módulo de **validação de qualidade de dados**. Não é um script autónomo — é importado pelo `consumer_kafka_cassandra.py` para validar cada batch antes de o persistir.

### Quando é usado
Chamado internamente pelo consumer a cada micro-batch:
```python
from data_quality_validation import split_valid_invalid, run_ge_validation
```

### O que valida
Define regras de validação para os campos numéricos dos eventos:

| Campo | Mínimo | Máximo | Justificação |
|---|---|---|---|
| `temp_celsius` | -10 | 60 | Temperaturas fora deste intervalo são impossíveis em Portugal |
| `humidity_pct` | 0 | 100 | Percentagem só pode ser 0-100 |
| `wind_kmh` | 0 | 150 | Acima de 150 são furacões — sensor danificado |
| `risk_score` | 0 | 100 | Índice normalizado 0-100 |
| `grid_id` | — | — | Não pode ser nulo (chave primária) |
| `risk_score` | — | — | Não pode ser nulo |

### Dois modos de validação

**Modo 1 — Great Expectations (GE):** usa a biblioteca de validação de dados GE para criar um relatório estruturado. Mais detalhado, gera métricas de qualidade precisas.

**Modo 2 — Manual (fallback):** se o GE falhar (incompatibilidades de versão, etc.), usa validação Python simples com as mesmas regras. Garante que o consumer nunca pára por problemas de dependências.

### Funções principais

**Para sensores IoT:**

**`split_valid_invalid(batch)`** — função mais usada. Recebe uma lista de eventos e devolve dois grupos:
```python
valid_events, invalid_events = split_valid_invalid(batch)
# valid_events   → prontos para Cassandra
# invalid_events → têm campo _rejection_reasons explicando o motivo
```

**`run_ge_validation(context, df)`** — corre Great Expectations sobre o batch e devolve percentagem de sucesso.

**`build_rejected_record(ev)`** — formata um evento inválido para enviar para a quarentena:
```python
{
  "grid_id":           "PT-CENTRO-01",
  "rejected_at":       "2026-06-09T19:03:00Z",
  "temp_celsius":      999.9,            # valor que falhou
  "rejection_reasons": ["temp_celsius_out_of_range(999.9)"]
}
```

**Para hotspots NASA (funções específicas):**

**`split_valid_invalid_nasa(batch)`** — aplica as regras NASA (FRP, brightness, coordenadas, grid_id) evento a evento. Análoga a `split_valid_invalid()` mas com regras físicas diferentes.

**`build_rejected_record_nasa(ev)`** — formata um hotspot inválido para quarentena com os campos relevantes da NASA (latitude, longitude, frp_mw, brightness, confidence).

---

## 6. data_quality.py

### O que é
Módulo de **escrita de métricas no InfluxDB**. Também não é autónomo — é importado pelo consumer para registar o estado da pipeline.

### Quando é usado
Chamado pelo consumer após cada batch processado:
```python
from data_quality import send_quality_metrics, send_latency
```

### O que escreve no InfluxDB

**`data_quality` measurement** — métricas gerais por batch:
```
success_percent          → % de eventos válidos no batch
successful_expectations  → nº de validações que passaram
failed_expectations      → nº de validações que falharam
total_rows               → total de eventos no batch
tag: source              → "sensor_readings" ou "satellite-hotspots"
```

**`pipeline_latency` measurement** — tempo entre criação e persistência:
```
latency_ms   → milissegundos desde o timestamp do evento até ser gravado
tag: topic   → de onde veio o evento
tag: grid_id → zona geográfica
```

**`rejected_events` measurement** — contagem de rejeições por zona:
```
count        → sempre 1 (um ponto por rejeição)
tag: grid_id → zona onde ocorreu a rejeição
tag: reason  → motivo(s) separados por vírgula
```

**`rejected_event_detail` measurement** — detalhe completo de cada evento rejeitado (útil para debugging no Grafana).

### Para que serve no Grafana
Estes dados são visualizados no dashboard Grafana:
- Gráfico de `success_percent` ao longo do tempo
- Alertas quando `success_percent < 80%`
- Mapa de calor de rejeições por zona (`grid_id`)
- Histograma de latência da pipeline

---

## 7. spark_streaming_agregacao.py

### O que é
Job **Apache Spark Structured Streaming** que lê os 3 topics do Kafka em simultâneo, faz um join temporal, e calcula um índice de risco composto por zona geográfica.

### Quando é usado
Arranca automaticamente como container Docker (`spark-streaming`) com `docker compose up`. Corre continuamente, processando um micro-batch a cada 30 segundos.

### O que faz — fluxo detalhado

```
Kafka (sensor-events)      ──► agrega por janela 10min/zona ──┐
Kafka (satellite-hotspots) ──► agrega por janela 10min/zona ──┼──► join ──► risco_composto
Kafka (weather-data)       ──► agrega por janela 10min/zona ──┘
                                                                    │
                                                          ┌─────────┴──────────┐
                                                          ▼                    ▼
                                                       Console            S3 Parquet
                                                    (demo ao vivo)     (arquivo data lake)
```

### O conceito de Sliding Window (janela deslizante)
Em vez de processar cada evento individualmente, o Spark agrupa eventos numa **janela de tempo deslizante**:
- **Janela de 10 minutos** — cada agregação cobre 10 minutos de dados
- **Slide de 5 minutos** — a janela avança de 5 em 5 minutos
- **Resultado:** cada zona aparece em duas janelas sobrepostas

Exemplo para `PT-NORTE-01`:
```
Janela 1: 19:00 → 19:10  (3 eventos, risk_medio=64.7)
Janela 2: 19:05 → 19:15  (5 eventos, risk_medio=71.2)  ← sobreposição de 5 min
```

### O Watermark de 2 minutos
Eventos de sensores podem chegar com atraso (problemas de rede, etc.). O watermark diz ao Spark:
> "Aceito eventos com até 2 minutos de atraso. Além disso, descarto."

Sem watermark, o Spark guardaria estado para sempre à espera de eventos tardios.

### O Join dos 3 Streams
Após a agregação individual de cada stream, o Spark faz um **left join** por `(janela, grid_id)`:
- `left join` garante que aparecem resultados mesmo quando um stream não tem dados numa zona (ex: sem hotspots NASA numa janela específica — as colunas `n_hotspots` e `frp_medio` ficam a 0)

### O Índice de Risco Composto
A coluna mais importante do output — combina as 3 fontes numa única métrica 0-100:

```
risco_composto =
  risk_medio_sensor   × 0.60    (60% — leituras IoT)
  + frp_medio/200×100 × 0.25    (25% — intensidade NASA, normalizado para max=200MW)
  + vento_max_ipma/150×100 × 0.15  (15% — vento IPMA, normalizado para max=150km/h)
```

**Justificação dos pesos:**
- Os sensores IoT têm a maior resolução temporal → 60%
- FRP da NASA é o indicador mais fiável de incêndio activo → 25%
- Vento é o factor meteorológico mais crítico para propagação → 15%

### Colunas do output

| Coluna | Origem | Descrição |
|---|---|---|
| `janela_inicio` | Spark | Início da janela de 10 min |
| `janela_fim` | Spark | Fim da janela de 10 min |
| `grid_id` | Todos | Zona de Portugal |
| `n_leituras_sensor` | sensor-events | Nº de leituras IoT na janela |
| `risk_medio_sensor` | sensor-events | Risco médio calculado pelo sensor |
| `risk_maximo_sensor` | sensor-events | Pico de risco na janela |
| `temp_media` | sensor-events | Temperatura média (°C) |
| `humidade_media` | sensor-events | Humidade média (%) |
| `vento_medio` | sensor-events | Vento médio (km/h) |
| `n_hotspots` | satellite-hotspots | Hotspots NASA detectados |
| `frp_medio` | satellite-hotspots | Fire Radiative Power médio (MW) |
| `frp_maximo` | satellite-hotspots | FRP máximo na janela (MW) |
| `temp_max_media` | weather-data | Temperatura máxima IPMA (°C) |
| `humidade_ipma` | weather-data | Humidade IPMA (%) |
| `vento_max_ipma` | weather-data | Vento máximo IPMA (km/h) |
| `precipitacao_media` | weather-data | Precipitação média (mm) |
| `risco_composto` | Calculado | **Índice final 0-100** |

### Dois destinos de escrita

**Console** (modo `update`) — mostra as janelas que mudaram a cada 30 segundos. Útil para demonstração ao vivo.

**S3 Parquet** (modo `append`) — só grava janelas completamente fechadas. Com watermark de 2 min e janelas de 10 min, os primeiros ficheiros aparecem ~12 minutos após o arranque. Caminho: `s3://forest-risk-datalake/agregados_streaming/`

### Como correr manualmente (se necessário)
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /home/jovyan/spark-jobs/spark_streaming_agregacao.py
```

---

## 8. carga_historico_s3.py

### O que é
Script Python que **carrega dados históricos das EDAs** para o S3 em formato Parquet particionado. Só é chamado quando o `check_and_load.sh` detecta que os Parquet das EDAs são mais recentes que os dados no S3.

### Quando é usado
Chamado pelo `check_and_load.sh` (container `carga-historico`) nas seguintes condições:
- S3 vazio e Parquet das EDAs disponíveis → carrega
- Parquet das EDAs mais recentes que os dados no S3 → recarrega

**Não é chamado automaticamente** se não houver Parquet das EDAs — o `check_and_load.sh` avisa e para sem chamar este script.

### Modo de funcionamento — apenas Parquet das EDAs

O script lê os Parquet gerados pelas EDAs da Pessoa B:
- `notebooks/Filtragem_Parquet/` → gerado pela `EDA_NASA.py` (hotspots NASA FIRMS)
- `notebooks/ERA5_Parquet/` → gerado pela `EDA_ERA5.py` (meteorologia ERA5)

Em ambos os casos, o script **adiciona o `grid_id`** mapeando as coordenadas GPS para a zona de Portugal mais próxima (os 10 centroides da grelha). Este enriquecimento não é feito pelas EDAs.

### O que grava no S3

**`s3://forest-risk-datalake/hotspots/`** — dados NASA FIRMS históricos:
```
hotspots/
└── ano=2020/mes=1/grid_id=PT-ALENTEJO-01/dados.parquet
└── ano=2020/mes=1/grid_id=PT-NORTE-02/dados.parquet
└── ... (particionado por ano, mês e zona)
```

**`s3://forest-risk-datalake/meteorologia/`** — dados ERA5 (só se EDA correu):
```
meteorologia/
└── ano=2020/mes=1/dados.parquet
└── ... (particionado por ano e mês)
```

### Particionamento e porque importa
O Parquet é particionado por `ano`, `mes`, e `grid_id`. Quando a Pessoa B quiser treinar o modelo ML, pode ler só os dados que precisa:
```python
# Lê só o verão de 2022 no Norte — muito mais rápido que ler tudo
df = spark.read.parquet("s3://forest-risk-datalake/hotspots/ano=2022/mes=8/grid_id=PT-NORTE-02/")
```

### Endpoint configurável (LocalStack vs AWS real)
```python
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
# Se definido → LocalStack (desenvolvimento)
# Se None     → AWS S3 real (produção)
```
O mesmo código funciona nos dois ambientes sem alterações.

### Como correr manualmente
```bash
# No terminal do Jupyter
python /home/jovyan/work/carga_historico_s3.py
```

---

## 9. Sequência de arranque

Quando se executa `docker compose up`, os serviços arrancam nesta ordem e os scripts são chamados assim:

```
1. zookeeper (base do Kafka)
2. kafka (broker de mensagens)
3. cassandra (base de dados)
4. localstack (S3 simulado)
5. influxdb (métricas)

--- após saúde confirmada ---

6. kafka-setup       → cria os 5 topics
7. cassandra-setup   → cria as tabelas CQL
8. carga-historico   → check_and_load.sh:
                         ├── sem Parquet EDAs → avisa e para
                         ├── Parquet EDAs mais recentes que S3 → carga_historico_s3.py
                         └── S3 actualizado → salta

9. grafana           → dashboard de monitorização
10. jupyter          → ambiente de desenvolvimento

11. producer-sensores → producer_sensores.py  (loop infinito)
12. producer-apis     → producer_apis_reais.py (loop com intervalos)
13. consumer          → consumer_kafka_cassandra.py (loop infinito)
14. spark-streaming   → spark_streaming_agregacao.py (loop infinito)
```

---

## 10. Dependências entre ficheiros

```
producer_sensores.py      ──► Kafka (sensor-events, weather-data)
                                      │
producer_apis_reais.py    ──► Kafka ──┤ (satellite-hotspots, weather-data, sensor-events)
                                      │
                         consumer_kafka_cassandra.py
                                │     importa:
                                │     ├── data_quality_validation.py
                                │     └── data_quality.py
                                │
                    ┌───────────┼───────────────┐
                    ▼           ▼               ▼
                Cassandra    InfluxDB         Kafka
            (sensor_readings) (métricas)  (fire-alerts)
            (fire_alerts)                 (data-quality-metrics)

Kafka (3 topics) ──► spark_streaming_agregacao.py
                                │
                         ┌──────┴──────┐
                         ▼             ▼
                      Console        S3 Parquet
                                (agregados_streaming/)

Parquet EDAs ──► check_and_load.sh ──► carga_historico_s3.py ──► S3 Parquet
(Filtragem_Parquet/                                          (hotspots/ + meteorologia/)
 ERA5_Parquet/)
```

### Ficheiros que importam outros ficheiros

| Ficheiro | Importa | Para quê |
|---|---|---|
| `consumer_kafka_cassandra.py` | `data_quality_validation.py` | Validar e separar eventos |
| `consumer_kafka_cassandra.py` | `data_quality.py` | Escrever métricas no InfluxDB |

### Ficheiros autónomos (não são importados por ninguém)
- `producer_sensores.py` — corre directamente
- `producer_apis_reais.py` — corre directamente
- `spark_streaming_agregacao.py` — corre via spark-submit
- `carga_historico_s3.py` — corre directamente (chamado pelo `check_and_load.sh`)
- `check_and_load.sh` — script bash, chamado pelo container `carga-historico`

### Ficheiros que são apenas módulos (não correm directamente)
- `data_quality_validation.py` — importado pelo consumer
- `data_quality.py` — importado pelo consumer

---

*Documentação gerada para o projeto Forest Risk Monitoring System — ISEP 2024/2025*
