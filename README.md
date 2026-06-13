# Forest Risk Monitoring System

**Plataforma integrada de deteção e previsão de risco de incêndio florestal**  
ISEP — Pós-Graduação em Big Data & Data Science 2024/2025

---

## Visão geral

Pipeline de inteligência florestal que unifica dados de sensores IoT, satélites NASA FIRMS, meteorologia IPMA e cartografia florestal ICNF para detetar e prever risco de incêndio em tempo real.

**Stack principal:** Kafka · Spark Structured Streaming · Cassandra · S3 (LocalStack) · InfluxDB · Grafana · Python (scikit-learn / XGBoost)

---

## Pré-requisitos

| Requisito | Versão mínima |
|---|---|
| Docker Desktop | 24.0 |
| Docker Compose | 2.20 |
| RAM disponível | 8 GB |
| Espaço em disco | 5 GB |

---

## Configuração inicial (obrigatória)

Antes de arrancar, cria o ficheiro `.env` na pasta do projecto:

```bash
cp .env.example .env
```

Edita o `.env` e preenche as credenciais:

```env
# NASA FIRMS API key — obrigatória para hotspots de satélite em tempo real
# Obtém gratuitamente em: https://firms.modaps.eosdis.nasa.gov/api/area/
NASA_FIRMS_KEY=a_tua_key_aqui

# Passwords internas (podes manter os valores por omissão em desenvolvimento)
INFLUXDB_PASSWORD=forestrisk123
INFLUXDB_TOKEN=forest-risk-influx-token-2024
GRAFANA_PASSWORD=forestrisk123
```

> **Nota:** Sem a `NASA_FIRMS_KEY`, o sistema funciona na mesma com dados simulados — apenas o producer de APIs reais não envia hotspots de satélite.

---

## Estrutura do repositório

```
projeto_docker/
├── docker-compose.yml              # Ambiente completo (15 serviços)
├── Dockerfile                      # Imagem dos producers/consumer (Python 3.11)
├── requirements.txt                # Dependências Python
├── .env                            # Credenciais (não entra no Git)
├── .env.example                    # Template de credenciais (entra no Git)
├── .gitignore
├── cassandra/
│   └── init.cql                   # Schema criado automaticamente ao arrancar
├── localstack/
│   ├── init-s3.sh                 # Buckets S3 criados automaticamente
│   └── check_and_load.sh          # Verifica Parquet EDAs vs S3 e carrega se necessário
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── influxdb.yml
│       └── dashboards/
│           └── grafana_dashboard_pipeline.json
├── notebooks/                      # Montado no Jupyter e nos containers
│   ├── NASACSV/                   # CSV históricos NASA FIRMS (2020-2024)
│   ├── producer_sensores.py       # Producer IoT simulado
│   ├── producer_apis_reais.py     # Producer APIs reais (NASA/IPMA/ICNF)
│   ├── consumer_kafka_cassandra.py # Consumer Kafka → validação → Cassandra
│   ├── data_quality.py            # Métricas de qualidade → InfluxDB
│   ├── data_quality_validation.py # Validação Great Expectations + NASA
│   ├── carga_historico_s3.py      # Carga histórica NASA/ERA5 → S3
│   └── validar_datalake.py        # Validação do data lake S3
└── spark/
    └── jobs/
        └── spark_streaming_agregacao.py  # Spark Structured Streaming
```

---

## Arrancar o ambiente

```bash
# Subir todos os serviços em background
docker compose up -d

# Verificar estado (aguardar todos "healthy")
docker compose ps

# Ver logs de um serviço específico
docker compose logs -f spark-streaming
```

A primeira vez demora 3–5 minutos (download das imagens + healthchecks).  
O Cassandra é o serviço mais lento a ficar pronto (~90 segundos).

**A pipeline completa arranca com um único comando** — os dois producers,
o consumer e o Spark Streaming começam automaticamente, sem intervenção manual.

---

## URLs de acesso

| Serviço | URL | Credenciais |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | Token: `forestrisk` |
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / ver `.env` |
| InfluxDB | http://localhost:8086 | admin / ver `.env` |
| Spark UI | http://localhost:4040 | — (activo enquanto job corre) |
| LocalStack S3 | http://localhost:4566 | key: test / secret: test |
| Cassandra | localhost:9042 | — |

---

## Serviços da pipeline

| Serviço | Função | Arranque |
|---|---|---|
| `producer-sensores` | Gera leituras IoT simuladas → `sensor-events` + `weather-data` | Automático |
| `producer-apis` | Consulta NASA FIRMS, IPMA, ICNF → topics reais | Automático |
| `consumer` | Lê Kafka, valida, grava no Cassandra + InfluxDB + `fire-alerts` | Automático |
| `spark-streaming` | Join 3 streams, calcula risco composto → console + S3 | Automático |
| `carga-historico` | Verifica Parquet das EDAs e carrega para o S3 se mais recentes | Automático |

---

## Fontes de dados

| Fonte | Tipo | Intervalo | Topic Kafka |
|---|---|---|---|
| Sensores IoT simulados | Mock | 2 segundos | `sensor-events`, `weather-data` |
| NASA FIRMS (satélite) | Real | 1 hora | `satellite-hotspots` |
| IPMA (meteorologia) | Real | 30 minutos | `weather-data`, `sensor-events` |
| ICNF (vegetação COS2018) | Real estático | 1 vez/dia | `sensor-events` |

---

## Topics Kafka

| Topic | Partições | Retenção | Conteúdo |
|---|---|---|---|
| `sensor-events` | 3 | 7 dias | Leituras IoT simuladas + observações IPMA reais |
| `satellite-hotspots` | 3 | 7 dias | Hotspots NASA FIRMS em tempo real |
| `weather-data` | 3 | 7 dias | Dados meteorológicos IPMA |
| `fire-alerts` | 1 | 30 dias | Alertas quando temp>35°C E hum<20% E vento>30km/h |
| `data-quality-metrics` | 1 | 7 dias | Eventos rejeitados na validação (quarentena) |

---

## Processamento Spark Structured Streaming

O serviço `spark-streaming` lê **3 streams em simultâneo** e calcula um
índice de risco composto por zona em janelas deslizantes de 10 minutos:

```
sensor-events      (60%) ─┐
satellite-hotspots (25%) ─┼─ join por grid_id + janela → risco_composto (0-100)
weather-data       (15%) ─┘
```

- **Sliding window:** 10 min / slide 5 min / watermark 2 min
- **Destinos:** console (demo ao vivo) + S3 Parquet (arquivo)

```bash
docker compose logs -f spark-streaming
```

---

## Data Lake S3

O data lake `forest-risk-datalake` é populado pelo `carga-historico` **quando as EDAs da Pessoa B tiverem corrido**:

```
s3://forest-risk-datalake/
├── hotspots/            ← NASA FIRMS histórico (após EDA_NASA.py)
│   └── ano=YYYY/mes=MM/grid_id=PT-XXX/*.parquet
├── meteorologia/        ← ERA5 (após EDA_ERA5.py)
└── agregados_streaming/ ← Spark streaming em tempo real (automático)
```

> **Nota:** O `carga-historico` só carrega dados para o S3 quando encontra Parquet gerados pelas EDAs. Se as EDAs ainda não correram, o serviço avisa nos logs e não carrega nada. Ver logs: `docker compose logs carga-historico`

Para validar o estado do data lake, corre no Jupyter:
```bash
python /home/jovyan/work/validar_datalake.py
```

---

## Qualidade de dados

O sistema tem validação em dois níveis:

**Sensores IoT** — Great Expectations valida cada micro-batch:
- `temp_celsius` entre -10°C e 60°C
- `humidity_pct` entre 0% e 100%
- `wind_kmh` entre 0 e 150 km/h
- `risk_score` entre 0 e 100
- `grid_id` não nulo

**Hotspots NASA** — regras físicas específicas:
- `frp_mw` entre 0 e 5000 MW
- `brightness` entre 200 K e 500 K
- Coordenadas dentro de Portugal Continental
- `grid_id` não pode ser PT-UNKNOWN

Eventos que falham a validação vão para quarentena (`data-quality-metrics`)
e as métricas aparecem no Grafana.

---

## Alertas de incêndio (`fire-alerts`)

O consumer publica automaticamente no topic `fire-alerts` quando:

```
temperatura > 35°C  E  humidade < 20%  E  vento > 30 km/h
```

Para ver alertas em tempo real:
```bash
docker compose logs consumer | grep "FIRE-ALERT"
```

---

## Ligar ao Cassandra

```bash
docker exec -it cassandra cqlsh
```

```cql
USE forest_risk;
DESCRIBE TABLES;
SELECT * FROM fire_alerts LIMIT 10;
SELECT COUNT(*) FROM sensor_readings;
```

---

## Dados históricos — população do S3

O S3 só é populado quando as EDAs da Pessoa B tiverem corrido. O `carga-historico` verifica automaticamente ao arrancar:

```
EDA_NASA.py correu → Filtragem_Parquet/ existe → carrega hotspots/ no S3
EDA_ERA5.py correu → ERA5_Parquet/ existe     → carrega meteorologia/ no S3
Nenhuma EDA correu → avisa nos logs e não carrega nada
```

Para ver o estado da verificação:
```bash
docker compose logs carga-historico
```

Para forçar uma re-carga manual após correr as EDAs:
```bash
docker compose restart carga-historico
```

Os CSV históricos em `notebooks/NASACSV/` são usados **apenas pelas EDAs** (não pelo carga-historico directamente).

---

## Parar o ambiente

```bash
# Parar mantendo os dados (volumes persistem — uso normal)
docker compose down

# Parar e apagar TUDO incluindo dados S3 e Cassandra
docker compose down -v
```

> **Atenção:** `docker compose down -v` apaga os dados históricos do S3 e do Cassandra.
> Na próxima vez que arrancar, o `carga-historico` recarrega automaticamente desde que os Parquet das EDAs existam.

---

## Resolução de problemas comuns

**Kafka unhealthy — `InconsistentClusterIdException`**
```bash
docker compose down
docker volume rm projeto_docker_kafka_data
docker compose up
```

**Cassandra demora a ficar healthy**  
Normal na primeira execução (~90s). Se falhar:
```bash
docker compose restart cassandra
```

**Variáveis de ambiente em falta**  
```
The "INFLUXDB_TOKEN" variable is not set
```
Verifica se o ficheiro `.env` existe na pasta do projecto e tem todas as variáveis do `.env.example`.

**Porta já em uso**  
Verifica se tens outro serviço nas portas 8080, 8086, 8888, 9042 ou 29092.

**Spark streaming demora a mostrar dados**  
Na primeira execução descarrega os packages (~1-2 min). As janelas de
10 min precisam de acumular eventos antes de fechar e mostrar agregações (~12 min).

**S3 histórico vazio após `docker compose down -v`**
O `carga-historico` verifica ao arrancar se há Parquet das EDAs e recarrega automaticamente se existirem. Se as EDAs ainda não tiverem corrido, o S3 fica vazio — é o comportamento esperado.

```bash
# Ver o que o carga-historico decidiu fazer
docker compose logs carga-historico
```

---

*ISEP — Big Data & Data Science | Projeto Forest Risk Monitoring System*
