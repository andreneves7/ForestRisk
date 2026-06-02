# Forest Risk Monitoring System

**Plataforma integrada de deteção e previsão de risco de incêndio florestal**  
ISEP — Pós-Graduação em Big Data & Data Science 2024/2025

---

## Visão geral

Pipeline de inteligência florestal que unifica dados de sensores IoT, satélites NASA FIRMS e meteorologia IPMA para detetar e prever risco de incêndio em tempo real.

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

## Estrutura do repositório

```
projeto_docker/
├── docker-compose.yml          # Ambiente completo em 1 ficheiro
├── Dockerfile                  # Imagem dos producers/consumer (Python 3.11)
├── requirements.txt            # Dependências Python
├── README.md                   # Este ficheiro
├── .gitignore
├── cassandra/
│   └── init.cql               # Schema criado automaticamente ao arrancar
├── localstack/
│   └── init-s3.sh             # Buckets S3 criados automaticamente ao arrancar
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── influxdb.yml   # Datasource InfluxDB pré-configurado
│       └── dashboards/
│           └── grafana_dashboard_pipeline.json
├── notebooks/                  # Montado no Jupyter e usado pelos containers
│   ├── 01_data_quality.ipynb
│   ├── producer_sensores.py        # Producer de dados simulados (mock)
│   ├── producer_apis_reais.py      # Producer de APIs reais (NASA/IPMA/ICNF)
│   ├── consumer_kafka_cassandra.py # Consumer Kafka -> validação -> Cassandra
│   ├── data_quality.py             # Envio de métricas para InfluxDB
│   └── data_quality_validation.py  # Validação Great Expectations
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

# Ver logs em tempo real de um serviço específico
docker compose logs -f spark-streaming
```

A primeira vez demora 3–5 minutos (download das imagens + healthchecks).  
O Cassandra é o serviço mais lento a ficar pronto (~90 segundos).

A pipeline completa arranca com um único comando: producer, consumer e Spark
Streaming começam a trabalhar automaticamente, sem intervenção manual.

---

## URLs de acesso

| Serviço | URL | Credenciais |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | Token: `forestrisk` |
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / forestrisk123 |
| InfluxDB | http://localhost:8086 | admin / forestrisk123 |
| Spark UI | http://localhost:4040 | — (ativo enquanto job corre) |
| LocalStack S3 | http://localhost:4566 | — |
| Cassandra | localhost:9042 | — |

---

## Componentes da pipeline

| Serviço | Função | Arranque |
|---|---|---|
| `producer` | Gera leituras de sensores e publica no Kafka | Automático |
| `consumer` | Lê do Kafka, valida e grava no Cassandra + InfluxDB | Automático |
| `spark-streaming` | Agrega risco por zona em janelas de 10 min | Automático |

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

## Producers — dados simulados vs reais

O projeto tem dois producers. Por omissão corre o de dados simulados
(definido no `docker-compose.yml`). Para usar APIs reais, troca o comando
do serviço `producer`:

```yaml
# Dados simulados (omissão)
command: python notebooks/producer_sensores.py

# Dados reais (requer NASA FIRMS API key)
#command: python notebooks/producer_apis_reais.py
```

O producer de APIs reais necessita de uma key gratuita da NASA FIRMS:
https://firms.modaps.eosdis.nasa.gov/api/area/

---

## Topics Kafka criados automaticamente

| Topic | Partições | Retenção | Conteúdo |
|---|---|---|---|
| `sensor-events` | 3 | 7 dias | Leituras IoT (temp, humidade, vento) |
| `satellite-hotspots` | 3 | 7 dias | Hotspots NASA FIRMS |
| `weather-data` | 3 | 7 dias | Dados IPMA |
| `fire-alerts` | 1 | 30 dias | Alertas gerados pelo motor de risco |
| `data-quality-metrics` | 1 | 7 dias | Eventos rejeitados na validação |

---

## Processamento Spark Structured Streaming

O serviço `spark-streaming` lê o topic `sensor-events` e calcula, para cada
zona geográfica, médias de risco numa janela deslizante de 10 minutos
(slide de 5 min, watermark de 2 min). Os resultados são impressos no log:

```bash
docker compose logs -f spark-streaming
```

---

## Parar o ambiente

```bash
# Parar mantendo os dados (volumes persistem)
docker compose down

# Parar e apagar TUDO (volumes incluídos)
docker compose down -v
```

---

## Resolução de problemas comuns

**Cassandra demora muito a ficar healthy**  
Normal na primeira execução. Aguarda 2–3 minutos. Se falhar, corre:
```bash
docker compose restart cassandra
```

**Porta já em uso**  
Verifica se tens outro serviço nas portas 8080, 8086, 8888, 9042 ou 29092.

**Jupyter não arranca**  
O Jupyter instala packages ao iniciar — aguarda 1–2 minutos e refresca.

**Spark streaming demora a mostrar dados**  
Na primeira execução descarrega o conector Kafka (~1 min). As janelas de
10 min também precisam de acumular eventos antes de mostrar agregações.

---

*ISEP — Big Data & Data Science | Projeto Forest Risk Monitoring System*
