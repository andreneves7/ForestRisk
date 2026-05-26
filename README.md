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
├── README.md                   # Este ficheiro
├── .gitignore
├── cassandra/
│   └── init.cql               # Schema criado automaticamente ao arrancar
├── localstack/
│   └── init-s3.sh             # Buckets S3 criados automaticamente ao arrancar
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── influxdb.yml   # Datasource InfluxDB pré-configurado
├── notebooks/                  # Montado no Jupyter (/home/jovyan/work)
│   ├── 01_data_quality.ipynb
│   ├── producer_sensores.py
│   └── producer_apis_reais.py
├── producers/                  # Producers Python (ver secção abaixo)
│   ├── producer_sensores.py
│   └── producer_apis_reais.py
└── spark/
    └── jobs/                  # Scripts Spark Structured Streaming
```

---

## Arrancar o ambiente

```bash
# Subir todos os serviços em background
docker compose up -d

# Verificar estado (aguardar todos "healthy")
docker compose ps

# Ver logs em tempo real de um serviço específico
docker compose logs -f jupyter
```

A primeira vez demora 3–5 minutos (download das imagens + healthchecks).  
O Cassandra é o serviço mais lento a ficar pronto (~90 segundos).

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

## Ligar ao Cassandra

```bash
docker exec -it cassandra cqlsh
```

```cql
USE forest_risk;
DESCRIBE TABLES;
SELECT * FROM fire_alerts LIMIT 10;
```

---

## Correr os producers

Os producers publicam dados nos topics Kafka em tempo real.  
Podem correr diretamente no host (com Python instalado) ou dentro do Jupyter.

```bash
# No host — requer: pip install kafka-python requests
python producers/producer_sensores.py
python producers/producer_apis_reais.py

# Ou dentro do Jupyter (terminal integrado):
# Os packages já estão instalados na imagem
```

---

## Topics Kafka criados automaticamente

| Topic | Partições | Retenção | Conteúdo |
|---|---|---|---|
| `sensor-events` | 3 | 7 dias | Leituras IoT simuladas (temp, humidade, vento) |
| `satellite-hotspots` | 3 | 7 dias | Hotspots NASA FIRMS |
| `weather-data` | 3 | 7 dias | Dados IPMA |
| `fire-alerts` | 1 | 30 dias | Alertas gerados pelo motor de risco |
| `data-quality-metrics` | 1 | 7 dias | Métricas de qualidade dos dados |

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
Verifica se tens outro serviço nas portas 8080, 8086, 8888, 9042 ou 29092:
```bash
lsof -i :8080
```

**Jupyter não arranca**  
O Jupyter instala packages ao iniciar — aguarda 1–2 minutos e refresca.

---

*ISEP — Big Data & Data Science | Projeto Forest Risk Monitoring System*
