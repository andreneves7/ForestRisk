# Design Spec — S3 Buckets + MLflow Integration

**Data:** 2026-05-29  
**Projeto:** Forest Risk Monitoring System — ISEP PG Big Data 2024/2025  
**Ambiente:** LocalStack (S3 emulado, desenvolvimento local)  
**Abordagem escolhida:** Opção A — Extensão direta do Consumer

---

## 1. Contexto

O projeto já tem dois buckets S3 criados no LocalStack via `localstack/init-s3.sh`:

- `forest-risk-datalake` — previsto para dados históricos Parquet
- `forest-risk-models` — previsto para modelos ML serializados com MLflow

Actualmente o `init-s3.sh` apenas cria os buckets sem configuração adicional, o consumer Kafka não escreve para S3, e o MLflow não está configurado. Esta spec define o que é necessário para tornar ambos os buckets funcionais.

---

## 2. Âmbito

### Incluído
- Configuração completa do `init-s3.sh` (versioning, estrutura de prefixos)
- Módulo `s3_writer.py` — escrita Parquet no consumer após validação
- Serviço MLflow Tracking Server no `docker-compose.yml`
- Integração MLflow ↔ LocalStack S3 (artefactos em `forest-risk-models`)
- Dependências (`pyarrow`, `mlflow`) no ambiente Jupyter
- Documentação de utilização

### Excluído
- SageMaker (documentado como trabalho futuro)
- AWS real (fora do scope académico)
- Power BI (sem emulação local)
- Spark Structured Streaming dedicado para S3

---

## 3. Arquitectura dos Buckets

### 3.1 `forest-risk-datalake`

Dados históricos em formato Parquet, particionados por tipo de fonte, ano, mês e zona geográfica.

```
forest-risk-datalake/
├── sensor_readings/
│   └── year=YYYY/month=MM/zone=<grid_id>/
│       └── batch_<timestamp_unix>.parquet
├── satellite_hotspots/
│   └── year=YYYY/month=MM/zone=<grid_id>/
│       └── batch_<timestamp_unix>.parquet
└── weather_data/
    └── year=YYYY/month=MM/zone=<grid_id>/
        └── batch_<timestamp_unix>.parquet
```

**Configuração:**
- Versioning: Enabled
- Região: eu-west-1
- Prefixo de partição derivado do timestamp UTC do primeiro registo do batch

### 3.2 `forest-risk-models`

Artefactos de modelos ML geridos pelo MLflow.

```
forest-risk-models/
└── mlflow/
    └── <experiment_id>/
        └── <run_id>/
            └── artifacts/
                └── model/
                    ├── model.pkl (ou model.xgb)
                    └── MLmodel
```

**Configuração:**
- Versioning: Enabled (necessário para MLflow Model Registry)
- Região: eu-west-1

---

## 4. Módulo `s3_writer.py`

Novo ficheiro em `notebooks/s3_writer.py`.

**Interface pública:**

```python
def write_parquet_to_s3(records: list[dict], topic: str) -> bool:
    """
    Agrupa records por grid_id, serializa cada grupo em Parquet e faz upload.
    Retorna True se todos os uploads tiverem sucesso, False se algum falhar.
    Nunca propaga excepção — pipeline não é bloqueado por falha S3.
    """
```

**Lógica interna:**
1. Agrupa `records` por `grid_id` (um batch pode conter múltiplas zonas)
2. Para cada grupo `(grid_id, records_do_grupo)`:
   a. Converte em `pandas.DataFrame`
   b. Serializa para buffer em memória via `pyarrow.parquet`
   c. Calcula prefixo: `<s3_prefix>/year=<Y>/month=<MM>/zone=<grid_id>/batch_<ts_unix>.parquet`
   d. Timestamp derivado do campo `timestamp` do primeiro registo do grupo
3. Upload via `boto3.client('s3')` com endpoint `AWS_ENDPOINT_URL`
4. Erros por grupo são capturados e logados; os restantes grupos continuam

**Mapeamento topic → prefixo S3:**

| Tópico Kafka | Prefixo S3 |
|---|---|
| `sensor-events` | `sensor_readings` |
| `satellite-hotspots` | `satellite_hotspots` |
| `weather-data` | `weather_data` |

**Variáveis de ambiente utilizadas** (já existentes no docker-compose):
- `AWS_ENDPOINT_URL`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

---

## 5. Alteração ao Consumer

Em `notebooks/consumer_kafka_cassandra.py`:

**`process_batch` (sensor-events)** — após o loop `for ev in valid_evs`:
```python
from s3_writer import write_parquet_to_s3
write_parquet_to_s3(valid_evs, topic=topic)
```

**`consume_satellite_hotspots`** — acumula eventos numa lista local e escreve por batch de 10 ou timeout 60s (o consumer existente é evento-a-evento; adicionar acumulação local antes do S3 para não gerar um ficheiro Parquet por evento):
```python
hotspot_buffer.append(ev)
if len(hotspot_buffer) >= 10:
    write_parquet_to_s3(hotspot_buffer, topic="satellite-hotspots")
    hotspot_buffer.clear()
```

**Regra geral:** falha no S3 **nunca** cancela a escrita no Cassandra já efectuada.

---

## 6. MLflow Tracking Server

### Novo serviço no `docker-compose.yml`

```yaml
mlflow:
  image: ghcr.io/mlflow/mlflow:v2.11.1
  container_name: mlflow
  ports:
    - "5000:5000"
  environment:
    - AWS_ENDPOINT_URL=http://localstack:4566
    - AWS_ACCESS_KEY_ID=test
    - AWS_SECRET_ACCESS_KEY=test
    - AWS_DEFAULT_REGION=eu-west-1
    - MLFLOW_S3_ENDPOINT_URL=http://localstack:4566
  command: >
    mlflow server
    --host 0.0.0.0
    --port 5000
    --backend-store-uri sqlite:///mlflow/mlruns.db
    --default-artifact-root s3://forest-risk-models/mlflow
  volumes:
    - mlflow_data:/mlflow
  depends_on:
    - localstack
```

### Utilização nos notebooks Jupyter

```python
import mlflow
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("forest-risk-fire-prediction")

with mlflow.start_run():
    mlflow.log_param("n_estimators", 200)
    mlflow.log_metric("f1_score", 0.83)
    mlflow.sklearn.log_model(model, "model")
```

**UI:** `http://localhost:5000`

---

## 7. Dependências a adicionar

No serviço Jupyter do `docker-compose.yml`, adicionar ao `pip install` existente na chave `command`:

```
pyarrow>=14.0.0 mlflow==2.11.1
```

Linha actual termina em `great-expectations==0.18.15`. Ficará:
```
... great-expectations==0.18.15 pyarrow>=14.0.0 mlflow==2.11.1 &&
```

No `requirements.txt` (producers/consumer):
```
pyarrow>=14.0.0
```

`boto3==1.34.0` já existe em ambos. `mlflow` só é necessário nos notebooks Jupyter.

---

## 8. Alterações ao `init-s3.sh`

```bash
#!/bin/bash
echo "A criar buckets S3 no LocalStack..."

awslocal s3 mb s3://forest-risk-datalake --region eu-west-1
awslocal s3 mb s3://forest-risk-models   --region eu-west-1

# Versioning em ambos os buckets
awslocal s3api put-bucket-versioning \
  --bucket forest-risk-datalake \
  --versioning-configuration Status=Enabled

awslocal s3api put-bucket-versioning \
  --bucket forest-risk-models \
  --versioning-configuration Status=Enabled

echo "Buckets criados:"
awslocal s3 ls
```

---

## 9. Fluxo de dados completo após implementação

```
Producers (sensor / NASA / IPMA)
    │
    ▼
Kafka Topics (sensor-events / satellite-hotspots / weather-data)
    │
    ▼
consumer_kafka_cassandra.py
    │
    ├── Great Expectations (validação)
    │
    ├── [VÁLIDO]
    │   ├──► Cassandra (sensor_readings / fire_alerts)   ← hot data
    │   └──► s3_writer.py ──► S3 forest-risk-datalake    ← histórico Parquet
    │
    └── [REJEITADO]
        └──► InfluxDB (rejected_events + rejected_event_detail)
                │
                ▼
            Grafana (dashboards qualidade)

S3 forest-risk-datalake
    │  (leitura para treino)
    ▼
Jupyter — treino XGBoost / Random Forest
    │
    ├── MLflow Tracking Server (http://localhost:5000)
    │   ├── params + métricas
    │   └── artefactos ──► S3 forest-risk-models
    │
    └── Cassandra risk_predictions (24h / 48h / 72h)
            │
            ▼
        Grafana / Power BI
```

---

## 10. Trabalho futuro (fora do scope)

- **AWS SageMaker** — substitui Jupyter training + MLflow em produção. Os buckets S3 são compatíveis sem alterações (mesmo prefixo, mesmo formato).
- **AWS real** — trocar `endpoint_url=http://localstack:4566` por credenciais AWS reais; resto do código não muda.
- **Power BI** — ligar ao Cassandra via ODBC ou exportar previsões de S3 para Power BI Desktop.

---

## 11. Ficheiros afectados

| Ficheiro | Tipo de alteração |
|---|---|
| `localstack/init-s3.sh` | Modificar — adicionar versioning no models bucket |
| `docker-compose.yml` | Modificar — adicionar serviço mlflow + dependências Jupyter |
| `notebooks/s3_writer.py` | Criar — módulo novo |
| `notebooks/consumer_kafka_cassandra.py` | Modificar — chamar s3_writer após Cassandra |
| `requirements.txt` | Modificar — adicionar pyarrow |
