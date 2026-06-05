# S3 Buckets + MLflow Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tornar os buckets `forest-risk-datalake` e `forest-risk-models` funcionais — escrita automática de Parquet pelo consumer Kafka e rastreamento de modelos ML via MLflow Tracking Server.

**Architecture:** O consumer Kafka existente é estendido para, após escrever no Cassandra, chamar um novo módulo `s3_writer.py` que agrupa registos válidos por `grid_id` e faz upload em Parquet particionado. Um serviço MLflow é adicionado ao docker-compose com artefactos apontando para `forest-risk-models` no LocalStack.

**Tech Stack:** Python 3.11, boto3 1.34, pyarrow ≥14, moto 4.x (testes), mlflow 2.11.1, LocalStack 3.0, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-05-29-s3-buckets-mlflow-design.md`

---

## File Map

| Ficheiro | Acção | Responsabilidade |
|---|---|---|
| `localstack/init-s3.sh` | Modificar | Activar versioning em ambos os buckets |
| `notebooks/s3_writer.py` | Criar | Upload Parquet para S3, agrupado por grid_id |
| `notebooks/tests/test_s3_writer.py` | Criar | Testes unitários com moto (mock S3) |
| `notebooks/consumer_kafka_cassandra.py` | Modificar | Chamar s3_writer após Cassandra nos dois consumers |
| `docker-compose.yml` | Modificar | Adicionar serviço mlflow + pyarrow/mlflow no Jupyter |
| `requirements.txt` | Modificar | Adicionar pyarrow |
| `docs/forest-risk-s3-mlflow-guide.md` | Criar | Documentação detalhada de utilização |

---

## Task 1: Activar versioning em `forest-risk-models`

**Files:**
- Modify: `localstack/init-s3.sh`

- [ ] **Step 1: Actualizar init-s3.sh**

Substituir o conteúdo completo do ficheiro por:

```bash
#!/bin/bash
echo "A criar buckets S3 no LocalStack..."

awslocal s3 mb s3://forest-risk-datalake --region eu-west-1
awslocal s3 mb s3://forest-risk-models   --region eu-west-1

awslocal s3api put-bucket-versioning \
  --bucket forest-risk-datalake \
  --versioning-configuration Status=Enabled

awslocal s3api put-bucket-versioning \
  --bucket forest-risk-models \
  --versioning-configuration Status=Enabled

echo "Buckets criados com versioning:"
awslocal s3 ls
```

- [ ] **Step 2: Verificar (com stack a correr)**

```bash
docker exec localstack awslocal s3api get-bucket-versioning --bucket forest-risk-models
```

Resultado esperado:
```json
{ "Status": "Enabled" }
```

- [ ] **Step 3: Commit**

```bash
git add localstack/init-s3.sh
git commit -m "feat: enable versioning on forest-risk-models bucket"
```

---

## Task 2: Criar `notebooks/s3_writer.py` com TDD

**Files:**
- Create: `notebooks/s3_writer.py`
- Create: `notebooks/tests/__init__.py`
- Create: `notebooks/tests/test_s3_writer.py`

- [ ] **Step 1: Instalar dependências de teste**

```bash
pip install pyarrow>=14.0.0 moto[s3]==4.2.14 boto3==1.34.0 pytest
```

- [ ] **Step 2: Criar directório de testes**

```bash
mkdir -p notebooks/tests
touch notebooks/tests/__init__.py
```

- [ ] **Step 3: Escrever os testes (falharão até Task 2 Step 5)**

Criar `notebooks/tests/test_s3_writer.py`:

```python
import io
import os
from unittest.mock import patch

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_s3

BUCKET = "forest-risk-datalake"

os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def s3_bucket():
    with mock_s3():
        client = boto3.client("s3", region_name="eu-west-1")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield client


def test_write_groups_by_grid_id(s3_bucket):
    """Dois grid_id distintos geram dois ficheiros Parquet separados."""
    from s3_writer import write_parquet_to_s3

    records = [
        {"grid_id": "norte", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 30.0},
        {"grid_id": "norte", "timestamp": "2025-05-01T10:01:00Z", "temp_celsius": 31.0},
        {"grid_id": "centro", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 28.0},
    ]

    result = write_parquet_to_s3(records, topic="sensor-events")

    assert result is True
    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    keys = [o["Key"] for o in objects]
    assert any("zone=norte" in k for k in keys)
    assert any("zone=centro" in k for k in keys)
    assert len(keys) == 2


def test_topic_maps_to_correct_prefix(s3_bucket):
    """O topic Kafka é mapeado para o prefixo S3 correcto."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "alentejo", "timestamp": "2025-05-01T10:00:00Z", "frp": 5.2}]

    write_parquet_to_s3(records, topic="satellite-hotspots")

    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    keys = [o["Key"] for o in objects]
    assert all(k.startswith("satellite_hotspots/") for k in keys)


def test_parquet_content_is_readable(s3_bucket):
    """O ficheiro Parquet gerado é legível e contém os dados originais."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "sul", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 35.0}]
    write_parquet_to_s3(records, topic="sensor-events")

    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    body = s3_bucket.get_object(Bucket=BUCKET, Key=objects[0]["Key"])["Body"].read()
    df = pq.read_table(io.BytesIO(body)).to_pandas()

    assert len(df) == 1
    assert df.iloc[0]["grid_id"] == "sul"
    assert float(df.iloc[0]["temp_celsius"]) == 35.0


def test_empty_records_returns_true(s3_bucket):
    """Lista vazia não gera upload mas retorna True."""
    from s3_writer import write_parquet_to_s3

    result = write_parquet_to_s3([], topic="sensor-events")

    assert result is True
    assert s3_bucket.list_objects_v2(Bucket=BUCKET).get("Contents") is None


def test_s3_failure_returns_false_without_exception(s3_bucket):
    """Falha S3 retorna False sem propagar excepção."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "norte", "timestamp": "2025-05-01T10:00:00Z"}]

    with patch("s3_writer.boto3") as mock_boto:
        mock_boto.client.return_value.put_object.side_effect = Exception("S3 timeout")
        result = write_parquet_to_s3(records, topic="sensor-events")

    assert result is False
```

- [ ] **Step 4: Correr testes — devem FALHAR**

```bash
cd notebooks
pytest tests/test_s3_writer.py -v
```

Resultado esperado: `ModuleNotFoundError: No module named 's3_writer'`

- [ ] **Step 5: Implementar `notebooks/s3_writer.py`**

```python
import io
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

BUCKET = "forest-risk-datalake"

TOPIC_TO_PREFIX = {
    "sensor-events":      "sensor_readings",
    "satellite-hotspots": "satellite_hotspots",
    "weather-data":       "weather_data",
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-1"),
    )


def _build_key(prefix: str, zone_id: str, ts: datetime) -> str:
    return (
        f"{prefix}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"zone={zone_id}/"
        f"batch_{int(ts.timestamp())}.parquet"
    )


def write_parquet_to_s3(records: list[dict], topic: str) -> bool:
    if not records:
        return True

    prefix = TOPIC_TO_PREFIX.get(topic, topic.replace("-", "_"))
    client = _s3_client()
    success = True

    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        grouped[r.get("grid_id", "unknown")].append(r)

    for zone_id, zone_records in grouped.items():
        try:
            raw_ts = zone_records[0].get("timestamp", datetime.now(timezone.utc).isoformat())
            ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))

            df = pd.DataFrame(zone_records)
            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            key = _build_key(prefix, zone_id, ts)
            client.put_object(Bucket=BUCKET, Key=key, Body=buf.read())
            log.info(f"S3 Parquet → s3://{BUCKET}/{key} ({len(zone_records)} registos)")
        except Exception as exc:
            log.error(f"Falha S3 upload zone={zone_id} topic={topic}: {exc}")
            success = False

    return success
```

- [ ] **Step 6: Correr testes — devem PASSAR**

```bash
cd notebooks
pytest tests/test_s3_writer.py -v
```

Resultado esperado:
```
test_write_groups_by_grid_id PASSED
test_topic_maps_to_correct_prefix PASSED
test_parquet_content_is_readable PASSED
test_empty_records_returns_true PASSED
test_s3_failure_returns_false_without_exception PASSED
5 passed in ...s
```

- [ ] **Step 7: Commit**

```bash
git add notebooks/s3_writer.py notebooks/tests/__init__.py notebooks/tests/test_s3_writer.py
git commit -m "feat: add s3_writer module with Parquet partitioning by grid_id"
```

---

## Task 3: Integrar s3_writer no consumer

**Files:**
- Modify: `notebooks/consumer_kafka_cassandra.py:169-235` (`process_batch`)
- Modify: `notebooks/consumer_kafka_cassandra.py:301-338` (`consume_satellite_hotspots`)

- [ ] **Step 1: Adicionar import no topo do ficheiro**

Em `notebooks/consumer_kafka_cassandra.py`, após os imports existentes (linha ~41), adicionar:

```python
from s3_writer import write_parquet_to_s3
```

- [ ] **Step 2: Chamar write_parquet_to_s3 em process_batch**

Na função `process_batch`, após o loop `for ev in valid_evs:` (linha ~204), adicionar:

```python
    # Escrita histórica em S3 Parquet (não bloqueia pipeline em caso de falha)
    if valid_evs:
        write_parquet_to_s3(valid_evs, topic=topic)
```

O bloco completo do passo 3 em `process_batch` fica assim (linhas 199-210):

```python
    # 3. Válidos → Cassandra
    for ev in valid_evs:
        try:
            persist_valid_event(session, insert_sensor, insert_alert, write_api, ev)
        except Exception as e:
            log.error(f"Erro Cassandra: {e} | grid={ev.get('grid_id')}")

    # 3b. Válidos → S3 Parquet (histórico)
    if valid_evs:
        write_parquet_to_s3(valid_evs, topic=topic)
```

- [ ] **Step 3: Adicionar buffer ao consume_satellite_hotspots**

Substituir a função `consume_satellite_hotspots` (linhas 301-338) por:

```python
def consume_satellite_hotspots(session, insert_sensor, write_api):
    consumer = KafkaConsumer(
        "satellite-hotspots",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="cassandra-hotspot-writer",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=-1
    )

    log.info("🛰️  Consumer satellite-hotspots iniciado")
    hotspot_buffer: list[dict] = []
    HOTSPOT_BATCH = 10

    for msg in consumer:
        try:
            ev          = msg.value
            ts_str      = ev.get("timestamp", datetime.now(timezone.utc).isoformat())
            ts          = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            hour_bucket = ts.strftime("%Y-%m-%dT%H:00:00")

            session.execute(insert_sensor, (
                ev.get("grid_id", "PT-UNKNOWN"),
                hour_bucket, ts, "nasa_firms",
                float(ev.get("brightness", 0.0)),
                0.0, 0.0, 1,
                float(ev.get("frp", 0.0)),
                float(ev.get("latitude", 0.0)),
                float(ev.get("longitude", 0.0)),
            ))

            latency_ms = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
            send_latency(write_api, INFLUX_BUCKET, INFLUX_ORG,
                         latency_ms, "satellite-hotspots", ev.get("grid_id", "PT-UNKNOWN"))
            log.info(
                f"🛰️  Hotspot registado — {ev.get('grid_id')} "
                f"FRP={ev.get('frp')} Latência={latency_ms:.1f}ms"
            )

            hotspot_buffer.append(ev)
            if len(hotspot_buffer) >= HOTSPOT_BATCH:
                write_parquet_to_s3(hotspot_buffer, topic="satellite-hotspots")
                hotspot_buffer.clear()

        except Exception as e:
            log.error(f"Erro hotspot: {e} | dados: {msg.value}")
```

- [ ] **Step 4: Verificar que o consumer arranca sem erros**

```bash
docker compose up consumer -d
docker compose logs consumer --tail=20
```

Resultado esperado (sem erros de import):
```
✅ Cassandra ligado!
✅ InfluxDB ligado!
📡 Consumer sensor-events iniciado
🛰️  Consumer satellite-hotspots iniciado
```

- [ ] **Step 5: Confirmar escrita em S3 após batch**

Aguardar um ciclo do producer (≈15 min) ou injectar manualmente:

```bash
docker exec localstack awslocal s3 ls s3://forest-risk-datalake/ --recursive
```

Resultado esperado (ao fim de pelo menos um batch):
```
2025-05-01 10:00:05    1234 sensor_readings/year=2025/month=05/zone=norte/batch_1234567890.parquet
```

- [ ] **Step 6: Commit**

```bash
git add notebooks/consumer_kafka_cassandra.py
git commit -m "feat: write validated events to S3 Parquet after Cassandra"
```

---

## Task 4: Adicionar MLflow ao docker-compose

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Adicionar dependências pyarrow e mlflow ao serviço Jupyter**

Em `docker-compose.yml`, na linha do `pip install` do serviço `jupyter` (linha ~226), acrescentar ao final da lista de pacotes (antes do `&&`):

```
pyarrow>=14.0.0 mlflow==2.11.1
```

A linha completa ficará:
```yaml
        pip install --quiet --no-cache-dir cassandra-driver==3.29.1 kafka-python==2.0.2 influxdb-client==1.38.0 boto3==1.34.0 xgboost==2.0.3 scikit-learn==1.4.0 matplotlib==3.8.2 seaborn==0.13.1 plotly==5.18.0 great-expectations==0.18.15 pyarrow>=14.0.0 mlflow==2.11.1 &&
```

- [ ] **Step 2: Adicionar serviço mlflow**

No `docker-compose.yml`, antes do bloco `# ─── Volumes`, adicionar:

```yaml
  # ─── MLflow Tracking Server ───────────────────────────────────────────────────
  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.11.1
    container_name: mlflow
    restart: unless-stopped
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
      --backend-store-uri sqlite:////mlflow/mlruns.db
      --default-artifact-root s3://forest-risk-models/mlflow
    volumes:
      - mlflow_data:/mlflow
    depends_on:
      - localstack
```

- [ ] **Step 3: Declarar volume mlflow_data**

No bloco `volumes:` no final do `docker-compose.yml`, adicionar:

```yaml
  mlflow_data:
```

- [ ] **Step 4: Adicionar mlflow ao environment do Jupyter**

No bloco `environment:` do serviço `jupyter`, adicionar:

```yaml
      MLFLOW_TRACKING_URI: http://mlflow:5000
```

- [ ] **Step 5: Arrancar e verificar MLflow**

```bash
docker compose up mlflow -d
docker compose logs mlflow --tail=20
```

Resultado esperado:
```
[INFO] Starting gunicorn 21.2.0
[INFO] Listening at: http://0.0.0.0:5000
```

Abrir browser em `http://localhost:5000` — deve mostrar a UI do MLflow.

- [ ] **Step 6: Verificar que Jupyter arranca com os novos pacotes**

```bash
docker compose up jupyter -d --build
docker compose logs jupyter --tail=30
```

Resultado esperado (sem erros de import):
```
Successfully installed pyarrow-... mlflow-2.11.1
[I] Jupyter Server ... is running at: http://localhost:8888
```

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add MLflow Tracking Server service with S3 artifact store"
```

---

## Task 5: Actualizar requirements.txt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Adicionar pyarrow**

Adicionar ao final de `requirements.txt`:

```
pyarrow>=14.0.0
```

Ficheiro completo resultante:
```
kafka-python
cassandra-driver
influxdb-client
requests
pandas
great-expectations==0.18.15
pyarrow>=14.0.0
```

- [ ] **Step 2: Rebuild do container producer/consumer**

```bash
docker compose build producer consumer
docker compose up producer consumer -d
docker compose logs consumer --tail=10
```

Resultado esperado: sem erros de `ModuleNotFoundError`.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add pyarrow to requirements"
```

---

## Task 6: Testar integração MLflow num notebook

**Files:**
- Create: `notebooks/02_mlflow_example.ipynb` (notebook de validação — pode ser apagado após confirmação)

- [ ] **Step 1: Abrir Jupyter em http://localhost:8888 (token: forestrisk)**

- [ ] **Step 2: Criar novo notebook e correr as células seguintes**

Célula 1 — configurar tracking:
```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("forest-risk-fire-prediction")
print("MLflow URI:", mlflow.get_tracking_uri())
```

Célula 2 — treino e registo:
```python
X, y = make_classification(n_samples=500, n_features=8, random_state=42)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

with mlflow.start_run(run_name="baseline-rf"):
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    f1 = f1_score(y_test, model.predict(X_test))

    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("f1_score", f1)
    mlflow.sklearn.log_model(model, "model")
    print(f"F1: {f1:.3f} | Run registado no MLflow")
```

- [ ] **Step 3: Confirmar no MLflow UI (http://localhost:5000)**

Deve aparecer o experimento `forest-risk-fire-prediction` com um run e o modelo em artefactos.

- [ ] **Step 4: Confirmar artefacto em S3**

```bash
docker exec localstack awslocal s3 ls s3://forest-risk-models/mlflow/ --recursive
```

Resultado esperado:
```
... mlflow/<experiment_id>/<run_id>/artifacts/model/MLmodel
... mlflow/<experiment_id>/<run_id>/artifacts/model/model.pkl
```

---

## Task 7: Documentação detalhada

**Files:**
- Create: `docs/forest-risk-s3-mlflow-guide.md`

- [ ] **Step 1: Criar o ficheiro de documentação**

Criar `docs/forest-risk-s3-mlflow-guide.md` com o conteúdo completo:

```markdown
# Forest Risk — Guia S3 Data Lake e MLflow

## Arquitectura de armazenamento

O sistema usa dois buckets S3 no LocalStack para persistência histórica e rastreamento de modelos:

| Bucket | Propósito |
|---|---|
| `forest-risk-datalake` | Dados históricos em Parquet (sensor, satélite, meteorologia) |
| `forest-risk-models` | Artefactos de modelos ML geridos pelo MLflow |

---

## 1. forest-risk-datalake

### Estrutura de prefixos

```
forest-risk-datalake/
├── sensor_readings/year=YYYY/month=MM/zone=<grid_id>/batch_<ts>.parquet
├── satellite_hotspots/year=YYYY/month=MM/zone=<grid_id>/batch_<ts>.parquet
└── weather_data/year=YYYY/month=MM/zone=<grid_id>/batch_<ts>.parquet
```

### Como os dados chegam ao bucket

O consumer Kafka (`consumer_kafka_cassandra.py`) escreve em Cassandra e depois chama `s3_writer.write_parquet_to_s3()`:

- **sensor-events**: após cada micro-batch validado (3 eventos ou 30s)
- **satellite-hotspots**: após acumular 10 eventos

Falhas S3 são logadas mas não bloqueiam o pipeline (Cassandra é a fonte primária).

### Listar ficheiros

```bash
# Com a stack a correr
docker exec localstack awslocal s3 ls s3://forest-risk-datalake/ --recursive
```

### Ler dados em Jupyter (boto3)

```python
import boto3
import io
import pyarrow.parquet as pq

s3 = boto3.client(
    "s3",
    endpoint_url="http://localstack:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="eu-west-1",
)

# Listar ficheiros de uma zona
response = s3.list_objects_v2(
    Bucket="forest-risk-datalake",
    Prefix="sensor_readings/year=2025/month=05/zone=norte/"
)
for obj in response.get("Contents", []):
    print(obj["Key"])

# Ler um ficheiro Parquet
body = s3.get_object(Bucket="forest-risk-datalake", Key="<key>")["Body"].read()
df = pq.read_table(io.BytesIO(body)).to_pandas()
print(df.head())
```

### Ler dados em Jupyter (Spark)

```python
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "test")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "test")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

df = spark.read.parquet("s3a://forest-risk-datalake/sensor_readings/")
df.filter("year = '2025'").show()
```

---

## 2. forest-risk-models (MLflow)

### MLflow Tracking Server

| Campo | Valor |
|---|---|
| UI | http://localhost:5000 |
| Backend store | SQLite em volume Docker (`mlflow_data`) |
| Artifact store | `s3://forest-risk-models/mlflow` |

### Registar uma experiência de treino

```python
import mlflow
import mlflow.sklearn

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("forest-risk-fire-prediction")

with mlflow.start_run(run_name="xgboost-v1"):
    # treino
    model.fit(X_train, y_train)
    
    # métricas
    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 6)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("roc_auc", auc)
    
    # guardar modelo em S3
    mlflow.sklearn.log_model(model, "model")
```

### Carregar modelo registado

```python
import mlflow

mlflow.set_tracking_uri("http://mlflow:5000")

# Carregar última versão do modelo registado
model = mlflow.sklearn.load_model("models:/forest-risk-classifier/latest")
predictions = model.predict(X_new)
```

### Verificar artefactos em S3

```bash
docker exec localstack awslocal s3 ls s3://forest-risk-models/mlflow/ --recursive
```

---

## 3. Módulo s3_writer

Ficheiro: `notebooks/s3_writer.py`

### Função principal

```python
from s3_writer import write_parquet_to_s3

# Escreve lista de eventos válidos em Parquet, agrupados por grid_id
# Retorna True se todos os uploads tiverem sucesso
ok = write_parquet_to_s3(valid_events, topic="sensor-events")
```

### Mapeamento topic → prefixo S3

| Tópico Kafka | Prefixo no bucket |
|---|---|
| `sensor-events` | `sensor_readings/` |
| `satellite-hotspots` | `satellite_hotspots/` |
| `weather-data` | `weather_data/` |

### Variáveis de ambiente necessárias

Já configuradas no `docker-compose.yml`:

```
AWS_ENDPOINT_URL=http://localstack:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=eu-west-1
```

---

## 4. Correr os testes

```bash
cd notebooks
pip install moto[s3]==4.2.14 pytest
pytest tests/test_s3_writer.py -v
```

---

## 5. Arrancar a stack completa

```bash
docker compose up -d
```

Serviços e portas:

| Serviço | URL | Credenciais |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | token: forestrisk |
| MLflow UI | http://localhost:5000 | — |
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / forestrisk123 |
| InfluxDB | http://localhost:8086 | admin / forestrisk123 |
| LocalStack S3 | http://localhost:4566 | key: test |

---

## 6. Migração futura para AWS real

Para migrar de LocalStack para AWS S3 real, apenas é necessário:

1. Remover `AWS_ENDPOINT_URL` das variáveis de ambiente
2. Substituir `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` por credenciais AWS reais
3. Criar os buckets em AWS com `aws s3 mb s3://forest-risk-datalake --region eu-west-1`
4. Para MLflow em produção, substituir o serviço local por **AWS SageMaker Experiments**

O código do consumer e dos notebooks não necessita de alterações.
```

- [ ] **Step 2: Commit final**

```bash
git add docs/forest-risk-s3-mlflow-guide.md
git commit -m "docs: add detailed S3 datalake and MLflow usage guide"
```

---

## Ordem de execução recomendada

```
Task 1 (init-s3.sh)
    ↓
Task 2 (s3_writer.py + testes)
    ↓
Task 3 (consumer integration)
    ↓
Task 4 (docker-compose MLflow)
    ↓
Task 5 (requirements.txt)
    ↓
Task 6 (validação MLflow em Jupyter)
    ↓
Task 7 (documentação)
```

Tasks 4 e 5 podem correr em paralelo com Task 3.
