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
- **satellite-hotspots**: após acumular 10 eventos (buffer `HOTSPOT_BATCH`)

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
| Backend store | SQLite em volume Docker (`mlflow_data:/mlflow/mlruns.db`) |
| Artifact store | `s3://forest-risk-models/mlflow` |

### Registar uma experiência de treino

```python
import mlflow
import mlflow.sklearn

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("forest-risk-fire-prediction")

with mlflow.start_run(run_name="xgboost-v1"):
    model.fit(X_train, y_train)

    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 6)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("roc_auc", auc)

    mlflow.sklearn.log_model(model, "model")
```

### Carregar modelo registado

```python
import mlflow

mlflow.set_tracking_uri("http://mlflow:5000")

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
# Retorna True se todos os uploads tiverem sucesso, False se algum falhar
# Nunca propaga excepção
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

Se `AWS_ENDPOINT_URL` não estiver definida (e.g. em testes), boto3 usa o endpoint real AWS — o que permite usar `moto` nos testes sem configuração adicional.

---

## 4. Correr os testes unitários

```bash
cd notebooks
pip install "moto[s3]==4.2.14" pytest
pytest tests/test_s3_writer.py -v
```

Resultado esperado: 6 testes passam.

---

## 5. Arrancar a stack completa

```bash
docker compose up -d
```

Serviços e portas:

| Serviço | URL | Credenciais |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | token: `forestrisk` |
| MLflow UI | http://localhost:5000 | — |
| Kafka UI | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / forestrisk123 |
| InfluxDB | http://localhost:8086 | admin / forestrisk123 |
| LocalStack S3 | http://localhost:4566 | key: `test` / secret: `test` |

### Ordem de arranque garantida por healthchecks

```
LocalStack → (bucket criado pelo init-s3.sh)
    ↓
MLflow → (gunicorn disponível em :5000)
    ↓
Jupyter → (pip install + Jupyter Lab)
```

---

## 6. Validação após arranque

Abrir `notebooks/02_mlflow_example.ipynb` no Jupyter e correr todas as células. O resultado esperado está documentado no próprio notebook.

---

## 7. Migração futura para AWS real

Para migrar de LocalStack para AWS S3 real:

1. Remover `AWS_ENDPOINT_URL` das variáveis de ambiente de todos os serviços
2. Substituir `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` por credenciais AWS reais (ou usar IAM roles)
3. Criar os buckets: `aws s3 mb s3://forest-risk-datalake --region eu-west-1`
4. Para MLflow em produção: substituir o serviço local por **AWS SageMaker Experiments** (interface boto3/S3 idêntica — sem alterações ao código)

O código do consumer (`s3_writer.py`) e dos notebooks não necessita de alterações.
