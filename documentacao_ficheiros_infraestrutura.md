# Forest Risk Monitoring System — Ficheiros de Infraestrutura

**Para quem é este documento:**  
Para qualquer pessoa que queira perceber o que são estes ficheiros de configuração,
porque existem, e o que aconteceria se não existissem — sem assumir conhecimento
prévio de Docker, bases de dados ou cloud.

---

## Índice

1. [Dockerfile](#1-dockerfile)
2. [requirements.txt](#2-requirementstxt)
3. [init.cql](#3-initcql)
4. [init-s3.sh](#4-init-s3sh)
5. [check_and_load.sh](#5-check_and_loadsh)
6. [Como os ficheiros se relacionam](#6-como-os-ficheiros-se-relacionam)

---

## 1. Dockerfile

### O que é em termos simples

Imagina que tens uma receita de culinária que diz exactamente o que precisas para fazer um prato: ingredientes, ordem de preparação, temperatura do forno. O `Dockerfile` é exactamente isso — uma receita que diz ao Docker como construir uma "caixa" (container) com tudo o que os nossos scripts Python precisam para correr.

### Porque existe

Sem o Dockerfile, cada pessoa da equipa teria de instalar manualmente no seu PC as mesmas versões das mesmas ferramentas Python — e invariavelmente haveria erros do tipo "no meu PC funciona mas no teu não". O Dockerfile resolve isto: garante que toda a gente usa exactamente o mesmo ambiente, independentemente do sistema operativo ou das versões instaladas localmente.

### O que está lá dentro e porque

```dockerfile
FROM python:3.11-slim
```
**O que faz:** "Começa com uma caixa que já tem o Python 3.11 instalado (versão slim = mais leve, sem extras desnecessários)."

**Porque esta versão:** Python 3.11 é a versão estável mais recente com as funcionalidades que o projecto usa. `slim` reduz o tamanho do container de ~1GB para ~130MB — importante para o tempo de download e espaço em disco.

---

```dockerfile
WORKDIR /app
```
**O que faz:** "Dentro da caixa, trabalha sempre na pasta `/app`." É como fazer `cd /app` antes de qualquer comando.

**Porque:** Organização — todos os ficheiros do projecto ficam numa pasta conhecida e previsível, em vez de espalhados pela raiz do sistema.

---

```dockerfile
COPY . .
```
**O que faz:** "Copia todos os ficheiros do projecto (do teu PC) para dentro da caixa (para a pasta `/app`)."

**Porque:** O container começa completamente vazio (só tem o Python). Precisamos de copiar o nosso código para dentro para o poder executar.

---

```dockerfile
RUN pip install --no-cache-dir \
    kafka-python \
    cassandra-driver \
    influxdb-client \
    requests \
    pandas \
    great-expectations==0.18.15
```
**O que faz:** "Instala as bibliotecas Python necessárias."

**`--no-cache-dir`:** Não guarda o cache de instalação — reduz o tamanho final do container porque esse cache não é necessário depois da instalação.

**Porque instala estas e não outras:** Cada biblioteca tem um papel específico (explicado em detalhe na secção `requirements.txt` abaixo).

### Quando é usado

O Dockerfile é usado **uma única vez** (ou quando há alterações) para construir a imagem. Isso acontece automaticamente quando fazes:
```bash
docker compose up --build
```
ou
```bash
docker build -t forest-risk .
```

Depois da imagem estar construída, o Docker reutiliza-a sem reconstruir. É como preparar a receita uma vez e usar o prato várias vezes.

### O que aconteceria sem ele

Cada pessoa teria de instalar Python 3.11, as 6 bibliotecas com as versões correctas, e configurar o ambiente manualmente. Em equipas com múltiplos sistemas operativos (Windows/Mac/Linux), isto causaria erros constantes de compatibilidade.

---

## 2. requirements.txt

### O que é em termos simples

Uma lista de compras — diz exactamente quais as bibliotecas Python externas que o projecto precisa. É separado do Dockerfile para poder ser usado também fora do Docker (ex: para instalar no Jupyter localmente).

### Porque existe separado do Dockerfile

O Dockerfile usa esta lista para instalar as dependências dentro do container. Mas a lista existe como ficheiro separado para ser reutilizável: qualquer pessoa pode instalar as dependências com `pip install -r requirements.txt` sem ter de ler o Dockerfile.

### O que cada biblioteca faz e porque é necessária

```
kafka-python
```
**O que é:** A biblioteca que permite ao Python comunicar com o Apache Kafka — o sistema de mensagens da pipeline.

**Porque é necessária:** Sem ela, os producers não conseguiriam enviar mensagens para o Kafka, e o consumer não conseguiria ler. É o "telefone" que liga os componentes.

**Quem a usa:** `producer_sensores.py`, `producer_apis_reais.py`, `consumer_kafka_cassandra.py`

---

```
cassandra-driver
```
**O que é:** A biblioteca oficial para ligar Python ao Apache Cassandra — a base de dados do sistema.

**Porque é necessária:** O consumer precisa de gravar os eventos processados no Cassandra. Sem este driver, não consegue falar com a base de dados.

**Quem a usa:** `consumer_kafka_cassandra.py` (para gravar em `sensor_readings` e `fire_alerts`)

---

```
influxdb-client
```
**O que é:** A biblioteca para comunicar com o InfluxDB — a base de dados de séries temporais onde ficam as métricas de qualidade.

**Porque é necessária:** O sistema envia métricas de qualidade (percentagem de eventos válidos, latência, etc.) para o InfluxDB, que o Grafana depois visualiza. Sem esta biblioteca, as métricas não chegam ao InfluxDB.

**Quem a usa:** `data_quality.py`, `consumer_kafka_cassandra.py`

---

```
requests
```
**O que é:** A biblioteca standard de Python para fazer pedidos HTTP — ou seja, para "falar" com APIs externas na internet.

**Porque é necessária:** O `producer_apis_reais.py` precisa de consultar a API da NASA FIRMS, do IPMA e do ICNF. Estas consultas são pedidos HTTP — sem a biblioteca `requests`, não há forma de os fazer.

**Quem a usa:** `producer_apis_reais.py` (para NASA, IPMA, ICNF)

---

```
pandas
```
**O que é:** A biblioteca de análise de dados mais popular em Python. Permite trabalhar com tabelas de dados (DataFrames) de forma eficiente.

**Porque é necessária:** A NASA FIRMS devolve os hotspots em formato CSV — o `pandas` lê esse CSV e converte para o formato que o sistema usa. Também é usado para manipular os dados históricos antes de os carregar para o S3.

**Quem a usa:** `producer_apis_reais.py` (parse do CSV NASA), `carga_historico_s3.py` (manipulação de dados), `data_quality_validation.py` (validação em DataFrame)

---

```
great-expectations==0.18.15
```
**O que é:** Uma biblioteca de validação de qualidade de dados. Permite definir "expectativas" sobre os dados (ex: "temperatura deve estar entre -10 e 60") e verificar automaticamente se são cumpridas.

**Porque a versão está fixada (==0.18.15):** Versões mais recentes do Great Expectations mudaram a API de forma incompatível com o código actual. Fixar a versão garante que o sistema não quebra quando alguém actualiza as dependências.

**Quem a usa:** `data_quality_validation.py` (validação dos eventos IoT)

### Quando é usado

Em dois momentos:
1. **Dentro do Docker:** O Dockerfile lê este ficheiro durante a construção da imagem
2. **Fora do Docker (manual):** Quando alguém quer instalar as dependências localmente:
```bash
pip install -r requirements.txt
```

---

## 3. init.cql

### O que é em termos simples

O Cassandra é como um armário vazio — quando arranca pela primeira vez, não tem gavetas nem organização. O `init.cql` é o plano que diz ao Cassandra como organizar esse armário: que "gavetas" (tabelas) criar, em que ordem guardar as coisas, e durante quanto tempo as guardar.

CQL é a linguagem do Cassandra — parecida com SQL mas adaptada à forma como o Cassandra funciona internamente.

### Porque existe

Sem este ficheiro, o container do Cassandra arrancava mas estaria completamente vazio. O consumer tentaria gravar dados e falharia imediatamente com o erro "tabela não existe". O `init.cql` garante que, quando o sistema arranca, o Cassandra já tem a estrutura correcta pronta a receber dados.

### Quando é executado

Automaticamente pelo container `cassandra-setup` ao arrancar com `docker compose up`. Corre uma única vez — se as tabelas já existirem (de uma sessão anterior), o `IF NOT EXISTS` garante que não tenta criar de novo.

### O que está lá dentro e porque

---

#### Keyspace `forest_risk`

```sql
CREATE KEYSPACE IF NOT EXISTS forest_risk
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;
```

**O que é um keyspace:** É o "envelope" que agrupa todas as tabelas do projecto. Equivale a uma base de dados no MySQL ou PostgreSQL.

**`SimpleStrategy`:** Estratégia de replicação simples — adequada para desenvolvimento local com um único servidor. Em produção com múltiplos servidores usaria `NetworkTopologyStrategy`.

**`replication_factor: 1`:** Os dados existem só numa cópia (num único nó). Em produção seria 3 — cada dado teria 3 cópias em 3 servidores diferentes para tolerância a falhas.

**`durable_writes: true`:** Garante que os dados são escritos no disco antes de confirmar o sucesso. Mais lento mas mais seguro — sem isto, uma falha de energia poderia corromper dados.

---

#### Tabela `sensor_readings`

```sql
CREATE TABLE IF NOT EXISTS sensor_readings (
    grid_id     TEXT,
    hour_bucket TEXT,       -- 'YYYY-MM-DD-HH'
    event_time  TIMESTAMP,
    source      TEXT,
    temp_celsius    FLOAT,
    humidity_pct    FLOAT,
    wind_kmh        FLOAT,
    hotspot_count   INT,
    risk_score      FLOAT,
    latitude        DOUBLE,
    longitude       DOUBLE,
    PRIMARY KEY ((grid_id, hour_bucket), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 259200;
```

**Para que serve:** Guarda todas as leituras de sensores válidas — tanto dos sensores IoT simulados como das observações reais do IPMA e hotspots NASA. É a tabela principal do sistema.

**Porquê `PRIMARY KEY ((grid_id, hour_bucket), event_time)`:**

O Cassandra organiza os dados de forma muito diferente de uma base de dados relacional. A `PRIMARY KEY` tem duas partes:

- **`(grid_id, hour_bucket)` — Partition Key:** Determina em que "servidor" ou "partição" os dados ficam. Todos os eventos da mesma zona (`grid_id`) na mesma hora (`hour_bucket`) ficam juntos no mesmo lugar físico no disco. Quando o Grafana pergunta "mostra-me as leituras de PT-NORTE-01 na última hora", o Cassandra sabe exactamente onde ir buscar — não precisa de varrer a tabela inteira.

- **`event_time` — Clustering Key:** Dentro de cada partição, os eventos são ordenados por timestamp. `CLUSTERING ORDER BY (event_time DESC)` significa que os mais recentes aparecem primeiro — ideal para "mostra os últimos N eventos".

**`hour_bucket`:** Campo derivado calculado pelo consumer (ex: `"2026-06-09T19:00:00"` para qualquer evento entre as 19:00 e 19:59). Isto é uma técnica essencial no Cassandra chamada "time bucketing" — sem ela, uma partição poderia acumular milhões de eventos da mesma zona para sempre, tornando-se ineficiente.

**`default_time_to_live = 259200`:** Os dados expiram automaticamente após 259.200 segundos = 72 horas (3 dias). O Cassandra apaga-os sozinho. Porquê? As leituras individuais de sensores são "hot data" — úteis para monitorização em tempo real mas não para arquivo de longo prazo. O arquivo de longo prazo é feito no S3 pelo Spark. Isto mantém o Cassandra leve e rápido.

---

#### Tabela `fire_alerts`

```sql
CREATE TABLE IF NOT EXISTS fire_alerts (
    alert_id    UUID,
    grid_id     TEXT,
    alert_time  TIMESTAMP,
    risk_score  FLOAT,
    risk_level  TEXT,
    trigger_temp     FLOAT,
    trigger_humidity FLOAT,
    trigger_wind     FLOAT,
    hotspot_count    INT,
    PRIMARY KEY (grid_id, alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC);
```

**Para que serve:** Guarda os alertas gerados quando o `risk_score >= 60` (HIGH) ou `>= 80` (CRITICAL). Diferente da `sensor_readings`, os alertas **não expiram** — ficam guardados indefinidamente para análise histórica e auditoria.

**`alert_id UUID`:** Identificador único gerado automaticamente para cada alerta. Garante que dois alertas na mesma zona ao mesmo tempo não se sobrepõem (o que aconteceria sem este campo na primary key).

**Sem `default_time_to_live`:** Os alertas são importantes para análise histórica — quantos alertas ocorreram no verão de 2024? Em que zonas? Apagá-los automaticamente destruiria essa informação.

**`risk_level TEXT`:** Texto em vez de número para legibilidade — `"HIGH"` é mais claro que `3` numa query de análise.

**`trigger_temp`, `trigger_humidity`, `trigger_wind`:** Guardam os valores exactos que causaram o alerta. Útil para auditoria: "por que é que este alerta foi gerado?" pode ser respondido olhando para estes campos.

---

#### Tabela `risk_predictions`

```sql
CREATE TABLE IF NOT EXISTS risk_predictions (
    grid_id         TEXT,
    prediction_date DATE,
    horizon_hours   INT,
    predicted_risk  FLOAT,
    confidence      FLOAT,
    model_version   TEXT,
    created_at      TIMESTAMP,
    PRIMARY KEY ((grid_id, prediction_date), horizon_hours)
);
```

**Para que serve:** Vai guardar as previsões do modelo ML (Fase 4 — Pessoa B). O modelo vai prever o risco de incêndio para as próximas 24, 48 e 72 horas em cada zona.

**`horizon_hours`:** Quantas horas no futuro é a previsão. Uma linha com `horizon_hours=24` diz "o risco previsto para daqui a 24 horas é X".

**`confidence`:** O modelo ML não diz apenas "risco=75" — diz também "tenho 85% de confiança nessa previsão". Valores de confiança baixos indicam que o modelo está incerto (ex: poucos dados históricos para aquela zona).

**`model_version`:** Regista qual versão do modelo gerou a previsão. Quando o modelo for re-treinado com dados mais recentes, é possível comparar previsões antigas vs novas para a mesma data.

**Estado actual:** A tabela existe mas está vazia — a Fase 4 (modelo ML) ainda não foi implementada.

---

## 4. init-s3.sh

### O que é em termos simples

O S3 da Amazon (Simple Storage Service) é como uma pen drive gigante na nuvem. O LocalStack simula esse serviço localmente durante o desenvolvimento. O `init-s3.sh` é o script que, quando o sistema arranca, cria as "pastas principais" (buckets) nessa pen drive simulada.

### Porque existe

Sem este script, o LocalStack arrancava mas não tinha nenhum bucket criado. Quando o Spark tentava gravar os agregados em `s3://forest-risk-datalake/agregados_streaming/`, falharia com "bucket não existe". O `init-s3.sh` garante que os buckets existem antes de qualquer componente tentar usá-los.

### Quando é executado

Automaticamente pelo container `localstack` ao arrancar, via o mecanismo de inicialização do LocalStack (`/etc/localstack/init/ready.d/`). Corre uma única vez por sessão.

### O que faz linha a linha

```bash
echo "A criar buckets S3 no LocalStack..."
```
Mensagem de log para confirmar que o script está a correr — visível com `docker compose logs localstack`.

---

```bash
awslocal s3 mb s3://forest-risk-datalake --region eu-west-1
```
**`awslocal`:** Versão do comando `aws` adaptada para LocalStack — redireciona automaticamente para `localhost:4566` em vez da AWS real.

**`s3 mb`:** "Make Bucket" — cria um novo bucket.

**`forest-risk-datalake`:** O bucket principal do projecto. Vai conter:
- `hotspots/` — dados históricos NASA FIRMS (carregados pela EDA + carga_historico_s3.py)
- `meteorologia/` — dados ERA5 (quando EDA_ERA5.py correr)
- `agregados_streaming/` — resultados do Spark em tempo real

**`--region eu-west-1`:** Região da AWS simulada (Ireland). Em produção, os dados ficariam fisicamente em servidores na Irlanda. Em desenvolvimento, é só um nome — o LocalStack não tem regiões reais.

---

```bash
awslocal s3 mb s3://forest-risk-models --region eu-west-1
```
Cria o segundo bucket — reservado para guardar os modelos ML treinados (ficheiros `.pkl`, `.joblib`, etc.). Separado do bucket de dados para facilitar permissões diferentes: os dados podem ser lidos por vários serviços, mas os modelos só deveriam ser escritos pelo processo de treino.

**Estado actual:** Existe mas está vazio — aguarda a Fase 4 (modelo ML da Pessoa B).

---

```bash
awslocal s3api put-bucket-versioning \
  --bucket forest-risk-datalake \
  --versioning-configuration Status=Enabled
```
**O que é versionamento:** Quando activo, o S3 guarda todas as versões anteriores de cada ficheiro. Se um ficheiro for sobrescrito ou apagado acidentalmente, é possível recuperar a versão anterior.

**Porque só no `forest-risk-datalake`:** Os dados históricos e os agregados do Spark são valiosos e difíceis de regenerar (especialmente os dados NASA de anos anteriores). Os modelos ML no outro bucket são regeneráveis re-treinando o modelo.

---

```bash
echo "Buckets criados:"
awslocal s3 ls
```
Lista todos os buckets criados para confirmar que tudo correu bem — visível nos logs.

### O que aconteceria sem ele

O Spark tentaria gravar em `s3://forest-risk-datalake/agregados_streaming/` e falharia com "NoSuchBucket". O `carga_historico_s3.py` também falharia ao tentar gravar os dados históricos. Nenhum dado chegaria ao S3.

---

## 5. check_and_load.sh

### O que é em termos simples

Um porteiro inteligente que, quando o sistema arranca, verifica se os dados históricos já estão actualizados no S3. Se não estiverem — seja porque o S3 está vazio, seja porque a Pessoa B actualizou as EDAs — carrega-os automaticamente. Se já estiverem em dia, não faz nada.

### Porque existe

Sem este script, haveria dois problemas:
1. **Carga manual:** Alguém teria de se lembrar de carregar os dados históricos sempre que o sistema arranca com um S3 vazio ou sempre que as EDAs fossem actualizadas
2. **Carga desnecessária:** Sem verificar se os dados já estão actualizados, o sistema carregaria os ~52.000 hotspots NASA todas as vezes que arrancasse — desnecessário e lento

### Quando é executado

Automaticamente pelo container `carga-historico` ao arrancar. Pode ser re-executado manualmente:
```bash
docker compose restart carga-historico
```

### Fluxo de decisão completo

```
Sistema arranca
      ↓
check_and_load.sh corre
      ↓
Verifica cada EDA:
  EDA_NASA.py → Filtragem_Parquet/ tem Parquet? ✅ ou ⏳
  EDA_ERA5.py → ERA5_Parquet/ tem Parquet?      ✅ ou ⏳
  EDA_ICNF.py → ICNF_Parquet/ tem Parquet?      ✅ ou ⏳
      ↓
Nenhuma tem Parquet?
  → Avisa: "Para ser possível popular o data lake, o responsável
            pelos EDAs tem de correr e validar primeiro os respectivos"
  → Para. S3 não é tocado.
      ↓
Algumas têm Parquet?
  → Avisa sobre as que faltam (não bloqueia)
  → Compara datas: Parquet mais recente que S3?
        SIM → Carrega / Recarrega
        NÃO → Salta (já está em dia)
```

### Os 5 passos em detalhe

**Passo 1 — Array de EDAs:** Lista todas as EDAs conhecidas no formato `NOME|PASTA|DESCRIÇÃO`. Para adicionar uma nova EDA no futuro, basta acrescentar uma linha a este array — o resto do script adapta-se automaticamente.

**Passo 2 — Verificação de Parquet:** Para cada EDA, conta os ficheiros `.parquet` na pasta correspondente. Uma pasta com Parquet = EDA correu com sucesso. Uma pasta vazia ou inexistente = EDA ainda não correu.

**Passo 3 — Parar se nenhuma EDA correu:** Se zero EDAs têm Parquet, não há dados para carregar. O script avisa com a lista exacta de comandos a correr e termina. `exit 0` (não `exit 1`) porque não é um erro do sistema — é um estado esperado durante o desenvolvimento.

**Passo 4 — Comparação de datas (Python interno):** Usa Python embutido no bash para comparar:
- Data de modificação do Parquet mais recente (de todas as EDAs disponíveis)
- Data do objecto mais recente no S3 em `hotspots/`

Se o Parquet é mais recente → a Pessoa B actualizou a EDA depois da última carga → recarregar.
Se o S3 é mais recente ou igual → já está em dia → saltar.

**Passo 5 — Carga:** Se necessário, chama `carga_historico_s3.py` que lê os Parquet e os grava no S3 particionados por `ano/mes/grid_id`.

### Os 4 cenários possíveis

| Cenário | O que acontece | Exemplo |
|---|---|---|
| Nenhuma EDA correu | Avisa e para | Primeiro `docker compose up` sem ter corrido as EDAs |
| S3 vazio + EDAs disponíveis | Carrega para o S3 | Após `docker compose down -v` e EDAs já existem |
| EDAs mais recentes que S3 | Recarrega o S3 | Pessoa B actualizou a EDA_NASA.py com novos dados |
| S3 actualizado | Salta sem fazer nada | Arranque normal do dia-a-dia |

---

## 6. Como os ficheiros se relacionam

```
docker compose up
      │
      ├──► Dockerfile
      │    └── Constrói a imagem com Python 3.11 + bibliotecas do requirements.txt
      │        └── requirements.txt define quais bibliotecas instalar
      │
      ├──► init-s3.sh (corre dentro do container localstack)
      │    └── Cria os buckets S3 antes de qualquer componente tentar usá-los
      │
      ├──► init.cql (corre dentro do container cassandra-setup)
      │    └── Cria as tabelas Cassandra antes do consumer tentar gravar
      │
      └──► check_and_load.sh (corre dentro do container carga-historico)
           └── Verifica Parquet das EDAs → decide se carrega para S3
               └── Se sim, chama carga_historico_s3.py
```

**Ordem de dependências:**
- O `init-s3.sh` tem de correr **antes** do Spark e do `carga_historico_s3.py` — senão não há buckets para gravar
- O `init.cql` tem de correr **antes** do consumer — senão não há tabelas para inserir dados
- O `check_and_load.sh` tem de correr **depois** do `init-s3.sh` — precisa que os buckets existam

O `docker-compose.yml` garante esta ordem através de `depends_on` e `healthcheck` — o Spark só arranca depois do LocalStack estar saudável, o consumer só arranca depois do Cassandra estar pronto, etc.

---

*Documentação de infraestrutura — Forest Risk Monitoring System — ISEP 2024/2025*
