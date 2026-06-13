# Forest Risk Monitoring System — Documentação do Docker Compose

**Para quem é este documento:**  
Para qualquer pessoa que queira perceber o que é o Docker Compose, porque o projecto
o usa, o que cada serviço faz, e o que aconteceria se alguma coisa faltasse —
sem assumir conhecimento prévio de Docker ou infraestrutura.

---

## Índice

1. [O que é o Docker e o Docker Compose](#1-o-que-é-o-docker-e-o-docker-compose)
2. [Conceitos essenciais antes de começar](#2-conceitos-essenciais-antes-de-começar)
3. [Visão geral dos 15 serviços](#3-visão-geral-dos-15-serviços)
4. [Serviços de base (infraestrutura)](#4-serviços-de-base-infraestrutura)
5. [Serviços de configuração (correm uma vez)](#5-serviços-de-configuração-correm-uma-vez)
6. [Serviços de dados (correm continuamente)](#6-serviços-de-dados-correm-continuamente)
7. [Serviços de visualização](#7-serviços-de-visualização)
8. [Serviços da pipeline](#8-serviços-da-pipeline)
9. [Volumes — como os dados sobrevivem](#9-volumes--como-os-dados-sobrevivem)
10. [Ordem de arranque e dependências](#10-ordem-de-arranque-e-dependências)

---

## 1. O que é o Docker e o Docker Compose

### Docker em termos simples

Imagina que tens uma aplicação que precisa de 15 programas diferentes a correr em simultâneo — uma base de dados, um sistema de mensagens, um servidor de monitorização, etc. Instalar tudo isto manualmente no teu PC seria um processo longo, propenso a erros de versão, e diferente em Windows vs Mac vs Linux.

O **Docker** resolve isto criando "caixas" isoladas chamadas **containers**. Cada container tem exactamente o que precisa para correr — o sistema operativo mínimo, as dependências, o código — e não interfere com o que está instalado no teu PC nem com os outros containers.

### Docker Compose em termos simples

O Docker permite criar um container de cada vez. O **Docker Compose** permite definir e gerir vários containers em simultâneo através de um único ficheiro (`docker-compose.yml`).

Em vez de abrir 15 terminais e correr 15 comandos diferentes, fazes apenas:
```bash
docker compose up -d
```
E o Compose arranca os 15 serviços pela ordem correcta, com todas as configurações certas, ligados entre si numa rede interna.

### Porque o projecto usa Docker Compose

- **Reprodutibilidade:** Qualquer pessoa com Docker instalado consegue correr o sistema exactamente igual, independentemente do PC
- **Isolamento:** Os 15 serviços correm separados — um problema num não afecta os outros
- **Rede interna:** Os containers comunicam entre si pelo nome (ex: `kafka:9092`) sem expor tudo para o exterior
- **Volumes:** Os dados persistem mesmo quando os containers são reiniciados

---

## 2. Conceitos essenciais antes de começar

### Image vs Container

Uma **image** é como uma fotografia — uma versão estática e pronta de um programa com todas as suas dependências. Um **container** é uma instância a correr dessa image — como imprimir uma fotografia e pô-la na parede.

No ficheiro vês:
- `image: confluentinc/cp-kafka:7.5.0` → usa uma image já existente (descarregada do Docker Hub)
- `build: .` → constrói uma nova image a partir do `Dockerfile` do projecto

### Ports (portas)

Os containers são caixas fechadas. Para aceder a um serviço de fora (ex: abrir o Grafana no browser), é preciso "abrir uma janela" mapeando uma porta do container para uma porta do teu PC.

```yaml
ports:
  - "3000:3000"   # porta_do_teu_PC:porta_do_container
```

Isto significa: quando acederes a `localhost:3000` no browser, o tráfego vai para a porta 3000 do container do Grafana.

### Environment (variáveis de ambiente)

São configurações passadas ao container quando arranca — como dizer ao programa "o Kafka está em kafka:9092" ou "a password é forestrisk123". Evitam ter configurações hardcoded no código.

### Volumes

São pastas partilhadas entre o teu PC e o container. Quando o container é eliminado, os dados no volume sobrevivem. Sem volumes, todos os dados desapareceriam ao fazer `docker compose down`.

### depends_on + healthcheck

`depends_on` define a ordem de arranque — "este container só arranca depois daquele". Mas "arrancar" não significa "estar pronto". O `healthcheck` define um teste periódico para verificar se o serviço está realmente a funcionar (não só ligado). Com `condition: service_healthy`, o Compose espera que o healthcheck passe antes de arrancar o próximo serviço.

### restart

Define o que acontece se um container falhar:
- `unless-stopped` — reinicia automaticamente sempre que cair, excepto se parado manualmente
- `"no"` — não reinicia (para serviços que só devem correr uma vez)

---

## 3. Visão geral dos 15 serviços

```
INFRAESTRUTURA BASE          CONFIGURAÇÃO (1 vez)    DADOS CONTÍNUOS
──────────────────────       ────────────────────     ───────────────
zookeeper                    kafka-setup              influxdb
kafka                        cassandra-setup          grafana
kafka-ui                     carga-historico          jupyter
cassandra
localstack

PIPELINE (contínua)
──────────────────────────────────────────────────────
producer-sensores   producer-apis   consumer   spark-streaming
```

**Regra geral:**
- Serviços com `restart: unless-stopped` correm indefinidamente
- Serviços com `restart: "no"` correm uma vez e terminam

---

## 4. Serviços de base (infraestrutura)

### zookeeper

```yaml
image: confluentinc/cp-zookeeper:7.5.0
ports:
  - "2181:2181"
environment:
  ZOOKEEPER_CLIENT_PORT: 2181
  ZOOKEEPER_TICK_TIME: 2000
```

**O que é:** O Zookeeper é um serviço de coordenação distribuída. Pensa nele como um "director de orquestra" que sabe onde está cada músico.

**Porque existe:** O Kafka precisa do Zookeeper para guardar metadados — quais os topics que existem, quantas partições têm, qual o broker líder de cada partição. Sem o Zookeeper, o Kafka não consegue arrancar.

**`ZOOKEEPER_CLIENT_PORT: 2181`:** A porta onde o Kafka (e outros clientes) se ligam ao Zookeeper.

**`ZOOKEEPER_TICK_TIME: 2000`:** O "batimento cardíaco" do Zookeeper em milissegundos — a cada 2 segundos verifica se os serviços ligados ainda estão vivos.

**Porta exposta (2181):** Normalmente não precisas de aceder ao Zookeeper directamente — é uma comunicação interna entre o Zookeeper e o Kafka.

---

### kafka

```yaml
image: confluentinc/cp-kafka:7.5.0
depends_on:
  - zookeeper
ports:
  - "9092:9092"
  - "29092:29092"
environment:
  KAFKA_BROKER_ID: 1
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
  KAFKA_LOG_RETENTION_HOURS: 168
  KAFKA_LOG_SEGMENT_BYTES: 1073741824
volumes:
  - kafka_data:/var/lib/kafka/data
```

**O que é:** O Apache Kafka é o sistema de mensagens central da pipeline — o "correio" por onde todos os dados passam. Os producers enviam mensagens, o consumer e o Spark lêem-nas.

**Porque existe:** Desacopla os producers dos consumers. Os producers enviam dados para o Kafka sem saber quem os vai ler. O consumer e o Spark lêem ao seu próprio ritmo, sem pressionar os producers.

**`KAFKA_BROKER_ID: 1`:** Identificador único deste broker. Em produção com múltiplos brokers teriam IDs 1, 2, 3, etc.

**Duas portas — porquê:**
- `kafka:9092` (porta interna) → usada pelos containers dentro da rede Docker (producers, consumer, Spark). O nome `kafka` resolve-se automaticamente dentro da rede Docker.
- `localhost:29092` (porta externa) → usada quando queres ligar ao Kafka de fora do Docker (ex: um script a correr no teu PC directamente)

**`KAFKA_LOG_RETENTION_HOURS: 168`:** As mensagens ficam guardadas 168 horas = 7 dias antes de serem apagadas automaticamente. Cada topic pode sobrescrever este valor (como vês no `kafka-setup`).

**`KAFKA_LOG_SEGMENT_BYTES: 1073741824`:** Cada ficheiro de log do Kafka tem no máximo 1 GB antes de criar um novo segmento. Facilita a gestão e limpeza.

**Volume `kafka_data`:** Guarda as mensagens em disco — sem este volume, todas as mensagens desapareceriam ao reiniciar o container.

**`sleep 20` no comando:** O Kafka precisa que o Zookeeper esteja completamente pronto antes de arrancar. O `sleep 20` dá tempo ao Zookeeper para inicializar — é uma medida de segurança extra.

**Healthcheck:** Testa se o Kafka responde listando os topics. Só quando este teste passa é que os outros serviços que dependem do Kafka começam a arrancar.

---

### kafka-ui

```yaml
image: provectuslabs/kafka-ui:latest
depends_on:
  kafka:
    condition: service_healthy
ports:
  - "8080:8080"
environment:
  KAFKA_CLUSTERS_0_NAME: floresta-cluster
  KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

**O que é:** Uma interface web para visualizar e gerir o Kafka — ver os topics, as mensagens, os consumer groups, etc. Acedes em `http://localhost:8080`.

**Porque existe:** O Kafka não tem interface visual por defeito. Sem o Kafka UI, a única forma de ver o que está a acontecer seria usar comandos de terminal complexos. O Kafka UI mostra em tempo real quantas mensagens há em cada topic, quem está a consumir, etc.

**`KAFKA_CLUSTERS_0_NAME: floresta-cluster`:** Nome do cluster tal como aparece na interface. Pode ser qualquer nome — é apenas visual.

**`condition: service_healthy`:** Só arranca depois do Kafka estar completamente pronto (healthcheck passou). Sem isto, o Kafka UI tentaria ligar a um Kafka que ainda não existe e falharia.

---

### cassandra

```yaml
image: cassandra:4.1
ports:
  - "9042:9042"
environment:
  CASSANDRA_CLUSTER_NAME: "ForestRiskCluster"
  MAX_HEAP_SIZE: "512M"
  HEAP_NEWSIZE: "128M"
volumes:
  - cassandra_data:/var/lib/cassandra
healthcheck:
  test: ["CMD", "cqlsh", "-e", "describe keyspaces"]
  start_period: 90s
```

**O que é:** O Apache Cassandra é a base de dados principal do sistema — guarda as leituras de sensores, alertas de incêndio, e previsões ML.

**Porque existe:** O sistema precisa de uma base de dados que aguente milhares de escritas por segundo com baixa latência. O Cassandra é optimizado exactamente para isso — foi criado pelo Facebook para escalar a escritas massivas.

**`MAX_HEAP_SIZE: "512M"` e `HEAP_NEWSIZE: "128M"`:** O Cassandra corre em Java e usa a JVM. Estes parâmetros limitam a memória que a JVM pode usar — sem eles, o Cassandra tentaria usar toda a RAM disponível, o que num ambiente de desenvolvimento com 8 GB partilhados entre 15 containers seria problemático.

**`start_period: 90s`:** O Cassandra é o serviço mais lento a arrancar — precisa de inicializar a JVM, carregar os dados do disco, e sincronizar com o cluster. O healthcheck só começa a testar após 90 segundos, evitando falhas prematuras.

**Volume `cassandra_data`:** Persiste os dados entre reinícios. Sem ele, todas as leituras de sensores e alertas desapareceriam ao fazer `docker compose restart cassandra`.

---

### localstack

```yaml
image: localstack/localstack:3.0
ports:
  - "4566:4566"
environment:
  SERVICES: s3
  PERSISTENCE: 1
volumes:
  - localstack_data:/var/lib/localstack
  - ./localstack/init-s3.sh:/etc/localstack/init/ready.d/init-s3.sh
```

**O que é:** O LocalStack simula serviços da Amazon Web Services (AWS) localmente no teu PC. Neste caso, simula apenas o S3 (Simple Storage Service) — o sistema de armazenamento de ficheiros da AWS.

**Porque existe:** O S3 real da AWS custa dinheiro e requer ligação à internet. Durante o desenvolvimento, o LocalStack permite usar exactamente as mesmas APIs do S3 gratuitamente e offline. O código que grava no LocalStack funciona sem alterações no S3 real da AWS.

**`SERVICES: s3`:** Só activa a simulação do S3 — o LocalStack suporta dezenas de serviços AWS mas activar todos usaria recursos desnecessários.

**`PERSISTENCE: 1`:** Os dados no S3 simulado sobrevivem a reinícios do container (guardados no volume `localstack_data`). Sem isto, todos os ficheiros Parquet desapareceriam ao reiniciar.

**`init-s3.sh` montado em `/etc/localstack/init/ready.d/`:** O LocalStack executa automaticamente qualquer script nesta pasta quando está pronto. É assim que os buckets são criados automaticamente ao arrancar — sem precisar de intervenção manual.

---

## 5. Serviços de configuração (correm uma vez)

Estes serviços têm `restart: "no"` — correm uma única vez para configurar o sistema e terminam. Não são serviços contínuos.

### kafka-setup

```yaml
restart: "no"
depends_on:
  kafka:
    condition: service_healthy
command: >
  bash -c "
    kafka-topics --create --topic sensor-events --partitions 3 --replication-factor 1 --config retention.ms=604800000 &&
    kafka-topics --create --topic satellite-hotspots --partitions 3 --replication-factor 1 --config retention.ms=604800000 &&
    kafka-topics --create --topic weather-data --partitions 3 --replication-factor 1 --config retention.ms=604800000 &&
    kafka-topics --create --topic fire-alerts --partitions 1 --replication-factor 1 --config retention.ms=2592000000 &&
    kafka-topics --create --topic data-quality-metrics --partitions 1 --replication-factor 1 --config retention.ms=604800000
  "
```

**O que faz:** Cria os 5 topics Kafka que a pipeline usa. O Kafka tem `AUTO_CREATE_TOPICS_ENABLE: true` mas criá-los explicitamente permite definir configurações específicas para cada um.

**Porque existe separado:** Configurações como o número de partições e retenção têm de ser definidas na criação. Se os topics fossem criados automaticamente pelo primeiro producer que tentasse publicar, ficariam com configurações por defeito que podem não ser as ideais.

**Porquê 3 partições para alguns e 1 para outros:**
- **3 partições** (`sensor-events`, `satellite-hotspots`, `weather-data`) — dados de alto volume que beneficiam de paralelismo. 3 partições permitem 3 consumers a ler em simultâneo.
- **1 partição** (`fire-alerts`, `data-quality-metrics`) — volume baixo, e a ordem das mensagens é importante (um alerta deve ser processado pela ordem em que foi gerado).

**`retention.ms=604800000` = 7 dias** para dados operacionais.
**`retention.ms=2592000000` = 30 dias** para `fire-alerts` — alertas são mais importantes e ficam mais tempo disponíveis.

---

### cassandra-setup

```yaml
restart: "no"
depends_on:
  cassandra:
    condition: service_healthy
volumes:
  - ./cassandra/init.cql:/init.cql
command: >
  bash -c "
    until cqlsh cassandra 9042 -f /init.cql; do
      echo 'A tentar novamente em 5s...';
      sleep 5;
    done
  "
```

**O que faz:** Aplica o schema do Cassandra (`init.cql`) — cria o keyspace `forest_risk` e as tabelas `sensor_readings`, `fire_alerts`, e `risk_predictions`.

**Porque o loop `until ... do`:** O Cassandra pode demorar até 90 segundos a arrancar completamente. Mesmo com o healthcheck, pode ainda não estar pronto para aceitar queries CQL imediatamente. O loop tenta continuamente até ter sucesso — robusto contra condições de corrida.

**`-f /init.cql`:** Executa o ficheiro CQL em vez de entrar no modo interactivo. O ficheiro é montado como volume a partir de `./cassandra/init.cql` no teu PC.

---

### carga-historico

```yaml
restart: "no"
image: quay.io/jupyter/pyspark-notebook:spark-3.5.0
depends_on:
  - localstack
command: >
  bash -c "
    sleep 15 &&
    pip install --quiet s3fs pyarrow pandas boto3 &&
    bash /check_and_load.sh
  "
```

**O que faz:** Instala as dependências necessárias e corre o `check_and_load.sh` que decide se carrega os dados históricos das EDAs para o S3.

**Porque usa a image `pyspark-notebook`:** Precisa de Python com suporte a Parquet (`pyarrow`) e S3 (`boto3`, `s3fs`). A image do Jupyter já tem muitas destas dependências — é mais rápido do que construir uma image nova.

**`sleep 15`:** Dá tempo ao LocalStack para inicializar completamente e ao `init-s3.sh` para criar os buckets. Sem esta espera, o `check_and_load.sh` tentaria aceder a buckets que ainda não existem.

**`user: root`:** Necessário para instalar packages com `pip` dentro do container (a image do Jupyter por defeito não permite instalações como utilizador normal).

---

## 6. Serviços de dados (correm continuamente)

### influxdb

```yaml
image: influxdb:2.7
env_file:
  - .env
ports:
  - "8086:8086"
environment:
  DOCKER_INFLUXDB_INIT_MODE: setup
  DOCKER_INFLUXDB_INIT_USERNAME: admin
  DOCKER_INFLUXDB_INIT_PASSWORD: ${INFLUXDB_PASSWORD}
  DOCKER_INFLUXDB_INIT_ORG: forest-risk
  DOCKER_INFLUXDB_INIT_BUCKET: metrics
  DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: ${INFLUXDB_TOKEN}
volumes:
  - influxdb_data:/var/lib/influxdb2
  - influxdb_config:/etc/influxdb2
```

**O que é:** O InfluxDB é uma base de dados de séries temporais — optimizada para guardar dados que mudam ao longo do tempo (métricas, medições, eventos). Guarda as métricas de qualidade da pipeline.

**Porque existe separado do Cassandra:** O Cassandra é optimizado para escritas de eventos de negócio (leituras de sensores, alertas). O InfluxDB é optimizado para métricas de monitorização e tem integração nativa com o Grafana — queries de "mostra a evolução da percentagem de qualidade ao longo do tempo" são muito mais eficientes no InfluxDB.

**`DOCKER_INFLUXDB_INIT_MODE: setup`:** Na primeira vez que arranca, o InfluxDB inicializa-se automaticamente com o utilizador, organização e bucket definidos nas variáveis de ambiente. Sem isto, teria de ser configurado manualmente via interface web.

**`${INFLUXDB_PASSWORD}` e `${INFLUXDB_TOKEN}`:** As credenciais vêm do ficheiro `.env` — nunca hardcoded no `docker-compose.yml` que entra no Git.

**`env_file: .env`:** Carrega todas as variáveis do ficheiro `.env` para o container. O `${INFLUXDB_TOKEN}` nos `environment` referencia essas variáveis.

**Dois volumes** (`influxdb_data` e `influxdb_config`): O InfluxDB 2.x separa os dados das configurações — cada um no seu volume para facilitar backups independentes.

---

## 7. Serviços de visualização

### grafana

```yaml
image: grafana/grafana:10.2.0
depends_on:
  influxdb:
    condition: service_healthy
ports:
  - "3000:3000"
environment:
  GF_SECURITY_ADMIN_USER: admin
  GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
  GF_USERS_ALLOW_SIGN_UP: "false"
  GF_PATHS_PROVISIONING: /etc/grafana/provisioning
volumes:
  - grafana_data:/var/lib/grafana
  - ./grafana/provisioning:/etc/grafana/provisioning
```

**O que é:** O Grafana é a ferramenta de visualização — cria dashboards com gráficos das métricas de qualidade da pipeline. Acedes em `http://localhost:3000`.

**Porque existe:** O InfluxDB guarda os dados mas não os visualiza. O Grafana liga-se ao InfluxDB e transforma os números em gráficos legíveis — percentagem de qualidade ao longo do tempo, latência da pipeline, mapa de alertas por zona, etc.

**`depends_on influxdb: condition: service_healthy`:** O Grafana precisa do InfluxDB para mostrar dados. Se o InfluxDB ainda não estiver pronto, o Grafana arranca mas os dashboards ficam vazios com erros de ligação.

**`GF_USERS_ALLOW_SIGN_UP: "false"`:** Desactiva o registo de novos utilizadores pela interface web — só o administrador definido aqui tem acesso.

**`GF_PATHS_PROVISIONING`:** Pasta onde o Grafana procura configurações automáticas de datasources e dashboards. O volume `./grafana/provisioning` monta a pasta local com o datasource do InfluxDB e o dashboard da pipeline pré-configurados — o Grafana carrega-os automaticamente ao arrancar, sem precisar de configuração manual.

---

### jupyter

```yaml
image: quay.io/jupyter/pyspark-notebook:spark-3.5.0
ports:
  - "8888:8888"
  - "4040:4040"
environment:
  JUPYTER_TOKEN: "forestrisk"
  CASSANDRA_HOST: cassandra
  KAFKA_BOOTSTRAP: kafka:9092
  INFLUXDB_URL: http://influxdb:8086
  AWS_ENDPOINT_URL: http://localstack:4566
volumes:
  - ./notebooks:/home/jovyan/work
  - ./spark/jobs:/home/jovyan/spark-jobs
command: >
  bash -c "
    pip install cassandra-driver kafka-python influxdb-client boto3
              xgboost scikit-learn matplotlib seaborn plotly
              great-expectations s3fs pyarrow &&
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
                --ServerApp.token='forestrisk'
  "
```

**O que é:** O ambiente de desenvolvimento interactivo onde a equipa trabalha — corre notebooks Python, explora dados, e corre scripts manualmente. Acedes em `http://localhost:8888` com o token `forestrisk`.

**Porque usa a image `pyspark-notebook`:** Esta image inclui Python, Jupyter Lab, e Apache Spark pré-instalados. Tem tudo o que a equipa precisa para desenvolvimento sem configuração adicional.

**Porta 4040:** A Spark UI — interface web do Spark que mostra o progresso dos jobs, DAGs de execução, e métricas de performance. Activa enquanto um job Spark está a correr.

**Variáveis de ambiente:** Passam os endereços de todos os serviços ao Jupyter. Quando um notebook corre `KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")`, obtém `kafka:9092` automaticamente — sem ter de hardcodar o endereço.

**`./notebooks:/home/jovyan/work`:** Monta a pasta local `notebooks/` dentro do container. Qualquer ficheiro que crias ou editas no Jupyter aparece imediatamente na pasta `notebooks/` do teu PC — e vice-versa. Os ficheiros não vivem "dentro do Docker", vivem no teu PC.

**`./spark/jobs:/home/jovyan/spark-jobs`:** Monta os jobs Spark para que possam ser submetidos com `spark-submit` dentro do container.

**Bibliotecas instaladas no arranque:** Para além das da `requirements.txt`, o Jupyter precisa de bibliotecas de análise de dados (`matplotlib`, `seaborn`, `plotly`), ML (`xgboost`, `scikit-learn`), e acesso ao S3 (`boto3`, `s3fs`, `pyarrow`). Estas são instaladas no arranque do container.

---

## 8. Serviços da pipeline

### producer-sensores

```yaml
build: .
command: python notebooks/producer_sensores.py
depends_on:
  kafka:
    condition: service_healthy
restart: unless-stopped
environment:
  - KAFKA_BOOTSTRAP=kafka:9092
```

**O que faz:** Gera leituras de sensores IoT simulados e envia continuamente para os topics `sensor-events` e `weather-data`.

**`build: .`:** Em vez de usar uma image pré-existente, constrói uma nova image a partir do `Dockerfile` do projecto. Isto inclui o Python 3.11-slim com as dependências do `requirements.txt`.

**`restart: unless-stopped`:** Se o script falhar por qualquer motivo (ex: perda temporária de ligação ao Kafka), o container reinicia automaticamente. A pipeline não pára por uma falha momentânea.

**Só depende do Kafka:** Não precisa do Cassandra nem do InfluxDB — a sua única responsabilidade é publicar mensagens no Kafka.

---

### producer-apis

```yaml
build: .
command: python notebooks/producer_apis_reais.py
depends_on:
  kafka:
    condition: service_healthy
restart: unless-stopped
env_file:
  - .env
environment:
  - KAFKA_BOOTSTRAP=kafka:9092
```

**O que faz:** Consulta APIs externas reais (NASA FIRMS, IPMA, ICNF) e publica os dados nos topics `satellite-hotspots`, `weather-data`, e `sensor-events`.

**`env_file: .env`:** Precisa do ficheiro `.env` porque lê a `NASA_FIRMS_KEY` — a chave de autenticação da API NASA. O `producer-sensores` não precisa do `.env` porque não faz chamadas externas.

**Diferença de comportamento:** O `producer-sensores` envia a cada 2 segundos continuamente. O `producer-apis` consulta as APIs em intervalos (NASA a cada hora, IPMA a cada 30 min, ICNF a cada dia) — e entre consultas fica em `sleep`.

---

### consumer

```yaml
build: .
command: python notebooks/consumer_kafka_cassandra.py
depends_on:
  kafka:
    condition: service_healthy
  cassandra:
    condition: service_healthy
  influxdb:
    condition: service_healthy
restart: unless-stopped
env_file:
  - .env
environment:
  - KAFKA_BOOTSTRAP=kafka:9092
  - CASSANDRA_HOST=cassandra
  - INFLUXDB_URL=http://influxdb:8086
  - INFLUXDB_TOKEN=${INFLUXDB_TOKEN}
  - INFLUXDB_ORG=forest-risk
  - INFLUXDB_BUCKET=metrics
```

**O que faz:** Lê mensagens do Kafka, valida a qualidade, persiste no Cassandra, envia métricas para o InfluxDB, e publica alertas no topic `fire-alerts`.

**Depende de três serviços:** É o serviço mais exigente em dependências porque precisa de todos os destinos de escrita prontos antes de começar a processar. Se o Cassandra não estiver pronto e o consumer tentar gravar, perde dados.

**`${INFLUXDB_TOKEN}`:** Referencia a variável do `.env`. O `env_file: .env` carrega o ficheiro e o `environment` usa as variáveis com `${}`.

---

### spark-streaming

```yaml
image: quay.io/jupyter/pyspark-notebook:spark-3.5.0
depends_on:
  kafka:
    condition: service_healthy
environment:
  KAFKA_BOOTSTRAP: kafka:9092
  AWS_ENDPOINT_URL: http://localstack:4566
volumes:
  - ./spark/jobs:/home/jovyan/spark-jobs
command: >
  bash -c "
    sleep 25 &&
    spark-submit
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4
      /home/jovyan/spark-jobs/spark_streaming_agregacao.py
  "
```

**O que faz:** Corre o job Spark Structured Streaming que lê os 3 topics em simultâneo, faz join, calcula o risco composto, e grava no console e no S3.

**`sleep 25`:** O Kafka pode estar tecnicamente "healthy" mas ainda a estabilizar internamente. O Spark tenta ligar imediatamente ao Kafka e pode falhar se este ainda está a inicializar os topics. Os 25 segundos de espera garantem que o Kafka está completamente operacional.

**`--packages`:** O Spark descarrega automaticamente os packages necessários ao arrancar:
- `spark-sql-kafka`: permite ao Spark ler directamente de topics Kafka
- `hadoop-aws`: permite ao Spark escrever directamente no S3

Estes packages não estão na image base — são descarregados do Maven Repository na primeira execução (~1-2 minutos).

**Não tem `env_file: .env`:** As credenciais S3 (`test`/`test` para LocalStack) estão hardcoded no `environment` porque são credenciais de desenvolvimento sem valor de segurança.

---

## 9. Volumes — como os dados sobrevivem

```yaml
volumes:
  kafka_data:
  cassandra_data:
  localstack_data:
  influxdb_data:
  influxdb_config:
  grafana_data:
  jupyter_data:
```

Os volumes são declarados no final do ficheiro e referenciados pelos serviços. O Docker cria-os automaticamente na primeira vez.

| Volume | Guarda | O que acontece sem ele |
|---|---|---|
| `kafka_data` | Mensagens Kafka | Todas as mensagens perdem-se ao reiniciar |
| `cassandra_data` | Leituras e alertas | Toda a base de dados perde-se ao reiniciar |
| `localstack_data` | Ficheiros Parquet no S3 | Todos os dados históricos perdem-se ao reiniciar |
| `influxdb_data` | Métricas de qualidade | Histórico de métricas perde-se ao reiniciar |
| `influxdb_config` | Configuração InfluxDB | Teria de reconfigurar o InfluxDB a cada reinício |
| `grafana_data` | Dashboards personalizados | Dashboards criados manualmente perdem-se |
| `jupyter_data` | Configuração do Jupyter | Preferências do Jupyter perdem-se |

**`docker compose down` vs `docker compose down -v`:**
- `down` — para os containers, mantém os volumes (dados sobrevivem)
- `down -v` — para os containers E apaga os volumes (apaga tudo, começa do zero)

---

## 10. Ordem de arranque e dependências

O `docker-compose.yml` define as dependências mas o Docker Compose resolve a ordem automaticamente. O diagrama abaixo mostra quem espera por quem:

```
zookeeper
    │
    ▼
kafka ──────────────────────────────────────────────┐
    │                                               │
    ├──► kafka-ui                                   │
    ├──► kafka-setup (cria os 5 topics)             │
    ├──► producer-sensores                          │
    ├──► producer-apis                              │
    ├──► consumer ◄── cassandra ◄── cassandra-setup │
    │         └──────────────────────────────────── │
    │                                               │
    └──► spark-streaming ◄── localstack ◄── carga-historico
                                    │
                                    └──► influxdb ──► grafana
                                              │
                                              └──► jupyter
```

**Em linguagem simples:**
1. O Zookeeper arranca primeiro — o Kafka depende dele
2. O Kafka arranca depois do Zookeeper
3. Só quando o Kafka está healthy é que os producers, consumer, kafka-ui, kafka-setup e Spark arrancam
4. O Cassandra arranca em paralelo com o Kafka (não dependem um do outro)
5. O consumer só arranca quando Kafka, Cassandra E InfluxDB estão todos healthy
6. O LocalStack arranca independentemente — o `carga-historico` aguarda o LocalStack
7. O Grafana só arranca quando o InfluxDB está healthy

**Porque importa esta ordem:** Se o consumer arrancar antes do Cassandra estar pronto, tenta gravar numa base de dados que não existe e perde dados. O `depends_on` com `condition: service_healthy` garante que isto nunca acontece.

---

*Documentação Docker Compose — Forest Risk Monitoring System — ISEP 2024/2025*
