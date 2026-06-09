"""
================================================================================
Forest Risk Monitoring System — Producer com APIs REAIS
================================================================================

PAPEL NA PIPELINE:
    Consulta três fontes de dados REAIS externas e publica os resultados
    no Kafka. É o complemento do producer_sensores.py — enquanto esse simula
    sensores IoT, este traz dados verdadeiros de satélites e estações meteo.

QUANDO CORRE:
    Arranca automaticamente com `docker compose up` (container producer-apis).
    Não é contínuo — verifica a cada 60 segundos se chegou a hora de consultar
    cada API (intervalos diferentes por fonte).

FONTES DE DADOS E TOPICS:
    ┌─────────────────┬────────────────────┬───────────────────────────────┐
    │ Fonte           │ Intervalo          │ Topic(s) destino              │
    ├─────────────────┼────────────────────┼───────────────────────────────┤
    │ NASA FIRMS      │ 1 hora             │ satellite-hotspots            │
    │ IPMA            │ 30 minutos         │ weather-data + sensor-events  │
    │ ICNF            │ 1 vez por dia      │ sensor-events                 │
    └─────────────────┴────────────────────┴───────────────────────────────┘

PRÉ-REQUISITOS:
    1. NASA FIRMS API KEY (obrigatório para hotspots de satélite)
       → Vai a: https://firms.modaps.eosdis.nasa.gov/api/area/
       → Clica "Get MAP_KEY" → Regista com o email do ISEP
       → Recebes a key em ~1 minuto
       → Adiciona ao ficheiro .env: NASA_FIRMS_KEY=a_tua_key_aqui

    2. IPMA — sem registo, API pública portuguesa
    3. ICNF — sem registo, mas o serviço WFS pode devolver 404 ocasionalmente

COMO CORRER MANUALMENTE (fora do Docker):
    pip install kafka-python requests pandas
    python producer_apis_reais.py

COMO CORRER DENTRO DO JUPYTER:
    Copia o código para uma célula e corre.
    Certifica que KAFKA_BOOTSTRAP está como "kafka:9092".
================================================================================
"""

import os
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

# NASA FIRMS API Key — lida do ambiente (ficheiro .env), nunca hardcoded.
# Sem esta key, o fetch de hotspots é saltado (producer continua com IPMA/ICNF).
# Obtém em: https://firms.modaps.eosdis.nasa.gov/api/area/
NASA_FIRMS_KEY = os.getenv("NASA_FIRMS_KEY", "")

# Endereço do broker Kafka.
# "kafka:9092"      → dentro da rede Docker (container-to-container)
# "localhost:29092" → fora do Docker (PC local)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Bounding box de Portugal Continental: lon_min, lat_min, lon_max, lat_max
# Limita o pedido à NASA aos hotspots em Portugal — sem este filtro,
# a NASA devolveria hotspots de toda a Europa.
PORTUGAL_BBOX = "-9.5,36.9,-6.2,42.2"

# Satélite NASA a usar.
# VIIRS_SNPP_NRT → Suomi NPP, resolução 375m, Near Real Time (mais recente)
# MODIS_NRT      → Terra/Aqua, resolução 1km, alternativa mais antiga
# NRT = Near Real Time: dados disponíveis ~3 horas após passagem do satélite
NASA_SATELLITE = "VIIRS_SNPP_NRT"

# Janela temporal de hotspots a pedir.
# 1 = apenas as últimas 24 horas (mais rápido, menos dados)
# 3 = últimos 3 dias (mais dados, útil em períodos de incêndios activos)
# Máximo suportado pela API: 10 dias
NASA_DAYS = 1

# Intervalos entre consultas a cada API (em segundos).
# Justificação:
# - NASA actualiza a ~3 horas (por passagem do satélite) → 1 hora é suficiente
# - IPMA actualiza de hora a hora → 30 minutos garante dados frescos
# - ICNF tem dados estáticos (carta de ocupação solo) → 1 dia chega
INTERVALO_NASA_SEGUNDOS  = 3600   # 1 hora
INTERVALO_IPMA_SEGUNDOS  = 1800   # 30 minutos
INTERVALO_ICNF_SEGUNDOS  = 86400  # 1 dia

# ══════════════════════════════════════════════════════════════════════════════
# MAPEAMENTO ESTAÇÕES IPMA → ZONAS DO SISTEMA
# ══════════════════════════════════════════════════════════════════════════════
# A API IPMA usa IDs numéricos internos para as estações. Este dicionário
# faz o mapeamento para o grid_id do sistema (PT-NORTE-01, etc.).
# Lista completa de estações: https://api.ipma.pt/open-data/observation/
#                              meteorology/stations/stations.json
# Para adicionar mais estações: vai ao link acima, copia o idEstacao
# e adiciona uma entrada aqui com o grid_id mais próximo.
ESTACOES_IPMA = {
    1200535: {"regiao": "Braga",          "grid_id": "PT-NORTE-01"},
    1200562: {"regiao": "Braganca",       "grid_id": "PT-NORTE-02"},
    1210713: {"regiao": "Coimbra",        "grid_id": "PT-CENTRO-01"},
    1200524: {"regiao": "Castelo Branco", "grid_id": "PT-CENTRO-02"},
    1200514: {"regiao": "Aveiro",         "grid_id": "PT-CENTRO-03"},
    1200609: {"regiao": "Santarem",       "grid_id": "PT-LVT-01"},
    1200519: {"regiao": "Lisboa",         "grid_id": "PT-LVT-02"},
    1200546: {"regiao": "Evora",          "grid_id": "PT-ALENTEJO-01"},
    1200502: {"regiao": "Beja",           "grid_id": "PT-ALENTEJO-02"},
    1200588: {"regiao": "Faro",           "grid_id": "PT-ALGARVE-01"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 1 — NASA FIRMS (hotspots de satélite)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_nasa_firms():
    """
    Consulta a API NASA FIRMS para obter hotspots térmicos activos em Portugal.
    Devolve lista de eventos prontos para publicar no topic satellite-hotspots.
    Devolve lista vazia em caso de erro (não levanta excepção — o producer
    continua a funcionar com as outras fontes).

    O QUE É O FIRMS:
    Fire Information for Resource Management System — sistema NASA que processa
    dados de satélites VIIRS e MODIS em tempo quase real. Cada hotspot representa
    um ponto onde o satélite detectou temperatura anormalmente elevada (possível
    incêndio ou queimada).

    CAMPO FRP (Fire Radiative Power):
    Medido em megawatts (MW). Quantifica a intensidade do fogo.
    Valores típicos: 5-50 MW = fogo pequeno, 100+ MW = grande incêndio.
    É o campo mais importante para o índice de risco composto do Spark.

    CAMPO CONFIDENCE:
    Para VIIRS: "low", "nominal", "high" (texto)
    Para MODIS: 0-100 (numérico)
    Este código filtra os "low" — menos fiáveis, mais falsos positivos.

    URL FORMAT:
    https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/{SAT}/{BBOX}/{DAYS}

    TRATAMENTO DE ERROS:
    - HTTPError (401) → key inválida ou expirada
    - HTTPError (429) → rate limit atingido (raramente acontece com 1h de intervalo)
    - Timeout (30s)   → NASA lenta, tenta na próxima hora
    - DataFrame vazio → sem incêndios activos (normal em dias sem incêndios)
    """
    # Guard clause: se a key não está configurada, salta silenciosamente.
    # O producer continua a enviar dados IPMA/ICNF mesmo sem a NASA key.
    if not NASA_FIRMS_KEY:
        log.warning("NASA FIRMS key não configurada — a saltar fetch de hotspots")
        return []

    # Constrói a URL da API com os parâmetros configurados
    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv"
        f"/{NASA_FIRMS_KEY}/{NASA_SATELLITE}/{PORTUGAL_BBOX}/{NASA_DAYS}"
    )

    try:
        log.info("A pedir hotspots à NASA FIRMS...")
        # timeout=30: se a NASA não responder em 30s, desiste e tenta na próxima hora
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()  # levanta HTTPError para códigos 4xx/5xx

        # A NASA devolve CSV (não JSON) — pandas trata da conversão
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))

        if df.empty:
            log.info("NASA FIRMS: nenhum hotspot detectado em Portugal")
            return []

        eventos = []
        for _, row in df.iterrows():
            # Normaliza confidence para minúsculas para comparação consistente
            # VIIRS: "low"/"nominal"/"high" → MODIS: "0"-"100" (string)
            confianca = str(row.get("confidence", "nominal")).lower()

            # Filtra detecções de baixa confiança — mais falsos positivos
            # "low" e "l" são o mesmo valor em formatos diferentes da API
            if confianca in ["low", "l"]:
                continue

            eventos.append({
                "grid_id":    _coords_para_grid(float(row["latitude"]), float(row["longitude"])),
                "latitude":   float(row["latitude"]),
                "longitude":  float(row["longitude"]),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
                # bright_ti4 = temperatura de brilho canal 4 (VIIRS)
                # Fallback para "brightness" (campo alternativo em versões mais antigas)
                "brightness": float(row.get("bright_ti4", row.get("brightness", 0))),
                "frp_mw":     float(row.get("frp", 0)),   # Fire Radiative Power em megawatts
                "confidence": confianca,
                "satellite":  str(row.get("satellite", NASA_SATELLITE)),
                "acq_date":   str(row.get("acq_date", "")),  # data de aquisição pelo satélite
                "acq_time":   str(row.get("acq_time", "")),  # hora de aquisição (UTC)
                "source":     "nasa_firms_real"
            })

        log.info(f"NASA FIRMS: {len(eventos)} hotspots válidos recebidos")
        return eventos

    except requests.exceptions.HTTPError as e:
        # 401 = key inválida, 429 = rate limit, 500 = erro servidor NASA
        log.error(f"NASA FIRMS HTTP erro: {e}")
        return []
    except Exception as e:
        log.error(f"NASA FIRMS erro inesperado: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 2 — IPMA (meteorologia Portugal)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ipma_observacoes():
    """
    Consulta a API pública do IPMA para obter observações meteorológicas
    em tempo real das estações de Portugal Continental.
    Sem autenticação — dados abertos do estado português.
    Devolve lista de eventos para os topics weather-data E sensor-events.

    API: https://api.ipma.pt/
    Formato de resposta: GeoJSON (FeatureCollection com uma Feature por estação)

    PORQUÊ DOIS TOPICS:
    Os dados IPMA vão para weather-data E sensor-events porque:
    - weather-data → consumido pelo Spark no join dos 3 streams
    - sensor-events → consumido pelo consumer para persistir no Cassandra
      (assim o Cassandra também tem observações reais, não só simuladas)

    TRATAMENTO DE NULOS:
    A IPMA frequentemente devolve null para campos em falta (estação sem
    cobertura de rede, sensor avariado, etc.). Este código:
    - Descarta a estação se temperatura ou humidade forem null (campos críticos)
    - Para os outros campos (vento, precipitação, pressão), usa 0.0 como default

    CAMPOS GeoJSON:
    O endpoint retorna FeatureCollection onde cada Feature tem:
    - geometry.coordinates → [longitude, latitude] (atenção: invertido!)
    - properties.idEstacao → ID numérico da estação
    - properties.temperatura → temperatura do ar (°C)
    - properties.humidade → humidade relativa (%)
    - properties.intensidadeVento → velocidade do vento (km/h)
    - properties.precAcum → precipitação acumulada (mm)
    """
    url = "https://api.ipma.pt/open-data/observation/meteorology/stations/obs-surface.geojson"
    # Endpoint alternativo (JSON simples, diferente estrutura):
    # url = "https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"

    try:
        log.info("A pedir observações ao IPMA...")
        # timeout=15: a IPMA é mais rápida que a NASA, 15s é suficiente
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        dados = resp.json()

        eventos = []
        features = dados.get("features", [])

        for feature in features:
            props      = feature.get("properties", {})
            estacao_id = props.get("idEstacao")

            # Só processa as 10 estações mapeadas em ESTACOES_IPMA.
            # Ignora as restantes ~100 estações do país (irrelevantes para o sistema)
            if estacao_id not in ESTACOES_IPMA:
                continue

            info   = ESTACOES_IPMA[estacao_id]
            # GeoJSON usa [longitude, latitude] — invertido em relação ao padrão GPS
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])

            # Extrai campos meteorológicos — podem ser None (null no JSON)
            temp      = props.get("temperatura")
            humidade  = props.get("humidade")
            vento     = props.get("intensidadeVento")
            dir_vento = props.get("direcaoVento")
            precipit  = props.get("precAcum")
            pressao   = props.get("pressao")

            # Descarta se temperatura ou humidade em falta — campos críticos para
            # o cálculo de risco. Os outros campos (vento, precipitação) são opcionais.
            if temp is None or humidade is None:
                log.debug(f"IPMA: dados em falta para estação {estacao_id} ({info['regiao']})")
                continue

            eventos.append({
                "grid_id":          info["grid_id"],
                "regiao":           info["regiao"],
                "estacao_id":       estacao_id,
                "latitude":         coords[1] if len(coords) > 1 else 0,  # índice 1 = lat
                "longitude":        coords[0] if len(coords) > 0 else 0,  # índice 0 = lon
                "timestamp":        datetime.now(timezone.utc).isoformat(),
                "temp_celsius":     float(temp),
                "humidity_pct":     float(humidade),
                # Para campos opcionais: usa 0.0 se None (não descarta a estação)
                "wind_kmh":         float(vento)     if vento     is not None else 0.0,
                "wind_direction":   float(dir_vento) if dir_vento is not None else 0.0,
                "precipitation_mm": float(precipit)  if precipit  is not None else 0.0,
                "pressure_hpa":     float(pressao)   if pressao   is not None else 0.0,
                "source":           "ipma_real"
            })

        log.info(f"IPMA: {len(eventos)} estações com dados válidos")
        return eventos

    except requests.exceptions.ConnectionError:
        # Sem internet — esperado em ambientes sem acesso externo
        log.error("IPMA: sem ligação à internet")
        return []
    except Exception as e:
        log.error(f"IPMA erro: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# FONTE 3 — ICNF (cartografia florestal — dados semi-estáticos)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_icnf_vegetacao():
    """
    Consulta o serviço WFS do ICNF para obter dados de uso do solo
    (Carta de Ocupação do Solo 2018 — COS2018).
    Dados semi-estáticos: consultados só uma vez por dia.

    O QUE É O COS2018:
    A Carta de Ocupação do Solo é um produto geográfico do ICNF que classifica
    o território por tipo de uso: eucaliptal, pinhal, mato, agrícola, urbano,
    etc. É relevante para o risco de incêndio porque diferentes vegetações
    têm diferentes susceptibilidades ao fogo.

    SERVIÇO WFS (Web Feature Service):
    Protocolo OGC para consultar dados geográficos vectoriais.
    A resposta é JSON com "features" (polígonos com atributos).
    O endpoint pode devolver 404 quando o serviço ICNF está em manutenção
    — isto é normal e o produtor trata silenciosamente.

    CAMPOS RELEVANTES:
    - DESCRICAO → tipo de uso do solo (ex: "Eucaliptal", "Pinhal bravo")
    - AREA_HA   → área do polígono em hectares
    - Shape_Area → área calculada pelo GIS

    NOTA SOBRE O RISCO DE VEGETAÇÃO:
    A classificação HIGH/MEDIUM/LOW é uma simplificação baseada em
    conhecimento empírico sobre inflamabilidade das espécies em Portugal.
    Para maior rigor científico, consultar estudos do CEABN (ISA Lisboa).
    """
    # URL do serviço ArcGIS REST do ICNF com filtros geográficos
    # where=1%3D1 → WHERE 1=1 → devolve todos os registos
    # geometry=   → bounding box de Portugal Continental
    # resultRecordCount=100 → limita a 100 polígonos (evita timeout por excesso de dados)
    url = (
        "https://sig.icnf.pt/arcgis/rest/services/ICNF/COS2018/MapServer/0/query"
        "?where=1%3D1"
        "&outFields=DESCRICAO,AREA_HA,Shape_Area"
        "&geometry=-9.5%2C36.9%2C-6.2%2C42.2"    # Portugal Continental (bbox)
        "&geometryType=esriGeometryEnvelope"
        "&inSR=4326"                               # sistema de referência WGS84
        "&spatialRel=esriSpatialRelIntersects"
        "&returnGeometry=false"                    # não precisa das geometrias, só atributos
        "&resultRecordCount=100"
        "&f=json"
    )

    try:
        log.info("A pedir dados de vegetação ao ICNF...")
        # timeout=30: o WFS do ICNF é mais lento (processamento geoespacial)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dados = resp.json()

        features = dados.get("features", [])
        if not features:
            log.warning("ICNF: sem dados de vegetação recebidos")
            return []

        eventos = []
        for feature in features:
            attrs     = feature.get("attributes", {})
            descricao = attrs.get("DESCRICAO", "Desconhecido")
            area_ha   = attrs.get("AREA_HA", 0)

            # Classifica risco de propagação com base no tipo de vegetação
            risco_veg = _risco_por_vegetacao(descricao)

            eventos.append({
                "timestamp":       datetime.now(timezone.utc).isoformat(),
                "tipo_vegetacao":  descricao,
                "area_ha":         float(area_ha) if area_ha else 0.0,
                "risco_vegetacao": risco_veg,   # "LOW", "MEDIUM" ou "HIGH"
                "source":          "icnf_cos2018"
            })

        log.info(f"ICNF: {len(eventos)} registos de vegetação recebidos")
        return eventos

    except requests.exceptions.Timeout:
        # O WFS do ICNF é frequentemente lento — timeout não é erro grave
        log.warning("ICNF: timeout — serviço lento, a tentar mais tarde")
        return []
    except Exception as e:
        # 404 quando o serviço ICNF está em manutenção — também não é erro grave
        log.error(f"ICNF erro: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# FUNÇÕES AUXILIARES
# ══════════════════════════════════════════════════════════════════════════════

def _coords_para_grid(lat, lon):
    """
    Converte coordenadas GPS (latitude, longitude) para o grid_id
    da zona de Portugal mais próxima.

    Algoritmo: distância euclidiana ao quadrado aos 10 centroides.
    Não é geograficamente perfeita (devia usar Haversine para grande escala)
    mas para Portugal Continental (~600km de distância máxima) a diferença
    é negligenciável — os erros de atribuição são mínimos.

    Exemplo:
        lat=38.5, lon=-8.0 → PT-ALENTEJO-01 (centroide: 38.57, -7.91)

    Nota: esta função é duplicada em carga_historico_s3.py por design —
    cada script é autónomo e não depende do outro.
    """
    CENTROIDES = {
        "PT-NORTE-01":    (41.55, -8.42),
        "PT-NORTE-02":    (41.69, -7.91),
        "PT-CENTRO-01":   (40.20, -8.41),
        "PT-CENTRO-02":   (39.82, -7.49),
        "PT-CENTRO-03":   (40.64, -8.65),
        "PT-LVT-01":      (39.35, -8.13),
        "PT-LVT-02":      (38.71, -9.14),
        "PT-ALENTEJO-01": (38.57, -7.91),
        "PT-ALENTEJO-02": (37.80, -7.49),
        "PT-ALGARVE-01":  (37.10, -8.67),
    }
    mais_perto = min(
        CENTROIDES.items(),
        key=lambda x: (x[1][0] - lat) ** 2 + (x[1][1] - lon) ** 2
    )
    return mais_perto[0]


def _risco_por_vegetacao(descricao):
    """
    Classifica o risco de propagação de incêndio pelo tipo de vegetação.
    Baseado em conhecimento empírico sobre inflamabilidade em Portugal.

    Classificação:
    HIGH   → eucaliptal, pinheiro bravo, mato arbustivo
             (espécies mais inflamáveis, maior velocidade de propagação)
    MEDIUM → sobreiro, carvalho, floresta de folha caduca
             (menor inflamabilidade, mais resistentes ao fogo)
    LOW    → áreas urbanas, massas de água, agrícola, pastagem
             (não propaga ou propaga muito lentamente)
    MEDIUM → default conservador para tipos desconhecidos

    Parâmetros:
        descricao → campo DESCRICAO da Carta de Ocupação do Solo (COS2018)
                    Ex: "Eucaliptal", "Pinhal bravo", "Sobreiro"
    """
    desc = descricao.lower()

    # Vegetação de alto risco: eucalipto e pinheiro têm óleos essenciais
    # inflamáveis; mato/arbustivo seco propaga rapidamente
    if any(t in desc for t in ["eucalipto", "pinheiro", "mato", "arbustivo"]):
        return "HIGH"

    # Vegetação de risco moderado: folha larga cria mais humidade e resiste melhor
    elif any(t in desc for t in ["carvalho", "sobreiro", "floresta"]):
        return "MEDIUM"

    # Baixo risco: não-vegetação ou vegetação que não propaga incêndio
    elif any(t in desc for t in ["urbano", "agua", "agricola", "pastagem"]):
        return "LOW"

    # Default conservador: preferível classificar como médio do que ignorar
    else:
        return "MEDIUM"


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCER PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Ciclo principal do producer de APIs reais. Corre indefinidamente até Ctrl+C.

    ESTRATÉGIA DE TEMPORIZAÇÃO (elapsed time vs sleep fixo):
    Em vez de `sleep(3600)` para esperar 1 hora, o loop dorme 60 segundos
    e verifica se passou tempo suficiente para cada API. Vantagens:
    1. Permite responder a Ctrl+C mais rapidamente (a cada 60s no máximo)
    2. Permite frequências diferentes por API no mesmo loop
    3. Mais robusto: se uma API demora muito, não atrasa as outras

    INICIALIZAÇÃO COM datetime.min:
    `ultimo_nasa = datetime.min` força a consulta imediatamente ao arrancar,
    sem esperar o intervalo configurado. Caso contrário, o primeiro fetch
    só aconteceria depois de 1 hora (INTERVALO_NASA_SEGUNDOS).

    TOPICS POR FONTE:
    NASA  → satellite-hotspots (1 topic — só lido pelo Spark)
    IPMA  → weather-data (Spark) + sensor-events (consumer + Spark)
            Vai para sensor-events também para que o Cassandra tenha dados
            meteorológicos reais, não só simulados.
    ICNF  → sensor-events (key="ICNF" — identifica a origem nos logs)
    """
    log.info(f"A ligar ao Kafka em {KAFKA_BOOTSTRAP}...")
    log.info("MODO: APIs REAIS (NASA FIRMS + IPMA + ICNF)")

    # Aviso visível se a NASA key não está configurada
    # O producer funciona sem ela mas satellite-hotspots ficará vazio
    if not NASA_FIRMS_KEY:
        log.warning("=" * 60)
        log.warning("NASA FIRMS key não configurada!")
        log.warning("   Hotspots de satélite não serão enviados.")
        log.warning("   Adiciona NASA_FIRMS_KEY ao ficheiro .env")
        log.warning("=" * 60)

    # KafkaProducer com serialização JSON e garantia de entrega
    # acks="all" → aguarda confirmação de todas as réplicas (mais seguro)
    # retries=5  → re-tenta automaticamente em falhas temporárias do broker
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all"
    )

    log.info("Ligado ao Kafka!")

    # datetime.min → garante fetch imediato na primeira iteração do loop
    # Sem isto, o primeiro fetch seria adiado pelo intervalo completo
    ultimo_nasa = datetime.min
    ultimo_ipma = datetime.min
    ultimo_icnf = datetime.min

    try:
        while True:
            agora = datetime.now()

            # ── NASA FIRMS — hotspots de satélite (a cada hora) ───────────────
            if (agora - ultimo_nasa).total_seconds() >= INTERVALO_NASA_SEGUNDOS:
                eventos = fetch_nasa_firms()
                for ev in eventos:
                    # key=grid_id garante que hotspots da mesma zona vão
                    # para a mesma partição Kafka (preserva ordem por zona)
                    producer.send("satellite-hotspots", key=ev["grid_id"], value=ev)
                if eventos:
                    producer.flush()  # confirma envio ao broker
                    log.info(f"{len(eventos)} hotspots enviados para satellite-hotspots")
                ultimo_nasa = agora

            # ── IPMA — meteorologia (a cada 30 min) ──────────────────────────
            if (agora - ultimo_ipma).total_seconds() >= INTERVALO_IPMA_SEGUNDOS:
                eventos = fetch_ipma_observacoes()
                for ev in eventos:
                    # weather-data → para o Spark (join do 3º stream)
                    producer.send("weather-data", key=ev["grid_id"], value=ev)
                    # sensor-events → para o consumer/Cassandra (dados reais misturados
                    # com os simulados do producer_sensores.py)
                    producer.send("sensor-events", key=ev["grid_id"], value=ev)
                if eventos:
                    producer.flush()
                    log.info(f"{len(eventos)} observações IPMA enviadas")
                ultimo_ipma = agora

            # ── ICNF — vegetação (1 vez por dia) ─────────────────────────────
            if (agora - ultimo_icnf).total_seconds() >= INTERVALO_ICNF_SEGUNDOS:
                eventos = fetch_icnf_vegetacao()
                for ev in eventos:
                    # key="ICNF" (não grid_id) porque os dados ICNF não têm
                    # coordenadas específicas — são polígonos de área
                    producer.send("sensor-events", key="ICNF", value=ev)
                if eventos:
                    producer.flush()
                    log.info(f"{len(eventos)} registos ICNF enviados")
                ultimo_icnf = agora

            # Log de tempo até próxima actualização (útil para monitorização)
            log.info(
                f"Proxima actualizacao — "
                f"NASA: {max(0, INTERVALO_NASA_SEGUNDOS - (agora - ultimo_nasa).total_seconds()):.0f}s  "
                f"IPMA: {max(0, INTERVALO_IPMA_SEGUNDOS - (agora - ultimo_ipma).total_seconds()):.0f}s"
            )

            # Dorme 60s antes de verificar novamente se é hora de actualizar.
            # Granularidade de 1 minuto — equilíbrio entre responsividade e CPU.
            time.sleep(60)

    except KeyboardInterrupt:
        log.info("\nProducer parado pelo utilizador.")
    finally:
        # Fecha a ligação Kafka de forma limpa (flush das mensagens pendentes)
        producer.close()
        log.info("Ligação Kafka fechada.")


if __name__ == "__main__":
    main()
