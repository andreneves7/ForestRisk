"""
Forest Risk Monitoring System
Producer com APIs REAIS — NASA FIRMS + IPMA + ICNF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANTES DE USAR ESTE FICHEIRO, FAZ ISTO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. NASA FIRMS API KEY (obrigatório para hotspots de satélite)
   → Vai a: https://firms.modaps.eosdis.nasa.gov/api/area/
   → Clica "Get MAP_KEY"
   → Regista com o teu email do ISEP
   → Recebes a key por email em ~1 minuto
   → Cola-a na variável NASA_FIRMS_KEY abaixo

2. IPMA — não precisa de registo, funciona imediatamente

3. ICNF — não precisa de registo, funciona imediatamente

4. Instala as dependências (se ainda não fizeste):
   pip install kafka-python requests pandas

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMO CORRER:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Fora do Docker (no teu PC):
      python producer_apis_reais.py

  Dentro do Jupyter (já tem as libs):
      Muda KAFKA_BOOTSTRAP para "kafka:9092"
      Copia o código para uma célula e corre

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# ══════════════════════════════════════════════════════════════════
# ⚙️  CONFIGURAÇÃO — ALTERA AQUI
# ══════════════════════════════════════════════════════════════════

# TODO: Cola aqui a tua API key da NASA FIRMS
# Obténs em: https://firms.modaps.eosdis.nasa.gov/api/area/
NASA_FIRMS_KEY = "579b22bcae291064c381d64a3375f069"

# Kafka — muda para "kafka:9092" se correres dentro do Jupyter/Docker
KAFKA_BOOTSTRAP = "localhost:29092"

# Bounding box de Portugal Continental (esquerda, baixo, direita, cima)
# Não precisas de alterar, cobre todo o território continental
PORTUGAL_BBOX = "-9.5,36.9,-6.2,42.2"

# Satélite NASA a usar — VIIRS_SNPP_NRT é o mais recente e preciso
# Alternativas: MODIS_NRT (mais antigo mas também funciona)
NASA_SATELLITE = "VIIRS_SNPP_NRT"

# Quantos dias de histórico de hotspots a pedir à NASA (1 a 10)
# 1 = só hoje, mais rápido. 3 = últimos 3 dias, mais dados.
NASA_DAYS = 1

# De quanto em quanto tempo actualiza os dados (segundos)
# NASA FIRMS actualiza a cada ~3 horas, não vale a pena pedir mais vezes
# IPMA actualiza de hora a hora
INTERVALO_NASA_SEGUNDOS  = 3600   # 1 hora
INTERVALO_IPMA_SEGUNDOS  = 1800   # 30 minutos
INTERVALO_ICNF_SEGUNDOS  = 86400  # 1 vez por dia (dados estáticos)

# ══════════════════════════════════════════════════════════════════
# IDs das estações IPMA para Portugal Continental
# Lista completa: https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json
# TODO: Se quiseres adicionar mais estações, vai ao link acima e copia os IDs
# ══════════════════════════════════════════════════════════════════
ESTACOES_IPMA = {
    1200535: {"regiao": "Braga",           "grid_id": "PT-NORTE-01"},
    1200562: {"regiao": "Braganca",        "grid_id": "PT-NORTE-02"},
    1210713: {"regiao": "Coimbra",         "grid_id": "PT-CENTRO-01"},
    1200524: {"regiao": "Castelo Branco",  "grid_id": "PT-CENTRO-02"},
    1200514: {"regiao": "Aveiro",          "grid_id": "PT-CENTRO-03"},
    1200609: {"regiao": "Santarem",        "grid_id": "PT-LVT-01"},
    1200519: {"regiao": "Lisboa",          "grid_id": "PT-LVT-02"},
    1200546: {"regiao": "Evora",           "grid_id": "PT-ALENTEJO-01"},
    1200502: {"regiao": "Beja",            "grid_id": "PT-ALENTEJO-02"},
    1200588: {"regiao": "Faro",            "grid_id": "PT-ALGARVE-01"},
}

# ══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# FONTE 1 — NASA FIRMS (hotspots de satélite)
# ──────────────────────────────────────────────────────────────────

def fetch_nasa_firms():
    """
    Vai buscar hotspots térmicos detetados por satélite à NASA.
    Devolve lista de eventos prontos para publicar no Kafka.

    API docs: https://firms.modaps.eosdis.nasa.gov/api/area/
    Formato de resposta: CSV com colunas latitude, longitude, brightness,
                         scan, track, acq_date, acq_time, satellite,
                         confidence, frp (Fire Radiative Power em MW)

    TODO: Se a API devolver erro 401, a tua key está errada ou expirou.
          Vai ao link acima e gera uma nova.
    TODO: Se quiseres mais precisão, muda NASA_DAYS para 3 ou 7.
    """
    if NASA_FIRMS_KEY == "579b22bcae291064c381d64a3375f069":
        log.warning("⚠️  NASA FIRMS key não configurada — a saltar fetch de hotspots")
        return []

    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv"
        f"/{NASA_FIRMS_KEY}/{NASA_SATELLITE}/{PORTUGAL_BBOX}/{NASA_DAYS}"
    )

    try:
        log.info("🛰️  A pedir hotspots à NASA FIRMS...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        # A NASA devolve CSV — usamos pandas para parsear
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))

        if df.empty:
            log.info("NASA FIRMS: nenhum hotspot detectado em Portugal")
            return []

        eventos = []
        for _, row in df.iterrows():
            # confidence pode ser "nominal", "high", "low" (VIIRS) ou 0-100 (MODIS)
            # TODO: Se usares MODIS_NRT, o campo confidence é numérico (0-100)
            #       Se usares VIIRS_SNPP_NRT, é texto: "low"/"nominal"/"high"
            confianca = str(row.get("confidence", "nominal")).lower()
            if confianca in ["low", "l"]:
                continue  # ignora detecções de baixa confiança

            eventos.append({
                "grid_id":      _coords_para_grid(float(row["latitude"]), float(row["longitude"])),
                "latitude":     float(row["latitude"]),
                "longitude":    float(row["longitude"]),
                "timestamp":    datetime.now(timezone.utc).isoformat(),
                "brightness":   float(row.get("bright_ti4", row.get("brightness", 0))),
                "frp_mw":       float(row.get("frp", 0)),       # Fire Radiative Power em megawatts
                "confidence":   confianca,
                "satellite":    str(row.get("satellite", NASA_SATELLITE)),
                "acq_date":     str(row.get("acq_date", "")),
                "acq_time":     str(row.get("acq_time", "")),
                "source":       "nasa_firms_real"
            })

        log.info(f"🛰️  NASA FIRMS: {len(eventos)} hotspots válidos recebidos")
        return eventos

    except requests.exceptions.HTTPError as e:
        log.error(f"NASA FIRMS HTTP erro: {e}")
        # TODO: Se acontecer frequentemente, verifica se a key é válida
        return []
    except Exception as e:
        log.error(f"NASA FIRMS erro inesperado: {e}")
        return []


# ──────────────────────────────────────────────────────────────────
# FONTE 2 — IPMA (meteorologia Portugal)
# ──────────────────────────────────────────────────────────────────

def fetch_ipma_observacoes():
    """
    Vai buscar observações meteorológicas em tempo real do IPMA.
    Sem autenticação — API pública portuguesa.

    API docs: https://api.ipma.pt/
    Devolve temperatura, humidade, velocidade e direção do vento,
    precipitação e pressão atmosférica por estação.

    TODO: A API do IPMA por vezes devolve null para alguns campos
          (estação sem dados naquele momento). O código já trata isso,
          mas se vires muitos warnings podes ignorar essas estações.
    """
    # A chave do endpoint é a data de hoje no formato YYYY-MM-DD
    hoje = datetime.now().strftime("%Y-%m-%d")
    hora_atual = datetime.now().strftime("%H") + ":00"

    url = f"https://api.ipma.pt/open-data/observation/meteorology/stations/obs-surface.geojson"

    # TODO: Se o endpoint acima falhar, tenta este alternativo:
    # url = f"https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json"

    try:
        log.info("🌤️  A pedir observações ao IPMA...")
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        dados = resp.json()

        eventos = []

        # O formato GeoJSON tem features com properties
        features = dados.get("features", [])

        for feature in features:
            props = feature.get("properties", {})
            estacao_id = props.get("idEstacao")

            if estacao_id not in ESTACOES_IPMA:
                continue  # só processa as estações que nos interessam

            info = ESTACOES_IPMA[estacao_id]
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])

            # IPMA usa null para dados em falta — substituímos por None
            temp     = props.get("temperatura")
            humidade = props.get("humidade")
            vento    = props.get("intensidadeVento")
            dir_vento = props.get("direcaoVento")
            precipit = props.get("precAcum")
            pressao  = props.get("pressao")

            # TODO: Se precisares de mais campos, o IPMA também fornece:
            #       radiacaoGlobal, insolacao, visibilidade
            #       Consulta a documentação em https://api.ipma.pt/

            if temp is None or humidade is None:
                log.debug(f"IPMA: dados em falta para estação {estacao_id} ({info['regiao']})")
                continue

            eventos.append({
                "grid_id":        info["grid_id"],
                "regiao":         info["regiao"],
                "estacao_id":     estacao_id,
                "latitude":       coords[1] if len(coords) > 1 else 0,
                "longitude":      coords[0] if len(coords) > 0 else 0,
                "timestamp":      datetime.now(timezone.utc).isoformat(),
                "temp_celsius":   float(temp),
                "humidity_pct":   float(humidade),
                "wind_kmh":       float(vento) if vento is not None else 0.0,
                "wind_direction": float(dir_vento) if dir_vento is not None else 0.0,
                "precipitation_mm": float(precipit) if precipit is not None else 0.0,
                "pressure_hpa":   float(pressao) if pressao is not None else 0.0,
                "source":         "ipma_real"
            })

        log.info(f"🌤️  IPMA: {len(eventos)} estações com dados válidos")
        return eventos

    except requests.exceptions.ConnectionError:
        log.error("IPMA: sem ligação à internet")
        return []
    except Exception as e:
        log.error(f"IPMA erro: {e}")
        return []


# ──────────────────────────────────────────────────────────────────
# FONTE 3 — ICNF (cartografia florestal — dados estáticos)
# ──────────────────────────────────────────────────────────────────

def fetch_icnf_vegetacao():
    """
    Vai buscar dados de uso do solo e tipo de vegetação do ICNF.
    Estes dados são semi-estáticos (mudam raramente) — por isso
    só buscamos uma vez por dia.

    WFS docs: https://sig.icnf.pt/
    Devolve polígonos com tipo de uso do solo (floresta, mato, etc.)

    TODO: O ICNF tem vários layers disponíveis. Este usa o COS2018
          (Carta de Ocupação do Solo 2018). Para dados mais recentes,
          verifica se existe COS2021 ou posterior no endpoint.
    TODO: O serviço WFS do ICNF pode ser lento (~5-10 segundos).
          É normal, não é erro.
    TODO: Se quiseres apenas uma zona específica, adiciona um filtro
          geográfico com &geometry= no URL.
    """
    url = (
        "https://sig.icnf.pt/arcgis/rest/services/ICNF/COS2018/MapServer/0/query"
        "?where=1%3D1"
        "&outFields=DESCRICAO,AREA_HA,Shape_Area"
        "&geometry=-9.5%2C36.9%2C-6.2%2C42.2"     # Portugal Continental
        "&geometryType=esriGeometryEnvelope"
        "&inSR=4326"
        "&spatialRel=esriSpatialRelIntersects"
        "&returnGeometry=false"
        "&resultRecordCount=100"                    # TODO: aumenta se quiseres mais registos
        "&f=json"
    )

    try:
        log.info("🌲 A pedir dados de vegetação ao ICNF...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dados = resp.json()

        features = dados.get("features", [])
        if not features:
            log.warning("ICNF: sem dados de vegetação recebidos")
            return []

        eventos = []
        for feature in features:
            attrs = feature.get("attributes", {})
            descricao = attrs.get("DESCRICAO", "Desconhecido")
            area_ha   = attrs.get("AREA_HA", 0)

            # Classifica o risco de propagação com base no tipo de vegetação
            # TODO: Ajusta estes pesos conforme a literatura científica
            #       que usares no relatório (ex: índice de inflamabilidade)
            risco_veg = _risco_por_vegetacao(descricao)

            eventos.append({
                "timestamp":       datetime.now(timezone.utc).isoformat(),
                "tipo_vegetacao":  descricao,
                "area_ha":         float(area_ha) if area_ha else 0.0,
                "risco_vegetacao": risco_veg,   # LOW / MEDIUM / HIGH
                "source":          "icnf_cos2018"
            })

        log.info(f"🌲 ICNF: {len(eventos)} registos de vegetação recebidos")
        return eventos

    except requests.exceptions.Timeout:
        log.warning("ICNF: timeout — serviço lento, a tentar mais tarde")
        return []
    except Exception as e:
        log.error(f"ICNF erro: {e}")
        return []


# ──────────────────────────────────────────────────────────────────
# Funções auxiliares
# ──────────────────────────────────────────────────────────────────

def _coords_para_grid(lat, lon):
    """
    Converte coordenadas GPS para o grid_id mais próximo.
    Usa distância euclidiana simples — suficiente para Portugal.

    TODO: Para maior precisão geográfica, usa a biblioteca geopy
          e distância de Haversine. Para o projecto é desnecessário.
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
    Classifica o risco de propagação de incêndio por tipo de vegetação.

    TODO: Refina esta classificação com base em estudos científicos
          sobre inflamabilidade de espécies florestais portuguesas.
          Sugestão: artigos do CEABN (Centro de Ecologia Aplicada
          Prof. Baeta Neves) do ISA Lisboa.
    """
    desc = descricao.lower()
    if any(t in desc for t in ["eucalipto", "pinheiro", "mato", "arbustivo"]):
        return "HIGH"
    elif any(t in desc for t in ["carvalho", "sobreiro", "floresta"]):
        return "MEDIUM"
    elif any(t in desc for t in ["urbano", "agua", "agricola", "pastagem"]):
        return "LOW"
    else:
        return "MEDIUM"  # default conservador


# ──────────────────────────────────────────────────────────────────
# Producer principal
# ──────────────────────────────────────────────────────────────────

def main():
    log.info(f"A ligar ao Kafka em {KAFKA_BOOTSTRAP}...")
    log.info("MODO: APIs REAIS (NASA FIRMS + IPMA + ICNF)")

    if NASA_FIRMS_KEY == "COLOCA_AQUI_A_TUA_KEY_NASA":
        log.warning("=" * 60)
        log.warning("⚠️  NASA FIRMS key não configurada!")
        log.warning("   Hotspots de satélite não serão enviados.")
        log.warning("   Segue as instruções no topo do ficheiro.")
        log.warning("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all"
    )

    log.info("✅ Ligado ao Kafka!")

    # Controlo de tempo para não pedir às APIs com demasiada frequência
    ultimo_nasa  = datetime.min
    ultimo_ipma  = datetime.min
    ultimo_icnf  = datetime.min

    try:
        while True:
            agora = datetime.now()

            # ── NASA FIRMS (hotspots) ──────────────────────────────
            if (agora - ultimo_nasa).total_seconds() >= INTERVALO_NASA_SEGUNDOS:
                eventos = fetch_nasa_firms()
                for ev in eventos:
                    producer.send("satellite-hotspots", key=ev["grid_id"], value=ev)
                if eventos:
                    producer.flush()
                    log.info(f"🛰️  {len(eventos)} hotspots enviados para satellite-hotspots")
                ultimo_nasa = agora

            # ── IPMA (meteorologia) ───────────────────────────────
            if (agora - ultimo_ipma).total_seconds() >= INTERVALO_IPMA_SEGUNDOS:
                eventos = fetch_ipma_observacoes()
                for ev in eventos:
                    producer.send("weather-data", key=ev["grid_id"], value=ev)
                    producer.send("sensor-events", key=ev["grid_id"], value=ev)
                if eventos:
                    producer.flush()
                    log.info(f"🌤️  {len(eventos)} observações IPMA enviadas")
                ultimo_ipma = agora

            # ── ICNF (vegetação — 1x por dia) ────────────────────
            if (agora - ultimo_icnf).total_seconds() >= INTERVALO_ICNF_SEGUNDOS:
                eventos = fetch_icnf_vegetacao()
                for ev in eventos:
                    producer.send("sensor-events", key="ICNF", value=ev)
                if eventos:
                    producer.flush()
                    log.info(f"🌲 {len(eventos)} registos ICNF enviados")
                ultimo_icnf = agora

            # Pausa entre ciclos de verificação
            log.info(
                f"Próxima actualização — "
                f"NASA: {max(0, INTERVALO_NASA_SEGUNDOS - (agora - ultimo_nasa).total_seconds()):.0f}s  "
                f"IPMA: {max(0, INTERVALO_IPMA_SEGUNDOS - (agora - ultimo_ipma).total_seconds()):.0f}s"
            )
            time.sleep(60)  # verifica a cada minuto se está na hora de actualizar

    except KeyboardInterrupt:
        log.info("\nProducer parado pelo utilizador.")
    finally:
        producer.close()
        log.info("Ligação Kafka fechada.")


if __name__ == "__main__":
    main()
EOF
cp /home/claude/producer_apis_reais.py /mnt/user-data/outputs/producer_apis_reais.py
echo "OK"