"""
================================================================================
Forest Risk Monitoring System — Carga histórica NASA FIRMS → S3 (Parquet)
================================================================================

OBJETIVO (Camada 3 — Data Lake | desbloqueia Fase 4 ML):
    Ler os CSV históricos da NASA FIRMS (5 anos, 2 satélites), limpar/normalizar
    os dados, e gravar no S3 em formato Parquet PARTICIONADO por ano/mês/zona.

    Estes dados históricos são a matéria-prima para o treino do modelo ML
    (Pessoa B, Fase 4) e populam o data lake forest-risk-datalake.

ENTRADA:
    notebooks/data/viirs-*.csv  (10 ficheiros: snpp + jpss1, 2020-2024)

SAÍDA (no S3):
    s3://forest-risk-datalake/hotspots/ano=YYYY/mes=MM/grid_id=PT-XXX/*.parquet

PADRÃO ENDPOINT CONFIGURÁVEL:
    Se AWS_ENDPOINT_URL estiver definido → grava no LocalStack (desenvolvimento)
    Se não estiver                       → grava no S3 real da AWS (produção)
    O mesmo código funciona nos dois ambientes sem alterações.

COMO CORRER (dentro do Jupyter, terminal):
    python /home/jovyan/work/carga_historico_s3.py
================================================================================
"""

import glob
import os

import boto3
import pandas as pd

# ── Configuração ──────────────────────────────────────────────────────────────
# Pasta onde estão os CSV (ajusta se necessário)
CSV_DIR = os.getenv("CSV_DIR", "/home/jovyan/work/data")

BUCKET = "forest-risk-datalake"
PREFIXO = "hotspots"   # "pasta" dentro do bucket

# Endpoint configurável (LocalStack vs AWS real)
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")  # None em produção → AWS real
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

# ── Grelha de zonas de Portugal (centroides para mapear coordenadas → grid_id) ─
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


def coords_para_grid(lat, lon):
    """Converte coordenadas GPS para o grid_id mais próximo (distância euclidiana)."""
    mais_perto = min(
        CENTROIDES.items(),
        key=lambda x: (x[1][0] - lat) ** 2 + (x[1][1] - lon) ** 2,
    )
    return mais_perto[0]


# ══════════════════════════════════════════════════════════════════════════════
# 1. LER E COMBINAR TODOS OS CSV
# ══════════════════════════════════════════════════════════════════════════════

def ler_todos_csv():
    """Lê os 10 ficheiros CSV e junta-os num único DataFrame."""
    ficheiros = sorted(glob.glob(os.path.join(CSV_DIR, "viirs-*.csv")))
    if not ficheiros:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {CSV_DIR}")

    print(f"Encontrados {len(ficheiros)} ficheiros CSV:")
    dfs = []
    for f in ficheiros:
        df = pd.read_csv(f)
        # extrai o nome do satélite do nome do ficheiro (snpp / jpss1)
        nome = os.path.basename(f)
        df["fonte_ficheiro"] = nome
        dfs.append(df)
        print(f"  {nome}: {len(df)} linhas")

    combinado = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal combinado: {len(combinado)} hotspots")
    return combinado


# ══════════════════════════════════════════════════════════════════════════════
# 2. LIMPAR E NORMALIZAR
# ══════════════════════════════════════════════════════════════════════════════

def limpar_dados(df):
    """
    Limpa e enriquece os dados:
    - remove deteções de baixa confiança (confidence == 'l')
    - converte datas e cria colunas de partição (ano, mês)
    - mapeia coordenadas para grid_id
    """
    n_inicial = len(df)

    # Remove deteções de baixa confiança (igual ao producer_apis_reais.py)
    df = df[df["confidence"] != "l"].copy()
    print(f"Removidas {n_inicial - len(df)} deteções de baixa confiança")

    # Converte data e cria colunas de partição
    df["acq_date"] = pd.to_datetime(df["acq_date"])
    df["ano"] = df["acq_date"].dt.year
    df["mes"] = df["acq_date"].dt.month

    # Mapeia cada hotspot à zona mais próxima
    print("A mapear coordenadas para grid_id...")
    df["grid_id"] = df.apply(
        lambda r: coords_para_grid(r["latitude"], r["longitude"]), axis=1
    )

    # Seleciona e renomeia as colunas relevantes para o data lake
    resultado = df[[
        "ano", "mes", "grid_id",
        "acq_date", "acq_time",
        "latitude", "longitude",
        "bright_ti4", "bright_ti5", "frp",
        "confidence", "daynight", "satellite",
    ]].rename(columns={
        "bright_ti4": "brightness",
        "frp": "frp_mw",
    })

    print(f"Dados limpos: {len(resultado)} hotspots prontos")
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# 3. GRAVAR NO S3 EM PARQUET PARTICIONADO
# ══════════════════════════════════════════════════════════════════════════════

def gravar_s3(df):
    """
    Grava o DataFrame no S3 em Parquet, particionado por ano/mes/grid_id.
    Usa o padrão de endpoint configurável (LocalStack ou AWS real).
    """
    # storage_options diz ao pandas/pyarrow como ligar ao S3
    storage_options = {
        "key": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "client_kwargs": {"region_name": AWS_REGION},
    }
    if AWS_ENDPOINT_URL:
        storage_options["client_kwargs"]["endpoint_url"] = AWS_ENDPOINT_URL
        print(f"A gravar no LocalStack: {AWS_ENDPOINT_URL}")
    else:
        print("A gravar no S3 REAL da AWS")

    caminho = f"s3://{BUCKET}/{PREFIXO}/"

    df.to_parquet(
        caminho,
        engine="pyarrow",
        partition_cols=["ano", "mes", "grid_id"],
        storage_options=storage_options,
        index=False,
    )
    print(f"\nGravado em {caminho}")
    print("Particionado por: ano / mes / grid_id")


# ══════════════════════════════════════════════════════════════════════════════
# 4. VERIFICAR O QUE FICOU NO S3
# ══════════════════════════════════════════════════════════════════════════════

def verificar_s3():
    """Lista alguns objetos gravados no bucket para confirmar."""
    s3 = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIXO, MaxKeys=10)
    objetos = resp.get("Contents", [])
    print(f"\nPrimeiros {len(objetos)} ficheiros Parquet no S3:")
    for obj in objetos:
        print(f"  {obj['Key']}  ({obj['Size']} bytes)")
    total = resp.get("KeyCount", 0)
    print(f"\n(mostrados {len(objetos)}; podem existir mais)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("CARGA HISTÓRICA NASA FIRMS → S3 (Parquet)")
    print("=" * 70)

    df = ler_todos_csv()
    df = limpar_dados(df)
    gravar_s3(df)
    verificar_s3()

    print("\n" + "=" * 70)
    print("CARGA COMPLETA!")
    print("Os dados históricos estão no data lake, prontos para o modelo ML.")
    print("=" * 70)


if __name__ == "__main__":
    main()
