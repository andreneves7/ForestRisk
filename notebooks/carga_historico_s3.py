"""
================================================================================
Forest Risk Monitoring System — Carga historica -> S3 (Parquet)
================================================================================

OBJETIVO (Camada 3 — Data Lake | desbloqueia Fase 4 ML):
    Carrega dados historicos para o S3 em formato Parquet.
    Suporta 2 modos por ordem de prioridade:

    MODO 1 — Parquet das EDAs (preferido):
        Filtragem_Parquet/firms_portugal_limpo_todos.parquet  <- EDA_NASA.py
        ERA5_Parquet/era5_portugal_todos.parquet              <- EDA_ERA5.py

    MODO 2 — CSV brutos da pasta NASACSV (fallback se EDA nao correu):
        NASACSV/viirs-*.csv  <- replica exactamente o que a EDA faria

SAIDA no S3:
    s3://forest-risk-datalake/hotspots/       <- NASA FIRMS
    s3://forest-risk-datalake/meteorologia/   <- ERA5 (so se EDA correu)

NOTA SOBRE O MODO 2:
    A EDA_NASA.py le de NASACSV/ e filtra por bbox de Portugal.
    NAO remove confidence='l' (a EDA guarda todos os niveis).
    Este script replica esse comportamento exactamente.

COMO CORRER:
    python /home/jovyan/work/carga_historico_s3.py
================================================================================
"""

import os
from pathlib import Path

import boto3
import pandas as pd

# ── Configuracao ──────────────────────────────────────────────────────────────
BASE_DIR         = Path(os.getenv("BASE_DIR", "/home/jovyan/work"))

# Modo 1 — Parquet das EDAs (gerados por EDA_NASA.py e EDA_ERA5.py)
PASTA_EDA_NASA   = BASE_DIR / "Filtragem_Parquet"
PASTA_EDA_ERA5   = BASE_DIR / "ERA5_Parquet"

# Modo 2 — CSV brutos (pasta que a EDA_NASA.py usa: NASACSV/)
PASTA_CSV_NASA   = BASE_DIR / "NASACSV"

BUCKET           = "forest-risk-datalake"
PREFIXO_NASA     = "hotspots"
PREFIXO_ERA5     = "meteorologia"

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_REGION       = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

# Bbox de Portugal — igual ao que a EDA usa
LAT_MIN, LAT_MAX = 36.9, 42.2
LON_MIN, LON_MAX = -9.5, -6.2

# Grelha de zonas (para adicionar grid_id — a EDA nao calcula isto)
CENTROIDES = {
    "PT-NORTE-01":    (41.55, -8.42), "PT-NORTE-02":    (41.69, -7.91),
    "PT-CENTRO-01":   (40.20, -8.41), "PT-CENTRO-02":   (39.82, -7.49),
    "PT-CENTRO-03":   (40.64, -8.65), "PT-LVT-01":      (39.35, -8.13),
    "PT-LVT-02":      (38.71, -9.14), "PT-ALENTEJO-01": (38.57, -7.91),
    "PT-ALENTEJO-02": (37.80, -7.49), "PT-ALGARVE-01":  (37.10, -8.67),
}

def coords_para_grid(lat, lon):
    return min(CENTROIDES.items(),
               key=lambda x: (x[1][0]-lat)**2 + (x[1][1]-lon)**2)[0]


# ══════════════════════════════════════════════════════════════════════════════
# MODO 1 — LER PARQUET DAS EDAs
# ══════════════════════════════════════════════════════════════════════════════

def ler_parquet_eda(pasta, prefixo, nome_eda):
    """Le Parquet ja processado pela EDA. Retorna None se nao existir."""
    pasta = Path(pasta)
    if not pasta.exists():
        return None

    # Tenta ficheiro combinado primeiro
    f_todos = pasta / f"{prefixo}_todos.parquet"
    if f_todos.exists():
        df = pd.read_parquet(f_todos)
        print(f"  [Parquet EDA] {f_todos.name}: {len(df):,} registos")
        return df

    # Fallback: ficheiros por ano
    ficheiros = sorted([f for f in pasta.glob(f"{prefixo}_*.parquet")
                        if "todos" not in f.name])
    if not ficheiros:
        return None

    dfs = []
    for f in ficheiros:
        df_ano = pd.read_parquet(f)
        dfs.append(df_ano)
        print(f"  [Parquet EDA] {f.name}: {len(df_ano):,} registos")
    combinado = pd.concat(dfs, ignore_index=True)
    print(f"  [Parquet EDA] Total: {len(combinado):,} registos")
    return combinado


# ══════════════════════════════════════════════════════════════════════════════
# MODO 2 — LER CSV BRUTOS (replica logica da EDA_NASA.py)
# ══════════════════════════════════════════════════════════════════════════════

def ler_csv_como_eda(pasta):
    """
    Le CSV de NASACSV/ e replica exactamente o que a EDA_NASA.py faz:
    - Le todos os CSV da pasta
    - Adiciona coluna 'satelite' com base no nome do ficheiro
    - Converte acq_date e cria ano/mes/dia
    - Filtra para bbox de Portugal (igual ao filtro da EDA)
    - NAO remove confidence='l' (a EDA tambem nao remove)
    """
    pasta = Path(pasta)
    if not pasta.exists():
        print(f"  Pasta '{pasta}' nao encontrada.")
        return None

    ficheiros = sorted(pasta.glob("*.csv"))
    if not ficheiros:
        print(f"  Nenhum CSV encontrado em '{pasta}'.")
        return None

    frames = []
    for f in ficheiros:
        nome = f.name.lower()
        if 'snpp' in nome or 's-npp' in nome:
            satelite = 'VIIRS S-NPP'
        elif 'noaa' in nome or 'jpss1' in nome or 'jpss2' in nome:
            satelite = 'VIIRS NOAA-20'
        else:
            satelite = 'VIIRS'

        df = pd.read_csv(f, low_memory=False)
        df['satelite'] = satelite

        if 'acq_date' in df.columns:
            df['acq_date'] = pd.to_datetime(df['acq_date'])
            df['ano'] = df['acq_date'].dt.year
            df['mes'] = df['acq_date'].dt.month
            df['dia'] = df['acq_date'].dt.day

        frames.append(df)
        print(f"  [CSV] {f.name}: {len(df):,} registos ({satelite})")

    combinado = pd.concat(frames, ignore_index=True)
    print(f"  [CSV] Total antes de filtrar: {len(combinado):,}")

    # Filtra para Portugal (bbox — igual ao que a EDA faz)
    df_pt = combinado[
        (combinado['latitude']  >= LAT_MIN) & (combinado['latitude']  <= LAT_MAX) &
        (combinado['longitude'] >= LON_MIN) & (combinado['longitude'] <= LON_MAX)
    ].copy()
    fora = len(combinado) - len(df_pt)
    print(f"  [CSV] Filtrados para Portugal: {len(df_pt):,} ({fora:,} fora do bbox)")

    # Selecciona as mesmas colunas que a EDA guarda
    COLUNAS_EDA = [
        'latitude', 'longitude', 'bright_ti4', 'bright_ti5', 'frp',
        'acq_date', 'acq_time', 'confidence', 'daynight', 'satellite',
        'satelite', 'ano', 'mes', 'dia'
    ]
    cols = [c for c in COLUNAS_EDA if c in df_pt.columns]
    return df_pt[cols].copy()


# ══════════════════════════════════════════════════════════════════════════════
# ENRIQUECIMENTO — adiciona grid_id (a EDA nao calcula)
# ══════════════════════════════════════════════════════════════════════════════

def adicionar_grid_id(df):
    """Adiciona grid_id com base nas coordenadas. A EDA nao faz isto."""
    if "grid_id" not in df.columns:
        print("  A mapear coordenadas -> grid_id...")
        df = df.copy()
        df["grid_id"] = df.apply(
            lambda r: coords_para_grid(r["latitude"], r["longitude"]), axis=1)
    print(f"  grid_id: {df['grid_id'].nunique()} zonas | anos: {sorted(df['ano'].unique())}")
    return df


def preparar_era5(df):
    df = df.copy()
    if "ano" not in df.columns and "time" in df.columns:
        dt = pd.to_datetime(df["time"])
        df["ano"] = dt.dt.year
        df["mes"] = dt.dt.month
    print(f"  ERA5 pronto: {len(df):,} registos | anos: {sorted(df['ano'].unique())}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# S3
# ══════════════════════════════════════════════════════════════════════════════

def storage_options():
    opts = {
        "key":    os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "client_kwargs": {"region_name": AWS_REGION},
    }
    if AWS_ENDPOINT_URL:
        opts["client_kwargs"]["endpoint_url"] = AWS_ENDPOINT_URL
        print(f"  Destino: LocalStack ({AWS_ENDPOINT_URL})")
    else:
        print("  Destino: S3 real da AWS")
    return opts


def gravar_s3(df, prefixo, partition_cols):
    caminho = f"s3://{BUCKET}/{prefixo}/"
    df.to_parquet(caminho, engine="pyarrow",
                  partition_cols=partition_cols,
                  storage_options=storage_options(), index=False)
    print(f"  Gravado: {caminho} | particionado por {' / '.join(partition_cols)}")


def verificar_s3(prefixo):
    s3 = boto3.client("s3", endpoint_url=AWS_ENDPOINT_URL,
                      region_name=AWS_REGION,
                      aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","test"),
                      aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","test"))
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo, MaxKeys=5)
    objs = [o for o in resp.get("Contents",[]) if o["Key"].endswith(".parquet")]
    print(f"  S3: {len(objs)} ficheiros Parquet")
    for o in objs[:3]:
        print(f"    {o['Key']}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("CARGA HISTORICA -> S3")
    print("=" * 65)

    resultados = {}

    # ── 1. NASA FIRMS ─────────────────────────────────────────────────────────
    print("\n1. NASA FIRMS (hotspots):")

    # Modo 1 — Parquet da EDA (preferido)
    df_nasa = ler_parquet_eda(PASTA_EDA_NASA, "firms_portugal_limpo", "EDA_NASA")

    if df_nasa is not None:
        print("  Modo: Parquet EDA_NASA.py (dados ja processados)")
    else:
        # Modo 2 — CSV brutos de NASACSV/
        print(f"  EDA nao correu. A ler CSV de '{PASTA_CSV_NASA}'...")
        df_nasa = ler_csv_como_eda(PASTA_CSV_NASA)
        if df_nasa is not None:
            print("  Modo: CSV brutos (replica logica EDA)")

    if df_nasa is not None:
        try:
            df_nasa = adicionar_grid_id(df_nasa)
            gravar_s3(df_nasa, PREFIXO_NASA, ["ano", "mes", "grid_id"])
            verificar_s3(PREFIXO_NASA)
            resultados["nasa"] = True
            print("  OK\n")
        except Exception as e:
            print(f"  ERRO: {e}\n")
            resultados["nasa"] = False
    else:
        print(f"  SALTADO: sem Parquet em '{PASTA_EDA_NASA}' nem CSV em '{PASTA_CSV_NASA}'\n")
        resultados["nasa"] = False

    # ── 2. ERA5 ───────────────────────────────────────────────────────────────
    print("2. ERA5 (meteorologia):")
    df_era5 = ler_parquet_eda(PASTA_EDA_ERA5, "era5_portugal", "EDA_ERA5")

    if df_era5 is not None:
        try:
            df_era5 = preparar_era5(df_era5)
            gravar_s3(df_era5, PREFIXO_ERA5, ["ano", "mes"])
            verificar_s3(PREFIXO_ERA5)
            resultados["era5"] = True
            print("  OK\n")
        except Exception as e:
            print(f"  ERRO: {e}\n")
            resultados["era5"] = False
    else:
        print(f"  SALTADO: EDA_ERA5.py ainda nao correu\n")
        resultados["era5"] = False

    # ── Resumo ────────────────────────────────────────────────────────────────
    print("=" * 65)
    print("RESUMO")
    print("=" * 65)
    print(f"  NASA FIRMS  : {'OK' if resultados.get('nasa') else 'SALTADO'}")
    print(f"  ERA5        : {'OK' if resultados.get('era5') else 'SALTADO — EDA_ERA5.py nao correu'}")
    if any(resultados.values()):
        print("\n  Data lake populado! Prontos para o modelo ML.")
    else:
        print(f"\n  Sem dados. Verifica '{PASTA_CSV_NASA}' ou corre as EDAs primeiro.")
    print("=" * 65)


if __name__ == "__main__":
    main()
