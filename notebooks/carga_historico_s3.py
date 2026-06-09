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
    """
    Lê os ficheiros Parquet produzidos pelas EDAs da Pessoa B.
    Devolve None (não levanta excepção) se a pasta não existir ou estiver
    vazia — permite ao main() tentar o Modo 2 (CSV) sem falhar.

    Estratégia de leitura:
    1. Tenta o ficheiro combinado: firms_portugal_limpo_todos.parquet
       (mais eficiente — um único ficheiro vs N ficheiros por ano)
    2. Se não existir, lê ficheiros por ano e concatena:
       firms_portugal_limpo_2020.parquet, _2021, _2022, ...

    Parâmetros:
        pasta     → caminho da pasta (ex: BASE_DIR / "Filtragem_Parquet")
        prefixo   → prefixo do nome dos ficheiros (ex: "firms_portugal_limpo")
        nome_eda  → nome da EDA para logs (ex: "EDA_NASA")
    """
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
    Lê os CSV brutos da NASA de NASACSV/ e replica EXACTAMENTE
    o comportamento da EDA_NASA.py. Usado quando a EDA ainda não correu.

    Decisões de design alinhadas com a EDA_NASA.py:
    - Detecção do satélite pelo nome do ficheiro (snpp→S-NPP, jpss1→NOAA-20)
    - Filtro por bounding box de Portugal (lat 36.9-42.2, lon -9.5–-6.2)
    - NÃO remove confidence="l" (baixa) — a EDA também não remove
    - Cria colunas ano/mes/dia a partir de acq_date
    - Selecciona as mesmas colunas que a EDA produziria

    Porquê [c for c in COLUNAS_EDA if c in df_pt.columns]:
    Alguns CSV mais antigos podem não ter todas as colunas. Esta expressão
    selecciona apenas as que existem, evitando KeyError.

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
    """
    Adiciona a coluna grid_id mapeando coordenadas GPS para a zona mais próxima.
    A EDA_NASA.py NÃO calcula o grid_id — é um enriquecimento feito por este script.

    Algoritmo: distância euclidiana aos 10 centroides das zonas de Portugal.
    Não é a distância geográfica real (que usaria haversine) mas é suficiente
    para a escala do Portugal Continental.

    df.copy(): cria uma cópia antes de modificar para evitar
    SettingWithCopyWarning do pandas (boa prática quando df vem de um slice).

    O grid_id resultante é a chave que liga:
    - hotspots/ (S3) ↔ agregados_streaming/ (S3) → join para modelo ML
    - Cassandra sensor_readings ↔ Spark streaming → análise em tempo real
    """
    if "grid_id" not in df.columns:
        print("  A mapear coordenadas -> grid_id...")
        df = df.copy()
        df["grid_id"] = df.apply(
            lambda r: coords_para_grid(r["latitude"], r["longitude"]), axis=1)
    print(f"  grid_id: {df['grid_id'].nunique()} zonas | anos: {sorted(df['ano'].unique())}")
    return df


def preparar_era5(df):
    """
    Garante que o DataFrame ERA5 tem as colunas ano e mes para particionamento.
    A EDA_ERA5.py já as cria normalmente, mas esta função é um safety net
    para o caso de versões mais antigas da EDA não as incluírem.
    ERA5 não tem grid_id — é particionado só por ano/mes (cobertura nacional).
    """
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
    """
    Devolve as opções de ligação ao S3 para o pandas/pyarrow.
    Endpoint configurável: LocalStack em dev, AWS real em produção.

    Como funciona o endpoint configurável:
    - Se AWS_ENDPOINT_URL está definido (ex: http://localstack:4566)
      → pandas usa o LocalStack local em vez da AWS
    - Se AWS_ENDPOINT_URL é None (não definido no ambiente)
      → pandas vai directamente para o S3 real da AWS
    O mesmo código funciona nos dois ambientes sem alterações.

    Para migrar para AWS real: remover AWS_ENDPOINT_URL do .env
    e definir credenciais AWS reais (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
    """
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
    """
    Grava o DataFrame no S3 em formato Parquet particionado.

    partition_cols=["ano","mes","grid_id"] diz ao pandas/pyarrow para
    NÃO incluir estas colunas dentro do ficheiro Parquet. Em vez disso,
    cria sub-pastas: ano=2020/mes=8/grid_id=PT-NORTE-01/dados.parquet

    Vantagem do particionamento:
    Quando a Pessoa B ler os dados para treinar o modelo, pode pedir
    só o verão de 2022 no Norte → Spark lê só as pastas relevantes
    em vez de carregar todos os 52k registos.
    Exemplo: spark.read.parquet(".../hotspots/ano=2022/mes=8/grid_id=PT-NORTE-02/")

    index=False: não grava o índice pandas (0,1,2...) como coluna — é irrelevante.
    """
    caminho = f"s3://{BUCKET}/{prefixo}/"
    df.to_parquet(caminho, engine="pyarrow",
                  partition_cols=partition_cols,
                  storage_options=storage_options(), index=False)
    print(f"  Gravado: {caminho} | particionado por {' / '.join(partition_cols)}")


def verificar_s3(prefixo):
    """
    Lista os primeiros ficheiros Parquet no S3 para confirmar a carga.
    Usa boto3 directamente (não pandas) para uma listagem rápida sem
    descarregar os dados. Mostra só os primeiros 5 para não sobrecarregar o log.
    """
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
