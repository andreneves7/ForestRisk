#!/bin/bash
# =============================================================================
# check_and_load.sh
# =============================================================================
#
# PAPEL NA PIPELINE:
#   Chamado pelo container carga-historico ao arrancar (docker compose up).
#   Decide se os dados históricos precisam de ser carregados para o S3,
#   comparando as datas dos Parquet das EDAs com os dados já no S3.
#
# QUANDO CORRE:
#   Automaticamente ao arrancar com docker compose up.
#   Pode ser re-executado manualmente após nova EDA:
#     docker compose restart carga-historico
#
# FLUXO DE DECISÃO:
#
#   Nenhuma EDA correu?
#     → Avisa quais EDAs faltam e para. S3 não é tocado.
#
#   Pelo menos uma EDA correu mas há EDAs em falta?
#     → Avisa sobre as EDAs em falta (não bloqueia)
#     → Continua com as EDAs disponíveis
#
#   S3 vazio + Parquet EDA existem?
#     → Carrega para o S3
#
#   Parquet EDA mais recente que dados no S3?
#     (Pessoa B correu a EDA outra vez com dados actualizados)
#     → Recarrega o S3 com os Parquet mais recentes
#
#   S3 já está actualizado?
#     → Salta sem fazer nada
#
# EDAs SUPORTADAS:
#   Cada EDA gera Parquet numa pasta específica.
#   Para adicionar uma EDA nova: acrescenta uma linha ao array EDAS abaixo.
#   Formato de cada entrada: "NOME_SCRIPT|PASTA_PARQUET|DESCRICAO"
#
#   EDA_NASA.py  → Filtragem_Parquet/  (hotspots NASA FIRMS)       [activo]
#   EDA_ERA5.py  → ERA5_Parquet/       (meteorologia ERA5)          [futuro]
#   EDA_ICNF.py  → ICNF_Parquet/       (cartografia ICNF)           [futuro]
#
# CÓDIGOS DE SAÍDA DO PYTHON INTERNO:
#   0 → S3 actualizado, salta carga
#   1 → S3 vazio ou desactualizado, carrega
#   2 → sem Parquet disponíveis (não devia acontecer aqui)
#
# =============================================================================

SEP="================================================================="

# ── Array de EDAs esperadas ───────────────────────────────────────────────────
# Para registar uma nova EDA: acrescenta uma linha com o formato:
#   "NOME_DO_SCRIPT|PASTA_DOS_PARQUET|DESCRICAO_LEGIVEL"
# O script verifica automaticamente cada entrada ao arrancar.
EDAS=(
    "EDA_NASA.py|/home/jovyan/work/Filtragem_Parquet|Hotspots NASA FIRMS (satélite)"
    "EDA_ERA5.py|/home/jovyan/work/ERA5_Parquet|Meteorologia ERA5 (Copernicus)"
    "EDA_ICNF.py|/home/jovyan/work/ICNF_Parquet|Cartografia florestal ICNF (COS2018)"
)

echo ""
echo "$SEP"
echo "  Forest Risk — Verificação do Data Lake"
echo "$SEP"

# ── PASSO 1: Verifica quais EDAs já correram ──────────────────────────────────
# Para cada EDA registada, conta os ficheiros .parquet na pasta correspondente.
# Uma pasta com Parquet significa que a EDA correu com sucesso.
# Popula dois arrays:
#   EDAS_PRONTAS → EDAs com Parquet disponíveis (podem ser carregadas para S3)
#   EDAS_EM_FALTA → EDAs sem Parquet (ainda não correram)
echo ""
echo "A verificar Parquet das EDAs..."
echo ""

ALGUMA_DISPONIVEL=0
EDAS_PRONTAS=()
EDAS_EM_FALTA=()

for entry in "${EDAS[@]}"; do
    # Extrai os 3 campos da entrada separados por |
    NOME=$(echo "$entry" | cut -d'|' -f1)
    PASTA=$(echo "$entry" | cut -d'|' -f2)
    DESC=$(echo "$entry" | cut -d'|' -f3)

    # Conta ficheiros .parquet na pasta (2>/dev/null suprime erro se pasta não existe)
    N=$(find "$PASTA" -name "*.parquet" 2>/dev/null | wc -l)

    if [ "$N" -gt 0 ]; then
        echo "  ✅ $NOME ($N Parquet) — $DESC"
        EDAS_PRONTAS+=("$entry")
        ALGUMA_DISPONIVEL=1
    else
        echo "  ⏳ $NOME (sem Parquet) — $DESC"
        EDAS_EM_FALTA+=("$NOME|$DESC")
    fi
done

# ── PASSO 2: Se nenhuma EDA correu, avisa e para ──────────────────────────────
# Sem dados das EDAs não há nada para carregar para o S3.
# Lista todas as EDAs em falta com o comando exacto para as correr.
# exit 0: saída limpa (não é erro, é um estado esperado durante o desenvolvimento)
if [ $ALGUMA_DISPONIVEL -eq 0 ]; then
    echo ""
    echo "  ──────────────────────────────────────────────────────────────"
    echo "  ⚠️  O S3 NÃO foi populado — nenhuma EDA correu ainda."
    echo ""
    echo "  Para ser possível popular o data lake, o responsável pelos"
    echo "  EDAs tem de correr e validar primeiro os respectivos:"
    echo ""
    for entry in "${EDAS_EM_FALTA[@]}"; do
        NOME=$(echo "$entry" | cut -d'|' -f1)
        DESC=$(echo "$entry" | cut -d'|' -f2)
        echo "    → python /home/jovyan/work/$NOME   ($DESC)"
    done
    echo ""
    echo "  Depois reinicia este serviço:"
    echo "    docker compose restart carga-historico"
    echo "  ──────────────────────────────────────────────────────────────"
    echo "$SEP"
    exit 0
fi

# ── PASSO 3: Avisa EDAs em falta sem bloquear as disponíveis ─────────────────
# Se algumas EDAs correram e outras não, continua com as que existem
# mas avisa sobre as que faltam — dados no S3 ficarão incompletos.
# Não bloqueia: é preferível ter dados parciais a não ter nada.
if [ ${#EDAS_EM_FALTA[@]} -gt 0 ]; then
    echo ""
    echo "  ──────────────────────────────────────────────────────────────"
    echo "  ⏳ As seguintes EDAs ainda não correram (dados parciais no S3):"
    echo ""
    for entry in "${EDAS_EM_FALTA[@]}"; do
        NOME=$(echo "$entry" | cut -d'|' -f1)
        DESC=$(echo "$entry" | cut -d'|' -f2)
        echo "    → python /home/jovyan/work/$NOME   ($DESC)"
    done
    echo ""
    echo "  Depois reinicia: docker compose restart carga-historico"
    echo "  ──────────────────────────────────────────────────────────────"
fi

# ── PASSO 4: Compara datas Parquet EDA vs dados no S3 ────────────────────────
# Lógica em Python (mais fácil para comparar datas e falar com o S3 via boto3).
# Constrói uma string com as pastas disponíveis para passar ao Python.
#
# Comparação:
#   - Pega na data de modificação do Parquet mais recente entre todas as EDAs
#   - Pega na data do objecto S3 mais recente em hotspots/
#   - Se Parquet > S3: a EDA foi actualizada depois da última carga → recarrega
#   - Se S3 >= Parquet: já está em dia → salta
echo ""
echo "A comparar datas dos Parquet com o S3..."

# Constrói string de pastas separadas por espaço para passar ao heredoc Python
PASTAS_DISPONIVEIS=""
for entry in "${EDAS_PRONTAS[@]}"; do
    PASTA=$(echo "$entry" | cut -d'|' -f2)
    PASTAS_DISPONIVEIS="$PASTAS_DISPONIVEIS $PASTA"
done

python3 << PYEOF
import boto3, sys, os
from pathlib import Path
from datetime import datetime, timezone

# Recebe a lista de pastas da variável bash
pastas = "$PASTAS_DISPONIVEIS".split()

# Encontra o Parquet modificado mais recentemente entre todas as pastas EDA
todos_parquets = []
for pasta in pastas:
    todos_parquets.extend(Path(pasta).glob('**/*.parquet'))

if not todos_parquets:
    print('  Sem Parquet para comparar.')
    sys.exit(2)

# stat().st_mtime = timestamp Unix da última modificação do ficheiro
data_parquet = max(f.stat().st_mtime for f in todos_parquets)
data_parquet_dt = datetime.fromtimestamp(data_parquet, tz=timezone.utc)
print(f'  Parquet EDA mais recente : {data_parquet_dt.strftime("%Y-%m-%d %H:%M:%S")} UTC')

# Liga ao S3 (LocalStack em dev, AWS real em produção via AWS_ENDPOINT_URL)
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localstack:4566'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-west-1')
)

try:
    # Verifica objectos em hotspots/ (prefixo principal dos dados históricos)
    resp = s3.list_objects_v2(
        Bucket='forest-risk-datalake',
        Prefix='hotspots/',
        MaxKeys=1000
    )
    objectos = resp.get('Contents', [])

    if not objectos:
        # S3 completamente vazio — carga obrigatória
        print('  S3 hotspots/  : VAZIO')
        print('  Decisão       : CARREGAR (S3 vazio)')
        sys.exit(1)

    # LastModified de cada objecto S3 já vem com timezone UTC
    data_s3 = max(o['LastModified'] for o in objectos)
    print(f'  S3 actualizado em        : {data_s3.strftime("%Y-%m-%d %H:%M:%S")} UTC')

    if data_parquet_dt > data_s3:
        # EDA mais recente que S3: Pessoa B actualizou os dados → recarregar
        diff_min = (data_parquet_dt - data_s3).total_seconds() / 60
        print(f'  Parquet EDA é mais recente ({diff_min:.0f} min)')
        print(f'  Decisão       : RECARREGAR (EDA actualizada)')
        sys.exit(1)
    else:
        # S3 mais recente que EDA: já está em dia → saltar
        diff_min = (data_s3 - data_parquet_dt).total_seconds() / 60
        print(f'  S3 está actualizado ({diff_min:.0f} min mais recente que EDA)')
        print(f'  Decisão       : SALTAR (dados em dia)')
        sys.exit(0)

except Exception as e:
    # Em caso de dúvida, tenta carregar — melhor tentar do que não fazer nada
    print(f'  Erro ao verificar S3: {e}')
    sys.exit(1)
PYEOF

STATUS=$?

# Código 2: sem Parquet disponíveis (não devia chegar aqui — verificado no Passo 1)
if [ $STATUS -eq 2 ]; then
    echo "  ⚠️  Sem Parquet para carregar."
    echo "$SEP"
    exit 0
fi

# Código 0: S3 já está actualizado — termina sem carregar
if [ $STATUS -eq 0 ]; then
    echo ""
    echo "  ✅ S3 está actualizado — carga saltada."
    echo "$SEP"
    exit 0
fi

# ── PASSO 5: Carrega os Parquet para o S3 ────────────────────────────────────
# Só chegamos aqui se:
#   - S3 estava vazio, ou
#   - Parquet EDA são mais recentes que os dados no S3
# O carga_historico_s3.py lê automaticamente as pastas disponíveis
# (Filtragem_Parquet, ERA5_Parquet, etc.) e carrega o que encontrar.
echo ""
echo "A carregar dados das EDAs para o S3..."
python3 /home/jovyan/work/carga_historico_s3.py

if [ $? -eq 0 ]; then
    echo ""
    echo "  ✅ Data lake populado com dados das EDAs disponíveis!"
else
    echo ""
    echo "  ❌ Erro durante a carga. Verifica os logs acima."
fi

echo "$SEP"
