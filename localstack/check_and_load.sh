#!/bin/bash
# check_and_load.sh
# Verifica se o data lake ja tem dados (hotspots + meteorologia).
# Se sim -> salta. Se nao -> corre a carga dos Parquet das EDAs.

echo "A verificar data lake no S3..."

python3 << 'PYEOF'
import boto3, sys, os

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localstack:4566'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-west-1')
)

try:
    # Verifica hotspots (NASA) e meteorologia (ERA5)
    nasa = s3.list_objects_v2(Bucket='forest-risk-datalake', Prefix='hotspots/', MaxKeys=1)
    era5 = s3.list_objects_v2(Bucket='forest-risk-datalake', Prefix='meteorologia/', MaxKeys=1)
    nasa_ok = nasa.get('KeyCount', 0) > 0
    era5_ok = era5.get('KeyCount', 0) > 0

    if nasa_ok and era5_ok:
        print('Data lake ja tem hotspots + meteorologia — a saltar carga.')
        sys.exit(0)
    elif nasa_ok:
        print('Hotspots ja existem, meteorologia em falta — a carregar ERA5.')
        sys.exit(1)
    elif era5_ok:
        print('Meteorologia ja existe, hotspots em falta — a carregar NASA.')
        sys.exit(1)
    else:
        print('S3 vazio — a iniciar carga completa...')
        sys.exit(1)
except Exception as e:
    print(f'Erro ao verificar S3: {e}')
    sys.exit(1)
PYEOF

STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "Carga saltada — data lake ja esta populado."
    exit 0
fi

echo "A correr carga historica das EDAs..."
python3 /home/jovyan/work/carga_historico_s3.py

if [ $? -eq 0 ]; then
    echo "Carga historica concluida!"
else
    echo "AVISO: Carga parcial ou sem dados EDA — as EDAs precisam de correr primeiro."
    exit 0
fi
