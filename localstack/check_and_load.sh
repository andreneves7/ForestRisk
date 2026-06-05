#!/bin/bash
# check_and_load.sh
# Verifica se o histórico já existe no S3.
# Se sim → salta. Se não → carrega os 52k hotspots.

echo "A verificar dados históricos no S3..."

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
    resp = s3.list_objects_v2(
        Bucket='forest-risk-datalake',
        Prefix='hotspots/',
        MaxKeys=1
    )
    if resp.get('KeyCount', 0) > 0:
        print('Historico ja existe no S3 — a saltar carga.')
        sys.exit(0)
    else:
        print('S3 vazio — a iniciar carga historica...')
        sys.exit(1)
except Exception as e:
    print(f'Erro ao verificar S3: {e}')
    sys.exit(1)
PYEOF

STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "Carga saltada — dados ja existem."
    exit 0
fi

echo "A correr carga historica NASA FIRMS..."
python3 /home/jovyan/work/carga_historico_s3.py

if [ $? -eq 0 ]; then
    echo "Carga historica concluida com sucesso!"
else
    echo "ERRO: Carga historica falhou."
    exit 1
fi
