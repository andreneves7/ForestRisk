import io
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

BUCKET = "forest-risk-datalake"

TOPIC_TO_PREFIX = {
    "sensor-events":      "sensor_readings",
    "satellite-hotspots": "satellite_hotspots",
    "weather-data":       "weather_data",
}


def _s3_client():
    kwargs: dict = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "region_name": os.getenv("AWS_DEFAULT_REGION", "eu-west-1"),
    }
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def _build_key(prefix: str, zone_id: str, ts: datetime) -> str:
    return (
        f"{prefix}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"zone={zone_id}/"
        f"batch_{int(ts.timestamp())}.parquet"
    )


def write_parquet_to_s3(records: list[dict], topic: str) -> bool:
    """
    Agrupa records por grid_id, serializa cada grupo em Parquet e faz upload.
    Retorna True se todos os uploads tiverem sucesso, False se algum falhar.
    Nunca propaga excepção — pipeline não é bloqueado por falha S3.
    """
    if not records:
        return True

    try:
        prefix = TOPIC_TO_PREFIX.get(topic, topic.replace("-", "_"))
        client = _s3_client()
    except Exception as exc:
        log.error(f"Falha ao criar cliente S3 topic={topic}: {exc}")
        return False

    success = True

    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        grouped[r.get("grid_id", "unknown")].append(r)

    for zone_id, zone_records in grouped.items():
        try:
            raw_ts = zone_records[0].get("timestamp", datetime.now(timezone.utc).isoformat())
            ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))

            df = pd.DataFrame(zone_records)
            table = pa.Table.from_pandas(df)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            key = _build_key(prefix, zone_id, ts)
            client.put_object(Bucket=BUCKET, Key=key, Body=buf.read())
            log.info(f"S3 Parquet → s3://{BUCKET}/{key} ({len(zone_records)} registos)")
        except Exception as exc:
            log.error(f"Falha S3 upload zone={zone_id} topic={topic}: {exc}")
            success = False

    return success
