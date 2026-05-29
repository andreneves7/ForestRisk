import io
import os
from unittest.mock import patch

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_s3

BUCKET = "forest-risk-datalake"

os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture()
def s3_bucket():
    with mock_s3():
        client = boto3.client("s3", region_name="eu-west-1")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield client


def test_write_groups_by_grid_id(s3_bucket):
    """Dois grid_id distintos geram dois ficheiros Parquet separados."""
    from s3_writer import write_parquet_to_s3

    records = [
        {"grid_id": "norte", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 30.0},
        {"grid_id": "norte", "timestamp": "2025-05-01T10:01:00Z", "temp_celsius": 31.0},
        {"grid_id": "centro", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 28.0},
    ]

    result = write_parquet_to_s3(records, topic="sensor-events")

    assert result is True
    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    keys = [o["Key"] for o in objects]
    assert any("zone=norte" in k for k in keys)
    assert any("zone=centro" in k for k in keys)
    assert len(keys) == 2


def test_topic_maps_to_correct_prefix(s3_bucket):
    """O topic Kafka é mapeado para o prefixo S3 correcto."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "alentejo", "timestamp": "2025-05-01T10:00:00Z", "frp": 5.2}]

    write_parquet_to_s3(records, topic="satellite-hotspots")

    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    keys = [o["Key"] for o in objects]
    assert all(k.startswith("satellite_hotspots/") for k in keys)


def test_parquet_content_is_readable(s3_bucket):
    """O ficheiro Parquet gerado é legível e contém os dados originais."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "sul", "timestamp": "2025-05-01T10:00:00Z", "temp_celsius": 35.0}]
    write_parquet_to_s3(records, topic="sensor-events")

    objects = s3_bucket.list_objects_v2(Bucket=BUCKET)["Contents"]
    body = s3_bucket.get_object(Bucket=BUCKET, Key=objects[0]["Key"])["Body"].read()
    df = pq.read_table(io.BytesIO(body)).to_pandas()

    assert len(df) == 1
    assert df.iloc[0]["grid_id"] == "sul"
    assert float(df.iloc[0]["temp_celsius"]) == 35.0


def test_empty_records_returns_true(s3_bucket):
    """Lista vazia não gera upload mas retorna True."""
    from s3_writer import write_parquet_to_s3

    result = write_parquet_to_s3([], topic="sensor-events")

    assert result is True
    assert s3_bucket.list_objects_v2(Bucket=BUCKET).get("Contents") is None


def test_s3_failure_returns_false_without_exception(s3_bucket):
    """Falha S3 retorna False sem propagar excepção."""
    from s3_writer import write_parquet_to_s3

    records = [{"grid_id": "norte", "timestamp": "2025-05-01T10:00:00Z"}]

    with patch("s3_writer.boto3") as mock_boto:
        mock_boto.client.return_value.put_object.side_effect = Exception("S3 timeout")
        result = write_parquet_to_s3(records, topic="sensor-events")

    assert result is False
