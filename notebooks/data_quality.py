"""
Forest Risk Monitoring System — Métricas de Qualidade

Contém apenas a escrita de métricas no InfluxDB.
"""

import logging
from datetime import datetime, timezone

from influxdb_client import Point, WritePrecision

log = logging.getLogger(__name__)

def send_quality_metrics(write_api, bucket, org,
                         source, success_pct, n_success, n_failed, total_rows):
    point = (
        Point("data_quality")
        .tag("source", source)
        .field("success_percent", float(success_pct))
        .field("successful_expectations", int(n_success))
        .field("failed_expectations", int(n_failed))
        .field("total_rows", int(total_rows))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)
    log.info(f"📤 InfluxDB data_quality escrito: {success_pct:.1f}% | rows={total_rows}")


def send_rejected_metrics(write_api, bucket, org, grid_id, reasons):
    point = (
        Point("rejected_events")
        .tag("grid_id", grid_id)
        .tag("reason", ",".join(reasons))
        .field("count", 1)
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)


def send_rejected_event_detail(write_api, bucket, org, rejected: dict):
    """Write a detailed rejected-event point for Grafana table/digging."""
    grid_id = rejected.get("grid_id", "UNKNOWN")
    reasons = rejected.get("rejection_reasons", [])

    detail = (
        Point("rejected_event_detail")
        .tag("grid_id", grid_id)
        .tag("source", rejected.get("source", "unknown"))
        .field("original_timestamp", str(rejected.get("original_timestamp")))
        .field("temp_celsius", _safe_float(rejected.get("temp_celsius")))
        .field("humidity_pct", _safe_float(rejected.get("humidity_pct")))
        .field("wind_kmh", _safe_float(rejected.get("wind_kmh")))
        .field("risk_score", _safe_float(rejected.get("risk_score")))
        .field("rejection_reasons", ",".join(reasons))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, detail)


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def send_latency(write_api, bucket, org, latency_ms, topic, grid_id):
    point = (
        Point("pipeline_latency")
        .tag("topic", topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", float(latency_ms))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)