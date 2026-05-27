"""
Forest Risk Monitoring System — Módulo de Qualidade de Dados
Versão corrigida — sem conflitos de contexto GE entre batches.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
from influxdb_client import Point, WritePrecision

log = logging.getLogger(__name__)

# ── Limites de validação ──────────────────────────────────────────────────────
RULES = {
    "temp_celsius": (-10,  60),
    "humidity_pct": (  0, 100),
    "wind_kmh":     (  0, 150),
    "risk_score":   (  0, 100),
}
NOT_NULL = ["grid_id", "risk_score"]


# ══════════════════════════════════════════════════════════════════════════════
# GREAT EXPECTATIONS — recria contexto a cada batch para evitar conflitos
# ══════════════════════════════════════════════════════════════════════════════

def build_ge_context():
    """Devolve None — contexto criado a cada batch em run_ge_validation."""
    return None


def run_ge_validation(context, df: pd.DataFrame, suite_name: str = "sensor_quality"):
    """
    Cria um contexto GE fresco a cada chamada para evitar conflitos
    de datasource entre batches consecutivos.
    """
    import great_expectations as gx

    try:
        ctx = gx.get_context(mode="ephemeral")

        datasource = ctx.sources.add_pandas("sensor_data")
        asset      = datasource.add_dataframe_asset("readings")
        batch      = asset.build_batch_request(dataframe=df)

        suite     = ctx.add_expectation_suite(suite_name)
        validator = ctx.get_validator(batch_request=batch, expectation_suite=suite)

        validator.expect_column_values_to_be_between("temp_celsius", min_value=-10, max_value=60)
        validator.expect_column_values_to_be_between("humidity_pct", min_value=0,   max_value=100)
        validator.expect_column_values_to_be_between("wind_kmh",     min_value=0,   max_value=150)
        validator.expect_column_values_to_not_be_null("grid_id")
        validator.expect_column_values_to_not_be_null("risk_score")
        validator.save_expectation_suite(discard_failed_expectations=False)

        checkpoint = ctx.add_checkpoint(
            name="quality_check",
            validations=[{
                "batch_request":          batch,
                "expectation_suite_name": suite_name
            }]
        )
        results = checkpoint.run()

        # Extrai estatísticas
        stats = results.get_statistics()
        vstats = list(stats.get("validation_statistics", {}).values())

        if vstats:
            s = vstats[0]
            success_pct = float(s.get("success_percent") or 0)
            n_success   = int(s.get("successful_expectations") or 0)
            n_failed    = int(s.get("unsuccessful_expectations") or 0)
        else:
            # Fallback: calcula manualmente
            success_pct, n_success, n_failed = _manual_validation(df)

        return success_pct, n_success, n_failed

    except Exception as e:
        log.warning(f"GE falhou ({e}) — a usar validação manual")
        return _manual_validation(df)


def _manual_validation(df: pd.DataFrame):
    """
    Validação manual como fallback — garante que as métricas
    chegam sempre ao InfluxDB mesmo que o GE falhe.
    """
    total_checks = 0
    failed = 0

    for col, (mn, mx) in RULES.items():
        if col in df.columns:
            total_checks += len(df)
            failed += int(((df[col] < mn) | (df[col] > mx) | df[col].isna()).sum())

    for col in NOT_NULL:
        if col in df.columns:
            total_checks += len(df)
            failed += int(df[col].isna().sum())

    n_success = total_checks - failed
    success_pct = (n_success / total_checks * 100) if total_checks > 0 else 100.0
    return round(success_pct, 2), n_success, failed


# ══════════════════════════════════════════════════════════════════════════════
# SPLIT LINHA A LINHA
# ══════════════════════════════════════════════════════════════════════════════

def split_valid_invalid(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    valid, invalid = [], []

    for ev in batch:
        reasons = []

        for col, (min_val, max_val) in RULES.items():
            val = ev.get(col)
            if val is None:
                reasons.append(f"{col}_null")
            else:
                try:
                    if not (min_val <= float(val) <= max_val):
                        reasons.append(f"{col}_out_of_range({val})")
                except (TypeError, ValueError):
                    reasons.append(f"{col}_invalid_type({val})")

        for col in NOT_NULL:
            if col not in RULES and ev.get(col) is None:
                reasons.append(f"{col}_null")

        if reasons:
            ev["_rejection_reasons"] = reasons
            invalid.append(ev)
        else:
            valid.append(ev)

    return valid, invalid


def build_rejected_record(ev: dict) -> dict:
    return {
        "grid_id":            ev.get("grid_id", "UNKNOWN"),
        "rejected_at":        datetime.now(timezone.utc).isoformat(),
        "original_timestamp": ev.get("timestamp"),
        "source":             ev.get("source", "unknown"),
        "temp_celsius":       ev.get("temp_celsius"),
        "humidity_pct":       ev.get("humidity_pct"),
        "wind_kmh":           ev.get("wind_kmh"),
        "risk_score":         ev.get("risk_score"),
        "rejection_reasons":  ev.get("_rejection_reasons", []),
    }


# ══════════════════════════════════════════════════════════════════════════════
# INFLUXDB — métricas para o Grafana
# ══════════════════════════════════════════════════════════════════════════════

def send_quality_metrics(write_api, bucket, org,
                         source, success_pct, n_success, n_failed, total_rows):
    point = (
        Point("data_quality")
        .tag("source", source)
        .field("success_percent",         float(success_pct))
        .field("successful_expectations", int(n_success))
        .field("failed_expectations",     int(n_failed))
        .field("total_rows",              int(total_rows))
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


def send_latency(write_api, bucket, org, latency_ms, topic, grid_id):
    point = (
        Point("pipeline_latency")
        .tag("topic", topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", float(latency_ms))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)