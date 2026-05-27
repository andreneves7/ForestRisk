"""
Forest Risk Monitoring System — Validação de dados

Contém apenas a lógica de validação e rejeição de eventos.
"""

from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd


RULES = {
    "temp_celsius": (-10,  60),
    "humidity_pct": (  0, 100),
    "wind_kmh":     (  0, 150),
    "risk_score":   (  0, 100),
}
NOT_NULL = ["grid_id", "risk_score"]


def build_ge_context():
    """Mantido para compatibilidade; o contexto é criado por batch."""
    return None


def run_ge_validation(context, df: pd.DataFrame, suite_name: str = "sensor_quality"):
    """
    Corre Great Expectations sobre um micro-batch.
    Devolve (success_pct, n_success, n_failed).
    """
    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_pandas("sensor_data")
    asset = datasource.add_dataframe_asset("readings")
    batch = asset.build_batch_request(dataframe=df)

    suite = context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=batch, expectation_suite=suite)

    validator.expect_column_values_to_be_between("temp_celsius", min_value=-10, max_value=60)
    validator.expect_column_values_to_be_between("humidity_pct", min_value=0,   max_value=100)
    validator.expect_column_values_to_be_between("wind_kmh",     min_value=0,   max_value=150)
    validator.expect_column_values_to_not_be_null("grid_id")
    validator.expect_column_values_to_not_be_null("risk_score")
    validator.save_expectation_suite(discard_failed_expectations=False)

    context.add_or_update_checkpoint(
        name="quality_check",
        validations=[{
            "batch_request": batch,
            "expectation_suite_name": suite_name,
        }],
    )
    results = context.run_checkpoint(checkpoint_name="quality_check")

    stats = results.get_statistics()
    validation_stats = list(stats["validation_statistics"].values())[0]

    success_pct = float(validation_stats.get("success_percent", 0) or 0)
    n_success = int(validation_stats.get("successful_expectations", 0))
    n_failed = int(validation_stats.get("unsuccessful_expectations", 0))

    return success_pct, n_success, n_failed


def _manual_validation(df: pd.DataFrame):
    """
    Validação manual como fallback.
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
