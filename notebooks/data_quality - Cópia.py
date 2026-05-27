"""
================================================================================
Forest Risk Monitoring System — Módulo de Qualidade de Dados
================================================================================

Extraído fielmente do notebook 01_data_quality.ipynb.
Usado pelo consumer_kafka_cassandra.py antes de persistir no Cassandra.

REGRAS DE VALIDAÇÃO (Great Expectations):
    - temp_celsius  : entre -10 e 60
    - humidity_pct  : entre 0 e 100
    - wind_kmh      : entre 0 e 150
    - risk_score    : entre 0 e 100
    - grid_id       : não nulo
    - risk_score    : não nulo

MÉTRICAS ENVIADAS PARA INFLUXDB (Grafana):
    - data_quality      → success_percent, successful_expectations,
                          failed_expectations, total_rows
    - rejected_events   → count, reason, grid_id
    - pipeline_latency  → latency_ms (só eventos válidos)
================================================================================
"""

import logging
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd
from influxdb_client import Point, WritePrecision

log = logging.getLogger(__name__)

# ── Limites de validação (fonte única de verdade — versão final do notebook) ──
RULES = {
    "temp_celsius": (-10,  60),
    "humidity_pct": (  0, 100),
    "wind_kmh":     (  0, 150),
    "risk_score":   (  0, 100),
}
NOT_NULL = ["grid_id", "risk_score"]


# ══════════════════════════════════════════════════════════════════════════════
# GREAT EXPECTATIONS — validação agregada do micro-batch
# Lógica idêntica à célula 3/4 do notebook 01_data_quality.ipynb
# ══════════════════════════════════════════════════════════════════════════════

def build_ge_context():
    """Cria um contexto GE efémero — igual ao notebook: gx.get_context(mode='ephemeral')."""
    return gx.get_context(mode="ephemeral")


def run_ge_validation(context, df: pd.DataFrame, suite_name: str = "sensor_quality"):
    """
    Corre Great Expectations sobre um DataFrame (micro-batch).
    Reproduz fielmente as células do notebook 01_data_quality.ipynb.

    Devolve (success_pct, n_success, n_failed).
    """
    # — igual ao notebook: context.sources.add_pandas('sensor_data') —
    datasource = context.sources.add_pandas("sensor_data")
    asset      = datasource.add_dataframe_asset("readings")
    batch      = asset.build_batch_request(dataframe=df)

    # 1. Criar o suite — igual ao notebook
    suite     = context.add_or_update_expectation_suite(suite_name)

    # 2. Criar o validator ligado ao suite — igual ao notebook
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite
    )

    # 3. Definir as regras — igual ao notebook (versão final, célula 4)
    validator.expect_column_values_to_be_between("temp_celsius", min_value=-10, max_value=60)
    validator.expect_column_values_to_be_between("humidity_pct", min_value=0,   max_value=100)
    validator.expect_column_values_to_be_between("wind_kmh",     min_value=0,   max_value=150)
    validator.expect_column_values_to_not_be_null("grid_id")
    validator.expect_column_values_to_not_be_null("risk_score")

    # 4. Guardar o suite — igual ao notebook
    validator.save_expectation_suite(discard_failed_expectations=False)

    # 5. Correr o checkpoint — igual ao notebook
    context.add_or_update_checkpoint(
        name="quality_check",
        validations=[{
            "batch_request":          batch,
            "expectation_suite_name": suite_name
        }]
    )
    results = context.run_checkpoint(checkpoint_name="quality_check")

    # Extrair estatísticas — igual ao notebook
    stats         = results.get_statistics()
    validation_stats = list(stats["validation_statistics"].values())[0]

    success_pct = float(validation_stats.get("success_percent", 0) or 0)
    n_success   = int(validation_stats.get("successful_expectations", 0))
    n_failed    = int(validation_stats.get("unsuccessful_expectations", 0))

    return success_pct, n_success, n_failed


# ══════════════════════════════════════════════════════════════════════════════
# SPLIT LINHA A LINHA — routing válidos / inválidos
# Usa os mesmos limites de RULES acima (fonte única de verdade)
# ══════════════════════════════════════════════════════════════════════════════

def split_valid_invalid(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Valida cada evento individualmente com as mesmas regras do notebook.
    Devolve (válidos, inválidos).
    Os inválidos têm o campo extra '_rejection_reasons'.
    """
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
    """
    Estrutura um evento rejeitado para quarentena.

    ─────────────────────────────────────────────────────────────────
    NOTA FUTURA: quando quiseres persistir os rejeitados em Cassandra
    (tabela rejected_readings) ou S3, adiciona o INSERT/upload aqui.
    O dicionário já tem todos os campos necessários.
    ─────────────────────────────────────────────────────────────────
    """
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
# Campos idênticos aos enviados no notebook (Point "data_quality")
# ══════════════════════════════════════════════════════════════════════════════

def send_quality_metrics(write_api, bucket: str, org: str,
                         source: str, success_pct: float,
                         n_success: int, n_failed: int, total_rows: int):
    """
    Measurement: data_quality — igual ao notebook.
    Campos consumidos pelo Grafana:
        success_percent, successful_expectations, failed_expectations, total_rows
    """
    point = (
        Point("data_quality")
        .tag("source", source)
        .field("success_percent",         success_pct)
        .field("successful_expectations", n_success)
        .field("failed_expectations",     n_failed)
        .field("total_rows",              total_rows)
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)


def send_rejected_metrics(write_api, bucket: str, org: str,
                          grid_id: str, reasons: list[str]):
    """
    Measurement: rejected_events
    Permite ao Grafana mostrar contagem e motivos de rejeição por zona.
    """
    point = (
        Point("rejected_events")
        .tag("grid_id", grid_id)
        .tag("reason", ",".join(reasons))
        .field("count", 1)
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)


def send_latency(write_api, bucket: str, org: str,
                 latency_ms: float, topic: str, grid_id: str):
    """
    Measurement: pipeline_latency — igual ao consumer original.
    Enviado apenas para eventos válidos.
    """
    point = (
        Point("pipeline_latency")
        .tag("topic", topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", latency_ms)
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)
