"""
================================================================================
Forest Risk Monitoring System — Métricas de Qualidade para InfluxDB
================================================================================

PAPEL NA PIPELINE:
    Módulo auxiliar importado pelo consumer_kafka_cassandra.py.
    Responsável exclusivamente por ESCREVER métricas no InfluxDB.
    Não faz validação (isso é feito em data_quality_validation.py).

QUANDO É USADO:
    Chamado pelo consumer após cada batch processado para registar:
    - Percentagem de qualidade dos dados (visível no Grafana)
    - Latência da pipeline (tempo entre criação e persistência)
    - Detalhes de eventos rejeitados (para debugging)

MEASUREMENTS NO INFLUXDB (equivalentes a "tabelas"):
    data_quality          → métricas agregadas por batch
    pipeline_latency      → latência individual por evento
    rejected_events       → contagem de rejeições por zona/motivo
    rejected_event_detail → campos completos de eventos rejeitados

MODELO DE DADOS INFLUXDB (tags vs fields):
    - tags   → indexados, usados para filtrar/agrupar no Grafana (strings)
    - fields → os valores numéricos ou texto que se quer guardar
    Exemplo: tag("grid_id") permite filtrar por zona; field("latency_ms")
    é o valor medido.
================================================================================
"""

import logging
from datetime import datetime, timezone

from influxdb_client import Point, WritePrecision

log = logging.getLogger(__name__)


def send_quality_metrics(write_api, bucket, org,
                         source, success_pct, n_success, n_failed, total_rows):
    """
    Escreve métricas de qualidade agregadas de um micro-batch no InfluxDB.
    Chamado uma vez por batch (não por evento).

    Parâmetros:
        write_api   → cliente InfluxDB write API (passado pelo consumer)
        bucket      → bucket InfluxDB ("metrics")
        org         → organização InfluxDB ("forest-risk")
        source      → origem dos dados (ex: "sensor_readings", "satellite-hotspots")
        success_pct → percentagem de eventos que passaram a validação (0-100)
        n_success   → número de validações que passaram (GE conta por expectativa)
        n_failed    → número de validações que falharam
        total_rows  → total de eventos no batch

    Measurement no InfluxDB: data_quality
    Tags:   source (para filtrar por fonte no Grafana)
    Fields: success_percent, successful_expectations, failed_expectations, total_rows

    O Grafana usa success_percent para mostrar a percentagem de qualidade
    ao longo do tempo. Se cair abaixo de 80%, pode configurar um alerta.
    """
    point = (
        Point("data_quality")
        .tag("source", source)                                    # indexado — permite filtrar por fonte
        .field("success_percent", float(success_pct))             # % qualidade do batch
        .field("successful_expectations", int(n_success))         # nº validações OK
        .field("failed_expectations", int(n_failed))              # nº validações NOK
        .field("total_rows", int(total_rows))                     # tamanho do batch
        .time(datetime.now(timezone.utc), WritePrecision.NS)      # timestamp com precisão nanosegundo
    )
    write_api.write(bucket, org, point)
    log.info(f"InfluxDB data_quality: {success_pct:.1f}% | rows={total_rows}")


def send_rejected_metrics(write_api, bucket, org, grid_id, reasons):
    """
    Regista uma contagem (+1) para cada evento rejeitado.
    Chamado uma vez por evento inválido.

    Parâmetros:
        grid_id → zona de Portugal onde ocorreu a rejeição
        reasons → lista de motivos (ex: ["temp_celsius_out_of_range(999.9)"])

    Measurement: rejected_events
    Tags:   grid_id (filtra por zona), reason (filtra por tipo de problema)
    Fields: count (sempre 1 — cada ponto é uma rejeição)

    No Grafana, um count de rejected_events por grid_id mostra quais zonas
    têm mais problemas de qualidade (possível sensor avariado).
    Nota: os reasons são concatenados com vírgula se houver mais de um.
    """
    point = (
        Point("rejected_events")
        .tag("grid_id", grid_id)
        .tag("reason", ",".join(reasons))    # ex: "temp_celsius_out_of_range(999.9)"
        .field("count", 1)                   # sempre 1 — soma no Grafana dá total de rejeições
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)


def send_rejected_event_detail(write_api, bucket, org, rejected: dict):
    """
    Escreve o detalhe completo de um evento rejeitado para análise no Grafana.
    Chamado uma vez por evento inválido (complementar a send_rejected_metrics).

    Enquanto send_rejected_metrics dá estatísticas agregadas (quantos por zona),
    esta função guarda os valores reais que causaram a rejeição — útil para
    perceber se é um sensor com leituras absurdas ou simplesmente dados em falta.

    Measurement: rejected_event_detail
    Tags:   grid_id, source
    Fields: original_timestamp, temp_celsius, humidity_pct, wind_kmh,
            risk_score, rejection_reasons (string concatenada)

    Parâmetros:
        rejected → dicionário no formato devolvido por build_rejected_record()
                   (de data_quality_validation.py)
    """
    grid_id = rejected.get("grid_id", "UNKNOWN")
    reasons = rejected.get("rejection_reasons", [])

    detail = (
        Point("rejected_event_detail")
        .tag("grid_id", grid_id)
        .tag("source", rejected.get("source", "unknown"))
        .field("original_timestamp", str(rejected.get("original_timestamp")))
        .field("temp_celsius",   _safe_float(rejected.get("temp_celsius")))  # pode ser None se era isso que falhou
        .field("humidity_pct",   _safe_float(rejected.get("humidity_pct")))
        .field("wind_kmh",       _safe_float(rejected.get("wind_kmh")))
        .field("risk_score",     _safe_float(rejected.get("risk_score")))
        .field("rejection_reasons", ",".join(reasons))                       # texto para debug
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, detail)


def send_latency(write_api, bucket, org, latency_ms, topic, grid_id):
    """
    Regista a latência de processamento de um evento individual.
    Chamado após cada evento ser gravado no Cassandra.

    A latência é calculada como:
        latency_ms = (agora - timestamp_do_evento) × 1000

    Onde timestamp_do_evento é o momento em que o producer gerou o evento.
    Uma latência alta indica que o consumer está com lag (a processar mais
    devagar do que o producer envia).

    Parâmetros:
        latency_ms → milissegundos desde criação até persistência
        topic      → de onde veio o evento ("sensor-events" ou "satellite-hotspots")
        grid_id    → zona do evento

    Measurement: pipeline_latency
    Tags:   topic, grid_id (permite ver latência por zona ou por fonte)
    Fields: latency_ms

    No Grafana, um pico de latência indica sobrecarga do consumer ou
    problemas de conectividade com o Cassandra.
    """
    point = (
        Point("pipeline_latency")
        .tag("topic",   topic)
        .tag("grid_id", grid_id)
        .field("latency_ms", float(latency_ms))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )
    write_api.write(bucket, org, point)


def _safe_float(x):
    """
    Converte um valor para float de forma segura.
    Devolve None se o valor for None ou não convertível.
    Necessário porque eventos rejeitados podem ter campos nulos ou
    com tipos inválidos — precisamente os que falharam a validação.
    O InfluxDB aceita None como ausência de valor (não grava o field).
    """
    try:
        return float(x)
    except Exception:
        return None
