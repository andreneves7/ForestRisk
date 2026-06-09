"""
================================================================================
Forest Risk Monitoring System — Validação de Qualidade de Dados
================================================================================

PAPEL NA PIPELINE:
    Módulo auxiliar importado pelo consumer_kafka_cassandra.py.
    Responsável por VALIDAR eventos e separar os válidos dos inválidos.
    Não escreve em lado nenhum (isso é feito em data_quality.py).

QUANDO É USADO:
    Chamado pelo consumer a cada micro-batch (grupo de 3-10 eventos).
    Dois modos complementares:
    1. run_ge_validation()   → valida o batch completo com Great Expectations
                               para obter métricas de qualidade (% sucesso)
    2. split_valid_invalid() → valida evento a evento para decidir
                               quem vai para Cassandra e quem vai para quarentena

REGRAS DE VALIDAÇÃO:
    ┌─────────────────┬──────────┬──────────┬──────────────────────────────┐
    │ Campo           │ Mínimo   │ Máximo   │ Motivo                       │
    ├─────────────────┼──────────┼──────────┼──────────────────────────────┤
    │ temp_celsius    │ -10      │ 60       │ Impossível em Portugal        │
    │ humidity_pct    │ 0        │ 100      │ Percentagem (0-100%)          │
    │ wind_kmh        │ 0        │ 150      │ Acima = sensor danificado     │
    │ risk_score      │ 0        │ 100      │ Índice normalizado            │
    │ grid_id         │ não nulo │ —        │ Chave primária obrigatória    │
    └─────────────────┴──────────┴──────────┴──────────────────────────────┘
================================================================================
"""

from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd


# Regras de intervalo para campos numéricos: {campo: (min, max)}
RULES = {
    "temp_celsius": (-10,  60),
    "humidity_pct": (  0, 100),
    "wind_kmh":     (  0, 150),
    "risk_score":   (  0, 100),
}

# Campos obrigatórios (não podem ser nulos)
NOT_NULL = ["grid_id", "risk_score"]


def build_ge_context():
    """
    Mantido para compatibilidade com a assinatura original.
    O contexto GE é agora criado dentro de run_ge_validation()
    em modo efémero (sem persistência em disco).
    Devolve None — o parâmetro context em run_ge_validation() é ignorado.
    """
    return None


def run_ge_validation(context, df: pd.DataFrame, suite_name: str = "sensor_quality"):
    """
    Corre Great Expectations sobre um micro-batch completo.
    Devolve estatísticas de qualidade agregadas para o Grafana.

    Nota: este método NÃO decide quais eventos são válidos individualmente.
    Serve apenas para calcular a percentagem de qualidade do batch e
    escrever no InfluxDB. A decisão por evento é feita em split_valid_invalid().

    Parâmetros:
        context    → ignorado (criado internamente em modo efémero)
        df         → DataFrame com os eventos do micro-batch
        suite_name → nome da suite GE (para identificação nos logs GE)

    Devolve:
        (success_pct, n_success, n_failed)
        - success_pct → percentagem de expectativas que passaram (0-100)
        - n_success   → número de expectativas OK (GE conta por coluna, não por linha)
        - n_failed    → número de expectativas NOK

    Modo ephemeral: não cria ficheiros em disco, não requer configuração prévia.
    Ideal para ambientes containerizados onde o disco pode não ser persistente.
    """
    # Cria contexto GE em memória (ephemeral = sem persistência)
    context = gx.get_context(mode="ephemeral")

    # Regista o DataFrame como fonte de dados GE
    datasource = context.sources.add_pandas("sensor_data")
    asset      = datasource.add_dataframe_asset("readings")
    batch      = asset.build_batch_request(dataframe=df)

    # Cria ou actualiza a suite de expectativas
    suite     = context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=batch, expectation_suite=suite)

    # Define as expectativas — equivalem às regras em RULES e NOT_NULL
    # Cada expect_* gera uma linha no relatório GE
    validator.expect_column_values_to_be_between("temp_celsius", min_value=-10, max_value=60)
    validator.expect_column_values_to_be_between("humidity_pct", min_value=0,   max_value=100)
    validator.expect_column_values_to_be_between("wind_kmh",     min_value=0,   max_value=150)
    validator.expect_column_values_to_not_be_null("grid_id")
    validator.expect_column_values_to_not_be_null("risk_score")
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Regista o checkpoint para execução
    context.add_or_update_checkpoint(
        name="quality_check",
        validations=[{
            "batch_request":         batch,
            "expectation_suite_name": suite_name,
        }],
    )

    # Corre a validação e extrai estatísticas
    results = context.run_checkpoint(checkpoint_name="quality_check")
    stats   = results.get_statistics()
    validation_stats = list(stats["validation_statistics"].values())[0]

    success_pct = float(validation_stats.get("success_percent", 0) or 0)
    n_success   = int(validation_stats.get("successful_expectations", 0))
    n_failed    = int(validation_stats.get("unsuccessful_expectations", 0))

    return success_pct, n_success, n_failed


def _manual_validation(df: pd.DataFrame):
    """
    Validação manual como fallback quando o GE falha.
    Aplica as mesmas regras de RULES e NOT_NULL diretamente com pandas.

    Usado internamente quando run_ge_validation() levanta uma excepção
    (ex: incompatibilidade de versão do GE, timeout, etc.).
    Garante que o consumer nunca pára por problemas de dependências.

    Devolve: (success_pct, n_success, n_failed)
    """
    total_checks = 0
    failed       = 0

    # Verifica intervalos numéricos
    for col, (mn, mx) in RULES.items():
        if col in df.columns:
            total_checks += len(df)
            failed += int(((df[col] < mn) | (df[col] > mx) | df[col].isna()).sum())

    # Verifica campos não nulos
    for col in NOT_NULL:
        if col in df.columns:
            total_checks += len(df)
            failed += int(df[col].isna().sum())

    n_success   = total_checks - failed
    success_pct = (n_success / total_checks * 100) if total_checks > 0 else 100.0
    return round(success_pct, 2), n_success, failed


def split_valid_invalid(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Separa um batch de eventos em válidos e inválidos, evento a evento.

    Este é o método de decisão — enquanto run_ge_validation() dá estatísticas,
    este método decide concretamente quem vai para Cassandra e quem vai para
    a quarentena (topic data-quality-metrics).

    Parâmetros:
        batch → lista de dicionários Python (eventos do micro-batch)

    Devolve:
        (valid, invalid)
        - valid   → lista de eventos que passaram todas as regras
        - invalid → lista de eventos que falharam, com campo "_rejection_reasons"
                    adicionado: lista de strings descrevendo cada falha

    Exemplos de rejection_reasons:
        ["temp_celsius_out_of_range(999.9)"]  → sensor com leitura absurda
        ["grid_id_null"]                      → evento sem zona identificada
        ["humidity_pct_null", "wind_kmh_null"] → múltiplos campos em falta

    Nota: um evento pode falhar por múltiplos motivos em simultâneo.
    Todos os motivos são registados para facilitar o debugging.
    """
    valid, invalid = [], []

    for ev in batch:
        reasons = []

        # Verifica cada regra numérica (intervalo válido)
        for col, (min_val, max_val) in RULES.items():
            val = ev.get(col)

            if val is None:
                # Campo ausente no evento
                reasons.append(f"{col}_null")
            else:
                try:
                    if not (min_val <= float(val) <= max_val):
                        # Valor fora do intervalo esperado
                        reasons.append(f"{col}_out_of_range({val})")
                except (TypeError, ValueError):
                    # Valor não é numérico (ex: string onde se espera float)
                    reasons.append(f"{col}_invalid_type({val})")

        # Verifica campos obrigatórios não cobertos pelas RULES numéricas
        # (grid_id é string, não tem intervalo — só verifica se existe)
        for col in NOT_NULL:
            if col not in RULES and ev.get(col) is None:
                reasons.append(f"{col}_null")

        if reasons:
            # Evento inválido: adiciona os motivos e envia para quarentena
            ev["_rejection_reasons"] = reasons
            invalid.append(ev)
        else:
            # Evento válido: pronto para Cassandra
            valid.append(ev)

    return valid, invalid


def build_rejected_record(ev: dict) -> dict:
    """
    Constrói o registo de quarentena para um evento inválido.
    Formata o evento para ser publicado no topic data-quality-metrics
    e para ser gravado no InfluxDB como rejected_event_detail.

    Parâmetros:
        ev → evento inválido (com campo "_rejection_reasons" adicionado
             por split_valid_invalid())

    Devolve dicionário com:
        - grid_id, rejected_at, original_timestamp → identificação
        - source → qual producer gerou o evento
        - valores originais (podem ser None se eram nulos)
        - rejection_reasons → lista de strings com os motivos de rejeição

    Os valores originais são incluídos mesmo que inválidos — permitem
    perceber no Grafana se o sensor está a enviar 999°C (avariado) ou
    simplesmente null (desligado). São problemas diferentes.
    """
    return {
        "grid_id":            ev.get("grid_id", "UNKNOWN"),
        "rejected_at":        datetime.now(timezone.utc).isoformat(),
        "original_timestamp": ev.get("timestamp"),
        "source":             ev.get("source", "unknown"),
        "temp_celsius":       ev.get("temp_celsius"),       # valor original (pode ser inválido)
        "humidity_pct":       ev.get("humidity_pct"),
        "wind_kmh":           ev.get("wind_kmh"),
        "risk_score":         ev.get("risk_score"),
        "rejection_reasons":  ev.get("_rejection_reasons", []),
    }


# ══════════════════════════════════════════════════════════════════════════════
# VALIDAÇÃO ESPECÍFICA PARA DADOS NASA FIRMS (satellite-hotspots)
# ══════════════════════════════════════════════════════════════════════════════
# Os dados NASA não passam pelas regras RULES/NOT_NULL acima porque têm
# campos diferentes dos sensores IoT. Este módulo define regras próprias
# para os hotspots de satélite.
#
# Gaps identificados no consumer original:
#   1. FRP sem limites    → valores impossíveis (ex: 99999 MW) eram gravados
#   2. Coordenadas livres → lat=0.0 (valor default) era gravado como válido
#   3. PT-UNKNOWN aceite  → grid_id inválido era gravado silenciosamente
#   4. brightness a zero  → campo ausente substituído por 0 sem aviso

# Regras de intervalo para campos numéricos dos hotspots NASA
RULES_NASA = {
    "frp_mw":     (0.0, 5000.0),  # Fire Radiative Power: 0 a ~5000 MW
                                   # maior incêndio registado historicamente ~3000 MW
                                   # valores acima de 5000 indicam erro de sensor
    "brightness": (200.0, 500.0), # temperatura de brilho VIIRS em Kelvin
                                   # fogo activo: ~300-500K; abaixo de 200K = sensor erro
    "latitude":   (36.9, 42.2),   # bounding box Portugal Continental
    "longitude":  (-9.5, -6.2),   # bounding box Portugal Continental
}

# Campos obrigatórios nos eventos NASA
NOT_NULL_NASA = ["grid_id", "latitude", "longitude", "frp_mw"]

# grid_id inválido — atribuído quando as coordenadas não mapearam para nenhuma zona
INVALID_GRID_IDS = {"PT-UNKNOWN", "", None}


def split_valid_invalid_nasa(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Valida eventos do topic satellite-hotspots (dados NASA FIRMS).
    Análogo a split_valid_invalid() mas com regras específicas para hotspots.

    Regras aplicadas:
    1. Campos obrigatórios: grid_id, latitude, longitude, frp_mw
    2. Intervalos físicos: FRP (0-5000 MW), brightness (200-500 K),
       coordenadas dentro de Portugal Continental
    3. grid_id não pode ser PT-UNKNOWN (indica falha de mapeamento)
    4. FRP=0 é tecnicamente válido (hotspot de baixa intensidade)
       mas registado como aviso nos logs

    Parâmetros:
        batch → lista de dicionários (eventos do topic satellite-hotspots)

    Devolve:
        (valid, invalid) — mesmo formato que split_valid_invalid()

    Exemplos de rejection_reasons específicos:
        ["frp_mw_out_of_range(99999.0)"]    → FRP impossível (erro sensor)
        ["latitude_out_of_range(0.0)"]      → coordenada default (não mapeada)
        ["grid_id_unknown(PT-UNKNOWN)"]     → falha no mapeamento de zona
        ["brightness_null"]                 → campo ausente na resposta NASA
    """
    valid, invalid = [], []

    for ev in batch:
        reasons = []

        # 1. Verifica campos obrigatórios
        for col in NOT_NULL_NASA:
            if ev.get(col) is None:
                reasons.append(f"{col}_null")

        # 2. Verifica intervalos físicos (só se o campo existe)
        for col, (min_val, max_val) in RULES_NASA.items():
            val = ev.get(col)
            if val is None:
                # Já capturado acima se é campo obrigatório
                # Para brightness (opcional): regista como aviso mas não rejeita
                if col == "brightness":
                    reasons.append(f"brightness_null")
                continue
            try:
                if not (min_val <= float(val) <= max_val):
                    reasons.append(f"{col}_out_of_range({val})")
            except (TypeError, ValueError):
                reasons.append(f"{col}_invalid_type({val})")

        # 3. Verifica grid_id inválido (PT-UNKNOWN = falha de mapeamento de coordenadas)
        grid = ev.get("grid_id")
        if grid in INVALID_GRID_IDS:
            reasons.append(f"grid_id_unknown({grid})")

        if reasons:
            ev["_rejection_reasons"] = reasons
            invalid.append(ev)
        else:
            valid.append(ev)

    return valid, invalid


def build_rejected_record_nasa(ev: dict) -> dict:
    """
    Constrói o registo de quarentena para um hotspot NASA inválido.
    Análogo a build_rejected_record() mas com os campos relevantes da NASA.

    Campos incluídos:
        - grid_id, rejected_at, timestamp → identificação
        - latitude, longitude → coordenadas originais (útil para debug de mapeamento)
        - frp_mw, brightness, confidence → valores NASA originais
        - rejection_reasons → lista de motivos
    """
    return {
        "grid_id":            ev.get("grid_id", "UNKNOWN"),
        "rejected_at":        datetime.now(timezone.utc).isoformat(),
        "original_timestamp": ev.get("timestamp"),
        "source":             ev.get("source", "nasa_firms_real"),
        "latitude":           ev.get("latitude"),
        "longitude":          ev.get("longitude"),
        "frp_mw":             ev.get("frp_mw"),
        "brightness":         ev.get("brightness"),
        "confidence":         ev.get("confidence"),
        "rejection_reasons":  ev.get("_rejection_reasons", []),
    }
