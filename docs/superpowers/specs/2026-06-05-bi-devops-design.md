# BI & DevOps Design — ForestRisk
**Date:** 2026-06-05  
**Author:** Vasco Sousa (Person C — BI & DevOps)  
**Project:** ForestRisk ISEP Postgraduate Big Data

---

## 1. Scope

Three Power BI dashboard pages + AWS security layer, fulfilling the BI & DevOps deliverables from the project spec.

Execution order: **pages first, security last** (Option A).

---

## 2. Data Architecture

All Power BI pages connect via a single ODBC DSN (`ForestRiskAthena`, eu-west-1, Import mode).

| Athena Table/View | S3 Source | Format | Page |
|---|---|---|---|
| `viirs_hotspots` | `s3://forest-risk-datalake/viirs/` | CSV (OpenCSVSerde) | Page 3 |
| `viirs_hotspots_by_year` | View over `viirs_hotspots` | — | Page 3 |
| `agregados_streaming` | `s3://forest-risk-datalake/agregados_streaming/` | Parquet | Page 1 |
| `mock_predictions` | `s3://forest-risk-datalake/mock/` | CSV | Page 2 |

**No Cassandra ODBC.** All data routes through Athena over S3.

### mock_predictions schema
```
grid_id, regiao, predicted_risk_score, confidence, prediction_date, model_version
```
~50 rows, Portugal regiões, risk 0–100, confidence 60–95%, model_version = "v0.1-mock".

---

## 3. Page 3 — Historical Analysis (VIIRS 2020–2024)

**Layout B:** sidebar slicers + KPI row + charts + map below.

### Layout
```
┌─────────────┬──────────────────────────────────────────┐
│  SLICERS    │  [KPI] Total    [KPI] FRP    [KPI] Pior  │
│             │  Hotspots       Médio        Ano         │
│  Ano        ├──────────────────────┬───────────────────┤
│  (list)     │  📊 Bar chart        │  📅 Heatmap       │
│             │  Hotspots por Ano    │  Mês × Ano        │
│  Mês        │                      │                   │
│  (list)     ├──────────────────────┴───────────────────┤
│             │  🗺️ Mapa Portugal — hotspots por distrito │
│  Satélite   │                                          │
│  (dropdown) │                                          │
│             │                                          │
│  Dia/Noite  │                                          │
│  (toggle)   │                                          │
└─────────────┴──────────────────────────────────────────┘
```

### Visuals
- **KPI — Total Hotspots:** `COUNT(*)`
- **KPI — FRP Médio:** `AVG(frp)`
- **KPI — Pior Ano:** year with MAX hotspot count (DAX measure)
- **Bar chart:** hotspots per year (x=year, y=count), sorted ascending
- **Heatmap (Matrix):** rows=month, columns=year, values=COUNT(*)
- **Map:** latitude/longitude, bubble size=frp, cross-filters with slicers

### Data source
`viirs_hotspots_by_year` view — columns: `year`, `month`, `latitude`, `longitude`, `frp`, `satellite`, `daynight`, `acq_date`.

### Cross-filter
All visuals cross-filter each other on selection.

---

## 4. Page 1 — Real-time Risk Map

**Near-real-time** (Spark writes Parquet every 30s, Athena reads on Power BI refresh).

### Layout
```
┌──────────────────────────────────────────────────────────┐
│  TÍTULO: Mapa de Risco em Tempo Real          [refresh]  │
├──────────┬───────────────────────────────────────────────┤
│ Slicer:  │  🗺️ Mapa Portugal                             │
│ grid_id  │  Bubble = risk_medio (cor: verde→amarelo→red) │
│          │  Tamanho = n_leituras                         │
│ Slicer:  │                                               │
│ janela   ├───────────────────┬───────────────────────────┤
│ (últimas │  📊 Risk por Zona  │  KPI: Zona mais crítica  │
│ N horas) │  Bar chart desc.  │  KPI: Risk médio global  │
└──────────┴───────────────────┴───────────────────────────┘
```

### Visuals
- **Map:** bubble per grid_id, color scale risk_medio (green <40, yellow 40–70, red >70), size = n_leituras
- **Bar chart:** top zones by risk_medio, descending
- **KPI — Zona mais crítica:** grid_id with MAX(risk_medio)
- **KPI — Risk médio global:** AVG(risk_medio) across all zones in selected window

### Data source
`agregados_streaming` — columns: `grid_id`, `janela_inicio`, `janela_fim`, `risk_medio`, `risk_maximo`, `n_leituras`, `temp_media`, `humidade_media`, `vento_medio`.

---

## 5. Page 2 — Predictive Dashboard

**Uses mock data** — ML model not yet integrated. Disclaimer shown prominently.

### Layout
```
┌──────────────────────────────────────────────────────────┐
│  TÍTULO: Previsão de Risco (Modelo ML)                   │
├────────────────────────────┬─────────────────────────────┤
│  🗺️ Mapa Portugal          │  KPI: Zonas Alto Risco (>70)│
│  Cor = predicted_risk_score│  KPI: Confiança Média       │
│  (mesmo esquema p.1)       │  KPI: Versão Modelo         │
│                            ├─────────────────────────────┤
│                            │  📊 Bar: Top 10 Zonas       │
│                            │  risco previsto desc.       │
├────────────────────────────┴─────────────────────────────┤
│  📋 Tabela: grid_id | regiao | predicted_risk | conf. %  │
│  (ordenável, filtrable)                                  │
├──────────────────────────────────────────────────────────┤
│  ℹ️ "Dados simulados — modelo em desenvolvimento"        │
└──────────────────────────────────────────────────────────┘
```

### Visuals
- **Map:** same color scheme as Page 1, bubble = predicted_risk_score
- **KPI — Zonas Alto Risco:** COUNTROWS WHERE predicted_risk_score > 70
- **KPI — Confiança Média:** AVG(confidence)
- **KPI — Versão Modelo:** first value of model_version column (text card)
- **Bar chart:** top 10 grid zones by predicted_risk_score
- **Table:** sortable detail grid
- **Disclaimer text box:** "Dados simulados — modelo em desenvolvimento"

### Data source
`mock_predictions` CSV in `s3://forest-risk-datalake/mock/`.

---

## 6. AWS Security (for report sections 6–7)

Deferred implementation. Design to be documented in report and implemented as JSON policies under `aws/iam/`.

### IAM Least Privilege
- **Current state:** `ForestRiskUser` has broad `AmazonAthenaFullAccess` + `AWSGlueConsoleFullAccess`
- **Target:** custom inline policy — S3 scoped to `forest-risk-*` buckets only, Athena scoped to `forest_risk` Glue database only
- **Power BI read-only role:** `s3:GetObject`, `athena:StartQueryExecution`, `athena:GetQueryResults`, `athena:GetQueryExecution` — nothing else

### Data Protection
- S3 SSE-S3 encryption (default on all new buckets — verify)
- S3 versioning enabled on `forest-risk-datalake`
- Block all public access on all `forest-risk-*` buckets

### Audit
- AWS CloudTrail: log S3 + Athena API calls → `forest-risk-logs` bucket
- S3 server access logs on `forest-risk-datalake`

### Deliverable
`aws/iam/forestrisk-user-policy.json` — least-privilege IAM policy as code, committed to repo.

---

## 7. Implementation Order

1. Create `mock_predictions` CSV and upload to S3, create Athena table
2. Build Page 3 (Historical Analysis) — data already working
3. Build Page 1 (Real-time Risk Map) — depends on `agregados_streaming` Parquet
4. Build Page 2 (Predictive) — depends on mock_predictions table
5. AWS Security — IAM policy JSON, CloudTrail setup, S3 hardening
6. Write report sections 6–7

---

## 8. Out of Scope

- True streaming in Power BI (requires Premium — not available)
- Cassandra ODBC connection
- Automatic Power BI refresh (manual refresh sufficient for academic demo)
- ML model integration (Page 2 uses mock data)
