# Page 1 — Risco em Tempo Real: Build Guide

## Data source
Athena table: `forest_risk.agregados_streaming` (Parquet, via ForestRiskAthena ODBC DSN, eu-west-1, Import mode)
Written by Spark Structured Streaming every 30 seconds.

## Layout
```
┌──────────────────────────────────────────────────────────┐
│  TÍTULO: Mapa de Risco em Tempo Real                     │
├──────────┬───────────────────────────────────────────────┤
│ Slicer:  │  🟦 Treemap: Risco por Zona                   │
│ grid_id  │  Size + colour = risk_medio                   │
│          │  (green < 40, yellow 40-70, red > 70)         │
│ Slicer:  │                                               │
│ janela   ├───────────────────┬───────────────────────────┤
│ (between)│  📊 Bar: Top Zonas│  KPI: Zona mais crítica  │
│          │  por risco desc.  │  KPI: Risco médio global  │
└──────────┴───────────────────┴───────────────────────────┘
```

## Note on map vs. treemap
The `agregados_streaming` Parquet table does not contain latitude/longitude columns — those come from the VIIRS sensor data (Page 3). A geographic map is not possible without enrichment. A Treemap provides equivalent zone-level risk overview and is recommended for this page.

## Visuals

### Slicers (left sidebar)
| Slicer | Field | Style |
|--------|-------|-------|
| Zona | grid_id | Dropdown |
| Janela Temporal | janela_inicio | Between (date/time range) |

### Treemap — Risco por Zona
- Group: `grid_id`
- Values: `AVERAGE of risk_medio`
- Conditional formatting (background): green → yellow → red (0–100 scale)
- Title: "Risco por Zona (tamanho = risco médio)"

### Bar Chart — Top 10 Zonas por Risco
- Y-axis: `grid_id`
- X-axis: `AVERAGE of risk_medio`
- Sort: descending
- Top N filter: 10
- Conditional formatting on bars: same green→red color scale
- Title: "Top 10 Zonas por Risco"

### KPI Cards
- **Zona mais crítica**: DAX measure (see below)
- **Risco médio global**: `AVERAGE of risk_medio`, 1 decimal place

## DAX Measures

```dax
Zona Critica =
CALCULATE(
    SELECTEDVALUE(agregados_streaming[grid_id]),
    TOPN(1,
        SUMMARIZE(
            agregados_streaming,
            agregados_streaming[grid_id],
            "r", AVERAGE(agregados_streaming[risk_medio])
        ),
        [r], DESC
    )
)
```

## Note on empty data
If Spark Structured Streaming is not running, `agregados_streaming` may have 0 rows. Visuals will show "(Blank)". Add a text box: *"Dados actualizados a cada 30s via Spark Streaming — use Home → Refresh para actualizar"*.

## Refresh
Import mode — use **Home → Refresh** to pull latest Athena data. Not true real-time (no Power BI Premium required).

## Steps to Build
1. Get Data → ODBC → ForestRiskAthena → forest_risk → agregados_streaming → Load
2. Add page tab, rename to "Risco em Tempo Real", drag to first position
3. Add 2 slicers (grid_id dropdown, janela_inicio between)
4. Add Treemap visual (grid_id / risk_medio / conditional formatting)
5. Add bar chart Top 10 zones
6. Add 2 KPI cards (Zona Critica DAX, Risco médio global)
7. Add text box note about data refresh
8. Save as ForestRisk.pbix
