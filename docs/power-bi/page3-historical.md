# Page 3 — Análise Histórica: Build Guide

## Data source
Athena view: `forest_risk.viirs_hotspots_by_year` (via ForestRiskAthena ODBC DSN, eu-west-1, Import mode)

## Layout (B — sidebar slicers)
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

## Visuals

### Slicers (left sidebar, ~20% width)
| Slicer | Field | Style |
|--------|-------|-------|
| Ano | year | List |
| Mês | month | List |
| Satélite | satellite | Dropdown |
| Dia/Noite | daynight | Dropdown |

### KPI Cards (top row, right of slicers)
- **Total Hotspots**: `COUNT of acq_date`
- **FRP Médio (MW)**: `AVERAGE of frp`, 1 decimal place
- **Ano com Mais Hotspots**: DAX measure (see below)

### Bar Chart — Hotspots por Ano
- X-axis: `COUNT of acq_date`
- Y-axis: `year`
- Sort: ascending by year
- Title: "Hotspots por Ano"

### Matrix Heatmap — Mês × Ano
- Rows: `month`
- Columns: `year`
- Values: `COUNT of acq_date`
- Conditional formatting: background color scale (white → orange → red)
- Title: "Intensidade por Mês × Ano"

### Map — Portugal Hotspots
- Latitude: `latitude`
- Longitude: `longitude`
- Size: `AVERAGE of frp`
- Title: "Hotspots em Portugal"
- Positioned at bottom, full width

## DAX Measures

```dax
Pior Ano =
CALCULATE(
    SELECTEDVALUE(viirs_hotspots_by_year[year]),
    TOPN(1,
        SUMMARIZE(
            viirs_hotspots_by_year,
            viirs_hotspots_by_year[year],
            "n", COUNT(viirs_hotspots_by_year[acq_date])
        ),
        [n], DESC
    )
)
```

## Cross-filtering
All visuals participate in cross-filtering (default Power BI behaviour). Selecting a year in the bar chart filters the map and heatmap; selecting a slicer value filters all visuals.

## Steps to Build
1. Get Data → ODBC → ForestRiskAthena → forest_risk → viirs_hotspots_by_year → Load
2. Add page tab, rename to "Análise Histórica"
3. Add 4 slicers in left column (Ano list, Mês list, Satélite dropdown, Dia/Noite dropdown)
4. Add 3 KPI card visuals top-right (Total Hotspots, FRP Médio, Pior Ano DAX measure)
5. Add clustered bar chart (Hotspots por Ano)
6. Add Matrix visual (Heatmap Mês × Ano with conditional formatting)
7. Add Map visual (latitude/longitude/frp)
8. Verify cross-filtering works: click year in bar → map and heatmap should filter
9. Save as ForestRisk.pbix
