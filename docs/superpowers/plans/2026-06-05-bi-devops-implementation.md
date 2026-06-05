# BI & DevOps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build 3 Power BI dashboard pages (historical, real-time, predictive) backed by AWS Athena over S3, plus an AWS IAM least-privilege security layer.

**Architecture:** Power BI Desktop connects via `ForestRiskAthena` ODBC DSN (eu-west-1) in Import mode. All data lives in S3 and is queried via Athena. Mock ML predictions are generated as CSV and uploaded to S3 for Page 2. IAM policies are stored as JSON in the repo.

**Tech Stack:** AWS Athena, AWS S3, AWS IAM, AWS CloudTrail, Power BI Desktop, Python (boto3), SQL (Presto/Athena dialect)

---

## Task 1: Generate mock_predictions CSV and upload to S3

**Files:**
- Create: `scripts/generate_mock_predictions.py`
- Create: `data/mock/mock_predictions.csv`

- [ ] **Step 1: Create the generator script**

Create `scripts/generate_mock_predictions.py`:

```python
import csv
import os
import random
from datetime import date

import boto3

REGIOES = [
    ("PT-AVR-001", "Aveiro Norte"), ("PT-AVR-002", "Aveiro Sul"),
    ("PT-BEJ-001", "Beja Interior"), ("PT-BEJ-002", "Beja Litoral"),
    ("PT-BRG-001", "Braga Norte"), ("PT-BRG-002", "Braga Sul"),
    ("PT-BGR-001", "Bragança Este"), ("PT-BGR-002", "Bragança Oeste"),
    ("PT-CBR-001", "Castelo Branco Norte"), ("PT-CBR-002", "Castelo Branco Sul"),
    ("PT-COI-001", "Coimbra Norte"), ("PT-COI-002", "Coimbra Sul"),
    ("PT-EVR-001", "Évora Norte"), ("PT-EVR-002", "Évora Sul"),
    ("PT-FAR-001", "Faro Litoral"), ("PT-FAR-002", "Faro Interior"),
    ("PT-GRD-001", "Guarda Norte"), ("PT-GRD-002", "Guarda Sul"),
    ("PT-LEI-001", "Leiria Norte"), ("PT-LEI-002", "Leiria Sul"),
    ("PT-LIS-001", "Lisboa Norte"), ("PT-LIS-002", "Lisboa Sul"),
    ("PT-PTG-001", "Portalegre Norte"), ("PT-PTG-002", "Portalegre Sul"),
    ("PT-PRT-001", "Porto Norte"), ("PT-PRT-002", "Porto Sul"),
    ("PT-STB-001", "Setúbal Norte"), ("PT-STB-002", "Setúbal Sul"),
    ("PT-VCT-001", "Viana do Castelo Norte"), ("PT-VCT-002", "Viana do Castelo Sul"),
    ("PT-VRL-001", "Vila Real Norte"), ("PT-VRL-002", "Vila Real Sul"),
    ("PT-VSU-001", "Viseu Norte"), ("PT-VSU-002", "Viseu Sul"),
    ("PT-MDE-001", "Madeira Este"), ("PT-AZO-001", "Açores Central"),
    ("PT-SNT-001", "Santarém Norte"), ("PT-SNT-002", "Santarém Sul"),
    ("PT-STP-001", "Setúbal Península"), ("PT-OEI-001", "Oeste Interior"),
    ("PT-ALG-001", "Algarve Este"), ("PT-ALG-002", "Algarve Oeste"),
    ("PT-ALE-001", "Alentejo Central"), ("PT-ALE-002", "Alentejo Litoral"),
    ("PT-TRS-001", "Trás-os-Montes Norte"), ("PT-TRS-002", "Trás-os-Montes Sul"),
    ("PT-DRO-001", "Douro Norte"), ("PT-DRO-002", "Douro Sul"),
    ("PT-MNH-001", "Minho Norte"), ("PT-MNH-002", "Minho Sul"),
]

random.seed(42)
today = date.today().isoformat()

rows = []
for grid_id, regiao in REGIOES:
    risk = round(random.uniform(5, 98), 1)
    confidence = round(random.uniform(60, 95), 1)
    rows.append({
        "grid_id": grid_id,
        "regiao": regiao,
        "predicted_risk_score": risk,
        "confidence": confidence,
        "prediction_date": today,
        "model_version": "v0.1-mock",
    })

os.makedirs("data/mock", exist_ok=True)
csv_path = "data/mock/mock_predictions.csv"

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"CSV gerado: {csv_path} ({len(rows)} linhas)")

# Upload to S3
bucket = "forest-risk-datalake"
s3_key = "mock/mock_predictions.csv"

s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
s3.upload_file(csv_path, bucket, s3_key)
print(f"Uploaded to s3://{bucket}/{s3_key}")
```

- [ ] **Step 2: Load .env variables and run the script**

```bash
# From project root (Windows PowerShell):
$env:AWS_ACCESS_KEY_ID = (Get-Content .env | Select-String "AWS_ACCESS_KEY_ID").ToString().Split("=")[1].Trim()
$env:AWS_SECRET_ACCESS_KEY = (Get-Content .env | Select-String "AWS_SECRET_ACCESS_KEY").ToString().Split("=")[1].Trim()
$env:AWS_DEFAULT_REGION = "eu-west-1"

python scripts/generate_mock_predictions.py
```

Expected output:
```
CSV gerado: data/mock/mock_predictions.csv (50 linhas)
Uploaded to s3://forest-risk-datalake/mock/mock_predictions.csv
```

- [ ] **Step 3: Verify file in S3**

```bash
aws s3 ls s3://forest-risk-datalake/mock/
```

Expected: `mock_predictions.csv` listed with non-zero size.

- [ ] **Step 4: Commit**

```bash
git add scripts/generate_mock_predictions.py data/mock/mock_predictions.csv
git commit -m "feat: add mock ML predictions generator and CSV data"
```

---

## Task 2: Create Athena tables for streaming and mock data

**Files:**
- Create: `aws/athena/create_agregados_streaming_table.sql`
- Create: `aws/athena/create_mock_predictions_table.sql`

- [ ] **Step 1: Create the Athena SQL files**

Create `aws/athena/create_agregados_streaming_table.sql`:

```sql
-- Run in AWS Athena console, database: forest_risk, region: eu-west-1
-- Output location: s3://forest-risk-athena-results/

CREATE EXTERNAL TABLE IF NOT EXISTS forest_risk.agregados_streaming (
  janela_inicio TIMESTAMP,
  janela_fim    TIMESTAMP,
  grid_id       STRING,
  n_leituras    BIGINT,
  risk_medio    DOUBLE,
  risk_maximo   DOUBLE,
  temp_media    DOUBLE,
  humidade_media DOUBLE,
  vento_medio   DOUBLE
)
STORED AS PARQUET
LOCATION 's3://forest-risk-datalake/agregados_streaming/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Create `aws/athena/create_mock_predictions_table.sql`:

```sql
-- Run in AWS Athena console, database: forest_risk, region: eu-west-1
CREATE EXTERNAL TABLE IF NOT EXISTS forest_risk.mock_predictions (
  grid_id               STRING,
  regiao                STRING,
  predicted_risk_score  DOUBLE,
  confidence            DOUBLE,
  prediction_date       STRING,
  model_version         STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '"',
  'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://forest-risk-datalake/mock/'
TBLPROPERTIES (
  'skip.header.line.count' = '1',
  'use.null.for.invalid.data' = 'true'
);
```

- [ ] **Step 2: Run both SQL files in the AWS Athena console**

1. Open AWS Athena console → eu-west-1 → Query editor
2. Select database: `forest_risk`
3. Paste and run `create_agregados_streaming_table.sql`
4. Paste and run `create_mock_predictions_table.sql`

- [ ] **Step 3: Verify tables with a test query**

Run in Athena Query Editor:
```sql
SELECT * FROM forest_risk.mock_predictions LIMIT 5;
```

Expected: 5 rows with grid_id, regiao, predicted_risk_score (numbers, not strings), confidence, prediction_date, model_version.

```sql
-- Verify streaming table structure (may return 0 rows if Spark hasn't written yet)
SELECT COUNT(*) FROM forest_risk.agregados_streaming;
```

Expected: query succeeds (0 or more rows).

- [ ] **Step 4: Commit**

```bash
git add aws/athena/
git commit -m "feat: add Athena DDL for streaming aggregates and mock predictions tables"
```

---

## Task 3: Build Page 3 — Historical Analysis in Power BI

**Files:**
- Create: `docs/power-bi/page3-historical.md` (build guide, saved for report)
- Modify: `ForestRisk.pbix` (Power BI file — manual steps)

**Pre-condition:** `ForestRiskAthena` ODBC DSN configured in Windows (64-bit), `viirs_hotspots_by_year` view exists in Athena.

- [ ] **Step 1: Connect Power BI to Athena and load viirs_hotspots_by_year**

1. Open Power BI Desktop → Get Data → ODBC
2. DSN: `ForestRiskAthena` → OK
3. Navigate: AwsDataCatalog → forest_risk → `viirs_hotspots_by_year`
4. Click Load (Import mode)

- [ ] **Step 2: Add a dedicated Page 3 tab**

Right-click tab bar → Add page → Rename to `"Análise Histórica"`

- [ ] **Step 3: Add sidebar slicers (left column ~20% width)**

Add 4 Slicer visuals stacked vertically on left side:

| Slicer | Field | Style |
|--------|-------|-------|
| Ano | viirs_hotspots_by_year[year] | List |
| Mês | viirs_hotspots_by_year[month] | List |
| Satélite | viirs_hotspots_by_year[satellite] | Dropdown |
| Dia/Noite | viirs_hotspots_by_year[daynight] | Dropdown |

- [ ] **Step 4: Add KPI cards (top row, right of slicers)**

Add 3 Card visuals in a row:

**Card 1 — Total Hotspots:**
- Field: `COUNT of acq_date` (drag acq_date, change aggregation to Count)
- Title: "Total Hotspots"

**Card 2 — FRP Médio:**
- Field: `Average of frp`
- Format: 1 decimal place
- Title: "FRP Médio (MW)"

**Card 3 — Pior Ano:**
- Create DAX measure:
  ```
  Pior Ano = 
  VAR TabelaAnos = SUMMARIZE(
      viirs_hotspots_by_year,
      viirs_hotspots_by_year[year],
      "total", COUNT(viirs_hotspots_by_year[acq_date])
  )
  RETURN MAXX(TabelaAnos, [total])
  ```
  Wait — this gives the count. Instead use:
  ```
  Pior Ano = 
  CALCULATE(
      SELECTEDVALUE(viirs_hotspots_by_year[year]),
      TOPN(1,
          SUMMARIZE(viirs_hotspots_by_year, viirs_hotspots_by_year[year], "n", COUNT(viirs_hotspots_by_year[acq_date])),
          [n], DESC
      )
  )
  ```
- Title: "Ano com Mais Hotspots"

- [ ] **Step 5: Add bar chart — Hotspots por Ano**

Add Clustered Bar Chart:
- X-axis: `COUNT of acq_date`
- Y-axis: `year`
- Sort: ascending by year
- Title: "Hotspots por Ano"
- Place in middle-left area (below KPIs)

- [ ] **Step 6: Add heatmap (Matrix visual) — Mês × Ano**

Add Matrix visual:
- Rows: `month`
- Columns: `year`
- Values: `COUNT of acq_date`
- Conditional formatting on Values: background color scale (white → orange → red)
- Title: "Intensidade por Mês × Ano"
- Place in middle-right area

- [ ] **Step 7: Add map — Portugal hotspots**

Add Map visual (built-in Bing Map or Azure Map):
- Location: leave empty (use lat/lon instead)
- Latitude: `viirs_hotspots_by_year[latitude]`
- Longitude: `viirs_hotspots_by_year[longitude]`
- Size: `Average of frp`
- Title: "Hotspots em Portugal"
- Place in bottom area spanning full width

- [ ] **Step 8: Verify cross-filtering works**

1. Click a year in the bar chart → all other visuals should filter
2. Click a cell in the heatmap → bar chart and map should filter
3. Select "2022" in the Ano slicer → map should show only 2022 points

- [ ] **Step 9: Save the Power BI file**

File → Save As → `ForestRisk.pbix` in project root.

- [ ] **Step 10: Create build guide doc**

Create `docs/power-bi/page3-historical.md`:

```markdown
# Page 3 — Análise Histórica: Build Guide

## Data source
Athena view: `forest_risk.viirs_hotspots_by_year` (via ForestRiskAthena DSN)

## Visuals
- 4 slicers (Ano, Mês, Satélite, Dia/Noite) — left sidebar
- 3 KPI cards (Total Hotspots, FRP Médio, Pior Ano) — top row
- Bar chart: Hotspots por Ano — middle left
- Matrix heatmap: Mês × Ano — middle right
- Map: lat/lon/frp — bottom full width

## DAX measures
- Pior Ano: TOPN/SUMMARIZE pattern to find year with max hotspot count

## Cross-filter
All visuals participate in cross-filtering (default Power BI behavior).
```

- [ ] **Step 11: Commit the guide**

```bash
git add docs/power-bi/page3-historical.md
git commit -m "docs: add Page 3 Historical Analysis Power BI build guide"
```

---

## Task 4: Build Page 1 — Real-time Risk Map in Power BI

**Files:**
- Create: `docs/power-bi/page1-realtime.md`
- Modify: `ForestRisk.pbix`

**Pre-condition:** `forest_risk.agregados_streaming` table exists in Athena (Task 2).

- [ ] **Step 1: Load agregados_streaming into Power BI**

1. Home → Transform data (or Get Data → ODBC → ForestRiskAthena)
2. Navigate to `forest_risk.agregados_streaming` → Load

- [ ] **Step 2: Add Page 1 tab**

Right-click tab bar → Add page → Rename to `"Risco em Tempo Real"` → drag tab to first position.

- [ ] **Step 3: Add slicers**

Add 2 Slicer visuals on left side:

**Slicer 1 — grid_id:**
- Field: `agregados_streaming[grid_id]`
- Style: Dropdown
- Title: "Zona"

**Slicer 2 — janela (time window):**
- Field: `agregados_streaming[janela_inicio]`
- Style: Between (date/time range)
- Title: "Janela Temporal"

- [ ] **Step 4: Add map**

Add Map visual:
- Latitude: `agregados_streaming[latitude]` — **NOTE:** this column doesn't exist in `agregados_streaming`. Use a workaround:
  - Create a relationship or lookup table, OR
  - Add a static lat/lon lookup by joining with `viirs_hotspots_by_year` on `grid_id` — but grid_id formats differ.
  - **Simpler approach:** Use the `grid_id` field as Location (Power BI will geocode Portuguese region names). Change to use `regiao` column from mock_predictions if available, or just plot without map and use bar chart only for Page 1.
  - **Actual fix:** `agregados_streaming` has no lat/lon. Use a bar chart for risk by zone instead of a map. The map is only meaningful on Page 3 (VIIRS has lat/lon).

For Page 1 map: skip geographic map, use **Treemap** instead:
- Group: `grid_id`
- Values: `Average of risk_medio`
- Color saturation: `Average of risk_medio`
- Title: "Risco por Zona (tamanho = risco médio)"

- [ ] **Step 5: Add KPI cards**

**Card 1 — Zona Mais Crítica:**
```
Zona Critica = 
CALCULATE(
    SELECTEDVALUE(agregados_streaming[grid_id]),
    TOPN(1,
        SUMMARIZE(agregados_streaming, agregados_streaming[grid_id], "r", AVERAGE(agregados_streaming[risk_medio])),
        [r], DESC
    )
)
```

**Card 2 — Risco Médio Global:**
- Field: `Average of risk_medio`
- Format: 1 decimal place
- Title: "Risco Médio Global"

- [ ] **Step 6: Add bar chart — Top zonas por risco**

Add Clustered Bar Chart:
- Y-axis: `grid_id`
- X-axis: `Average of risk_medio`
- Sort by risk_medio descending
- Top N filter: show top 10 zones
- Title: "Top 10 Zonas por Risco"
- Conditional formatting on bars: color scale (green → red) matching risk values

- [ ] **Step 7: Verify**

1. Confirm bar chart shows zones with risk values
2. Confirm slicers filter the visuals
3. Confirm KPI updates with slicer selection
4. Note: if `agregados_streaming` has 0 rows (Spark not running), visuals show "(Blank)" — this is expected. Add a text box: "Dados actualizados a cada 30s via Spark Streaming"

- [ ] **Step 8: Save and create guide**

Save `ForestRisk.pbix`.

Create `docs/power-bi/page1-realtime.md`:

```markdown
# Page 1 — Risco em Tempo Real: Build Guide

## Data source
Athena table: `forest_risk.agregados_streaming` (Parquet, via ForestRiskAthena DSN)
Written by Spark Structured Streaming every 30s.

## Visuals
- 2 slicers (grid_id dropdown, janela_inicio between)
- Treemap: risco por zona (size + color = risk_medio)
- 2 KPI cards (Zona mais crítica, Risco médio global)
- Bar chart: top 10 zonas por risco médio (descending, top N filter)

## Note
No lat/lon in streaming table — geographic map not possible without enrichment.
Treemap provides equivalent zone-level risk overview.

## Refresh
Import mode — use Home → Refresh to pull latest Athena data.
```

- [ ] **Step 9: Commit**

```bash
git add docs/power-bi/page1-realtime.md
git commit -m "docs: add Page 1 Real-time Risk Map Power BI build guide"
```

---

## Task 5: Build Page 2 — Predictive Dashboard in Power BI

**Files:**
- Create: `docs/power-bi/page2-predictive.md`
- Modify: `ForestRisk.pbix`

**Pre-condition:** `forest_risk.mock_predictions` table loaded (Task 2 + Task 1).

- [ ] **Step 1: Load mock_predictions into Power BI**

1. Get Data → ODBC → ForestRiskAthena
2. Navigate to `forest_risk.mock_predictions` → Load
3. In Power Query: verify `predicted_risk_score` and `confidence` are Decimal Number type (not Text). If Text, change type.

- [ ] **Step 2: Add Page 2 tab**

Right-click tab bar → Add page → Rename to `"Previsão ML"`

- [ ] **Step 3: Add map with predicted risk**

Add Map visual:
- **Note:** `mock_predictions` has no lat/lon. Use same Treemap approach as Page 1:

Add Treemap:
- Group: `regiao`
- Values: `Average of predicted_risk_score`
- Title: "Risco Previsto por Região"
- Conditional formatting: same green→red color scale

- [ ] **Step 4: Add KPI cards (right column)**

**Card 1 — Zonas Alto Risco:**
```
Zonas Alto Risco = 
CALCULATE(
    COUNTROWS(mock_predictions),
    mock_predictions[predicted_risk_score] > 70
)
```

**Card 2 — Confiança Média:**
- Field: `Average of confidence`
- Format: 1 decimal + "%" suffix
- Title: "Confiança Média do Modelo"

**Card 3 — Versão Modelo:**
- Field: `First of model_version` (aggregation: First)
- Title: "Versão do Modelo"

- [ ] **Step 5: Add bar chart — Top 10 zonas**

Add Clustered Bar Chart:
- Y-axis: `regiao`
- X-axis: `Average of predicted_risk_score`
- Top N filter: top 10
- Sort descending
- Title: "Top 10 Regiões — Risco Previsto"

- [ ] **Step 6: Add detail table**

Add Table visual:
- Columns: `grid_id`, `regiao`, `predicted_risk_score`, `confidence`, `model_version`
- Sort default: predicted_risk_score descending
- Format `predicted_risk_score` and `confidence` as 1 decimal
- Title: "Detalhe por Zona"

- [ ] **Step 7: Add disclaimer text box**

Add Text Box visual at bottom:
- Text: `"⚠️ Dados simulados — modelo ML em desenvolvimento. Não usar para decisões operacionais."`
- Font: 11px, italic, color #666666
- Background: light yellow #FFFACD
- Full width at bottom of page

- [ ] **Step 8: Verify**

1. Treemap shows all 50 regions with colored risk scores
2. Zonas Alto Risco KPI shows correct count (should be ~15–20)
3. Table is sortable by clicking column headers
4. Disclaimer visible at bottom

- [ ] **Step 9: Save and create guide**

Save `ForestRisk.pbix`.

Create `docs/power-bi/page2-predictive.md`:

```markdown
# Page 2 — Previsão ML: Build Guide

## Data source
Athena table: `forest_risk.mock_predictions` (CSV, via ForestRiskAthena DSN)
Generated by `scripts/generate_mock_predictions.py`.

## Visuals
- Treemap: risco previsto por região (color = predicted_risk_score)
- 3 KPI cards (Zonas Alto Risco >70, Confiança Média, Versão Modelo)
- Bar chart: top 10 regiões por risco previsto
- Table: detalhe por zona (sortável)
- Disclaimer text box: dados simulados

## DAX measures
- Zonas Alto Risco: COUNTROWS WHERE predicted_risk_score > 70

## Note
Replace mock_predictions.csv with real model output when ML pipeline is ready.
Update model_version field to reflect the deployed model.
```

- [ ] **Step 10: Commit**

```bash
git add docs/power-bi/page2-predictive.md
git commit -m "docs: add Page 2 Predictive Dashboard Power BI build guide"
```

---

## Task 6: AWS Security — IAM Least-Privilege Policy

**Files:**
- Create: `aws/iam/forestrisk-user-policy.json`
- Create: `aws/iam/README.md`

- [ ] **Step 1: Create the least-privilege IAM policy JSON**

Create `aws/iam/forestrisk-user-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataLakeReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::forest-risk-datalake",
        "arn:aws:s3:::forest-risk-datalake/*",
        "arn:aws:s3:::forest-risk-models",
        "arn:aws:s3:::forest-risk-models/*"
      ]
    },
    {
      "Sid": "S3AthenaResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::forest-risk-athena-results",
        "arn:aws:s3:::forest-risk-athena-results/*"
      ]
    },
    {
      "Sid": "AthenaForestRiskDatabase",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:ListQueryExecutions",
        "athena:GetWorkGroup",
        "athena:ListWorkGroups"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueForestRiskDatabase",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:DeleteTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:eu-west-1:*:catalog",
        "arn:aws:glue:eu-west-1:*:database/forest_risk",
        "arn:aws:glue:eu-west-1:*:table/forest_risk/*"
      ]
    },
    {
      "Sid": "MLflowS3ReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::forest-risk-models",
        "arn:aws:s3:::forest-risk-models/*"
      ]
    }
  ]
}
```

- [ ] **Step 2: Create `aws/iam/README.md`**

```markdown
# AWS IAM Policies

## forestrisk-user-policy.json

Least-privilege policy for the `ForestRiskUser` IAM user.

**Replaces:** AmazonAthenaFullAccess + AWSGlueConsoleFullAccess (overly broad)

**Grants:**
- S3: read/write on `forest-risk-datalake`, `forest-risk-models`, `forest-risk-athena-results` only
- Athena: query execution in any workgroup (required for ODBC)
- Glue: read/write on `forest_risk` database only (no other databases)

**How to apply:**
1. IAM console → Users → ForestRiskUser → Permissions
2. Remove: AmazonAthenaFullAccess, AWSGlueConsoleFullAccess
3. Add permissions → Attach policies → Create inline policy
4. Paste JSON from this file → name: `ForestRiskLeastPrivilege`

**Test after applying:**
- Power BI refresh still works
- `aws s3 ls s3://forest-risk-datalake/` works
- `aws s3 ls s3://other-bucket/` returns AccessDenied (expected)
```

- [ ] **Step 3: Apply the policy in AWS IAM console**

1. Open IAM console → Users → `ForestRiskUser` → Permissions tab
2. Click "Add permissions" → "Attach policies directly"
3. Search for and **remove** (detach): `AmazonAthenaFullAccess`, `AWSGlueConsoleFullAccess`
4. Click "Add permissions" → "Create inline policy" → JSON tab
5. Paste content of `forestrisk-user-policy.json`
6. Name: `ForestRiskLeastPrivilege`
7. Click Create policy

- [ ] **Step 4: Verify least-privilege works**

```bash
# Should succeed
aws s3 ls s3://forest-risk-datalake/

# Should return AccessDenied (no access to other buckets)
aws s3 ls s3://some-other-bucket/ 2>&1 | grep -i "access denied"
```

Also verify Power BI still refreshes Page 3 data successfully.

- [ ] **Step 5: Commit**

```bash
git add aws/iam/
git commit -m "feat: add least-privilege IAM policy for ForestRiskUser"
```

---

## Task 7: AWS Security — S3 Hardening + CloudTrail

**Files:**
- Create: `aws/security/s3-hardening-checklist.md`
- Create: `aws/security/cloudtrail-setup.md`

- [ ] **Step 1: Enable S3 versioning on forest-risk-datalake**

1. AWS S3 console → `forest-risk-datalake` → Properties tab
2. Bucket Versioning → Enable → Save

- [ ] **Step 2: Verify Block Public Access is enabled**

1. S3 console → `forest-risk-datalake` → Permissions tab
2. Block public access → all 4 options should be ON
3. Repeat for `forest-risk-models` and `forest-risk-athena-results`

- [ ] **Step 3: Verify SSE-S3 encryption is the default**

1. S3 console → `forest-risk-datalake` → Properties tab
2. Default encryption → should show "SSE-S3" (AES-256)
3. If not: Edit → Enable → SSE-S3 → Save

- [ ] **Step 4: Enable CloudTrail**

1. AWS CloudTrail console → Create trail
2. Trail name: `forest-risk-audit`
3. S3 bucket: create new `forest-risk-logs` in eu-west-1
4. Log file SSE-KMS encryption: disabled (SSE-S3 is fine for academic)
5. CloudWatch Logs: skip for academic
6. Under "Events": Management events → Read + Write
7. Under "Data events": Add S3 → select `forest-risk-datalake` → Read + Write
8. Create trail

- [ ] **Step 5: Create security documentation**

Create `aws/security/s3-hardening-checklist.md`:

```markdown
# S3 Security Hardening Checklist

| Bucket | Versioning | Block Public Access | SSE-S3 |
|--------|-----------|---------------------|--------|
| forest-risk-datalake | ✅ Enabled | ✅ All ON | ✅ Default |
| forest-risk-models | ✅ Enabled | ✅ All ON | ✅ Default |
| forest-risk-athena-results | — | ✅ All ON | ✅ Default |

## Notes
- `forest-risk-athena-results`: versioning not needed (Athena result files are ephemeral)
- All buckets in eu-west-1 (Ireland)
```

Create `aws/security/cloudtrail-setup.md`:

```markdown
# CloudTrail Audit Setup

**Trail name:** forest-risk-audit  
**Region:** eu-west-1  
**Log destination:** s3://forest-risk-logs/  
**Events logged:**
- Management events: Read + Write (all API calls)
- Data events: S3 Read + Write on forest-risk-datalake

**Purpose:** Audit trail for S3 and Athena access — required for compliance section of project report.

**Retention:** CloudTrail delivers logs within ~15 minutes. Logs stored in `forest-risk-logs` bucket indefinitely (no lifecycle rule set).
```

- [ ] **Step 6: Commit**

```bash
git add aws/security/
git commit -m "feat: add AWS S3 hardening checklist and CloudTrail audit documentation"
```

---

## Task 8: Final verification and push

- [ ] **Step 1: Verify all 3 Power BI pages work end-to-end**

1. Open `ForestRisk.pbix`
2. Home → Refresh All
3. Confirm Page 3 loads VIIRS data with year/month slicers working
4. Confirm Page 1 loads streaming data (or shows empty if Spark not running)
5. Confirm Page 2 loads mock predictions with disclaimer visible

- [ ] **Step 2: Push branch to origin**

```bash
git push origin BI_DevOps
```

- [ ] **Step 3: Check final repo structure**

```
aws/
  athena/
    create_agregados_streaming_table.sql
    create_mock_predictions_table.sql
  iam/
    forestrisk-user-policy.json
    README.md
  security/
    s3-hardening-checklist.md
    cloudtrail-setup.md
data/
  mock/
    mock_predictions.csv
docs/
  power-bi/
    page1-realtime.md
    page2-predictive.md
    page3-historical.md
  superpowers/
    specs/2026-06-05-bi-devops-design.md
    plans/2026-06-05-bi-devops-implementation.md
scripts/
  generate_mock_predictions.py
```
