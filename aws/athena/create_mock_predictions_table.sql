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
