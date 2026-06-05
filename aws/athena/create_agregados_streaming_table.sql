CREATE EXTERNAL TABLE IF NOT EXISTS forest_risk.agregados_streaming (
  janela_inicio TIMESTAMP,
  janela_fim TIMESTAMP,
  grid_id STRING,
  n_leituras BIGINT,
  risk_medio DOUBLE,
  risk_maximo DOUBLE,
  temp_media DOUBLE,
  humidade_media DOUBLE,
  vento_medio DOUBLE
)
STORED AS PARQUET
LOCATION 's3://forest-risk-datalake/agregados_streaming/';
