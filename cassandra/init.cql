-- =========================================================
-- Forest Risk Monitoring System — Cassandra Schema
-- =========================================================

CREATE KEYSPACE IF NOT EXISTS forest_risk
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;

USE forest_risk;

-- Leituras de sensores IoT em tempo real
CREATE TABLE IF NOT EXISTS sensor_readings (
    grid_id     TEXT,
    hour_bucket TEXT,          -- 'YYYY-MM-DD-HH'
    event_time  TIMESTAMP,
    source      TEXT,
    temp_celsius    FLOAT,
    humidity_pct    FLOAT,
    wind_kmh        FLOAT,
    hotspot_count   INT,
    risk_score      FLOAT,
    latitude        DOUBLE,
    longitude       DOUBLE,
    PRIMARY KEY ((grid_id, hour_bucket), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
  AND default_time_to_live = 259200;   -- 72h de retenção (hot data)

-- Alertas gerados pelo motor de risco
CREATE TABLE IF NOT EXISTS fire_alerts (
    alert_id    UUID,
    grid_id     TEXT,
    alert_time  TIMESTAMP,
    risk_score  FLOAT,
    risk_level  TEXT,          -- LOW / MEDIUM / HIGH / CRITICAL
    trigger_temp    FLOAT,
    trigger_humidity FLOAT,
    trigger_wind    FLOAT,
    hotspot_count   INT,
    PRIMARY KEY (grid_id, alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC);

-- Previsões ML (horizonte 24-72h)
CREATE TABLE IF NOT EXISTS risk_predictions (
    grid_id         TEXT,
    prediction_date DATE,
    horizon_hours   INT,       -- 24, 48 ou 72
    predicted_risk  FLOAT,
    confidence      FLOAT,
    model_version   TEXT,
    created_at      TIMESTAMP,
    PRIMARY KEY ((grid_id, prediction_date), horizon_hours)
);
