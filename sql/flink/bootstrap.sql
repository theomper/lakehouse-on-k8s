-- SAFETY: Drop existing table to ensure schema update (WARNING: Deletes data!)
DROP TABLE IF EXISTS sensor_data_raw;

-- RAW (7 days): partitioned by dt so partition expiration can drop old data.
-- Note: primary key includes partition key to avoid cross-partition updates in fixed-bucket mode.

CREATE TABLE IF NOT EXISTS sensor_data_raw (
  dt STRING,
  home_id STRING,
  plug_id STRING,
  voltage DOUBLE,
  current_amp DOUBLE,
  power_factor DOUBLE,
  energy_wh DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (dt, home_id, plug_id, ts) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'bucket' = '6',
  'partition.expiration-time' = '7 d',
  'partition.expiration-check-interval' = '1 h',
  'snapshot.time-retained' = '7 d'
);

-- HOURLY (historical): keep long-term; dashboards derive day/week/month/year from this table.
CREATE TABLE IF NOT EXISTS sensor_data_hourly (
  dt STRING,
  home_id STRING,
  plug_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_voltage DOUBLE,
  avg_current DOUBLE,
  avg_pf DOUBLE,
  energy_kwh DOUBLE,
  samples BIGINT,
  peak_power_w DOUBLE,
  PRIMARY KEY (dt, home_id, plug_id, window_start) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  -- keep data indefinitely; just trim snapshot history to control metadata growth
  'snapshot.time-retained' = '30 d'
);

-- 10-MINUTE aggregation: higher resolution for near-real-time dashboards
CREATE TABLE IF NOT EXISTS sensor_data_10min (
  dt STRING,
  home_id STRING,
  plug_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_voltage DOUBLE,
  avg_current DOUBLE,
  avg_pf DOUBLE,
  energy_kwh DOUBLE,
  samples BIGINT,
  peak_power_w DOUBLE,
  PRIMARY KEY (dt, home_id, plug_id, window_start) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  -- keep data indefinitely; just trim snapshot history to control metadata growth
  'snapshot.time-retained' = '30 d'
);

-- 30-MINUTE aggregation: per-tier rollup for the experiment loop.
-- Window aligned to wall-clock minute :00 / :30. Mirrors hourly schema
-- so existing dashboards/queries can switch tables with one rename.
CREATE TABLE IF NOT EXISTS sensor_data_30min (
  dt STRING,
  home_id STRING,
  plug_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_voltage DOUBLE,
  avg_current DOUBLE,
  avg_pf DOUBLE,
  energy_kwh DOUBLE,
  samples BIGINT,
  peak_power_w DOUBLE,
  PRIMARY KEY (dt, home_id, plug_id, window_start) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  'snapshot.time-retained' = '30 d'
);
