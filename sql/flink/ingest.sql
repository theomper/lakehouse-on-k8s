-- Source: Kafka JSON -> compute event-time ts_ltz
CREATE TEMPORARY TABLE sensor_data_kafka_home  (
  home_id STRING,
  plug_id STRING,
  voltage DOUBLE,
  `current` DOUBLE,
  power_factor DOUBLE,
  energy_wh DOUBLE,
  `timestamp` STRING,
  ts_ltz AS TRY_CAST(`timestamp` AS TIMESTAMP_LTZ(3)),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-sensor-general',
  'properties.bootstrap.servers' = 'kafka-0.kafka-svc.data-ingest.svc.cluster.local:9092,kafka-1.kafka-svc.data-ingest.svc.cluster.local:9092',
  --- 'properties.group.id' = 'flink-iot-ingest',
  'properties.group.id' = 'flink-iot-ingest-v7',
  'scan.startup.mode' = 'earliest-offset',
  ---'value.format' = 'json',
  ---'json.timestamp-format.standard' = 'ISO-8601',
  ---'json.fail-on-missing-field' = 'false',
  ---'json.ignore-parse-errors' = 'true'
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601',
  --- 'value.json.fail-on-missing-field' = 'false',
  'value.json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE sensor_data_kafka_plug  (
  home_id STRING,
  plug_id STRING,
  voltage DOUBLE,
  `current` DOUBLE,
  power_factor DOUBLE,
  energy_wh DOUBLE,
  `timestamp` STRING,
  ts_ltz AS TRY_CAST(`timestamp` AS TIMESTAMP_LTZ(3)),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot-sensor-plug',
  'properties.bootstrap.servers' = 'kafka-0.kafka-svc.data-ingest.svc.cluster.local:9092,kafka-1.kafka-svc.data-ingest.svc.cluster.local:9092',
  --- 'properties.group.id' = 'flink-iot-ingest',
  'properties.group.id' = 'flink-iot-ingest-v7',
  'scan.startup.mode' = 'earliest-offset',
  ---'value.format' = 'json',
  ---'json.timestamp-format.standard' = 'ISO-8601',
  ---'json.fail-on-missing-field' = 'false',
  ---'json.ignore-parse-errors' = 'true'
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601',
  --- 'value.json.fail-on-missing-field' = 'false',
  'value.json.ignore-parse-errors' = 'true'
);

-- Union the two Kafka sources
CREATE TEMPORARY VIEW sensor_data_kafka_all AS
SELECT * FROM sensor_data_kafka_home
UNION ALL
SELECT * FROM sensor_data_kafka_plug;

-- Keep PK NOT NULL: coalesce plug_id; drop rows with bad timestamps/home_ids
-- Ingest into RAW and derive dt from ts (dt drives partition TTL)
INSERT INTO sensor_data_raw
SELECT
  DATE_FORMAT(ts, 'yyyy-MM-dd') AS dt,
  home_id,
  COALESCE(plug_id, 'general') AS plug_id,
  voltage,
  `current` AS current_amp,
  power_factor,
  energy_wh,
  ts
FROM (
  SELECT
    home_id,
    plug_id,
    voltage,
    `current`,
    power_factor,
    energy_wh,
    COALESCE(
      CAST(ts_ltz AS TIMESTAMP(3)),            -- preferred: parsed event-time
      CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))  -- fallback: processing-time
    ) AS ts
  FROM sensor_data_kafka_all
  WHERE home_id IS NOT NULL
) t;
