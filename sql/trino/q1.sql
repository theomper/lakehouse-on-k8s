-- Q1: COUNT raw rows
-- Baseline metric for raw data volume; latency scales linearly with row count.
SELECT COUNT(*) FROM paimon.iot.sensor_data_raw;
