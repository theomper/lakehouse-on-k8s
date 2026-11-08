-- Q2: GROUP BY + multi-aggregate on raw
-- Full-scan aggregation that scales with raw data volume.
-- Returns the top 50 (home, plug) combinations by computed peak power.
SELECT
  home_id,
  plug_id,
  AVG(voltage)                              AS avg_v,
  AVG(current_amp)                          AS avg_i,
  MAX(voltage * current_amp * power_factor) AS peak_power_w,
  COUNT(*)                                  AS samples
FROM paimon.iot.sensor_data_raw
GROUP BY home_id, plug_id
ORDER BY peak_power_w DESC
LIMIT 50;
