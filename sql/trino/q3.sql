-- Q3: Window function (rolling average) on raw
-- 60-row trailing window over voltage, partitioned by home_id.
-- Heavy: PARTITION + SORT scale with the entire dataset.
SELECT
  home_id,
  ts,
  voltage,
  AVG(voltage) OVER (
    PARTITION BY home_id
    ORDER BY ts
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ) AS rolling_avg
FROM paimon.iot.sensor_data_raw
WHERE plug_id = 'general'
ORDER BY ts DESC
LIMIT 100;
