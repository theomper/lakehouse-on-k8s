-- Q5: Percentile and statistical analysis on raw
-- Heavy statistical computation: p50/p95/p99 percentiles + standard deviation.
-- Scales with raw data volume. Returns the top 20 homes by voltage stddev.
SELECT
  home_id,
  approx_percentile(voltage, 0.5)  AS p50_voltage,
  approx_percentile(voltage, 0.95) AS p95_voltage,
  approx_percentile(voltage, 0.99) AS p99_voltage,
  stddev(voltage)                  AS voltage_stddev
FROM paimon.iot.sensor_data_raw
WHERE plug_id = 'general'
GROUP BY home_id
ORDER BY voltage_stddev DESC
LIMIT 20;
