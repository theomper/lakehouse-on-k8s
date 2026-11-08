-- Q4: Cross-table JOIN raw <-> 10-minute aggregate (with full-bucket filter)
--
-- Demonstrates the analytical JOIN pattern between raw measurements and
-- pre-aggregated 10-minute buckets. The lakehouse design intent is that
-- analytical workloads can mix recent fine-grained data with older
-- coarse-grained aggregates.
--
-- The "samples >= 0.8 * MAX(samples)" filter retains only fully-populated
-- 10-min buckets, keeping cross-tier comparisons fair regardless of how the
-- experiment run window aligns with bucket boundaries. Without it, edge
-- buckets (partial 10-minute windows at the start and end of the run)
-- would skew the results inconsistently across tiers.
--
-- Returns the top 20 homes by voltage drift (raw avg vs aggregated avg).
SELECT
  r.home_id,
  COUNT(*)                            AS raw_samples,
  t.energy_kwh                        AS tenmin_energy,
  AVG(r.voltage)                      AS raw_avg_voltage,
  t.avg_voltage                       AS tenmin_avg_voltage,
  ABS(AVG(r.voltage) - t.avg_voltage) AS voltage_drift
FROM paimon.iot.sensor_data_raw r
JOIN paimon.iot.sensor_data_10min t
  ON  r.home_id = t.home_id
  AND r.plug_id = t.plug_id
  AND r.dt      = t.dt
WHERE r.plug_id = 'general'
  AND t.samples >= (
    SELECT CAST(MAX(samples) AS DOUBLE) * 0.8
    FROM paimon.iot.sensor_data_10min
  )
GROUP BY r.home_id, t.energy_kwh, t.avg_voltage
ORDER BY voltage_drift DESC
LIMIT 20;
