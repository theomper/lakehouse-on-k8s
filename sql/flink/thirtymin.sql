-- Aggregate RAW -> 30-MINUTE.
-- Per-experiment-tier rollup: fired once per sub-phase post-flush by the
-- orchestrator. Same WATERMARK-stripping trick as tenmin/hourly: strip
-- WATERMARK FIRST so the WHERE on ts binds to the cast alias, not the
-- watermark-annotated column.
--
-- Bounded lookback (1h) covers one completed 30-min window + 30 min slack
-- for late data. Requires session timezone to match the timezone the
-- streaming ingest used to write `ts` (both UTC, set via
-- -Dtable.local-time-zone=UTC on the CronJob).
CREATE TEMPORARY VIEW raw_no_wm_30m AS
SELECT
  dt, home_id, plug_id, voltage, current_amp, power_factor, energy_wh,
  CAST(ts AS TIMESTAMP(3)) AS ts
FROM sensor_data_raw;

CREATE TEMPORARY VIEW raw_recent_30m AS
SELECT * FROM raw_no_wm_30m
WHERE dt >= DATE_FORMAT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '1' DAY, 'yyyy-MM-dd')
  AND ts >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '1' HOUR;

INSERT INTO sensor_data_30min
SELECT
  DATE_FORMAT(window_start, 'yyyy-MM-dd')         AS dt,
  home_id,
  plug_id,
  window_start,
  window_start + INTERVAL '30' MINUTE             AS window_end,
  AVG(voltage)                                    AS avg_voltage,
  AVG(current_amp)                                AS avg_current,
  AVG(power_factor)                               AS avg_pf,
  SUM(energy_wh) / 1000.0                         AS energy_kwh,
  COUNT(*)                                        AS samples,
  MAX(voltage * current_amp * power_factor)        AS peak_power_w
FROM (
  SELECT *,
    CAST(
      DATE_FORMAT(ts, 'yyyy-MM-dd HH:') ||
      LPAD(CAST((CAST(DATE_FORMAT(ts, 'm') AS INT) / 30) * 30 AS STRING), 2, '0') ||
      ':00'
      AS TIMESTAMP(3)
    ) AS window_start
  FROM raw_recent_30m
) t
WHERE window_start + INTERVAL '30' MINUTE <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
GROUP BY home_id, plug_id, window_start;
