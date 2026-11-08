-- Aggregate RAW -> 10-MINUTE.
-- Two-view pattern: strip WATERMARK rowtime FIRST (raw_no_wm_10m), then
-- apply ts/dt filters (raw_recent_10m). The watermark-strip cast must be
-- in its own view so the WHERE on ts binds to the cast alias, not the
-- rowtime column.
--
-- Bounded lookback (15 min) covers one completed 10-min window + 5 min
-- slack for late data. Reduced from 40 min: at high RPS (25k+) the 40-min
-- window scans 60M rows per run, saturating the TM and starving the
-- streaming job's checkpoint threads. Experiment events are near-real-time
-- so 5 min of slack is sufficient.
CREATE TEMPORARY VIEW raw_no_wm_10m AS
SELECT
  dt, home_id, plug_id, voltage, current_amp, power_factor, energy_wh,
  CAST(ts AS TIMESTAMP(3)) AS ts
FROM sensor_data_raw;

CREATE TEMPORARY VIEW raw_recent_10m AS
SELECT * FROM raw_no_wm_10m
WHERE dt >= DATE_FORMAT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '1' DAY, 'yyyy-MM-dd')
  AND ts >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '15' MINUTE;

-- Only emit windows whose end has elapsed; partial windows would otherwise be
-- written and immediately overwritten on the next run (wasted I/O + UPSERTs).
INSERT INTO sensor_data_10min
SELECT
  DATE_FORMAT(window_start, 'yyyy-MM-dd')         AS dt,
  home_id,
  plug_id,
  window_start,
  window_start + INTERVAL '10' MINUTE             AS window_end,
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
      LPAD(CAST((CAST(DATE_FORMAT(ts, 'm') AS INT) / 10) * 10 AS STRING), 2, '0') ||
      ':00'
      AS TIMESTAMP(3)
    ) AS window_start
  FROM raw_recent_10m
) t
WHERE window_start + INTERVAL '10' MINUTE <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
GROUP BY home_id, plug_id, window_start;
