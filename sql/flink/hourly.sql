-- Aggregate RAW -> HOURLY.
-- Two-view pattern: strip WATERMARK rowtime FIRST (raw_no_wm), then apply
-- ts/dt filters (raw_recent). The watermark-strip cast must be in its own
-- view so the WHERE on ts binds to the cast alias, not the rowtime column.
--
-- Bounded lookback (3h) keeps scan cost roughly constant: one completed
-- window + slack for late data. Requires session timezone to match the
-- timezone the streaming ingest used to write `ts` (both UTC, set via
-- -Dtable.local-time-zone=UTC on the CronJob).
CREATE TEMPORARY VIEW raw_no_wm AS
SELECT
  dt, home_id, plug_id, voltage, current_amp, power_factor, energy_wh,
  CAST(ts AS TIMESTAMP(3)) AS ts
FROM sensor_data_raw;

CREATE TEMPORARY VIEW raw_recent AS
SELECT * FROM raw_no_wm
WHERE dt >= DATE_FORMAT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '1' DAY, 'yyyy-MM-dd')
  AND ts >= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '3' HOUR;

-- Only emit windows whose end has elapsed; partial windows would otherwise be
-- written and immediately overwritten on the next run (wasted I/O + UPSERTs).
INSERT INTO sensor_data_hourly
SELECT
  DATE_FORMAT(window_start, 'yyyy-MM-dd')         AS dt,
  home_id,
  plug_id,
  window_start,
  window_start + INTERVAL '1' HOUR                AS window_end,
  AVG(voltage)                                    AS avg_voltage,
  AVG(current_amp)                                AS avg_current,
  AVG(power_factor)                               AS avg_pf,
  SUM(energy_wh) / 1000.0                         AS energy_kwh,
  COUNT(*)                                        AS samples,
  MAX(voltage * current_amp * power_factor)        AS peak_power_w
FROM (
  SELECT *,
    CAST(DATE_FORMAT(ts, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) AS window_start
  FROM raw_recent
) t
WHERE window_start + INTERVAL '1' HOUR <= CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
GROUP BY home_id, plug_id, window_start;
