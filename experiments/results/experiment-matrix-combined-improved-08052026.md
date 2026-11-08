# experiment-matrix-lakehouse-on-k8s (IMPROVED)

> Exported from `experiment-matrix-lakehouse-on-k8s.xlsx` as plain Markdown tables.
> **IMPROVED VERSION** — Trino queries Q2-Q5 upgraded to test analytical capabilities at scale.

## Experiment Matrix

**Inputs & Formula**

Expected RPS formula: $R_{total} = \text{Homes} \times \frac{(1+P)}{\text{interval}}$

| Input | Value |
| --- | --- |
| Plugs per home (P) | 4 |
| Sampling interval (s) | 1 |
| Target homes per Go pod | 3500 |
| Measurement window (min) | 30 |
| Flush cooldown (s) | 90 |

**Trino Query Suite (IMPROVED)**

| Query ID | Description | SQL | Expected Scaling Behavior |
| --- | --- | --- | --- |
| Q1 | COUNT raw rows | `SELECT COUNT(*) FROM paimon.iot.sensor_data_raw` | Linear with data volume — baseline metric |
| Q2 | GROUP BY + multi-aggregate on raw | `SELECT home_id, plug_id, AVG(voltage), AVG(current_amp), MAX(voltage * current_amp * power_factor) AS peak_power_w, COUNT(*) FROM sensor_data_raw GROUP BY home_id, plug_id ORDER BY peak_power_w DESC LIMIT 50` | Scales with data volume — full-scan aggregation |
| Q3 | Window function (rolling avg) on raw | `SELECT home_id, ts, voltage, AVG(voltage) OVER (PARTITION BY home_id ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS rolling_avg FROM sensor_data_raw WHERE plug_id='general' ORDER BY ts DESC LIMIT 100` | Heavy — PARTITION + SORT entire dataset |
| Q4 | Cross-table JOIN raw ↔ 10min (full-bucket filter) | `SELECT r.home_id, COUNT(*), t.energy_kwh, AVG(r.voltage), t.avg_voltage, ABS(AVG(r.voltage) - t.avg_voltage) AS voltage_drift FROM sensor_data_raw r JOIN sensor_data_10min t ON r.home_id = t.home_id AND r.plug_id = t.plug_id AND r.dt = t.dt WHERE r.plug_id='general' AND t.samples >= (SELECT CAST(MAX(samples) AS DOUBLE) * 0.8 FROM sensor_data_10min) GROUP BY r.home_id, t.energy_kwh, t.avg_voltage ORDER BY voltage_drift DESC LIMIT 20` | Scales with raw volume — demonstrates analytical JOIN pattern. The `samples >= 0.8 × max(samples)` filter retains only fully-populated 10-min buckets to ensure cross-tier comparability regardless of run-window alignment with bucket boundaries. The `sensor_data_10min` table is populated by three in-run firings of the tenmin aggregation at run-elapsed +10/+20/+30 min. |
| Q5 | Percentile/statistical analysis on raw | `SELECT home_id, approx_percentile(voltage, 0.5) AS p50, approx_percentile(voltage, 0.95) AS p95, approx_percentile(voltage, 0.99) AS p99, stddev(voltage) FROM sensor_data_raw WHERE plug_id='general' GROUP BY home_id ORDER BY stddev DESC LIMIT 20` | Heavy statistical computation — scales with volume |

**Run Matrix — Phase 1: Nonbatched Producers (30-minute measurement window)**

| Run ID | Homes | Go Pods | Homes/Go Pod | Expected RPS | Observed RPS | Avg Cycle (s) | Max Cycle (s) | Saturated? | OK (Δ) | Errors (Δ) | Error Rate | Kafka-0 CPU (cores) | Kafka-0 Mem (GiB) | Kafka-0 Net RX (MiB/s) | Kafka-0 Net TX (MiB/s) | Kafka-0 PVC (GiB) | Kafka-1 CPU (cores) | Kafka-1 Mem (GiB) | Kafka-1 Net RX (MiB/s) | Kafka-1 Net TX (MiB/s) | Kafka-1 PVC (GiB) | Flink JM CPU (cores) | Flink JM Mem (GiB) | Flink JM Net RX (MiB/s) | Flink JM Net TX (MiB/s) | Flink JM FS (GiB) | Flink TM-1 CPU (cores) | Flink TM-1 Mem (GiB) | Flink TM-1 Net RX (MiB/s) | Flink TM-1 Net TX (MiB/s) | Flink TM-1 FS (GiB) | Flink TM-2 CPU (cores) | Flink TM-2 Mem (GiB) | Flink TM-2 Net RX (MiB/s) | Flink TM-2 Net TX (MiB/s) | Flink TM-2 FS (GiB) | Ckpt Completed (Δ) | Ckpt Failed (Δ) | Ckpt Restored (Δ) | Failover? | Azure SA Tx (sum) | Azure SA Ingress (GiB) | Azure SA Egress (GiB) | Azure SA E2E Lat (ms) | Azure SA Srv Lat (ms) | Azure SA Avail (%) | Azure SA Capacity (GiB) | Trino Q1 p95 (s) | Trino Q2 p95 (s) | Trino Q3 p95 (s) | Trino Q4 p95 (s) | Trino Q5 p95 (s) | Trino Failures | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| N1 | 50 | 1 | 50 | 250 | 996 | 0,002 | 0,003 | FALSE | 149004 | 0 | 0 | 0,074 | 0,935 | 0,081 | 0,075 | 0,015 | 0,03 | 1,166 | 0,08 | 0,076 | 0,014 | 0,002 | 0,773 | 0,001 | 0 | 0 | 0,142 | 3,342 | 0,068 | 0,026 | 0 | 0,002 |  | 0 | 0,001 |  | 0,099 | 2,721 | 0,03 | 0,014 | 0 | 0,002 |  | 0 | 0 |  | 9 | 0 | 0 | N | 7853 | 0,007 | 0,072 | 12,8 | 11,6 | 100,0 | 0,002 | 3,9 | 3,76 | 4,11 | 3,95 | 3,98 | 0 |  |
| N2 | 500 | 1 | 500 | 2500 | 89970 | 0.016 | 0.024 | FALSE | 99821875 | 0 | 0 | 0.134 | 1.596 | 0.985 | 0.885 | 0.924 | 0.121 | 1.635 | 0.978 | 0.872 | 0.929 | 0.015 | 0.803 | 0.01 | 0.005 | 0 | 0.117 | 3.815 | 0.706 | 0.253 | 0 | 0.07 | 3.604 | 0.431 | 0.148 | 0 |  |  |  |  |  |  |  |  |  |  | -14 | 0 | 0 | N | 16499 | 0.217 | 0.779 | 14.2 | 13.5 | 100.0 | 0.019 | 5.06 | 7.23 | 7.12 | 6.72 | 6.36 | 0 |  |
| N3 | 1500 | 1 | 1500 | 7500 | 269925 | 0.056 | 0.105 | FALSE | 299470751 | 0 | 0 | 0.319 | 2.089 | 2.904 | 2.585 | 2.69 | 0.324 | 2.275 | 2.866 | 2.603 | 2.674 | 0.016 | 0.764 | 0.011 | 0.006 | 0 | 0.27 | 3.866 | 2.072 | 0.722 | 0 | 0.176 | 3.668 | 1.349 | 0.447 | 0 |  |  |  |  |  |  |  |  |  |  | -14 | 0 | 0 | N | 17736 | 0.650 | 2.451 | 16.8 | 15.6 | 100.0 | 0.023 | 12.28 | 15.59 | 14.61 | 14.34 | 13.44 | 0 |  |
| N4 | 3500 | 1 | 3500 | 17500 | 647262 | 0.166 | 0.27 | FALSE | 737559743 | 0 | 0 | 0.656 | 3.27 | 6.664 | 5.945 | 6.809 | 0.678 | 3.924 | 6.527 | 5.991 | 6.763 | 0.017 | 0.788 | 0.01 | 0.005 | 0 | 0.611 | 4.004 | 5.29 | 1.79 | 0 | 0.28 | 3.984 | 2.43 | 0.738 | 0 |  |  |  |  |  |  |  |  |  |  | -15 | 0 | 0 | N | 17878 | 1.639 | 4.000 | 20.1 | 18.1 | 100.0 | 0.075 | 26.12 | 31.82 | 30.26 | 29.76 | 29.3 | 0 |  |
| N5 | 5500 | 2 | 2750 | 27500 | 1017189 | 0.144 | 0.361 | FALSE | 1159024761 | 0 | 0 | 0.874 | 5.201 | 9.942 | 9.035 | 11.009 | 0.764 | 4.952 | 9.847 | 8.865 | 10.947 | 0.015 | 0.776 | 0.011 | 0.006 | 0 | 0.416 | 4.274 | 3.687 | 1.112 | 0 | 0.883 | 4.196 | 8.173 | 2.686 | 0 |  |  |  |  |  |  |  |  |  |  | 72 | 0 | 0 | N | 18807 | 2.654 | 5.933 | 21.8 | 19.3 | 100.0 | 0.089 | 45.2 | 64.62 | 59.85 | 56.71 | 56.49 | 0 |  |
| N6 | 7500 | 3 | 2500 | 37500 | 1387092 | 0.158 | 0.318 | FALSE | 1580504656 | 0 | 0 | 1.174 | 7.321 | 13.459 | 12.27 | 16.009 | 1.013 | 6.59 | 13.356 | 11.99 | 15.978 | 0.016 | 0.764 | 0.01 | 0.005 | 0 | 1.13 | 4.264 | 11.021 | 3.508 | 0 | 0.608 | 4.218 | 5.413 | 1.471 | 0 |  |  |  |  |  |  |  |  |  |  | 78 | 0 | 0 | N | 18290 | 3.601 | 8.317 | 26.6 | 22.8 | 100.0 | 0.103 | 74.99 | 91.62 | 86.37 | 78.33 | 86.06 | 0 |  |
| N7 | 10000 | 3 | 3333 | 50000 | 1899493 | 0.191 | 0.324 | FALSE | 2221258052 | 0 | 0 | 1.522 | 9.047 | 18.224 | 16.238 | 24.043 | 1.577 | 9.815 | 18.075 | 16.487 | 23.968 | 0.015 | 0.822 | 0.011 | 0.005 | 0 | 0.95 | 4.357 | 7.633 | 2.012 | 0 | 1.617 | 4.292 | 14.717 | 4.818 | 0 |  |  |  |  |  |  |  |  |  |  | 88 | 0 | 0 | N | 15212 | 4.820 | 6.631 | 30.0 | 26.0 | 100.0 | 0.131 | 79.76 | 109.68 | 101.81 | 98.17 | 100.69 | 0 |  |
| N8 | 12500 | 4 | 3125 | 62500 | 62518 | 0.165 | 0.259 | FALSE | 146186468 | 0 | 0 | 1.153 | 18.183 | 17.648 | 15.926 | 43.292 | 1.153 | 18.288 | 17.601 | 15.889 | 43.283 | 0.015 | 0.925 | 0.007 | 0.004 | 0 | 2.453 | 4.281 | 17.368 | 3.992 | 0 | 1.192 | 3.899 | 1.731 | 0.019 | 0 | 69 | 1 | 0 | N | 13555 | 5.555 | 5.787 | 37.1 | 30.5 | 100.0 | 14.239 | 123.17 | 148.59 | 141.66 | 135.7 | 133.56 | 0 |  |
| N9 | 15000 | 5 | 3000 | 75000 | 75012 | 0.303 | 0.526 | FALSE | 170931403 | 0 | 0 | 1.315 | 21.126 | 20.73 | 18.491 | 52.693 | 1.302 | 21.199 | 20.7 | 18.475 | 52.616 | 0.016 | 0.813 | 0.007 | 0.006 | 0 | 3.256 | 4.324 | 22.359 | 5.048 | 0.001 | 0.876 | 3.863 | 1.304 | 0.015 | 0 | 54 | 13 | 9 | Y | 15733 | 6.108 | 8.628 | 47.1 | 37.7 | 100.0 | 19.069 | 252.84 | 281.82 | 273.68 | 249.86 | 255.01 | 0 |  |
| N10 | 17500 | 5 | 3500 | 87500 | 87558 | 0.239 | 0.59 | FALSE | 204665492 | 0 | 0 | 1.402 | 22.626 | 23.607 | 20.846 | 59.322 | 1.386 | 22.569 | 23.632 | 20.791 | 59.274 | 0.015 | 0.807 | 0.007 | 0.003 | 0 | 3.414 | 4.314 | 23.279 | 6.011 | 0.001 | 0.942 | 3.863 | 1.388 | 0.017 | 0 | 63 | 5 | 0 | N | 16883 | 8.130 | 9.438 | 42.6 | 33.5 | 100.0 | 23.668 | 198.7 | 230.94 | 0 | 170.85 | 174.88 | 5 |  |

**Run Matrix — Phase 2: Batched Producers (30-minute measurement window)**

| Run ID | Homes | Go Pods | Homes/Go Pod | Expected RPS | Observed RPS | Avg Cycle (s) | Max Cycle (s) | Saturated? | OK (Δ) | Errors (Δ) | Error Rate | Kafka-0 CPU (cores) | Kafka-0 Mem (GiB) | Kafka-0 Net RX (MiB/s) | Kafka-0 Net TX (MiB/s) | Kafka-0 PVC (GiB) | Kafka-1 CPU (cores) | Kafka-1 Mem (GiB) | Kafka-1 Net RX (MiB/s) | Kafka-1 Net TX (MiB/s) | Kafka-1 PVC (GiB) | Flink JM CPU (cores) | Flink JM Mem (GiB) | Flink JM Net RX (MiB/s) | Flink JM Net TX (MiB/s) | Flink JM FS (GiB) | Flink TM-1 CPU (cores) | Flink TM-1 Mem (GiB) | Flink TM-1 Net RX (MiB/s) | Flink TM-1 Net TX (MiB/s) | Flink TM-1 FS (GiB) | Flink TM-2 CPU (cores) | Flink TM-2 Mem (GiB) | Flink TM-2 Net RX (MiB/s) | Flink TM-2 Net TX (MiB/s) | Flink TM-2 FS (GiB) | Ckpt Completed (Δ) | Ckpt Failed (Δ) | Ckpt Restored (Δ) | Failover? | Azure SA Tx (sum) | Azure SA Ingress (GiB) | Azure SA Egress (GiB) | Azure SA E2E Lat (ms) | Azure SA Srv Lat (ms) | Azure SA Avail (%) | Azure SA Capacity (GiB) | Trino Q1 p95 (s) | Trino Q2 p95 (s) | Trino Q3 p95 (s) | Trino Q4 p95 (s) | Trino Q5 p95 (s) | Trino Failures | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | 50 | 1 | 50 | 250 | 997 | 0,102 | 0,102 | FALSE | 149000 | 0 | 0 | 0,056 | 1,026 | 0,049 | 0,046 | 0,012 | 0,011 | 1,162 | 0,05 | 0,054 | 0,013 | 0,044 | 0,758 | 0,016 | 0,013 | 0 | 0,087 | 3,113 | 0,052 | 0,023 | 0 | 0,098 | 2,825 | 0,034 | 0,015 | 0 |  |  |  |  |  |  |  |  |  |  | 8 | 0 | 0 | N | 3648 | 0,004 | 0,018 | 14,8 | 14,0 | 100,0 | 0,003 | 2,97 | 3,35 | 3,8 | 0 | 3,78 | 5 |  |
| B2 | 500 | 1 | 500 | 2500 | 89964 | 0.101 | 0.104 | FALSE | 99810000 | 0 | 0 | 0.015 | 1.576 | 0.561 | 0.555 | 0.743 | 0.01 | 1.628 | 0.56 | 0.568 | 0.736 | 0.018 | 0.782 | 0.011 | 0.006 | 0 | 0.068 | 3.839 | 0.56 | 0.189 | 0 | 0.052 | 3.62 | 0.306 | 0.091 | 0 |  |  |  |  |  |  |  |  |  |  | 68 | 0 | 0 | N | 17040 | 0.210 | 0.722 | 15.1 | 14.4 | 100.0 | 0.023 | 4.59 | 6.87 | 6.76 | 6.46 | 6.49 | 0 |  |
| B3 | 1500 | 1 | 1500 | 7500 | 269894 | 0.097 | 0.114 | FALSE | 299430000 | 0 | 0 | 0.018 | 2.27 | 1.679 | 1.671 | 2.124 | 0.014 | 2.32 | 1.681 | 1.692 | 2.114 | 0.017 | 0.762 | 0.011 | 0.006 | 0 | 0.085 | 3.603 | 0.821 | 0.326 | 0 | 0.153 | 3.955 | 1.69 | 0.359 | 0 |  |  |  |  |  |  |  |  |  |  | -13 | 0 | 0 | N | 17432 | 0.612 | 2.209 | 16.9 | 15.7 | 100.0 | 0.075 | 11.22 | 14.94 | 13.98 | 13.49 | 13.16 | 0 |  |
| B4 | 3500 | 1 | 3500 | 17500 | 647289 | 0.058 | 0.117 | FALSE | 737547996 | 0 | 0 | 0.026 | 3.671 | 3.93 | 3.927 | 5.494 | 0.023 | 3.809 | 3.923 | 3.927 | 5.458 | 0.015 | 0.775 | 0.01 | 0.005 | 0 | 0.173 | 3.968 | 2.016 | 0.564 | 0 | 0.296 | 4.046 | 3.848 | 1.209 | 0 |  |  |  |  |  |  |  |  |  |  | 70 | 0 | 0 | N | 18981 | 1.599 | 3.967 | 20.5 | 18.7 | 100.0 | 0.089 | 24.78 | 30.86 | 29.76 | 27.78 | 29.04 | 0 |  |
| B5 | 5500 | 2 | 2750 | 27500 | 989734 | 0.065 | 0.115 | FALSE | 1097965736 | 0 | 0 | 0.032 | 3.78 | 6.174 | 4.035 | 8.305 | 0.028 | 3.893 | 6.169 | 4.09 | 8.285 | 0.04 | 0.715 | 0.013 | 0.004 | 0 | 0.057 | 3.802 | 0.608 | 0.233 | 0 | 0.218 | 4.186 | 2.293 | 0.341 | 0 |  |  |  |  |  |  |  |  |  |  | -15 | 0 | 0 | N | 8573 | 0.477 | 4.168 | 22.9 | 19.2 | 100.0 | 0.103 | 17.3 | 20.82 | 19.58 | 19.55 | 18.21 | 0 |  |
| B6 | 7500 | 3 | 2500 | 37500 | 1387173 | 0.104 | 0.12 | FALSE | 1580479145 | 0 | 0 | 0.045 | 6.763 | 8.472 | 8.454 | 12.088 | 0.042 | 6.833 | 8.463 | 8.493 | 11.999 | 0.017 | 0.772 | 0.011 | 0.007 | 0 | 0.604 | 4.236 | 7.637 | 2.437 | 0 | 0.506 | 4.204 | 5.854 | 1.72 | 0 |  |  |  |  |  |  |  |  |  |  | -16 | 0 | 0 | N | 19761 | 3.191 | 7.147 | 22.4 | 19.6 | 100.0 | 0.119 | 48.1 | 67.66 | 62.15 | 59.82 | 59.11 | 0 |  |
| B7 | 10000 | 3 | 3333 | 50000 | 1849567 | 0.068 | 0.139 | FALSE | 2107317711 | 0 | 0 | 0.054 | 9.042 | 11.285 | 11.314 | 17.045 | 0.054 | 9.04 | 11.3 | 11.33 | 16.944 | 0.018 | 0.776 | 0.011 | 0.006 | 0 | 0.799 | 4.287 | 10.15 | 3.222 | 0 | 0.648 | 4.297 | 7.795 | 2.27 | 0 |  |  |  |  |  |  |  |  |  |  | -15 | 0 | 0 | N | 16669 | 4.380 | 7.042 | 26.9 | 23.5 | 100.0 | 0.131 | 71.71 | 92.64 | 87.03 | 83.39 | 83.46 | 0 |  |
| B8 | 12500 | 4 | 3125 | 62500 | 62562 | 0.079 | 0.115 | FALSE | 138693499 | 0 | 0 | 0.064 | 15.255 | 11.379 | 11.596 | 31.956 | 0.061 | 15.408 | 11.381 | 11.713 | 31.984 | 0.017 | 0.853 | 0.01 | 0.004 | 0 | 1.098 | 3.873 | 1.673 | 0.018 | 0 | 2.179 | 4.273 | 14.373 | 3.633 | 0 | 65 | 1 | 0 | N | 13403 | 4.940 | 5.369 | 34.1 | 28.3 | 100.0 | 13.966 | 119.76 | 137.27 | 131.69 | 121.06 | 118.6 | 0 |  |
| B9 | 15000 | 5 | 3000 | 75000 | 74961 | 0.116 | 0.182 | FALSE | 166427742 | 0 | 0 | 0.073 | 17.467 | 13.665 | 14.08 | 40.435 | 0.07 | 17.483 | 13.683 | 14.163 | 40.355 | 0.015 | 0.814 | 0.008 | 0.005 | 0 | 1.282 | 3.884 | 1.762 | 0.022 | 0 | 2.716 | 4.28 | 17.567 | 4.253 | 0.001 | 69 | 2 | 2 | Y | 15350 | 5.699 | 8.449 | 40.2 | 32.2 | 100.0 | 17.057 | 177.52 | 221.31 | 196.64 | 175.97 | 191.06 | 0 |  |
| B10 | 17500 | 5 | 3500 | 87500 | 87474 | 0.106 | 0.179 | FALSE | 199418249 | 0 | 0 | 0.083 | 19.564 | 15.753 | 16.512 | 44.399 | 0.079 | 19.401 | 15.793 | 16.457 | 44.357 | 0.016 | 0.81 | 0.007 | 0.005 | 0 | 0.941 | 3.935 | 1.343 | 0.015 | 0 | 3.235 | 4.33 | 20.78 | 4.486 | 0.001 | 58 | 7 | 4 | Y | 17113 | 7.755 | 9.713 | 46.0 | 37.5 | 100.0 | 16.912 | 224.91 | 244.45 | 0 | 223.56 | 218.26 | 5 |  |

## Improvements Over Previous Version

| Change | Old (run-experiment-combined.ps1) | New (run-experiment-combined-improved.ps1) | Why |
| --- | --- | --- | --- |
| **Q2** | `COUNT(*) FROM sensor_data_hourly` (flat ~2s) | GROUP BY + multi-aggregate on raw table | Scales with data volume; shows real analytical work |
| **Q3** | Top-10 GROUP BY on hourly (flat ~2s) | Window function (rolling avg) on raw table | Heavy computation (PARTITION + SORT); dramatic scaling |
| **Q4** | N/A | Cross-table JOIN raw ↔ hourly | Demonstrates data consistency verification at scale |
| **Q5** | N/A | Percentile/statistical query on raw | Showcases advanced analytics (approx_percentile, stddev) |
| **Query runs** | 1 measurement | 3 runs per query; report min/median/max | Reduces cold-start noise |
| **Flink health gate** | None | Verify streaming job RUNNING before proceeding | Avoids 35 min of garbage zeros |
| **Flush cooldown** | None (T1 collected immediately) | 90s wait after stopping producers | Lets Flink complete 2-3 checkpoint cycles |
| **Aggregation strategy** | Relied on CronJob schedule (with concurrent-job pile-up at high tiers) | Hourly auto-CronJob suspended; tenmin fires three times in-run at +10/+20/+30 min; a single 30-min rollup runs after flush cooldown | Eliminates concurrent batch jobs writing the same Kafka stream into Paimon (which had inflated the kafka-vs-paimon ratio to 1.07-1.20 at N7-B8); produces one rollup row per (home, plug, run) |
| **Kafka consumer lag** | Not collected | `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max` | Shows if Flink keeps up with producers |
| **Flink backpressure** | Not collected | busyTime, idleTime, backPressuredTime (ms/sec) | Directly shows pipeline bottleneck |
| **E2E latency** | Not collected | MAX(ts) in raw vs wall clock | Shows data freshness under load |
| **Pre-run validation** | None | Checks Flink REST, Kafka topics, Trino catalog, Azure Storage | Catches misconfigurations early |
| **Trino warm-up** | None | `SELECT 1` before timed suite | Avoids cold-start penalty on first query |
| **Paimon data guards** | None | Warnings if raw/30min row counts are 0 at T0/T1 | Early detection of pipeline failures |

## HowTo

| Step | Details |
| --- | --- |
| 1) Define run inputs | Set plugs/home (P), interval (s), and Target homes per Go pod (blue cells in Experiment Matrix). |
| 2) Run the load tier | Use `run-experiment-combined-improved.ps1` to deploy Go producer pods and start the tier. The improved script handles Flink health gates, flush cooldown, and manual aggregation automatically. |
| 3) Measurement window | After warm-up, select an exact 30-minute window in Grafana (time range picker). |
| 4) Export averages from Grafana | For each component, export the time-series over the exact 30-minute window. Use panel menu → Inspect → Data → Download CSV, then compute the mean. Fill CPU, memory, network RX/TX, and storage (PVC used / filesystem used) columns. |
| 5) Go pods performance | Copy from Go producer logs: observed rps, avg/max cycle time, saturated flag, ok/errors (Δ over 30 minutes). |
| 6) Flink checkpoint stability | From Flink UI (Checkpoints tab), record Completed/Failed/Restored counts at start and end of the 30-minute window; enter deltas. Internal Failover = Y if Restored increased. |
| 7) Trino benchmark | Q1–Q5 run 5 iterations each (automated by improved script). p95 latency (seconds) per query and failure count entered automatically. Q2-Q5 are designed to show scaling behavior on raw data. |
| 8) Azure Storage Account metrics | In Azure Portal → Storage Account → Monitoring → Metrics, select the same 30-minute window. Record Transactions (sum), Ingress/Egress (sum), SuccessE2ELatency/SuccessServerLatency (avg), Availability (avg), and Used Capacity. |
| 9) Notes | Record anything unusual: scaling events, restarts, throttling, checkpoint expiry, etc. Also note Flink health gate results, flush cooldown effectiveness, and Kafka consumer lag values. |

## Assumptions & Sources

| Item | Value | Where used | Notes | Source URL |
| --- | --- | --- | --- | --- |
| Plugs per home (P) | 4 | Expected RPS | Set to match Go generator config (SIM_PLUGS_PER_HOME). | https://grafana.com/docs/grafana/latest/panels-visualizations/panel-inspector/ |
| Sampling interval (s) | 1 | Expected RPS | Fixed at 1s for steady-state experiments. | https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/ |
| Target homes per Go pod | 3500 | Go Pods (count) | Planning value; adjust based on cycle saturation in logs. | https://pkg.go.dev/log |
| 30-minute window | 30 | All Avg 30' metrics | Set Grafana dashboard time range to this window; export CSV and compute mean. | https://grafana.com/docs/grafana/latest/panels-visualizations/panel-inspector/ |
| Flush cooldown | 90s | Post-run | Allows Flink to complete 2-3 checkpoint cycles (30s interval) before T1 collection. |  |
| In-run tenmin aggregation | Three firings at run-elapsed +10/+20/+30 min | tenmin-aggregation CronJob (suspended; invoked via `kubectl create job --from=cronjob/...`) | Exercises streaming + batch coexistence on Flink slots and populates `sensor_data_10min` for Q4 JOIN. A per-call slot guard skips a firing if a previous batch is still RUNNING. |  |
| Post-flush 30-min aggregation | One firing per sub-phase after flush cooldown | thirtymin-aggregation CronJob (suspended; invoked via `kubectl create job --from=cronjob/...`) | Per-tier rollup into `sensor_data_30min` (one row per home/plug); replaces the deprecated hourly aggregation in this experiment loop. |  |
| Kubernetes QoS note |  | Optional | If requests=limits for CPU+mem → Guaranteed QoS (more stable under pressure). | https://kubernetes.io/docs/concepts/workloads/pods/pod-qos |
| Kubernetes network metrics (Prometheus) | container_network_receive_bytes_total, container_network_transmit_bytes_total (use rate()) |  |  |  |
| Kubernetes storage metrics (PVC) | kubelet_volume_stats_used_bytes, kubelet_volume_stats_capacity_bytes |  |  |  |
| Kubernetes container filesystem metric (optional) | container_fs_usage_bytes (if available in your dashboards) |  |  |  |
| Azure Storage Account metrics (doc) | https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-storage-storageaccounts-metrics |  |  |  |
| Azure metrics used | Transactions, Ingress, Egress, SuccessE2ELatency, SuccessServerLatency, Availability, UsedCapacity |  |  |  |
| Azure time grain note | UsedCapacity is typically PT1H (hourly); record the value closest to your 30-minute window. |  |  |  |
| Kafka consumer lag metric | flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max | Pipeline health | Shows if Flink is keeping up with Kafka producers. |  |
| Flink backpressure metrics | busyTimeMsPerSecond, idleTimeMsPerSecond, backPressuredTimeMsPerSecond | Pipeline health | Directly indicates whether Flink tasks are bottlenecked. |  |
