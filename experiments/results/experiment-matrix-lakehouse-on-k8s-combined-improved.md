# experiment-matrix-lakehouse-on-k8s (IMPROVED)

> Exported from `experiment-matrix-lakehouse-on-k8s.xlsx` as plain Markdown tables.
> **IMPROVED VERSION** — Trino queries Q2-Q5 upgraded to test analytical capabilities at scale.
> **Blank template.** Rows N1-N10 (non-batched) and B1-B10 (batched) are filled in place by `scripts/run-experiment-combined-improved.ps1`, which matches rows by Run ID. Copy this file first if you want to keep it blank. The completed results of the thesis runs are in `experiment-matrix-combined-improved-08052026.md`.

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
| N1 | 50 | 1 | 50 | 250 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N2 | 500 | 1 | 500 | 2500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N3 | 1500 | 1 | 1500 | 7500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N4 | 3500 | 1 | 3500 | 17500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N5 | 5500 | 2 | 2750 | 27500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N6 | 7500 | 3 | 2500 | 37500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N7 | 10000 | 3 | 3333 | 50000 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N8 | 12500 | 4 | 3125 | 62500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N9 | 15000 | 5 | 3000 | 75000 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| N10 | 17500 | 5 | 3500 | 87500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

**Run Matrix — Phase 2: Batched Producers (30-minute measurement window)**

| Run ID | Homes | Go Pods | Homes/Go Pod | Expected RPS | Observed RPS | Avg Cycle (s) | Max Cycle (s) | Saturated? | OK (Δ) | Errors (Δ) | Error Rate | Kafka-0 CPU (cores) | Kafka-0 Mem (GiB) | Kafka-0 Net RX (MiB/s) | Kafka-0 Net TX (MiB/s) | Kafka-0 PVC (GiB) | Kafka-1 CPU (cores) | Kafka-1 Mem (GiB) | Kafka-1 Net RX (MiB/s) | Kafka-1 Net TX (MiB/s) | Kafka-1 PVC (GiB) | Flink JM CPU (cores) | Flink JM Mem (GiB) | Flink JM Net RX (MiB/s) | Flink JM Net TX (MiB/s) | Flink JM FS (GiB) | Flink TM-1 CPU (cores) | Flink TM-1 Mem (GiB) | Flink TM-1 Net RX (MiB/s) | Flink TM-1 Net TX (MiB/s) | Flink TM-1 FS (GiB) | Flink TM-2 CPU (cores) | Flink TM-2 Mem (GiB) | Flink TM-2 Net RX (MiB/s) | Flink TM-2 Net TX (MiB/s) | Flink TM-2 FS (GiB) | Ckpt Completed (Δ) | Ckpt Failed (Δ) | Ckpt Restored (Δ) | Failover? | Azure SA Tx (sum) | Azure SA Ingress (GiB) | Azure SA Egress (GiB) | Azure SA E2E Lat (ms) | Azure SA Srv Lat (ms) | Azure SA Avail (%) | Azure SA Capacity (GiB) | Trino Q1 p95 (s) | Trino Q2 p95 (s) | Trino Q3 p95 (s) | Trino Q4 p95 (s) | Trino Q5 p95 (s) | Trino Failures | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | 50 | 1 | 50 | 250 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B2 | 500 | 1 | 500 | 2500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B3 | 1500 | 1 | 1500 | 7500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B4 | 3500 | 1 | 3500 | 17500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B5 | 5500 | 2 | 2750 | 27500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B6 | 7500 | 3 | 2500 | 37500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B7 | 10000 | 3 | 3333 | 50000 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B8 | 12500 | 4 | 3125 | 62500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B9 | 15000 | 5 | 3000 | 75000 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| B10 | 17500 | 5 | 3500 | 87500 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

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
