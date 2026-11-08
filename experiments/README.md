# Scalable IoT Simulator Experiments

## Overview

The experiment runner (`scripts/run-experiment-combined-improved.ps1`) is a fully
automated, end-to-end script that evaluates the lakehouse pipeline at increasing
scale tiers. For each tier it runs **two sub-phases** — Go **nonbatched** and Go
**batched** producers — each with its own full environment reset, data collection,
and metrics recording cycle.

A smoke-test variant (`run-experiment-combined-improved-smoke.ps1`) is available
for quick end-to-end validation with reduced timing and a single tier.

## How it works

### Per-tier lifecycle

For every tier (number of homes) the script executes:

**Sub-phase A — Nonbatched producers:**

| Step | Action |
|------|--------|
| 1 | Full environment reset (delete producers, cancel Flink jobs, purge Kafka topics, delete Paimon blobs from Azure Storage, restart Flink cluster) |
| 2 | Re-deploy data-processing pipeline (Paimon bootstrap, Flink streaming ingest job, aggregation CronJobs) with retry on Flink job failure (up to 3 attempts) |
| 3 | Generate & deploy Go **nonbatched** producer pods |
| 4 | Warm-up period (default 5 min) |
| 5 | Collect T0 baseline metrics + Flink checkpoint T0 (with automatic Flink recovery if streaming job died during warm-up) |
| 6 | Steady-state run (default 30 min) |
| 7 | Parse producer stats **while pods are still alive** (observed RPS, cycle times, saturation, ok/error counts) |
| 8 | Collect T1 metrics snapshot (producers still running) |
| 9 | Stop producers + flush cooldown (default 90s for Flink to commit pending data) |
| 10 | Trigger manual hourly + 10-min aggregation CronJobs and wait for completion |
| 11 | Record Flink checkpoint T1 (Flink still RUNNING) |
| 12 | Collect granular Prometheus averages over the measurement window |
| 13 | Collect Azure Storage Account metrics (transactions, ingress/egress, latency, capacity) |
| 14 | Run Trino benchmark (5 queries x 5 iterations, parallel per iteration) |
| 15 | Persist all phase artifacts (JSON files) and update experiment matrix row immediately |

**Sub-phase B — Batched producers:** identical steps using Go batched producers.

### Key design decisions

- Each sub-phase starts from a **clean slate** (Kafka, Paimon, Flink all fresh)
- Flink streaming job is verified **RUNNING** (stability gate: 3 consecutive checks) before proceeding
- Producer logs/stats/saturation are captured **while pods are still alive**
- Producer stats use the **last `[stats]`/`[cycle]` line per pod**, then sum across pods — avoids cumulative-counter over-reporting from pods that run for different durations
- Prometheus `avg_over_time` window covers the **actual measurement period**
- Matrix rows are updated **immediately** after each sub-phase with fresh data
- Pre-run validation checks Flink REST, Kafka topics, Trino catalog, and Azure Storage before starting
- Azure blob operations use **storage account key authentication** (read from `$env:AZURE_STORAGE_ACCOUNT_KEY` or the `azure-storage-secret` Kubernetes Secret) for Windows CLI compatibility
- A **full PowerShell transcript** (`orchestrator-transcript.log`) is captured in the results directory for forensic traceability

### Producer pod scaling

Each experiment tier deploys **multiple Go producer pods**, each responsible for a
**non-overlapping range** of home IDs via `SIM_HOME_ID_START` / `SIM_HOME_ID_END`.
Generator scripts split homes across pods automatically (3500 homes/pod).

| Generator script | Producer type | Homes/pod (calibrated) | CPU req/pod | CPU limit/pod |
|------------------|---------------|------------------------|-------------|---------------|
| `generate-go-experiment.ps1` | Nonbatched | 3500 | 500m | 2 |
| `generate-go-batched-experiment.ps1` | Batched | 3500 | 500m | 2 |

> **Homes/pod values determined by calibration.** See [`k8s/producers/calibration/`](../k8s/producers/calibration/) for methodology and results.

### Tier scaling reference

Each home produces 1 general record + 4 plug records per second = **5 records/s/home**.

| Run ID | Homes | Records/sec | Go pods |
|--------|-------|-------------|---------|
| N1/B1  | 50    | **250**     | 1       |
| N2/B2  | 500   | **2,500**   | 1       |
| N3/B3  | 1,500 | **7,500**   | 1       |
| N4/B4  | 3,500 | **17,500**  | 1       |
| N5/B5  | 5,500 | **27,500**  | 2       |
| N6/B6  | 7,500 | **37,500**  | 3       |
| N7/B7  | 10,000| **50,000**  | 3       |
| N8/B8  | 12,500| **62,500**  | 4       |
| N9/B9  | 15,000| **75,000**  | 5       |
| N10/B10| 17,500| **87,500**  | 5       |

Run IDs prefixed with **N** = nonbatched, **B** = batched.

## Usage

```powershell
# Run all standard tiers
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500, 1500, 3500, 5500, 7500, 10000, 12500, 15000, 17500)

# Run a subset of tiers
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500, 1500)

# Custom timing (shorter warm-up and run for smoke testing)
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -WarmupMinutes 3 -RunMinutes 15

# Stop AKS cluster after all tiers complete to save costs
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -ClusterShutdown

# Skip environment reset between tiers (useful for re-runs)
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(3500) -SkipCleanup
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-Tiers` | *(required)* | Array of home counts — one experiment iteration per tier |
| `-WarmupMinutes` | 5 | Minutes to wait after deploying producers before taking T0 baseline |
| `-RunMinutes` | 30 | Minutes of steady-state data collection after warm-up |
| `-FlushCooldownSeconds` | 90 | Seconds to wait after stopping producers for Flink to flush pending data |
| `-ExperimentTag` | UTC timestamp | Short label appended to the results folder |
| `-ExperimentMatrixPath` | *(auto-resolved)* | Path to the experiment matrix markdown file |
| `-SkipCleanup` | `$false` | Skip the full environment reset between tiers |
| `-ClusterShutdown` | `$false` | Stop the AKS cluster after all tiers complete |

## Metrics collected

### Per-snapshot metrics (T0 and T1)

- **Kafka**: per-topic offsets (general + plug), raw partition offsets
- **Trino health checks** (8 parallel queries): raw/hourly/10min row counts, distinct homes, total energy, peak power, avg voltage
- **Timed query suite** (Q1–Q5, 3 reps each, parallel): min/median/max latency per query
- **End-to-end latency**: newest raw record timestamp vs wall clock
- **Flink**: job status, running job count, per-job metrics (records in/out per sec, bytes in/out per sec, watermark)
- **Producer stats**: final counts, last stats/cycle lines, raw log tail
- **Prometheus** (18 parallel queries): CPU/memory for Flink, Kafka, producers, Trino; node-level CPU/memory; Flink records in/out per sec, checkpoint duration/size, Kafka consumer lag, Flink busy/idle/backpressured time
- **kubectl top**: pod-level resource usage for Flink, Kafka, producers, Trino; node-level usage

### Granular Prometheus averages

Collected after each sub-phase over the actual measurement window using `avg_over_time`:
- Per-pod CPU, memory, network rx/tx for Kafka brokers, Flink JM/TMs, Trino
- Kafka PVC usage, Flink filesystem usage

### Azure Storage Account metrics

Collected via `az monitor metrics` over the measurement window:
- Transactions (total), Ingress/Egress (GiB)
- E2E latency, server latency (ms), availability (%)
- Used capacity (GiB)

### Flink checkpoint deltas

- Completed, failed, restored checkpoint counts (T1 - T0)
- Failover flag (Y/N based on restored > 0)

## Trino query suite (Q1–Q5)

All timed queries run **3 times**; **median** is reported along with min/max.
Queries run **in parallel** across Trino workers.

| Query | Description | SQL pattern |
|-------|-------------|-------------|
| Q1 | COUNT raw | `SELECT COUNT(*) FROM sensor_data_raw` |
| Q2 | GROUP BY + multi-aggregate on raw | `GROUP BY home_id, plug_id` with AVG, MAX, COUNT |
| Q3 | Window function (rolling avg) on raw | `AVG(voltage) OVER (PARTITION BY home_id ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)` |
| Q4 | Cross-table JOIN raw ↔ hourly | `JOIN sensor_data_hourly` on home_id, plug_id, dt with voltage drift calculation |
| Q5 | Percentile/statistical on raw | `approx_percentile` (p50/p95/p99) + `stddev` |

The **Trino benchmark** additionally runs all 5 queries for **5 iterations** (parallel per iteration) and reports P95 latency.

## Output artifacts

Results are written to `experiments/results/combined-improved-<tag>/`:

| File | Contents |
|------|----------|
| `summary.csv` | One row per sub-phase with key deltas and latencies |
| `matrix-full.csv` | Wide-format CSV matching the experiment matrix columns |
| `orchestrator-transcript.log` | Full PowerShell console transcript for traceability |
| `tier-<N>h-nonbatched.txt` / `tier-<N>h-batched.txt` | Full T1 metrics snapshot |
| `tier-<N>h-nonbatched-t0.txt` / `tier-<N>h-batched-t0.txt` | Full T0 baseline snapshot |
| `tier-<N>h-<type>-producer-stats.json` | Parsed Go producer stats (last-per-pod) |
| `tier-<N>h-<type>-granular-prometheus.json` | Per-pod Prometheus averages |
| `tier-<N>h-<type>-checkpoints.json` | Flink checkpoint T0/T1/delta |
| `tier-<N>h-<type>-azure-storage.json` | Azure Storage Account metrics |
| `tier-<N>h-<type>-trino-benchmark.json` | Trino benchmark P95 results |

The script also updates the experiment matrix markdown file in-place (row matching by Run ID).

## Home ID uniqueness

- Pod 0 of a 7000h experiment: homes 1–3500
- Pod 1 of a 7000h experiment: homes 3501–7000
- Each home ID becomes `iot-sensor-general/<N>` in messages
- Each plug ID becomes `iot-sensor-plug/<M>` (1–4 per home)

**Never run two experiment tiers simultaneously** — their ID ranges overlap.

## Producer log monitoring

Go producers log stats **once per minute** to stdout:

```
[stats] ok=18002 err=0 total=18002 rps=18000
[cycle] homes=2000 avg=586ms max=736ms interval=1.0s saturated=false
```

On shutdown they print a final summary:

```
[final] ok=1080120 err=0 total=1080120
```

### Fields

| Field | Description |
|-------|-------------|
| `ok`  | Messages successfully sent to Kafka |
| `err` | Messages that failed to send |
| `total` | `ok + err` |
| `rps` | Records per second over the last 60s window |

### Checking SLO (generated vs stored)

```powershell
# View live stats from a Go nonbatched pod
kubectl logs -f -l app=go-iot-sim-producer -n data-ingest --tail=5

# View live stats from a Go batched pod
kubectl logs -f -l app=go-iot-sim-producer-batched -n data-ingest --tail=5

# Compare against Kafka topic offsets
kubectl exec -n data-ingest kafka-0 -- `
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell `
  --broker-list localhost:9092 `
  --topic iot-sensor-general --time -1

# Compare against lakehouse row counts (via Trino)
kubectl exec -n analytics deploy/trino-standalone-trino-coordinator -- `
  trino --execute "SELECT COUNT(*) FROM paimon.iot.sensor_data_raw"
```

The SLO check: `sum(ok across all pods) ≈ kafka topic offset delta ≈ lakehouse row count delta`.
