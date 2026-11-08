# Scripts

PowerShell utility scripts for managing and operating the **lakehouse-on-k8s** platform on AKS.

## Prerequisites

- **Azure CLI** (`az`) – authenticated and configured
- **kubectl** – configured to point at the target AKS cluster
- PowerShell 5.1+

---

## `setup-vm.ps1`

Bootstraps a fresh **Windows Server VM** with all tooling required to run the experiment orchestrator and interact with AKS.

### What it installs

- Git
- Azure CLI
- VS Code
- kubectl (latest stable)
- Helm (via winget)

### Usage

```powershell
# Run as Administrator
.\setup-vm.ps1
```

Verifies each installation and refreshes the system `PATH` at the end.

---

## `connect_to_aks.ps1`

Authenticates to Azure using a **Service Principal** and fetches AKS cluster credentials so that `kubectl` is ready to use.

### What it does

1. Prompts for Service Principal credentials (Client ID, Client Secret, Tenant ID).
2. Reads the target subscription from `$env:AZURE_SUBSCRIPTION_ID` (or prompts for it).
3. Runs `az login --service-principal` and sets the subscription.
4. Fetches AKS credentials (`az aks get-credentials`) for the cluster (`aks-lakehouse-on-k8s-01`).
5. Verifies connectivity by listing pods and services across namespaces.
6. Retrieves the Grafana admin password from the `monitoring` namespace.

### Usage

```powershell
.\connect_to_aks.ps1
```

> Run this script first before using any other script that interacts with the cluster.

---

## `restart-flink-env.ps1`

Performs a **full reset** of the Flink data-processing environment so that the streaming pipeline can be re-bootstrapped from scratch.

### What it does

1. Deletes all Paimon blobs from the Azure Storage `warehouse` container.
2. Applies the environment-specific Flink ConfigMap.
3. Applies the Flink PodMonitor for Prometheus scraping.
4. Restarts Flink TaskManager and JobManager deployments.
5. Deletes the `paimon-bootstrap` Job and `flink-submit-kafka-to-paimon` Job.
6. Deletes the `hourly-aggregation` and `tenmin-aggregation` CronJobs.
7. Removes the IoT simulator Deployment.

The script prints next-step instructions for re-deploying producers and the
data-processing manifest afterwards.

### Usage

```powershell
.\restart-flink-env.ps1
```

---

## `run-experiment-combined-improved.ps1`

Primary **end-to-end experiment orchestrator** for the lakehouse-on-k8s scalability study. This is the latest and most complete version of the experiment runner.

For each tier (number of simulated homes) the script runs two sub-phases — **nonbatched** and **batched** Go producers — each with its own full environment reset, data collection, and metrics recording cycle.

### Per-tier lifecycle

**Sub-phase A — Nonbatched producers** (Sub-phase B repeats with batched producers):

| Step | Action |
|------|--------|
| 1 | Full environment reset (producers, Flink jobs, Kafka topics, Paimon blobs, Flink cluster) |
| 2 | Re-bootstrap: Paimon catalog/tables, Flink streaming ingest job, aggregation CronJobs (up to 3 retry attempts) |
| 3 | Generate & deploy Go producer pods |
| 4 | Warm-up period (default 5 min) |
| 5 | Collect T0 baseline + Flink checkpoint T0 (Flink verified RUNNING) |
| 6 | Steady-state run (default 30 min) with periodic Flink health checks |
| 7 | Parse producer stats **while pods are still alive** (last `[stats]`/`[cycle]` per pod, summed) |
| 8 | Collect T1 metrics snapshot |
| 9 | Stop producers + flush cooldown (default 90s for Flink to commit pending data) |
| 10 | Trigger manual hourly + 10-min aggregation CronJobs and wait for completion |
| 11 | Record Flink checkpoint T1 |
| 12 | Collect granular Prometheus averages over the measurement window |
| 13 | Collect Azure Storage Account metrics |
| 14 | Run Trino benchmark (5 queries × 5 iterations, parallel per iteration) |
| 15 | Persist all phase artifacts (JSON files) and update experiment matrix row immediately |

### Pre-run initialisation

Before the per-tier loop the script:
- Resolves the **storage account key** from `$env:AZURE_STORAGE_ACCOUNT_KEY` or the `azure-storage-secret` Kubernetes Secret (required for Azure blob operations on Windows)
- Runs **pre-run validation** (Flink REST, Kafka topics, Trino catalog, Azure Storage reachability)
- Starts a **PowerShell transcript** (`orchestrator-transcript.log`) in the results directory for forensic traceability

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

### Usage

```powershell
# Run all standard tiers
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500, 1500, 3500, 5500, 7500, 10000, 12500, 15000, 17500)

# Run a subset of tiers
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500, 1500)

# Smoke test with reduced timing
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50) -WarmupMinutes 2 -RunMinutes 10

# Stop AKS after completion to save cost
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -ClusterShutdown

# Resume from a specific tier without resetting the environment
.\scripts\run-experiment-combined-improved.ps1 -Tiers @(3500) -SkipCleanup -ExperimentTag "20260329_120000"
```

### Tier scaling reference

Each home produces 1 general record + 4 plug records per second = **5 records/s/home**.

| Run ID | Homes | Records/sec | Go pods |
|--------|-------|-------------|---------|
| N1/B1 | 50 | **250** | 1 |
| N2/B2 | 500 | **2,500** | 1 |
| N3/B3 | 1,500 | **7,500** | 1 |
| N4/B4 | 3,500 | **17,500** | 1 |
| N5/B5 | 5,500 | **27,500** | 2 |
| N6/B6 | 7,500 | **37,500** | 3 |
| N7/B7 | 10,000 | **50,000** | 3 |
| N8/B8 | 12,500 | **62,500** | 4 |
| N9/B9 | 15,000 | **75,000** | 5 |
| N10/B10 | 17,500 | **87,500** | 5 |

Run IDs prefixed with **N** = nonbatched, **B** = batched. Homes/pod = 3500 (calibrated).

### Output

Results are written to `experiments/results/combined-improved-<tag>/`. See [`experiments/README.md`](../experiments/README.md) for the full artifact list.

---

## `run-experiment-combined-improved-smoke.ps1`

Smoke-test variant of the experiment runner. Runs a single small tier with reduced warm-up and run times to validate the end-to-end orchestration pipeline without committing to a full experiment run.

---

## `validate-experiment-combined-improved-results.ps1`

Post-run validation script that checks whether all expected artifacts were produced and are structurally correct for a completed experiment.

### What it checks

- Required result files per phase (nonbatched + batched)
- JSON artifact structure and presence of key metric fields
- `matrix-full.csv` row and column completeness for the given tier Run IDs
- Optional: matrix markdown row width consistency

### Usage

```powershell
.\validate-experiment-combined-improved-results.ps1 `
  -ResultsDir experiments\results\combined-improved-<tag> `
  -Tier 50
```

Exit code 0 = passed, 1 = failed.
