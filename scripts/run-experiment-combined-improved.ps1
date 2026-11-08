<#
.SYNOPSIS
  End-to-end experiment runner for the lakehouse-on-k8s scalability study
  running Go nonbatched and batched producers SERIALIZED (all nonbatched tiers
  first, then all batched tiers).

  IMPROVED VERSION — Enhanced Trino query suite, Flink health gates,
  flush cooldown, in-run tenmin + post-flush 30-min aggregation triggers,
  Kafka consumer lag, Flink backpressure metrics, and end-to-end latency
  measurement.

.DESCRIPTION
  For each tier (number of homes) this script runs BOTH producer types,
  each with its own full environment reset cycle:

  SUB-PHASE A (NONBATCHED) — for a given tier:
    1. Full environment reset (clean producers, Flink, Kafka topics, Paimon blobs).
    2. Re-bootstrap Flink catalog/tables, resubmit streaming ingest + CronJobs.
    3. Deploy Go NONBATCHED producers, warm up, verify Flink health gate.
    4. Record T0 baseline + checkpoint T0 (Flink RUNNING).
    5. 30-min steady-state run.
    6. Parse producer stats WHILE PODS ARE ALIVE (logs, RPS, cycle, saturation).
    7. Collect T1 metrics snapshot (producers still running).
    8. Stop producers, flush cooldown, trigger manual aggregation.
    9. Record checkpoint T1 (Flink STILL RUNNING).
   10. Collect granular Prometheus, Azure SA metrics, Trino benchmark.
   11. Update nonbatched (N) matrix row immediately with fresh data.

  SUB-PHASE B (BATCHED) — same tier, identical reset + collect cycle:
    Repeats steps 1-11 using Go BATCHED producers, updates batched (B) row.

  This architecture ensures:
  - Each sub-phase starts from a clean slate (Kafka, Paimon, Flink all fresh)
  - Flink streaming job is RUNNING when checkpoints are read (T0 and T1)
  - Producer logs/stats/saturation captured while pods are still alive
  - Prometheus avg_over_time window covers the actual measurement period
  - Matrix rows updated immediately after each sub-phase with fresh data

  The script targets the AKS cluster and registry configured in the constants below.

  IMPROVEMENTS OVER run-experiment-combined.ps1:
  - Per-tier combined loop (not two separate phase loops)
  - Trino Q2 replaced: GROUP BY + multi-aggregate on raw table (scales with volume)
  - Trino Q3 replaced: Window function (rolling avg) on raw table (heavy, scales)
  - Trino Q4 added: Cross-table JOIN raw ↔ 10min (analytical pattern)
  - Trino Q5 added: Percentile/statistical query on raw (approx_percentile, stddev)
  - Flink health gate: Verifies streaming job is RUNNING before proceeding
  - Flush cooldown: 90s wait after producers stop for Flink to commit pending data
  - Aggregation strategy: hourly auto-CronJob is dead (suspended in YAML, not
    triggered by orchestrator). Tenmin fires three times per sub-phase at
    run-elapsed +10/+20/+30 min to exercise streaming + batch coexistence on
    Flink slots; a per-call slot guard skips a firing if a previous tenmin batch
    is still RUNNING. After flush cooldown a single 30-minute aggregation
    (sensor_data_30min) rolls up the full window into one row per (home, plug)
    and feeds the per-tier energy / peak power / voltage metrics.
  - Kafka consumer lag metric collected via Prometheus
  - Flink backpressure metrics (busy/idle/backpressured time)
  - End-to-end latency (newest raw record timestamp vs wall clock)
  - Trino warm-up query before timed suite to avoid cold-start penalty
  - Pre-run validation (Flink REST, Kafka topics, Trino catalog)
  - Inter-query parallelism: Q1-Q5 timed suite, health checks, Prometheus
    queries, and benchmark iterations all run concurrently via Start-Job
    to utilise all 3 Trino workers and reduce experiment wall-clock time
  - All timed queries run 3 times; median reported plus min/max

.PARAMETER Tiers
  Array of home counts, one experiment iteration per tier per phase.
  Example: @(50, 500, 2000) → runs 3 nonbatched tiers then 3 batched tiers.

.PARAMETER WarmupMinutes
  Minutes to wait after deploying producers before taking T0 baseline (default: 5).

.PARAMETER RunMinutes
  Minutes of steady-state data collection after warm-up (default: 30).

.PARAMETER FlushCooldownSeconds
  Seconds to wait after stopping producers for Flink to flush pending data (default: 90).

.PARAMETER ExperimentTag
  A short label appended to the results folder (default: current UTC timestamp).

.PARAMETER RunPhase
  Which sub-phase(s) to execute per tier. Accepted values:
    both        — run sub-phase A (nonbatched) and sub-phase B (batched) [default]
    nonbatched  — run sub-phase A only (skip batched producers)
    batched     — run sub-phase B only (skip nonbatched producers)

.PARAMETER SkipCleanup
  If set, skip the full environment reset between tiers (useful for re-runs).

.PARAMETER ClusterShutdown
  If set, stop the AKS cluster after all tiers complete to save costs.

.EXAMPLE
  .\run-experiment-combined-improved.ps1 -Tiers @(50, 500, 2000, 6000, 10000)
  .\run-experiment-combined-improved.ps1 -Tiers @(1000) -WarmupMinutes 3 -RunMinutes 15
  .\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -ClusterShutdown
  .\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -RunPhase nonbatched
  .\run-experiment-combined-improved.ps1 -Tiers @(50, 500) -RunPhase batched
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [int[]]$Tiers,

    [int]$WarmupMinutes = 5,

    [int]$RunMinutes = 30,

    [int]$FlushCooldownSeconds = 90,

    [string]$ExperimentTag = (Get-Date -Format "yyyyMMdd_HHmmss"),

    [string]$ExperimentMatrixPath = "",

    [ValidateSet("both", "nonbatched", "batched")]
    [string]$RunPhase = "both",

    [switch]$SkipCleanup,

    [switch]$ClusterShutdown
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ─────────────────────────────────────────────────────────────
# Constants / Paths
# ─────────────────────────────────────────────────────────────
$RepoRoot            = Split-Path -Parent $PSScriptRoot
$ExperimentsDir      = Join-Path $RepoRoot "experiments"
$DataProcessingYaml  = Join-Path $RepoRoot "k8s\manifests\data-processing.yaml"
$ResultsDir          = Join-Path $ExperimentsDir "results\combined-improved-$ExperimentTag"
$GenerateGoScript         = Join-Path $ExperimentsDir "generate-go-experiment.ps1"
$GenerateGoBatchedScript  = Join-Path $ExperimentsDir "generate-go-batched-experiment.ps1"

# Environment-specific constants
$StorageAccountName  = "stlakehouseonk8s01"
$AcrRegistry  = "acrlakehouseonk8s01.azurecr.io"
$FlinkImage          = "acrlakehouseonk8s01.azurecr.io/flink-paimon-kafka:latest"

# Kafka
$KafkaPod            = "kafka-0"
$KafkaNamespace      = "data-ingest"
$KafkaBrokerInternal = "localhost:9092"
$TopicGeneral        = "iot-sensor-general"
$TopicPlug           = "iot-sensor-plug"

# Flink REST
$FlinkJobManagerSvc  = "flink-jobmanager.data-processing.svc.cluster.local"
$FlinkRestPort       = 8081

# Trino
$TrinoNamespace      = "analytics"
$TrinoPod            = ""

# Prometheus
$PrometheusNamespace = "monitoring"

# Plugs per home (matches generator)
$PlugsPerHome        = 4

# Homes per Go pod (matches generators)
$HomesPerPod         = 3500

# Experiment matrix markdown - homes -> Run ID mapping
$DefaultMatrixMd     = Join-Path $RepoRoot "experiments\results\experiment-matrix-lakehouse-on-k8s-combined-improved.md"
$ExperimentMatrixMd  = $DefaultMatrixMd
$TierRunIdIndex = @{
    50    = 1
    500   = 2
    1500  = 3
    3500  = 4
    5500  = 5
    7500  = 6
    10000 = 7
    12500 = 8
    15000 = 9
    17500 = 10
}

# Full matrix CSV headers (same column order as markdown matrix rows)
$MatrixWideCsvHeaders = @(
    "run_id",
    "homes",
    "go_pods",
    "homes_per_go_pod",
    "expected_rps",
    "observed_rps",
    "avg_cycle_s",
    "max_cycle_s",
    "saturated",
    "ok_delta",
    "errors_delta",
    "error_rate",
    "kafka_0_cpu_cores",
    "kafka_0_mem_gib",
    "kafka_0_net_rx_mib_s",
    "kafka_0_net_tx_mib_s",
    "kafka_0_pvc_gib",
    "kafka_1_cpu_cores",
    "kafka_1_mem_gib",
    "kafka_1_net_rx_mib_s",
    "kafka_1_net_tx_mib_s",
    "kafka_1_pvc_gib",
    "flink_jm_cpu_cores",
    "flink_jm_mem_gib",
    "flink_jm_net_rx_mib_s",
    "flink_jm_net_tx_mib_s",
    "flink_jm_fs_gib",
    "flink_tm_1_cpu_cores",
    "flink_tm_1_mem_gib",
    "flink_tm_1_net_rx_mib_s",
    "flink_tm_1_net_tx_mib_s",
    "flink_tm_1_fs_gib",
    "flink_tm_2_cpu_cores",
    "flink_tm_2_mem_gib",
    "flink_tm_2_net_rx_mib_s",
    "flink_tm_2_net_tx_mib_s",
    "flink_tm_2_fs_gib",
    "ckpt_completed_delta",
    "ckpt_failed_delta",
    "ckpt_restored_delta",
    "failover",
    "azure_sa_tx_sum",
    "azure_sa_ingress_gib",
    "azure_sa_egress_gib",
    "azure_sa_e2e_latency_ms",
    "azure_sa_server_latency_ms",
    "azure_sa_availability_pct",
    "azure_sa_capacity_gib",
    "trino_q1_p95_s",
    "trino_q2_p95_s",
    "trino_q3_p95_s",
    "trino_q4_p95_s",
    "trino_q5_p95_s",
    "trino_failures",
    "notes"
)

function Get-RunId([int]$Homes, [string]$Phase) {
    $idx = $TierRunIdIndex[$Homes]
    if ($Phase -eq "nonbatched") { return "N$idx" }
    else { return "B$idx" }
}

# Azure Storage Account resource ID (for az monitor metrics)
$StorageAccountRG    = "rg-lakehouse-on-k8s-01"

# Storage account key — populated at startup so all az storage commands can
# authenticate via --account-key. Avoids the "no credentials" warnings that az
# prints to stdout when no auth is supplied, and the cmd.exe argument-parsing
# glitches that surface as "--output was unexpected at this time".
$StorageAccountKey   = ""

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
function Initialize-StorageAccountKey {
    # 1. Env var — fastest, takes precedence
    if ($env:AZURE_STORAGE_ACCOUNT_KEY) {
        $script:StorageAccountKey = $env:AZURE_STORAGE_ACCOUNT_KEY
        return
    }
    # 2. K8s secret — works when ARM access is unavailable
    try {
        $b64 = kubectl get secret azure-storage-secret -n data-processing `
            -o jsonpath='{.data.storage-account-key}' 2>$null
        if ($b64) {
            $script:StorageAccountKey = [System.Text.Encoding]::UTF8.GetString(
                [Convert]::FromBase64String(($b64 | Out-String).Trim()))
            if ($script:StorageAccountKey) { return }
        }
    } catch { }
    # 3. ARM fallback — works when logged into az with rights on the account
    try {
        $azKey = az storage account keys list `
            --resource-group $StorageAccountRG `
            --account-name $StorageAccountName `
            --query "[0].value" -o tsv 2>$null
        if ($LASTEXITCODE -eq 0 -and $azKey) {
            $script:StorageAccountKey = ($azKey | Out-String).Trim()
            if ($script:StorageAccountKey) { return }
        }
    } catch { }
    # All sources exhausted — fail fast. Continuing with an empty key produces
    # a misleading "ContainerBeingDeleted" loop because every `az storage` call
    # fails with "argument --account-key: expected one argument", and 2>$null
    # hides the real cause.
    throw "FATAL: Storage account key not found. Set `$env:AZURE_STORAGE_ACCOUNT_KEY, create k8s secret azure-storage-secret/storage-account-key in namespace data-processing, or `az login` with rights on storage account '$StorageAccountName' (rg '$StorageAccountRG')."
}

function Confirm-StorageContainerExists([string]$containerName, [int]$maxCycles = 6, [int]$verifyTimeoutSec = 60) {
    # Robust create+verify: `az storage container create` can return exit 0 without
    # actually creating the container (e.g., when the account is mid-delete-finalize
    # or under transient throttling). A separate verify-only loop will then spin
    # forever. Unified cycle: try create, poll exists, and if verify never confirms
    # within $verifyTimeoutSec, recreate. Idempotent — safe if the container is
    # already present.
    $cycle = 0
    $lastErr = ""
    while ($cycle -lt $maxCycles) {
        $cycle++
        $createOut = az storage container create `
            --account-name $StorageAccountName `
            --account-key $StorageAccountKey `
            --name $containerName 2>&1
        if ($LASTEXITCODE -ne 0) {
            $lastErr = "create exit=${LASTEXITCODE}: $createOut"
            Write-SubStep "  ${containerName} create cycle $cycle/$maxCycles failed; retrying in 10s..."
            Start-Sleep -Seconds 10
            continue
        }
        $verifyWaited = 0
        $verifyExists = ""
        while ($verifyWaited -lt $verifyTimeoutSec) {
            $verifyExists = az storage container exists `
                --account-name $StorageAccountName `
                --account-key $StorageAccountKey `
                --name $containerName `
                --query exists --output tsv 2>$null
            if ($verifyExists -eq "true") {
                Write-SubStep "  ${containerName} container ready (cycle $cycle, verify after ${verifyWaited}s)."
                return
            }
            Start-Sleep -Seconds 5
            $verifyWaited += 5
        }
        $lastErr = "create returned exit 0 but exists stayed 'false' for ${verifyTimeoutSec}s (Azure silent no-op)"
        Write-SubStep "  ${containerName} cycle ${cycle}/${maxCycles}: create succeeded but verify never saw it; recreating in 10s..."
        Start-Sleep -Seconds 10
    }
    throw "FATAL: Could not ensure container '${containerName}' exists after $maxCycles cycles. Last error: $lastErr"
}

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host (">>> " + $msg) -ForegroundColor Cyan
}

function Write-SubStep([string]$msg) {
    Write-Host ("    " + $msg) -ForegroundColor Gray
}

function Get-Timestamp {
    return (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
}

function Upsert-MatrixWideCsvRow([object[]]$rowValues) {
    $csvPath = Join-Path $ResultsDir "matrix-full.csv"

    if ($rowValues.Count -ne $MatrixWideCsvHeaders.Count) {
        Write-SubStep "  WARNING: matrix-full.csv row width mismatch ($($rowValues.Count) vs $($MatrixWideCsvHeaders.Count)) — skipping CSV upsert."
        return
    }

    $rowMap = [ordered]@{}
    for ($i = 0; $i -lt $MatrixWideCsvHeaders.Count; $i++) {
        $rowMap[$MatrixWideCsvHeaders[$i]] = [string]$rowValues[$i]
    }

    $runId = $rowMap["run_id"]
    $existing = @()
    if (Test-Path $csvPath) {
        $existing = @(Import-Csv -Path $csvPath)
    }

    $merged = @($existing | Where-Object { $_.run_id -ne $runId })
    $merged += [pscustomobject]$rowMap
    $merged | Export-Csv -Path $csvPath -NoTypeInformation -Encoding utf8
}

function Resolve-ExperimentMatrixPath([string]$explicitPath, [string]$fallbackPath) {
    if ($explicitPath -and (Test-Path $explicitPath)) {
        return (Resolve-Path $explicitPath).Path
    }

    $resultsRoot = Split-Path -Parent $fallbackPath
    $todayStamp = Get-Date -Format "ddMMyyyy"
    $todayCandidate = Join-Path $resultsRoot "experiment-matrix-lakehouse-on-k8s-combined-improved-${todayStamp}.md"
    if (Test-Path $todayCandidate) {
        return (Resolve-Path $todayCandidate).Path
    }

    $datedCandidates = @(Get-ChildItem -Path $resultsRoot -File -Filter "experiment-matrix-lakehouse-on-k8s-combined-improved-*.md" -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notmatch 'smoke' } |
        Sort-Object LastWriteTime -Descending)
    if ($datedCandidates.Count -gt 0) {
        return $datedCandidates[0].FullName
    }

    if (Test-Path $fallbackPath) {
        return (Resolve-Path $fallbackPath).Path
    }

    return $fallbackPath
}

function Write-PhaseArtifacts(
    [int]$Homes,
    [string]$Phase,
    [string]$ProducerType,
    [hashtable]$GoStats,
    [hashtable]$GranularProm,
    [hashtable]$CkptT0,
    [hashtable]$CkptT1,
    [hashtable]$AzSA,
    [hashtable]$TrinoBench,
    [hashtable]$RestartDelta = @{}
) {
    $prefix = "tier-${Homes}h-${ProducerType}"

    $goStatsPath = Join-Path $ResultsDir "${prefix}-producer-stats.json"
    $GoStats | ConvertTo-Json -Depth 8 | Out-File -FilePath $goStatsPath -Encoding utf8

    $promPath = Join-Path $ResultsDir "${prefix}-granular-prometheus.json"
    $GranularProm | ConvertTo-Json -Depth 10 | Out-File -FilePath $promPath -Encoding utf8

    $ckptPath = Join-Path $ResultsDir "${prefix}-checkpoints.json"
    $ckptDelta = @{
        completed = ($CkptT1.completed - $CkptT0.completed)
        failed = ($CkptT1.failed - $CkptT0.failed)
        restored = ($CkptT1.restored - $CkptT0.restored)
    }
    # Guard: if delta is negative, the job restarted and the counter reset
    if ($ckptDelta.completed -lt 0) {
        Write-Warning "Checkpoint completed delta is negative ($($ckptDelta.completed)) — job likely restarted; reporting T1 absolute values instead"
        $ckptDelta.completed = $CkptT1.completed
        $ckptDelta.note = "job_restarted_negative_delta_replaced_with_t1_absolute"
    }
    @{
        phase = $Phase
        t0 = $CkptT0
        t1 = $CkptT1
        delta = $ckptDelta
        job_id_changed = [bool]$CkptT1.job_id_changed
    } | ConvertTo-Json -Depth 8 | Out-File -FilePath $ckptPath -Encoding utf8

    $azurePath = Join-Path $ResultsDir "${prefix}-azure-storage.json"
    $AzSA | ConvertTo-Json -Depth 8 | Out-File -FilePath $azurePath -Encoding utf8

    $trinoPath = Join-Path $ResultsDir "${prefix}-trino-benchmark.json"
    $TrinoBench | ConvertTo-Json -Depth 8 | Out-File -FilePath $trinoPath -Encoding utf8

    $restartPath = Join-Path $ResultsDir "${prefix}-pod-restarts.json"
    $restartedPods = @($RestartDelta.Keys | Where-Object { $RestartDelta[$_] -gt 0 })
    @{
        any_restarts    = ($restartedPods.Count -gt 0)
        restarted_count = $restartedPods.Count
        delta           = $RestartDelta
    } | ConvertTo-Json -Depth 8 | Out-File -FilePath $restartPath -Encoding utf8
}

function Get-PodRestartCounts {
    $namespaces = @("data-processing", "data-ingest", "analytics")
    $result = [ordered]@{}
    foreach ($ns in $namespaces) {
        try {
            $jsonRaw = kubectl get pods -n $ns -o json 2>$null
            if (-not $jsonRaw) { continue }
            $podList = ($jsonRaw | Out-String) | ConvertFrom-Json -ErrorAction SilentlyContinue
            if (-not $podList -or -not $podList.items) { continue }
            foreach ($pod in $podList.items) {
                $name = $pod.metadata.name
                $statuses = @($pod.status.containerStatuses)
                if ($statuses.Count -eq 0) { continue }
                $sum = ($statuses | ForEach-Object { [int]$_.restartCount } |
                        Measure-Object -Sum).Sum
                $result["${ns}/${name}"] = [int]$sum
            }
        } catch { }
    }
    return $result
}

function Wait-ForPodReady([string]$label, [string]$namespace, [int]$timeoutSec = 300) {
    Write-SubStep "Waiting for pods with label '$label' in ns '$namespace' to be ready (timeout ${timeoutSec}s)..."
    kubectl wait --for=condition=ready pod -l $label -n $namespace --timeout="${timeoutSec}s" 2>$null | Out-Null
}

function Get-KafkaTopicOffsets([string]$topic) {
    $raw = kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell `
        --broker-list $KafkaBrokerInternal `
        --topic $topic --time -1 2>&1

    $total = 0
    foreach ($line in $raw -split "`n") {
        $parts = $line -split ":"
        if ($parts.Count -ge 3) {
            $offset = 0
            if ([int64]::TryParse($parts[2].Trim(), [ref]$offset)) {
                $total += $offset
            }
        }
    }
    return $total
}

function Get-KafkaTopicOffsetsRaw([string]$topic) {
    $raw = kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell `
        --broker-list $KafkaBrokerInternal `
        --topic $topic --time -1 2>&1
    return ($raw -join "`n")
}

$script:TrinoPodName = $null

function Invoke-TrinoQuery([string]$sql, [string]$label = "") {
    if (-not $script:TrinoPodName) {
        $script:TrinoPodName = "deploy/trino-standalone-trino-coordinator"
    }
    $displayLabel = if ($label) { $label } else { ($sql -replace '\s+', ' ').Substring(0, [math]::Min(70, $sql.Length)) }
    Write-Host ("        [trino] $displayLabel ...") -NoNewline -ForegroundColor DarkGray
    $qSw = [System.Diagnostics.Stopwatch]::StartNew()
    $raw = kubectl exec -n $TrinoNamespace $script:TrinoPodName -- trino --execute "$sql" 2>&1
    $exitCode = $LASTEXITCODE
    $qSw.Stop()
    if ($exitCode -eq 137) {
        Write-Host (" [OOM-KILLED exit=137]") -ForegroundColor Red
        return "OOM_EXIT_137"
    }
    if ($exitCode -ne 0 -and -not ($raw -join "").Trim()) {
        Write-Host (" [kubectl exit=$exitCode, no output - possible pod issue]") -ForegroundColor Yellow
    } else {
        Write-Host (" done ($([int]$qSw.Elapsed.TotalSeconds)s)") -ForegroundColor DarkGray
    }
    $cleaned = @($raw | Where-Object {
        $_ -notmatch 'org\.jline' -and
        $_ -notmatch '^WARNING:' -and
        $_ -notmatch '^\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2}'
    })
    $result = ($cleaned -join "`n").Trim().Trim('"')
    if ($result -match "exit code 137|terminated|container not found") {
        Write-Warning "Trino query result indicates OOM or pod failure: $($result.Substring(0, [math]::Min(120, $result.Length)))"
        return "OOM_EXIT_137"
    }
    return $result
}

function Get-FlinkJobsJson {
    $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
        curl -s "http://localhost:$FlinkRestPort/jobs" 2>&1
    return ($raw -join "")
}

# /jobs/overview is the only Flink REST endpoint that returns job NAMES
# (along with .jid and .state). /jobs returns only id+status. Anywhere we
# need to filter by job name (sensor_data_10min, sensor_data_30min, etc.)
# we MUST go through this helper, not Get-FlinkJobsJson.
function Get-FlinkJobsOverviewJson {
    $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
        curl -s "http://localhost:$FlinkRestPort/jobs/overview" 2>&1
    return ($raw -join "")
}

function Get-FlinkJobMetricsJson([string]$jobId, [string]$metricsCsv) {
    $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
        curl -s "http://localhost:${FlinkRestPort}/jobs/${jobId}/metrics?get=${metricsCsv}" 2>&1
    return ($raw -join "")
}

function Query-Prometheus([string]$query) {
    $encoded = [Uri]::EscapeDataString($query)
    $raw = kubectl exec -n $PrometheusNamespace deployment/kube-prometheus-stack-prometheus -- `
        wget -qO- "http://localhost:9090/api/v1/query?query=${encoded}" 2>&1
    if (-not $raw -or $raw -like "*error*") {
        $promPod = (kubectl get pods -n $PrometheusNamespace -l "app.kubernetes.io/name=prometheus" -o jsonpath='{.items[0].metadata.name}' 2>&1).Trim()
        $raw = kubectl exec -n $PrometheusNamespace $promPod -- wget -qO- "http://localhost:9090/api/v1/query?query=${encoded}" 2>&1
    }
    return ($raw -join "`n")
}

function Get-ProducerFinalStats([string]$appLabel) {
    $raw = kubectl logs -l "app=${appLabel}" -n $KafkaNamespace --tail=50 2>&1
    $finals  = ($raw | Select-String "\[final\]") -join "`n"
    $lastStats = ($raw | Select-String "\[stats\]" | Select-Object -Last 5) -join "`n"
    $lastCycle = ($raw | Select-String "\[cycle\]" | Select-Object -Last 5) -join "`n"
    return @{
        Final = $finals
        LastStats = $lastStats
        LastCycle = $lastCycle
        RawTail = ($raw -join "`n")
    }
}

function Get-PodResourceUsage([string]$namespace, [string]$labelSelector) {
    $raw = kubectl top pods -n $namespace -l $labelSelector 2>&1
    return ($raw -join "`n")
}

function Get-NodeResourceUsage {
    $raw = kubectl top nodes 2>&1
    return ($raw -join "`n")
}

# ─────────────────────────────────────────────────────────────
# Wait for Flink REST API + TaskManager slots to be available
# ─────────────────────────────────────────────────────────────
function Wait-FlinkClusterReady([int]$timeoutSec = 180) {
    Write-SubStep "Waiting for Flink REST API and TaskManager slots (timeout ${timeoutSec}s)..."
    $elapsed = 0
    while ($elapsed -lt $timeoutSec) {
        try {
            $overviewRaw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                curl -s "http://localhost:${FlinkRestPort}/overview" 2>&1
            $overview = ($overviewRaw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
            if ($overview -and $overview.PSObject.Properties['taskmanagers'] -and $overview.PSObject.Properties['slots-available']) {
                $tmCount = [int]$overview.taskmanagers
                $slotsAvail = [int]$overview.'slots-available'
                $slotsTotal = [int]$overview.'slots-total'
                if ($tmCount -gt 0 -and $slotsAvail -gt 0) {
                    Write-SubStep "  Flink cluster ready: $tmCount TM(s), $slotsAvail/$slotsTotal slots available"
                    return $true
                }
                Write-SubStep "  Flink not ready yet: TMs=$tmCount, slots=$slotsAvail/$slotsTotal"
            }
        } catch { }
        Start-Sleep -Seconds 10
        $elapsed += 10
    }
    Write-Warning "Flink cluster not fully ready after ${timeoutSec}s — proceeding anyway"
    return $false
}

# ─────────────────────────────────────────────────────────────
# NEW: Pre-run validation
# ─────────────────────────────────────────────────────────────
function Test-PreRunHealth {
    Write-Step "Pre-run validation..."
    $allOk = $true

    # Check Flink REST
    Write-SubStep "Checking Flink REST API..."
    try {
        $flinkResp = kubectl exec -n data-processing deployment/flink-jobmanager -- `
            curl -s -o /dev/null -w "%{http_code}" "http://localhost:${FlinkRestPort}/overview" 2>&1
        if (($flinkResp -join "").Trim() -ne "200") {
            Write-Warning "Flink REST API not reachable (HTTP $($flinkResp -join ''))"
            $allOk = $false
        } else { Write-SubStep "  Flink REST OK" }
    } catch {
        Write-Warning "Flink REST API check failed: $($_.Exception.Message)"
        $allOk = $false
    }

    # Check Kafka topics
    Write-SubStep "Checking Kafka topics..."
    $topics = kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server $KafkaBrokerInternal --list 2>&1
    $topicList = ($topics -join "`n")
    if ($topicList -notmatch $TopicGeneral) {
        Write-Warning "Kafka topic '$TopicGeneral' not found"
        $allOk = $false
    }
    if ($topicList -notmatch $TopicPlug) {
        Write-Warning "Kafka topic '$TopicPlug' not found"
        $allOk = $false
    }
    if ($allOk -or $topicList -match $TopicGeneral) { Write-SubStep "  Kafka topics OK" }

    # Check Trino
    Write-SubStep "Checking Trino catalog..."
    try {
        $trinoCheck = Invoke-TrinoQuery "SELECT 1" "health-check"
        if ($trinoCheck.Trim() -ne "1") {
            Write-Warning "Trino health check returned unexpected value: $trinoCheck"
            $allOk = $false
        } else { Write-SubStep "  Trino OK" }
    } catch {
        Write-Warning "Trino health check failed: $($_.Exception.Message)"
        $allOk = $false
    }

    # Check Azure Storage
    Write-SubStep "Checking Azure Storage..."
    try {
        $azCheck = az storage container exists `
            --account-name $StorageAccountName `
            --account-key $StorageAccountKey `
            --name warehouse `
            --output tsv 2>$null
        if ($azCheck -match "True") {
            Write-SubStep "  Azure Storage OK"
        } else {
            Write-Warning "Azure Storage warehouse container not accessible"
            $allOk = $false
        }
    } catch {
        Write-Warning "Azure Storage check failed: $($_.Exception.Message)"
        $allOk = $false
    }

    if (-not $allOk) {
        Write-Host "  WARNING: Some pre-run checks failed. Continuing anyway..." -ForegroundColor Yellow
    } else {
        Write-SubStep "All pre-run checks passed."
    }
    return $allOk
}

# ─────────────────────────────────────────────────────────────
# Flink health gate — verify streaming job is RUNNING and stays RUNNING
# ─────────────────────────────────────────────────────────────
function Test-FlinkStreamingJobRunning([int]$timeoutSec = 120, [int]$stabilityChecks = 3, [int]$stabilityIntervalSec = 10) {
    Write-SubStep "Flink health gate: verifying streaming ingest job is RUNNING (stability: $stabilityChecks consecutive checks)..."
    $elapsed = 0
    $consecutiveRunning = 0
    $lastRunningJobId = $null
    $consecutiveDeadChecks = 0

    while ($elapsed -lt $timeoutSec) {
        try {
            $jobsRaw = Get-FlinkJobsJson
            $jobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
            if ($jobs -and $jobs.jobs) {
                $running = @($jobs.jobs | Where-Object { $_.status -eq "RUNNING" })
                if ($running.Count -gt 0) {
                    $consecutiveDeadChecks = 0
                    $jobId = $running[0].id
                    if ($jobId -eq $lastRunningJobId) {
                        $consecutiveRunning++
                    } else {
                        $consecutiveRunning = 1
                        $lastRunningJobId = $jobId
                    }
                    if ($consecutiveRunning -ge $stabilityChecks) {
                        Write-SubStep "  Flink streaming job is RUNNING and stable (job ID: $jobId, verified $consecutiveRunning times)"
                        return $true
                    }
                    Write-SubStep "  Flink job RUNNING (check $consecutiveRunning/$stabilityChecks)..."
                } else {
                    $consecutiveRunning = 0
                    $lastRunningJobId = $null
                    $pending = @($jobs.jobs | Where-Object { $_.status -in @("CREATED","INITIALIZING","RECONCILING","RESTARTING") })
                    if ($pending.Count -gt 0) {
                        $consecutiveDeadChecks = 0
                        Write-SubStep "  Flink job starting: $(($pending | ForEach-Object { "$($_.id):$($_.status)" }) -join ', ')"
                    } else {
                        $consecutiveDeadChecks++
                        $statuses = ($jobs.jobs | ForEach-Object { "$($_.id):$($_.status)" }) -join ', '
                        Write-SubStep "  WARNING: No RUNNING/starting Flink jobs (check $consecutiveDeadChecks/3). Statuses: $statuses"
                        if ($consecutiveDeadChecks -ge 3) {
                            Write-Warning "FAST-FAIL: Flink job is dead (only FAILED/CANCELED) — not waiting full timeout"
                            return $false
                        }
                    }
                }
            }
        } catch { }
        Start-Sleep -Seconds $stabilityIntervalSec
        $elapsed += $stabilityIntervalSec
    }
    Write-Warning "ABORT: Flink streaming job not stably RUNNING after ${timeoutSec}s"
    return $false
}

# ─────────────────────────────────────────────────────────────
# In-run tenmin aggregation trigger.
# Fired at run-elapsed minutes 10, 20, 30 of every sub-phase to exercise the
# streaming-plus-batch coexistence pattern of the lakehouse. Each firing
# creates a fresh K8s Job from the suspended `tenmin-aggregation` CronJob
# template; the underlying Flink batch job runs asynchronously and only
# blocks if a previous tenmin batch is still RUNNING (slot guard).
#
# Returns $true if the trigger fired, $false if it was skipped (previous
# tenmin still running). Non-blocking — does NOT wait for FINISH so it
# doesn't steal time from the 30-min run window.
# ─────────────────────────────────────────────────────────────
function Invoke-DuringRunTenmin([string]$tierLabel, [int]$elapsedMinute) {
    # Slot guard: skip if a previous tenmin batch is still alive.
    # Must use /jobs/overview to filter on job name (.name) — /jobs returns
    # only id+status and the name match always fails against it.
    try {
        $ovRaw = Get-FlinkJobsOverviewJson | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($ovRaw -and $ovRaw.jobs) {
            $running = @($ovRaw.jobs | Where-Object {
                $_.name -match 'sensor_data_10min' -and
                $_.state -in @("RUNNING","CREATED","RESTARTING","RECONCILING","INITIALIZING")
            })
            if ($running.Count -gt 0) {
                Write-Warning "  Tenmin trigger at +${elapsedMinute}m SKIPPED: $($running.Count) previous tenmin batch(es) still RUNNING (would cause slot pile-up)."
                return $false
            }
        }
    } catch { }

    $suffix = $tierLabel.ToLower() -replace '[^a-z0-9]', ''
    $ts = (Get-Date -Format "HHmmss")
    $jobName = "in-run-10min-${suffix}-m${elapsedMinute}-${ts}"
    Write-SubStep "  Triggering in-run tenmin aggregation (+${elapsedMinute}m, job: $jobName)..."
    kubectl create job $jobName --from=cronjob/tenmin-aggregation -n data-processing 2>$null | Out-Null
    return $true
}

# ─────────────────────────────────────────────────────────────
# Post-flush 30-minute aggregation trigger.
# Fired once per sub-phase after the flush cooldown to produce a single
# rollup row per (home, plug) for the tier. Blocks until the underlying
# Flink batch job reaches FINISHED so downstream metrics collection can
# read populated `sensor_data_30min` rows.
# ─────────────────────────────────────────────────────────────
function Invoke-30minAggregation([string]$tierLabel) {
    $suffix = $tierLabel.ToLower() -replace '[^a-z0-9]', ''
    $ts = (Get-Date -Format "HHmmss")
    $jobName = "post-flush-30min-${suffix}-${ts}"
    Write-SubStep "Triggering post-flush 30-minute aggregation (job: $jobName)..."
    kubectl create job $jobName --from=cronjob/thirtymin-aggregation -n data-processing 2>$null | Out-Null

    # Phase 1: wait for K8s Job to show succeeded (Flink submit returned)
    Write-SubStep "Waiting for 30-min aggregation K8s Job submit (timeout 300s)..."
    $aggTimeout = 300
    $aggElapsed = 0
    $submitDone = $false
    while ($aggElapsed -lt $aggTimeout -and -not $submitDone) {
        $status = kubectl get job $jobName -n data-processing -o jsonpath='{.status.succeeded}' 2>$null
        if ($status -eq "1") { $submitDone = $true; Write-SubStep "  30-min aggregation submitted (K8s Job complete)." ; break }
        Start-Sleep -Seconds 10
        $aggElapsed += 10
    }
    if (-not $submitDone) { Write-Warning "30-min aggregation K8s Job did not complete within ${aggTimeout}s" }

    # Phase 2: wait for the underlying Flink batch job to actually FINISH.
    # Must use /jobs/overview — /jobs returns no name field, so name-based
    # filtering against it always failed and the loop ran out the full
    # timeout even when the job had already finished within seconds.
    Write-SubStep "Waiting for Flink 30-min batch job to FINISH (timeout 600s)..."
    # [datetime]::UnixEpoch only exists on .NET Core 2.1+ (PowerShell 7+).
    # Windows PowerShell 5.1 uses .NET Framework 4.x — use the 8-arg ctor instead.
    $epoch = [datetime]::new(1970,1,1,0,0,0,0,[DateTimeKind]::Utc)
    $submitEpochMs = [int64](((Get-Date).AddMinutes(-1).ToUniversalTime() - $epoch).TotalMilliseconds)
    $flinkAggTimeout = 600
    $flinkAggElapsed = 0
    $flinkDone = $false

    while ($flinkAggElapsed -lt $flinkAggTimeout -and -not $flinkDone) {
        Start-Sleep -Seconds 10
        $flinkAggElapsed += 10
        try {
            $ovRaw = Get-FlinkJobsOverviewJson | ConvertFrom-Json -ErrorAction SilentlyContinue
            if (-not $ovRaw -or -not $ovRaw.jobs) { continue }

            $finished = @($ovRaw.jobs | Where-Object {
                $_.name -match 'sensor_data_30min' -and $_.state -eq "FINISHED" -and
                [int64]$_.'start-time' -gt $submitEpochMs
            })
            if ($finished.Count -gt 0) {
                $flinkDone = $true
                Write-SubStep "  30-min Flink batch job FINISHED."
                break
            }
            $failed = @($ovRaw.jobs | Where-Object {
                $_.name -match 'sensor_data_30min' -and $_.state -eq "FAILED" -and
                [int64]$_.'start-time' -gt $submitEpochMs
            })
            if ($failed.Count -gt 0) {
                $flinkDone = $true
                Write-Warning "  30-min Flink batch job FAILED — sensor_data_30min may be empty."
                break
            }
        } catch { }
    }
    if (-not $flinkDone) { Write-Warning "30-min Flink batch job did not reach FINISHED within ${flinkAggTimeout}s" }

    return @{ thirtymin = $flinkDone }
}

# ─────────────────────────────────────────────────────────────
# Post-aggregation: verify 30-min rollup rows > 0 and re-query
# 30-min-dependent metrics (energy, peak power, average voltage).
# ─────────────────────────────────────────────────────────────
function Update-PostAggregationMetrics([System.Collections.IDictionary]$t1Metrics) {
    # WS PowerShell 5.1: [hashtable] cast on an [ordered]@{} (OrderedDictionary)
    # creates a NEW Hashtable, so updates done here would not propagate back to
    # the caller. IDictionary accepts both Hashtable and OrderedDictionary by
    # reference, so $t1Metrics["..."] = ... mutates the caller's object.
    Write-SubStep "Verifying 30-min aggregation produced rows..."
    $maxRetries = 8
    $retryDelay = 30
    $thirtyMinRows = 0

    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        $result = Invoke-TrinoQuery "SELECT COUNT(*) FROM paimon.iot.sensor_data_30min" "post-agg-30min-check"
        [int64]::TryParse(($result -replace '[^\d]',''), [ref]$thirtyMinRows) | Out-Null
        if ($thirtyMinRows -gt 0) {
            Write-SubStep "  30-min rows after aggregation: $thirtyMinRows (attempt $attempt)"
            break
        }
        if ($attempt -lt $maxRetries) {
            Write-SubStep "  30-min rows still 0 — waiting ${retryDelay}s before retry ($attempt/$maxRetries)..."
            Start-Sleep -Seconds $retryDelay
        }
    }

    if ($thirtyMinRows -eq 0) {
        Write-Warning "30-min table still has 0 rows after aggregation + retries (waited $($maxRetries * $retryDelay)s)"
        return
    }

    # Re-query 30-min-dependent metrics and update t1
    Write-SubStep "Re-querying 30-min-dependent metrics after aggregation..."
    $postAggResults = Invoke-ParallelTrinoJobs @(
        @{ Key="30min_count";          Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_30min" },
        @{ Key="10min_count";          Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_10min" },
        @{ Key="distinct_homes_30min"; Sql="SELECT COUNT(DISTINCT home_id) FROM paimon.iot.sensor_data_30min" },
        @{ Key="total_energy";         Sql="SELECT COALESCE(SUM(energy_kwh),0) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" },
        @{ Key="peak_power";           Sql="SELECT COALESCE(MAX(peak_power_w),0) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" },
        @{ Key="avg_voltage";          Sql="SELECT ROUND(AVG(avg_voltage),2) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" }
    )
    $t1Metrics["trino_30min_count"]          = $postAggResults["30min_count"].result
    $t1Metrics["trino_10min_count"]          = $postAggResults["10min_count"].result
    $t1Metrics["trino_distinct_homes_30min"] = $postAggResults["distinct_homes_30min"].result
    $t1Metrics["trino_total_energy_kwh"]     = $postAggResults["total_energy"].result
    $t1Metrics["trino_peak_power_w"]         = $postAggResults["peak_power"].result
    $t1Metrics["trino_avg_voltage"]          = $postAggResults["avg_voltage"].result
    Write-SubStep "  Updated 30-min rows: $($t1Metrics['trino_30min_count']), energy: $($t1Metrics['trino_total_energy_kwh']) kWh"
}

# ─────────────────────────────────────────────────────────────
# NEW: Measure timed query with 3 repetitions (min/median/max)
# ─────────────────────────────────────────────────────────────
function Measure-TrinoQueryLatency([string]$sql, [string]$label, [int]$repetitions = 3) {
    $times = @()
    for ($i = 1; $i -le $repetitions; $i++) {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            Invoke-TrinoQuery $sql "${label}-run${i}" | Out-Null
            $sw.Stop()
            $times += $sw.ElapsedMilliseconds
        } catch {
            $sw.Stop()
            $times += $sw.ElapsedMilliseconds
        }
        if ($i -lt $repetitions) { Start-Sleep -Seconds 2 }
    }
    $sorted = $times | Sort-Object
    $median = $sorted[[math]::Floor($sorted.Count / 2)]
    return @{
        min_ms    = $sorted[0]
        median_ms = $median
        max_ms    = $sorted[-1]
        all_ms    = $times
    }
}

# ─────────────────────────────────────────────────────────────
# Parallel query helpers — run multiple kubectl-based queries
# concurrently via Start-Job to utilise all Trino workers
# ─────────────────────────────────────────────────────────────
function Invoke-ParallelTrinoJobs {
    <# Runs multiple Trino queries in parallel via Start-Job.
       Input: array of @{ Key="label"; Sql="SELECT ..."; Reps=1 }
       Reps > 1 runs that query N times sequentially within its job (for min/median/max).
       Returns: hashtable keyed by Key -> @{ result; min_ms; median_ms; max_ms; all_ms } #>
    param([array]$Queries)

    $ns  = $TrinoNamespace
    $pod = if ($script:TrinoPodName) { $script:TrinoPodName } else { "deploy/trino-standalone-trino-coordinator" }

    $jobs = @()
    foreach ($q in $Queries) {
        $reps = 1
        if ($q -is [hashtable]) {
            if ($q.ContainsKey("Reps") -and $null -ne $q["Reps"]) {
                $reps = [int]$q["Reps"]
            }
        } elseif ($q.PSObject.Properties["Reps"] -and $null -ne $q.Reps) {
            $reps = [int]$q.Reps
        }
        if ($reps -lt 1) { $reps = 1 }
        $jobs += @{
            Key = $q.Key
            Job = Start-Job -ScriptBlock {
                param($sql, $reps, $ns, $pod)
                $times = @(); $lastResult = ""
                for ($i = 1; $i -le $reps; $i++) {
                    $sw = [System.Diagnostics.Stopwatch]::StartNew()
                    try {
                        $raw = kubectl exec -n $ns $pod -- trino --execute $sql 2>&1
                        $sw.Stop()
                        $cleaned = @($raw | Where-Object {
                            $_ -notmatch 'org\.jline' -and $_ -notmatch '^WARNING:' -and
                            $_ -notmatch '^\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2}'
                        })
                        $lastResult = ($cleaned -join "`n").Trim().Trim('"')
                    } catch { $sw.Stop() }
                    $times += $sw.ElapsedMilliseconds
                    if ($i -lt $reps) { Start-Sleep -Seconds 2 }
                }
                $sorted = $times | Sort-Object
                @{
                    result    = $lastResult
                    min_ms    = $sorted[0]
                    median_ms = $sorted[[math]::Floor($sorted.Count / 2)]
                    max_ms    = $sorted[-1]
                    all_ms    = $times
                }
            } -ArgumentList $q.Sql, $reps, $ns, $pod
        }
    }

    Write-SubStep "    (waiting for $($jobs.Count) parallel Trino jobs...)"
    $results = @{}
    foreach ($j in $jobs) {
        $j.Job | Wait-Job | Out-Null
        $results[$j.Key] = Receive-Job $j.Job
        Remove-Job $j.Job
    }
    return $results
}

function Invoke-ParallelPromJobs {
    <# Runs multiple Prometheus queries in parallel via Start-Job.
       Input: array of @{ Key="label"; Query="promql..." }
       Returns: hashtable keyed by Key -> raw JSON string #>
    param([array]$Queries)

    $ns = $PrometheusNamespace

    $jobs = @()
    foreach ($q in $Queries) {
        $jobs += @{
            Key = $q.Key
            Job = Start-Job -ScriptBlock {
                param($query, $ns)
                $encoded = [Uri]::EscapeDataString($query)
                $raw = kubectl exec -n $ns deployment/kube-prometheus-stack-prometheus -- `
                    wget -qO- "http://localhost:9090/api/v1/query?query=${encoded}" 2>&1
                if (-not $raw -or $raw -like "*error*") {
                    $promPod = (kubectl get pods -n $ns -l "app.kubernetes.io/name=prometheus" `
                        -o jsonpath='{.items[0].metadata.name}' 2>&1).Trim()
                    $raw = kubectl exec -n $ns $promPod -- `
                        wget -qO- "http://localhost:9090/api/v1/query?query=${encoded}" 2>&1
                }
                ($raw -join "`n")
            } -ArgumentList $q.Query, $ns
        }
    }

    Write-SubStep "    (waiting for $($jobs.Count) parallel Prometheus jobs...)"
    $results = @{}
    foreach ($j in $jobs) {
        $j.Job | Wait-Job | Out-Null
        $results[$j.Key] = Receive-Job $j.Job
        Remove-Job $j.Job
    }
    return $results
}

# ─────────────────────────────────────────────────────────────
# Clean-up function (full environment reset)
# ─────────────────────────────────────────────────────────────
function Reset-Environment {
    Write-Step "Resetting environment..."

    Write-SubStep "Deleting Go nonbatched experiment deployments..."
    kubectl delete deployments -n $KafkaNamespace -l "app=go-iot-sim-producer" --ignore-not-found 2>$null | Out-Null

    Write-SubStep "Deleting Go batched experiment deployments..."
    kubectl delete deployments -n $KafkaNamespace -l "app=go-iot-sim-producer-batched" --ignore-not-found 2>$null | Out-Null

    Write-SubStep "Deleting Python experiment deployments..."
    kubectl delete deployments -n $KafkaNamespace -l "app=iot-sim-producer" --ignore-not-found 2>$null | Out-Null

    kubectl delete deployment iot-sim-producer -n $KafkaNamespace --ignore-not-found 2>$null | Out-Null
    kubectl delete deployment go-iot-sim-producer -n $KafkaNamespace --ignore-not-found 2>$null | Out-Null
    kubectl delete deployment go-iot-sim-producer-batched -n $KafkaNamespace --ignore-not-found 2>$null | Out-Null

    # Clean up any leftover manual aggregation jobs
    Write-SubStep "Deleting leftover manual aggregation jobs..."
    kubectl delete jobs -n data-processing -l "job-name" --field-selector "metadata.name!=paimon-bootstrap,metadata.name!=flink-submit-kafka-to-paimon" --ignore-not-found 2>$null | Out-Null

    Write-SubStep "Deleting Flink jobs and CronJobs..."
    kubectl delete job paimon-bootstrap -n data-processing --ignore-not-found 2>$null | Out-Null
    kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
    kubectl delete cronjob hourly-aggregation -n data-processing --ignore-not-found 2>$null | Out-Null
    kubectl delete cronjob tenmin-aggregation -n data-processing --ignore-not-found 2>$null | Out-Null
    kubectl delete cronjob thirtymin-aggregation -n data-processing --ignore-not-found 2>$null | Out-Null

    Write-SubStep "Cancelling running Flink jobs..."
    try {
        $jobsJson = Get-FlinkJobsJson
        $jobs = $jobsJson | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($jobs -and $jobs.jobs) {
            foreach ($j in $jobs.jobs) {
                if ($j.status -in @("RUNNING","CREATED","RESTARTING","RECONCILING","INITIALIZING")) {
                    Write-SubStep "  Cancelling Flink job $($j.id)..."
                    kubectl exec -n data-processing deployment/flink-jobmanager -- `
                        curl -s -X PATCH "http://localhost:${FlinkRestPort}/jobs/$($j.id)?mode=cancel" 2>$null | Out-Null
                }
            }
            # Wait for all jobs to reach a terminal state before scaling down
            $cancelDeadline = (Get-Date).AddSeconds(60)
            while ((Get-Date) -lt $cancelDeadline) {
                Start-Sleep -Seconds 3
                $stillAlive = @()
                try {
                    $refreshJson = Get-FlinkJobsJson | ConvertFrom-Json -ErrorAction SilentlyContinue
                    if ($refreshJson -and $refreshJson.jobs) {
                        $stillAlive = @($refreshJson.jobs | Where-Object {
                            $_.status -in @("RUNNING","CREATED","RESTARTING","RECONCILING","INITIALIZING","CANCELLING")
                        })
                    }
                } catch { }
                if ($stillAlive.Count -eq 0) {
                    Write-SubStep "  All Flink jobs confirmed cancelled."
                    break
                }
            }
            if ($stillAlive.Count -gt 0) {
                Write-Warning "  $($stillAlive.Count) Flink job(s) still alive after 60s — scaling JM/TM=0 will forcibly terminate them."
            }
        }
    } catch { Write-SubStep "  (no running Flink jobs or REST unreachable)" }

    Write-SubStep "Scaling down Flink cluster before storage cleanup..."
    kubectl scale deployment flink-taskmanager -n data-processing --replicas=0 2>$null | Out-Null
    kubectl scale deployment flink-jobmanager -n data-processing --replicas=0 2>$null | Out-Null
    kubectl wait --for=delete pod -l "app=flink" -n data-processing --timeout=120s 2>$null | Out-Null
    Start-Sleep -Seconds 5

    Write-SubStep "Recycling Paimon warehouse container (delete + poll-until-gone + recreate)..."
    az storage container delete `
        --account-name $StorageAccountName `
        --account-key $StorageAccountKey `
        --name warehouse 2>$null | Out-Null

    # Azure container delete is async — name can sit in 'ContainerBeingDeleted'
    # for 30s–several minutes. Poll until the container is actually gone before
    # attempting create, otherwise we get silent 409s that leave the warehouse
    # in an unreachable state and break Paimon writes/reads downstream.
    $maxDeleteWait = 240
    $deleteWaited = 0
    $deletedConfirmed = $false
    while ($deleteWaited -lt $maxDeleteWait) {
        Start-Sleep -Seconds 5
        $deleteWaited += 5
        $existsRaw = az storage container exists `
            --account-name $StorageAccountName `
            --account-key $StorageAccountKey `
            --name warehouse `
            --query exists --output tsv 2>$null
        if ($existsRaw -eq "false") { $deletedConfirmed = $true; break }
        if (($deleteWaited % 30) -eq 0) {
            Write-SubStep "  Still waiting for delete to finalize... (${deleteWaited}s)"
        }
    }
    if (-not $deletedConfirmed) {
        Write-Warning "  Container delete did not finalize within ${maxDeleteWait}s — recreate may fail with ContainerBeingDeleted"
    } else {
        Write-SubStep "  Container deletion confirmed after ${deleteWaited}s."
    }

    # Recreate via unified create+verify helper (handles silent-no-op create case)
    Confirm-StorageContainerExists -containerName "warehouse"

    # Each tier/sub-phase is a standalone experiment, so the Flink checkpoint
    # state must also be reset — otherwise a previous run's checkpoint metadata
    # can be picked up on streaming-job restart and skew checkpoint-delta
    # metrics. Same delete + poll + recreate pattern as warehouse above; the
    # ABFS Hadoop driver does NOT auto-create the container, so this is the
    # only path that guarantees an empty `checkpoints` container at run start.
    Write-SubStep "Recycling Flink checkpoints container (delete + poll-until-gone + recreate)..."
    az storage container delete `
        --account-name $StorageAccountName `
        --account-key $StorageAccountKey `
        --name checkpoints 2>$null | Out-Null

    $ckptDeleteWaited = 0
    $ckptDeleteConfirmed = $false
    while ($ckptDeleteWaited -lt $maxDeleteWait) {
        Start-Sleep -Seconds 5
        $ckptDeleteWaited += 5
        $ckptExistsRaw = az storage container exists `
            --account-name $StorageAccountName `
            --account-key $StorageAccountKey `
            --name checkpoints `
            --query exists --output tsv 2>$null
        if ($ckptExistsRaw -eq "false") { $ckptDeleteConfirmed = $true; break }
        if (($ckptDeleteWaited % 30) -eq 0) {
            Write-SubStep "  Still waiting for checkpoints delete to finalize... (${ckptDeleteWaited}s)"
        }
    }
    if (-not $ckptDeleteConfirmed) {
        Write-Warning "  Checkpoints container delete did not finalize within ${maxDeleteWait}s — recreate may fail with ContainerBeingDeleted"
    } else {
        Write-SubStep "  Checkpoints container deletion confirmed after ${ckptDeleteWaited}s."
    }

    Confirm-StorageContainerExists -containerName "checkpoints"

    Write-SubStep "Deleting Kafka topics to reset offsets..."
    kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server $KafkaBrokerInternal `
        --delete --topic $TopicGeneral --if-exists 2>$null | Out-Null
    kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server $KafkaBrokerInternal `
        --delete --topic $TopicPlug --if-exists 2>$null | Out-Null

    Write-SubStep "Waiting 20s for Kafka topic metadata to propagate after deletion..."
    Start-Sleep -Seconds 20

    Write-SubStep "Recreating Kafka topics..."
    kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server $KafkaBrokerInternal `
        --create --topic $TopicGeneral --replication-factor 2 --partitions 4 2>$null | Out-Null
    kubectl exec -n $KafkaNamespace $KafkaPod -- `
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server $KafkaBrokerInternal `
        --create --topic $TopicPlug --replication-factor 2 --partitions 8 2>$null | Out-Null

    Write-SubStep "Ensuring Flink PodMonitor exists for Prometheus scraping..."
    $podMonitorPath = Join-Path $RepoRoot "k8s\flink\flink-podmonitor.yaml"
    if (Test-Path $podMonitorPath) {
        kubectl apply -f $podMonitorPath 2>$null | Out-Null
    }

    Write-SubStep "Starting Flink cluster..."
    kubectl scale deployment flink-jobmanager -n data-processing --replicas=1 2>$null | Out-Null
    $flinkTmReplicas = 2
    Write-SubStep "Scaling Flink TaskManagers to $flinkTmReplicas replicas..."
    kubectl scale deployment flink-taskmanager -n data-processing --replicas=$flinkTmReplicas 2>$null | Out-Null
    Write-SubStep "Waiting for Flink cluster to be ready..."
    Start-Sleep -Seconds 30
    kubectl wait --for=condition=ready pod -l "app=flink,component=jobmanager" -n data-processing --timeout=180s 2>$null | Out-Null
    kubectl wait --for=condition=ready pod -l "app=flink,component=taskmanager" -n data-processing --timeout=180s 2>$null | Out-Null
    Start-Sleep -Seconds 10

    # Deep-readiness: wait for REST API + TaskManager slots
    Wait-FlinkClusterReady -timeoutSec 180

    # Pre-flight: assert cluster is empty before declaring ready
    try {
        $preflightJobs = @()
        $pfRaw = Get-FlinkJobsJson | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($pfRaw -and $pfRaw.jobs) {
            $preflightJobs = @($pfRaw.jobs | Where-Object {
                $_.status -in @("RUNNING","RESTARTING","CREATED","RECONCILING","INITIALIZING")
            })
        }
        if ($preflightJobs.Count -gt 0) {
            Write-Warning "Pre-flight: $($preflightJobs.Count) Flink job(s) still alive at reset completion — IDs: $(($preflightJobs | ForEach-Object { $_.id }) -join ', ')"
        }
    } catch { }

    Write-SubStep "Environment reset complete."
}

# ─────────────────────────────────────────────────────────────
# Re-apply data-processing manifests
# ─────────────────────────────────────────────────────────────
function Deploy-DataProcessing {
    Write-Step "Deploying data-processing manifests..."

    kubectl apply -f $DataProcessingYaml 2>&1 | ForEach-Object { Write-SubStep $_ }

    # kubectl apply starts both paimon-bootstrap and flink-submit-kafka-to-paimon
    # simultaneously. flink-submit uses sql-client.sh which exits 0 even when
    # "Cannot find table sensor_data_raw" — so it gets marked K8s-succeeded without
    # actually submitting any Flink job. Kill it now; re-create after bootstrap.
    Write-SubStep "Holding flink-submit-kafka-to-paimon until paimon-bootstrap completes..."
    kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null

    Write-SubStep "Waiting for paimon-bootstrap job to complete..."
    $bsTimeout = 120
    $elapsed = 0
    while ($elapsed -lt $bsTimeout) {
        $status = kubectl get job paimon-bootstrap -n data-processing -o jsonpath='{.status.succeeded}' 2>&1
        if ($status -eq "1") { break }
        Start-Sleep -Seconds 5
        $elapsed += 5
    }
    if ($elapsed -ge $bsTimeout) {
        Write-Warning "paimon-bootstrap did not complete within ${bsTimeout}s — check logs"
    } else {
        Write-SubStep "paimon-bootstrap completed."
    }

    # Schema is now guaranteed present — re-create the ingest job.
    Write-SubStep "Submitting flink-submit-kafka-to-paimon (schema confirmed)..."
    kubectl apply -f $DataProcessingYaml 2>&1 | Out-Null

    Write-SubStep "Waiting for flink-submit-kafka-to-paimon job to complete..."
    $submitTimeout = 180
    $elapsed = 0
    while ($elapsed -lt $submitTimeout) {
        $status = kubectl get job flink-submit-kafka-to-paimon -n data-processing -o jsonpath='{.status.succeeded}' 2>&1
        if ($status -eq "1") { break }
        # Also check for failure early so we don't wait the full timeout
        $failedStatus = kubectl get job flink-submit-kafka-to-paimon -n data-processing -o jsonpath='{.status.failed}' 2>&1
        if ($failedStatus -match '^\d+$' -and [int]$failedStatus -ge 3) {
            Write-Warning "flink-submit-kafka-to-paimon job FAILED (backoffLimit reached)"
            break
        }
        Start-Sleep -Seconds 5
        $elapsed += 5
    }
    if ($status -eq "1") {
        Write-SubStep "Flink streaming ingest job submitted."
        # Verify the submit pod didn't exit with a silent error by checking Flink REST.
        # Retry several times because there can be a brief lag between sql-client
        # exiting and the JobManager exposing the job in /jobs.
        $verifyAttempts = 0
        $verifyMaxAttempts = 6
        $verifyOk = $false
        $allStatusesSeen = ""
        while (-not $verifyOk -and $verifyAttempts -lt $verifyMaxAttempts) {
            $verifyAttempts++
            Start-Sleep -Seconds 5
            try {
                $jobsRaw = Get-FlinkJobsJson
                $jobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                if ($jobs -and $jobs.jobs) {
                    $allStatusesSeen = ($jobs.jobs | ForEach-Object { "$($_.id):$($_.status)" }) -join ', '
                    $runningOrCreated = @($jobs.jobs | Where-Object { $_.status -in @("RUNNING","CREATED","INITIALIZING","RECONCILING") })
                    if ($runningOrCreated.Count -gt 0) {
                        Write-SubStep "  Verified (attempt $verifyAttempts/$verifyMaxAttempts): $($runningOrCreated.Count) Flink job(s) in status: $(($runningOrCreated | ForEach-Object { $_.status }) -join ', ')"
                        $verifyOk = $true
                        break
                    }
                    # Surface terminal-state jobs so silent failures stop being silent
                    $terminal = @($jobs.jobs | Where-Object { $_.status -in @("FAILED","CANCELED","FINISHED") })
                    if ($terminal.Count -gt 0) {
                        Write-Warning "  Submit job succeeded but Flink shows job(s) in TERMINAL state already: $allStatusesSeen"
                        # Try to dump the submit pod logs (last attempt only) to help diagnose
                        if ($verifyAttempts -ge 2) {
                            Write-SubStep "  Dumping flink-submit-kafka-to-paimon pod logs (last 60 lines)..."
                            kubectl logs -n data-processing job/flink-submit-kafka-to-paimon --tail=60 2>$null | ForEach-Object { Write-SubStep "    $_" }
                            break
                        }
                    } else {
                        Write-SubStep "  No RUNNING jobs yet (attempt $verifyAttempts/$verifyMaxAttempts). Statuses: $allStatusesSeen"
                    }
                } else {
                    Write-SubStep "  Flink REST shows no jobs (attempt $verifyAttempts/$verifyMaxAttempts). Waiting for registration..."
                }
            } catch {
                Write-SubStep "  (Could not verify Flink job via REST attempt ${verifyAttempts}: $($_.Exception.Message))"
            }
        }
        if (-not $verifyOk) {
            Write-Warning "Submit job succeeded but NO running/initializing Flink jobs found after $verifyMaxAttempts attempts — submission failed silently. Last statuses: $allStatusesSeen"
            Write-SubStep "  Dumping flink-submit-kafka-to-paimon pod logs (last 80 lines) for diagnosis..."
            kubectl logs -n data-processing job/flink-submit-kafka-to-paimon --tail=80 2>$null | ForEach-Object { Write-SubStep "    $_" }
        }
    } else {
        Write-Warning "flink-submit-kafka-to-paimon did not complete within ${submitTimeout}s — check logs"
    }
}

# ─────────────────────────────────────────────────────────────
# Collect all metrics at a point in time (IMPROVED)
# ─────────────────────────────────────────────────────────────
function Collect-Metrics([int]$homes, [string]$phase, [string]$producerType = "nonbatched", [string]$AfterTs = "") {
    $ts = Get-Timestamp
    $appLabel = if ($producerType -eq "batched") { "go-iot-sim-producer-batched" } else { "go-iot-sim-producer" }
    $metrics = [ordered]@{}
    $metrics["timestamp"]        = $ts
    $metrics["phase"]            = $phase
    $metrics["homes"]            = $homes
    $metrics["producer_type"]    = $producerType
    $metrics["expected_rps"]     = $homes + ($homes * $PlugsPerHome)

    # ── Kafka offsets ────────────────────────────────────
    Write-SubStep "  Kafka offsets..."
    $metrics["kafka_general_offset"]  = Get-KafkaTopicOffsets $TopicGeneral
    $metrics["kafka_plug_offset"]     = Get-KafkaTopicOffsets $TopicPlug
    $metrics["kafka_total_offset"]    = $metrics["kafka_general_offset"] + $metrics["kafka_plug_offset"]
    $metrics["kafka_general_raw"]     = Get-KafkaTopicOffsetsRaw $TopicGeneral
    $metrics["kafka_plug_raw"]        = Get-KafkaTopicOffsetsRaw $TopicPlug

    # ── Trino warm-up query (discard result) ─────────────
    Write-SubStep "  Trino warm-up query..."
    Invoke-TrinoQuery "SELECT 1" "warmup" | Out-Null

    # ── Trino: Paimon row counts + analytics (PARALLEL) ────
    Write-SubStep "  Trino health checks + analytics (parallel, 8 queries)..."
    $healthResults = Invoke-ParallelTrinoJobs @(
        @{ Key="raw_count";            Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_raw" },
        @{ Key="30min_count";          Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_30min" },
        @{ Key="10min_count";          Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_10min" },
        @{ Key="distinct_homes_raw";   Sql="SELECT COUNT(DISTINCT home_id) FROM paimon.iot.sensor_data_raw" },
        @{ Key="distinct_homes_30min"; Sql="SELECT COUNT(DISTINCT home_id) FROM paimon.iot.sensor_data_30min" },
        @{ Key="total_energy";         Sql="SELECT COALESCE(SUM(energy_kwh),0) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" },
        @{ Key="peak_power";           Sql="SELECT COALESCE(MAX(peak_power_w),0) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" },
        @{ Key="avg_voltage";          Sql="SELECT ROUND(AVG(avg_voltage),2) FROM paimon.iot.sensor_data_30min WHERE plug_id='general'" }
    )
    $metrics["trino_raw_count"]             = $healthResults["raw_count"].result
    $metrics["trino_count_raw_latency_ms"]  = $healthResults["raw_count"].min_ms
    $metrics["trino_30min_count"]           = $healthResults["30min_count"].result
    $metrics["trino_10min_count"]           = $healthResults["10min_count"].result
    $metrics["trino_distinct_homes_raw"]    = $healthResults["distinct_homes_raw"].result
    $metrics["trino_distinct_homes_30min"]  = $healthResults["distinct_homes_30min"].result
    $metrics["trino_total_energy_kwh"]      = $healthResults["total_energy"].result
    $metrics["trino_peak_power_w"]          = $healthResults["peak_power"].result
    $metrics["trino_avg_voltage"]           = $healthResults["avg_voltage"].result

    # ══════════════════════════════════════════════════════
    # TIMED QUERY SUITE (3 runs each, PARALLEL across Q1-Q5)
    # Each query runs its 3 reps sequentially within its own
    # job, but all 5 queries execute concurrently across the
    # 3 Trino workers.
    # ══════════════════════════════════════════════════════
    Write-SubStep "  Timed query suite (3 reps each, parallel across Q1-Q5)..."

    $timedResults = Invoke-ParallelTrinoJobs @(
        @{ Key="q1"; Reps=3; Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_raw" },
        @{ Key="q2"; Reps=3; Sql="SELECT home_id, plug_id, AVG(voltage) AS avg_v, AVG(current_amp) AS avg_i, MAX(voltage * current_amp * power_factor) AS peak_power_w, COUNT(*) AS samples FROM paimon.iot.sensor_data_raw GROUP BY home_id, plug_id ORDER BY peak_power_w DESC LIMIT 50" },
        @{ Key="q3"; Reps=3; Sql="SELECT home_id, ts, voltage, AVG(voltage) OVER (PARTITION BY home_id ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS rolling_avg FROM paimon.iot.sensor_data_raw WHERE plug_id = 'general' ORDER BY ts DESC LIMIT 100" },
        @{ Key="q4"; Reps=3; Sql="SELECT r.home_id, COUNT(*) AS raw_samples, t.energy_kwh AS tenmin_energy, AVG(r.voltage) AS raw_avg_voltage, t.avg_voltage AS tenmin_avg_voltage, ABS(AVG(r.voltage) - t.avg_voltage) AS voltage_drift FROM paimon.iot.sensor_data_raw r JOIN paimon.iot.sensor_data_10min t ON r.home_id = t.home_id AND r.plug_id = t.plug_id AND r.dt = t.dt WHERE r.plug_id = 'general' AND t.samples >= (SELECT CAST(MAX(samples) AS DOUBLE) * 0.8 FROM paimon.iot.sensor_data_10min) GROUP BY r.home_id, t.energy_kwh, t.avg_voltage ORDER BY voltage_drift DESC LIMIT 20" },
        @{ Key="q5"; Reps=3; Sql="SELECT home_id, approx_percentile(voltage, 0.5) AS p50_voltage, approx_percentile(voltage, 0.95) AS p95_voltage, approx_percentile(voltage, 0.99) AS p99_voltage, stddev(voltage) AS voltage_stddev FROM paimon.iot.sensor_data_raw WHERE plug_id = 'general' GROUP BY home_id ORDER BY voltage_stddev DESC LIMIT 20" }
    )

    $metrics["trino_q1_count_raw_min_ms"]    = $timedResults["q1"].min_ms
    $metrics["trino_q1_count_raw_median_ms"] = $timedResults["q1"].median_ms
    $metrics["trino_q1_count_raw_max_ms"]    = $timedResults["q1"].max_ms

    $metrics["trino_q2_raw_groupby_min_ms"]    = $timedResults["q2"].min_ms
    $metrics["trino_q2_raw_groupby_median_ms"] = $timedResults["q2"].median_ms
    $metrics["trino_q2_raw_groupby_max_ms"]    = $timedResults["q2"].max_ms

    $metrics["trino_q3_window_fn_min_ms"]    = $timedResults["q3"].min_ms
    $metrics["trino_q3_window_fn_median_ms"] = $timedResults["q3"].median_ms
    $metrics["trino_q3_window_fn_max_ms"]    = $timedResults["q3"].max_ms

    $metrics["trino_q4_join_min_ms"]    = $timedResults["q4"].min_ms
    $metrics["trino_q4_join_median_ms"] = $timedResults["q4"].median_ms
    $metrics["trino_q4_join_max_ms"]    = $timedResults["q4"].max_ms

    $metrics["trino_q5_percentile_min_ms"]    = $timedResults["q5"].min_ms
    $metrics["trino_q5_percentile_median_ms"] = $timedResults["q5"].median_ms
    $metrics["trino_q5_percentile_max_ms"]    = $timedResults["q5"].max_ms

    # ── NEW: End-to-end latency ───────────────────────────
    Write-SubStep "  End-to-end latency..."
    try {
        $e2eSql = if ($AfterTs -ne "") {
            "SELECT MAX(ts) FROM paimon.iot.sensor_data_raw WHERE ts >= TIMESTAMP '$AfterTs'"
        } else {
            "SELECT MAX(ts) FROM paimon.iot.sensor_data_raw"
        }
        $latestTs = (Invoke-TrinoQuery $e2eSql "e2e-latest-ts").Trim()
        if ($latestTs -and $latestTs -ne "" -and $latestTs -ne "NULL") {
            # Trino's session timezone is UTC, so MAX(ts) prints as a bare
            # UTC wall-clock value (e.g., "2026-05-06 15:30:40.495").
            # [datetime]::Parse(...) returns Kind=Unspecified, which subtracts
            # against (Get-Date) (Kind=Local) by raw Ticks — that silently
            # mixes local-clock and UTC-clock and inflates the result by the
            # local-UTC offset (e.g., +3h on EEST machines).
            # Pin the parsed value to UTC and compare against UTC wall clock
            # so the math is correct regardless of the host's timezone.
            $parsedTs = [datetime]::SpecifyKind([datetime]::Parse($latestTs), [DateTimeKind]::Utc)
            $e2eLatency = ((Get-Date).ToUniversalTime() - $parsedTs).TotalSeconds
            $metrics["e2e_latency_seconds"] = [math]::Round($e2eLatency, 1)
        } else {
            $metrics["e2e_latency_seconds"] = ""
        }
        $metrics["e2e_latest_raw_ts"] = $latestTs
    } catch {
        $metrics["e2e_latency_seconds"] = ""
        $metrics["e2e_latest_raw_ts"] = ""
    }

    # ── Flink job metrics ─────────────────────────────────
    Write-SubStep "  Flink jobs..."
    try {
        $jobsRaw = Get-FlinkJobsJson
        $metrics["flink_jobs_json"] = $jobsRaw
        $jobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($jobs -and $jobs.jobs) {
            $running = @($jobs.jobs | Where-Object { $_.status -eq "RUNNING" })
            $metrics["flink_running_jobs"] = $running.Count
            if ($running.Count -gt 0) {
                $jid = $running[0].id
                $metrics["flink_job_id"] = $jid
                $mCsv = "numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,currentInputWatermark"
                $mJson = Get-FlinkJobMetricsJson $jid $mCsv
                $metrics["flink_job_metrics"] = $mJson
            }
        }
    } catch {
        $metrics["flink_jobs_error"] = $_.Exception.Message
    }

    # ── Producer logs (active type) ───────────────────────
    Write-SubStep "  Producer stats ($producerType)..."
    $prod = Get-ProducerFinalStats $appLabel
    $metrics["producer_final"]     = $prod.Final
    $metrics["producer_last_stats"] = $prod.LastStats
    $metrics["producer_last_cycle"] = $prod.LastCycle

    # ── Prometheus: resource utilisation (PARALLEL, 18 queries) ──
    Write-SubStep "  Prometheus metrics (parallel)..."
    $promResults = Invoke-ParallelPromJobs @(
        @{ Key="prom_flink_cpu";                           Query='sum(rate(container_cpu_usage_seconds_total{namespace="data-processing",pod=~"flink.*"}[5m])) by (pod)' },
        @{ Key="prom_flink_mem";                           Query='sum(container_memory_working_set_bytes{namespace="data-processing",pod=~"flink.*"}) by (pod)' },
        @{ Key="prom_kafka_cpu";                           Query='sum(rate(container_cpu_usage_seconds_total{namespace="data-ingest",pod=~"kafka.*"}[5m])) by (pod)' },
        @{ Key="prom_kafka_mem";                           Query='sum(container_memory_working_set_bytes{namespace="data-ingest",pod=~"kafka.*"}) by (pod)' },
        @{ Key="prom_producer_cpu";                        Query='sum(rate(container_cpu_usage_seconds_total{namespace="data-ingest",pod=~".*iot-sim.*"}[5m])) by (pod)' },
        @{ Key="prom_producer_mem";                        Query='sum(container_memory_working_set_bytes{namespace="data-ingest",pod=~".*iot-sim.*"}) by (pod)' },
        @{ Key="prom_trino_cpu";                           Query='sum(rate(container_cpu_usage_seconds_total{namespace="analytics",pod=~"trino.*"}[5m])) by (pod)' },
        @{ Key="prom_trino_mem";                           Query='sum(container_memory_working_set_bytes{namespace="analytics",pod=~"trino.*"}) by (pod)' },
        @{ Key="prom_node_cpu_total";                      Query='sum(rate(container_cpu_usage_seconds_total[5m])) by (node)' },
        @{ Key="prom_node_mem_total";                      Query='sum(container_memory_working_set_bytes) by (node)' },
        @{ Key="prom_flink_records_in_per_sec";            Query='sum(flink_taskmanager_job_task_numRecordsInPerSecond)' },
        @{ Key="prom_flink_records_out_per_sec";           Query='sum(flink_taskmanager_job_task_numRecordsOutPerSecond)' },
        @{ Key="prom_flink_checkpoint_duration";           Query='flink_jobmanager_job_lastCheckpointDuration' },
        @{ Key="prom_flink_checkpoint_size";               Query='flink_jobmanager_job_lastCheckpointFullSize' },
        @{ Key="prom_kafka_consumer_lag_max";              Query='sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)' },
        @{ Key="prom_flink_busy_time_ms_per_sec";          Query='avg(flink_taskmanager_job_task_busyTimeMsPerSecond)' },
        @{ Key="prom_flink_idle_time_ms_per_sec";          Query='avg(flink_taskmanager_job_task_idleTimeMsPerSecond)' },
        @{ Key="prom_flink_backpressured_time_ms_per_sec"; Query='avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)' }
    )
    foreach ($pk in $promResults.Keys) { $metrics[$pk] = $promResults[$pk] }

    # ── kubectl top ───────────────────────────────────────
    Write-SubStep "  kubectl top..."
    $metrics["kubectl_top_flink"]              = Get-PodResourceUsage "data-processing" "app=flink"
    $metrics["kubectl_top_kafka"]              = Get-PodResourceUsage $KafkaNamespace "app=kafka"
    $metrics["kubectl_top_producer"]           = Get-PodResourceUsage $KafkaNamespace "app=$appLabel"
    $metrics["kubectl_top_trino"]              = Get-PodResourceUsage $TrinoNamespace "app.kubernetes.io/name=trino"
    $metrics["kubectl_top_nodes"]              = Get-NodeResourceUsage

    return $metrics
}

# ─────────────────────────────────────────────────────────────
# Write metrics to a file
# ─────────────────────────────────────────────────────────────
function Write-MetricsToFile([hashtable]$metrics, [string]$filePath) {
    $lines = @()
    $lines += "=" * 80
    $lines += "  Experiment Metrics ($($metrics['producer_type'].ToUpper())) - $($metrics['phase'])"
    $lines += "  Homes: $($metrics['homes'])  |  Type: $($metrics['producer_type'])  |  Timestamp: $($metrics['timestamp'])"
    $lines += "=" * 80
    $lines += ""

    foreach ($key in $metrics.Keys) {
        $val = $metrics[$key]
        if ($val -is [string] -and $val.Contains("`n")) {
            $lines += "── $key ──"
            $lines += $val
            $lines += ""
        } else {
            $lines += ("{0,-50} = {1}" -f $key, $val)
        }
    }
    $lines | Out-File -FilePath $filePath -Encoding utf8
}

# ─────────────────────────────────────────────────────────────
# Write the summary CSV row (IMPROVED — new columns)
# ─────────────────────────────────────────────────────────────
function Append-SummaryCsv([hashtable]$t0, [hashtable]$t1, [string]$csvPath) {
    $headerNeeded = -not (Test-Path $csvPath)

    $kafkaGeneralDelta = [int64]$t1["kafka_general_offset"] - [int64]$t0["kafka_general_offset"]
    $kafkaPlugDelta    = [int64]$t1["kafka_plug_offset"]    - [int64]$t0["kafka_plug_offset"]
    $kafkaTotalDelta   = $kafkaGeneralDelta + $kafkaPlugDelta

    $homes       = $t1["homes"]
    $expectedRps = $t1["expected_rps"]
    $runSeconds  = $RunMinutes * 60
    $expectedTotal = $expectedRps * $runSeconds
    $actualRps   = if ($runSeconds -gt 0) { [math]::Round($kafkaTotalDelta / $runSeconds, 1) } else { 0 }

    $row = [ordered]@{
        "experiment_tag"             = $ExperimentTag
        "producer_type"              = $t1["producer_type"]
        "tier_homes"                 = $homes
        "plugs_per_home"             = $PlugsPerHome
        "expected_rps"               = $expectedRps
        "warmup_min"                 = $WarmupMinutes
        "run_min"                    = $RunMinutes
        "flush_cooldown_s"           = $FlushCooldownSeconds
        "t0_timestamp"               = $t0["timestamp"]
        "t1_timestamp"               = $t1["timestamp"]
        "kafka_general_t0"           = $t0["kafka_general_offset"]
        "kafka_general_t1"           = $t1["kafka_general_offset"]
        "kafka_general_delta"        = $kafkaGeneralDelta
        "kafka_plug_t0"              = $t0["kafka_plug_offset"]
        "kafka_plug_t1"              = $t1["kafka_plug_offset"]
        "kafka_plug_delta"           = $kafkaPlugDelta
        "kafka_total_delta"          = $kafkaTotalDelta
        "actual_rps"                 = $actualRps
        "expected_total_msgs"        = $expectedTotal
        "ingestion_ratio"            = if ($expectedTotal -gt 0) { [math]::Round($kafkaTotalDelta / $expectedTotal, 4) } else { 0 }
        "paimon_raw_rows_t0"         = $t0["trino_raw_count"]
        "paimon_raw_rows_t1"         = $t1["trino_raw_count"]
        "paimon_raw_delta"           = (&{ $v1=0; $v0=0; [int64]::TryParse(($t1['trino_raw_count'] -replace '[^\d]',''),[ref]$v1)|Out-Null; [int64]::TryParse(($t0['trino_raw_count'] -replace '[^\d]',''),[ref]$v0)|Out-Null; $v1-$v0 })
        "paimon_30min_rows_t1"       = $t1["trino_30min_count"]
        "paimon_10min_rows_t1"       = $t1["trino_10min_count"]
        "distinct_homes_raw"         = $t1["trino_distinct_homes_raw"]
        "distinct_homes_30min"       = $t1["trino_distinct_homes_30min"]
        "slo_kafka_vs_paimon_raw"    = (&{ $v1=0; $v0=0; [int64]::TryParse(($t1['trino_raw_count'] -replace '[^\d]',''),[ref]$v1)|Out-Null; [int64]::TryParse(($t0['trino_raw_count'] -replace '[^\d]',''),[ref]$v0)|Out-Null; if ($kafkaTotalDelta -gt 0) { [math]::Round(($v1-$v0)/$kafkaTotalDelta,4) } else { 0 } })
        "total_energy_kwh"           = $t1["trino_total_energy_kwh"]
        "peak_power_w"               = $t1["trino_peak_power_w"]
        "avg_voltage_v"              = $t1["trino_avg_voltage"]
        # Q1: COUNT raw (original, kept for continuity)
        "trino_q1_count_raw_median_ms" = $t1["trino_q1_count_raw_median_ms"]
        "trino_q1_count_raw_min_ms"    = $t1["trino_q1_count_raw_min_ms"]
        "trino_q1_count_raw_max_ms"    = $t1["trino_q1_count_raw_max_ms"]
        # Q2: GROUP BY raw (NEW — replaces old COUNT hourly)
        "trino_q2_raw_groupby_median_ms" = $t1["trino_q2_raw_groupby_median_ms"]
        "trino_q2_raw_groupby_min_ms"    = $t1["trino_q2_raw_groupby_min_ms"]
        "trino_q2_raw_groupby_max_ms"    = $t1["trino_q2_raw_groupby_max_ms"]
        # Q3: Window function (NEW — replaces old top-10 hourly)
        "trino_q3_window_fn_median_ms" = $t1["trino_q3_window_fn_median_ms"]
        "trino_q3_window_fn_min_ms"    = $t1["trino_q3_window_fn_min_ms"]
        "trino_q3_window_fn_max_ms"    = $t1["trino_q3_window_fn_max_ms"]
        # Q4: JOIN raw ↔ hourly (NEW)
        "trino_q4_join_median_ms" = $t1["trino_q4_join_median_ms"]
        "trino_q4_join_min_ms"    = $t1["trino_q4_join_min_ms"]
        "trino_q4_join_max_ms"    = $t1["trino_q4_join_max_ms"]
        # Q5: Percentile/stats (NEW)
        "trino_q5_percentile_median_ms" = $t1["trino_q5_percentile_median_ms"]
        "trino_q5_percentile_min_ms"    = $t1["trino_q5_percentile_min_ms"]
        "trino_q5_percentile_max_ms"    = $t1["trino_q5_percentile_max_ms"]
        # NEW: Pipeline health metrics
        "e2e_latency_seconds"            = $t1["e2e_latency_seconds"]
        "kafka_consumer_lag_max"         = $t1["prom_kafka_consumer_lag_max"]
        "flink_busy_time_ms_per_sec"     = $t1["prom_flink_busy_time_ms_per_sec"]
        "flink_backpressure_ms_per_sec"  = $t1["prom_flink_backpressured_time_ms_per_sec"]
        "flink_running_jobs"             = $t1["flink_running_jobs"]
        "producer_final"                 = $t1["producer_final"]
    }

    if ($headerNeeded) {
        ($row.Keys -join ",") | Out-File -FilePath $csvPath -Encoding utf8
    }
    ($row.Values -join ",") | Out-File -FilePath $csvPath -Encoding utf8 -Append
}

# ─────────────────────────────────────────────────────────────
# Prometheus helpers
# ─────────────────────────────────────────────────────────────
function Parse-PromValue([string]$json) {
    $out = [ordered]@{}
    try {
        $obj = $json | ConvertFrom-Json -ErrorAction SilentlyContinue
        if ($obj.data.resultType -eq 'vector') {
            foreach ($r in $obj.data.result) {
                $pod = if ($r.metric.pod) { $r.metric.pod } else { '__scalar__' }
                $val = [double]$r.value[1]
                $out[$pod] = $val
            }
        }
    } catch { }
    return $out
}

function Query-PromAvg([string]$query) {
    return Parse-PromValue (Query-Prometheus $query)
}

function Collect-GranularPrometheus([int]$windowMinutes = $RunMinutes) {
    $window = "${windowMinutes}m"
    $gm = [ordered]@{}

    $gm["kafka_cpu"]    = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=`"data-ingest`",pod=~`"kafka.*`",container!=`"`"}[5m]))[${window}:])"
    $gm["kafka_mem"]    = Query-PromAvg "avg_over_time(sum by (pod)(container_memory_working_set_bytes{namespace=`"data-ingest`",pod=~`"kafka.*`",container!=`"`"})[${window}:])"
    $gm["kafka_netrx"]  = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_network_receive_bytes_total{namespace=`"data-ingest`",pod=~`"kafka.*`"}[5m]))[${window}:])"
    $gm["kafka_nettx"]  = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_network_transmit_bytes_total{namespace=`"data-ingest`",pod=~`"kafka.*`"}[5m]))[${window}:])"
    $gm["kafka_pvc"]    = Query-PromAvg "avg_over_time(label_replace(kubelet_volume_stats_used_bytes{namespace=`"data-ingest`",persistentvolumeclaim=~`"kafka-storage-kafka.*`"},`"pod`",`"`$1`",`"persistentvolumeclaim`",`"(.+)`")[${window}:])"

    $gm["flink_cpu"]    = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=`"data-processing`",pod=~`"flink.*`",container!=`"`"}[5m]))[${window}:])"
    $gm["flink_mem"]    = Query-PromAvg "avg_over_time(sum by (pod)(container_memory_working_set_bytes{namespace=`"data-processing`",pod=~`"flink.*`",container!=`"`"})[${window}:])"
    $gm["flink_netrx"]  = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_network_receive_bytes_total{namespace=`"data-processing`",pod=~`"flink.*`"}[5m]))[${window}:])"
    $gm["flink_nettx"]  = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_network_transmit_bytes_total{namespace=`"data-processing`",pod=~`"flink.*`"}[5m]))[${window}:])"
    $gm["flink_fs"]     = Query-PromAvg "avg_over_time(sum by (pod)(kubelet_container_log_filesystem_used_bytes{namespace=`"data-processing`",pod=~`"flink.*`"})[${window}:])"

    $gm["trino_cpu"]    = Query-PromAvg "avg_over_time(sum by (pod)(rate(container_cpu_usage_seconds_total{namespace=`"analytics`",pod=~`"trino.*`",container!=`"`"}[5m]))[${window}:])"
    $gm["trino_mem"]    = Query-PromAvg "avg_over_time(sum by (pod)(container_memory_working_set_bytes{namespace=`"analytics`",pod=~`"trino.*`",container!=`"`"})[${window}:])"

    return $gm
}

function Get-FlinkCheckpointCounts {
    param(
        [string]$JobId = ""   # If provided, query this specific job ID (for T1 consistency with T0)
    )
    $counts = @{ completed = 0; failed = 0; restored = 0; job_id = ""; job_id_changed = $false }
    try {
        if ($JobId) {
            # T1 path: query the same job ID captured at T0
            $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                curl -s "http://localhost:${FlinkRestPort}/jobs/${JobId}/checkpoints" 2>&1
            $ckJson = ($raw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
            if ($ckJson -and $ckJson.counts) {
                $counts.completed = [int]$ckJson.counts.completed
                $counts.failed    = [int]$ckJson.counts.failed
                $counts.restored  = [int]$ckJson.counts.restored
                $counts.job_id    = $JobId
            } else {
                # Original job gone — fall back to the current RUNNING job
                $jobsRaw = Get-FlinkJobsJson
                $jobs    = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                $running = @($jobs.jobs | Where-Object { $_.status -eq "RUNNING" })
                if ($running.Count -gt 0 -and $running[0].id -ne $JobId) {
                    $newJid = $running[0].id
                    Write-Warning "Flink job ID changed between T0 ($JobId) and T1 ($newJid) — checkpoint counter may have reset"
                    $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                        curl -s "http://localhost:${FlinkRestPort}/jobs/${newJid}/checkpoints" 2>&1
                    $ckJson = ($raw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
                    if ($ckJson -and $ckJson.counts) {
                        $counts.completed = [int]$ckJson.counts.completed
                        $counts.failed    = [int]$ckJson.counts.failed
                        $counts.restored  = [int]$ckJson.counts.restored
                    }
                    $counts.job_id = $newJid
                    $counts.job_id_changed = $true
                }
            }
        } else {
            # T0 path: use /jobs/overview which includes job names (.jid, .name, .state).
            # /jobs returns only id+status with no name field, so name-based filtering
            # against it always returned $null and job_id was never captured.
            $ovRaw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                curl -s "http://localhost:${FlinkRestPort}/jobs/overview" 2>&1
            $ov = ($ovRaw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
            if ($ov -and $ov.jobs) {
                $ingestJob = @($ov.jobs | Where-Object {
                    $_.name -match 'sensor_data_raw|kafka.to.paimon|insert-into_paimon' -and
                    $_.state -in @("RUNNING","RESTARTING","RECONCILING","INITIALIZING","CREATED")
                }) | Select-Object -First 1
                # Fallback: pick any RUNNING job; at T0 only the ingest job is running
                if (-not $ingestJob) {
                    $ingestJob = @($ov.jobs | Where-Object { $_.state -eq "RUNNING" }) |
                                 Select-Object -First 1
                }
                if ($ingestJob) {
                    $jid = $ingestJob.jid   # /jobs/overview uses .jid, not .id
                    if ($ingestJob.state -ne "RUNNING") {
                        $waitEnd = (Get-Date).AddSeconds(60)
                        while ((Get-Date) -lt $waitEnd) {
                            Start-Sleep -Seconds 5
                            $ovRefRaw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                                curl -s "http://localhost:${FlinkRestPort}/jobs/overview" 2>&1
                            $ovRef = ($ovRefRaw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
                            if ($ovRef -and $ovRef.jobs) {
                                $r = @($ovRef.jobs | Where-Object { $_.jid -eq $jid -and $_.state -eq "RUNNING" })
                                if ($r.Count -gt 0) { break }
                            }
                        }
                        if ($ingestJob.state -ne "RUNNING") { $counts.job_id_unstable = $true }
                    }
                    $raw = kubectl exec -n data-processing deployment/flink-jobmanager -- `
                        curl -s "http://localhost:${FlinkRestPort}/jobs/${jid}/checkpoints" 2>&1
                    $ckJson = ($raw -join "") | ConvertFrom-Json -ErrorAction SilentlyContinue
                    if ($ckJson -and $ckJson.counts) {
                        $counts.completed = [int]$ckJson.counts.completed
                        $counts.failed    = [int]$ckJson.counts.failed
                        $counts.restored  = [int]$ckJson.counts.restored
                    }
                    $counts.job_id = $jid
                }
            }
        }
    } catch { }
    return $counts
}

function Get-AzureStorageMetrics([datetime]$startUtc, [datetime]$endUtc) {
    $sa = [ordered]@{}
    try {
        $startStr = $startUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
        $endStr   = $endUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")

        $subIdRaw = @(az account show --query id -o tsv 2>$null)
        $subId = ($subIdRaw | ForEach-Object { $_.Trim() } |
                  Where-Object { $_ -match '^[0-9a-f]{8}-' }) | Select-Object -First 1
        if (-not $subId) { throw "Could not determine Azure subscription ID" }
        $resId = "/subscriptions/${subId}/resourceGroups/${StorageAccountRG}/providers/Microsoft.Storage/storageAccounts/${StorageAccountName}"

        function _SafeProp($obj, [string]$prop) {
            if ($null -eq $obj) { return $null }
            $p = $obj.PSObject.Properties[$prop]
            if ($p -and $null -ne $p.Value) { return $p.Value }
            return $null
        }

        $txRaw = az monitor metrics list --resource $resId `
            --metric Transactions Ingress Egress `
            --start-time $startStr --end-time $endStr `
            --interval PT30M --aggregation Total --output json 2>$null
        if (-not $txRaw) { throw "az monitor metrics returned no output for Transactions/Ingress/Egress" }
        $txObj = ($txRaw -join "") | ConvertFrom-Json -ErrorAction Stop
        foreach ($m in $txObj.value) {
            $name = $m.name.value
            $ts = $m.timeseries | Select-Object -First 1
            if (-not $ts -or -not $ts.data) { continue }
            foreach ($dp in $ts.data) {
                $val = _SafeProp $dp 'total'
                if ($null -ne $val) {
                    if ($name -eq "Transactions")   { $sa["transactions"] = [long]$val }
                    if ($name -eq "Ingress")        { $sa["ingress_gib"]  = [math]::Round($val / 1GB, 3) }
                    if ($name -eq "Egress")         { $sa["egress_gib"]   = [math]::Round($val / 1GB, 3) }
                    break
                }
            }
        }

        $latRaw = az monitor metrics list --resource $resId `
            --metric SuccessE2ELatency SuccessServerLatency Availability `
            --start-time $startStr --end-time $endStr `
            --interval PT30M --aggregation Average --output json 2>$null
        if (-not $latRaw) { throw "az monitor metrics returned no output for Latency/Availability" }
        $latObj = ($latRaw -join "") | ConvertFrom-Json -ErrorAction Stop
        foreach ($m in $latObj.value) {
            $name = $m.name.value
            $ts = $m.timeseries | Select-Object -First 1
            if (-not $ts -or -not $ts.data) { continue }
            foreach ($dp in $ts.data) {
                $val = _SafeProp $dp 'average'
                if ($null -ne $val) {
                    if ($name -eq "SuccessE2ELatency")    { $sa["e2e_latency_ms"]    = [math]::Round($val, 1) }
                    if ($name -eq "SuccessServerLatency")  { $sa["server_latency_ms"] = [math]::Round($val, 1) }
                    if ($name -eq "Availability")          { $sa["availability_pct"]  = [math]::Round($val, 2) }
                    break
                }
            }
        }

        # Direct blob enumeration — Azure Monitor UsedCapacity has up to 24h reporting delay
        $blobSizeRaw = az storage blob list `
            --account-name $StorageAccountName `
            --account-key $StorageAccountKey `
            --container-name warehouse `
            --query "[].properties.contentLength" -o tsv 2>$null
        $totalBytes = 0
        if ($blobSizeRaw) {
            foreach ($line in ($blobSizeRaw -split "`n")) {
                $trimmed = $line.Trim()
                if ($trimmed -match '^\d+$') { $totalBytes += [long]$trimmed }
            }
        }
        $sa["used_capacity_gib"] = [math]::Round($totalBytes / 1GB, 3)
    } catch {
        Write-SubStep "  (Azure Storage metrics unavailable: $($_.Exception.Message))"
    }
    return $sa
}

# IMPROVED: Run-TrinoBenchmark — parallel Q1-Q5 per iteration
function Run-TrinoBenchmark([int]$iterations = 5) {
    Write-SubStep "  Running Trino benchmark ($iterations iterations, 5 queries, parallel per iteration)..."
    $failures = 0

    $ns  = $TrinoNamespace
    $pod = if ($script:TrinoPodName) { $script:TrinoPodName } else { "deploy/trino-standalone-trino-coordinator" }

    $benchQueries = @(
        @{ Key="q1"; Sql="SELECT COUNT(*) FROM paimon.iot.sensor_data_raw" },
        @{ Key="q2"; Sql="SELECT home_id, plug_id, AVG(voltage) AS avg_v, AVG(current_amp) AS avg_i, MAX(voltage * current_amp * power_factor) AS peak_power_w, COUNT(*) AS samples FROM paimon.iot.sensor_data_raw GROUP BY home_id, plug_id ORDER BY peak_power_w DESC LIMIT 50" },
        @{ Key="q3"; Sql="SELECT home_id, ts, voltage, AVG(voltage) OVER (PARTITION BY home_id ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS rolling_avg FROM paimon.iot.sensor_data_raw WHERE plug_id = 'general' ORDER BY ts DESC LIMIT 100" },
        @{ Key="q4"; Sql="SELECT r.home_id, COUNT(*) AS raw_samples, t.energy_kwh AS tenmin_energy, AVG(r.voltage) AS raw_avg_voltage, t.avg_voltage AS tenmin_avg_voltage, ABS(AVG(r.voltage) - t.avg_voltage) AS voltage_drift FROM paimon.iot.sensor_data_raw r JOIN paimon.iot.sensor_data_10min t ON r.home_id = t.home_id AND r.plug_id = t.plug_id AND r.dt = t.dt WHERE r.plug_id = 'general' AND t.samples >= (SELECT CAST(MAX(samples) AS DOUBLE) * 0.8 FROM paimon.iot.sensor_data_10min) GROUP BY r.home_id, t.energy_kwh, t.avg_voltage ORDER BY voltage_drift DESC LIMIT 20" },
        @{ Key="q5"; Sql="SELECT home_id, approx_percentile(voltage, 0.5) AS p50_voltage, approx_percentile(voltage, 0.95) AS p95_voltage, approx_percentile(voltage, 0.99) AS p99_voltage, stddev(voltage) AS voltage_stddev FROM paimon.iot.sensor_data_raw WHERE plug_id = 'general' GROUP BY home_id ORDER BY voltage_stddev DESC LIMIT 20" }
    )

    $allTimes = @{ q1=@(); q2=@(); q3=@(); q4=@(); q5=@() }

    for ($i = 0; $i -lt $iterations; $i++) {
        Write-SubStep ("  [benchmark iteration $($i+1)/$iterations — parallel Q1-Q5]")

        # Launch all 5 queries in parallel
        $iterJobs = @()
        foreach ($q in $benchQueries) {
            $iterJobs += @{
                Key = $q.Key
                Job = Start-Job -ScriptBlock {
                    param($sql, $ns, $pod)
                    $sw = [System.Diagnostics.Stopwatch]::StartNew()
                    try {
                        $raw = kubectl exec -n $ns $pod -- trino --execute $sql 2>&1
                        $sw.Stop()
                        $cleaned = @($raw | Where-Object {
                            $_ -notmatch 'org\.jline' -and $_ -notmatch '^WARNING:' -and
                            $_ -notmatch '^\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2}'
                        })
                        $result = ($cleaned -join "`n").Trim().Trim('"')
                        $failed = ($result -like "*error*" -or $result -like "*FAILED*")
                        @{ elapsed_s = $sw.Elapsed.TotalSeconds; failed = $failed }
                    } catch {
                        $sw.Stop()
                        @{ elapsed_s = $sw.Elapsed.TotalSeconds; failed = $true }
                    }
                } -ArgumentList $q.Sql, $ns, $pod
            }
        }

        # Collect results
        foreach ($j in $iterJobs) {
            $j.Job | Wait-Job | Out-Null
            $r = Receive-Job $j.Job
            Remove-Job $j.Job
            if ($r.failed) {
                $failures++
            } else {
                $allTimes[$j.Key] += $r.elapsed_s
            }
        }

        if ($i -lt ($iterations - 1)) { Start-Sleep -Seconds 5 }
    }

    function Get-P95([double[]]$arr) {
        if ($arr.Count -eq 0) { return 0 }
        $sorted = $arr | Sort-Object
        $idx = [math]::Ceiling($sorted.Count * 0.95) - 1
        if ($idx -lt 0) { $idx = 0 }
        return [math]::Round($sorted[$idx], 2)
    }

    return @{
        q1_p95_s  = Get-P95 $allTimes["q1"]
        q2_p95_s  = Get-P95 $allTimes["q2"]
        q3_p95_s  = Get-P95 $allTimes["q3"]
        q4_p95_s  = Get-P95 $allTimes["q4"]
        q5_p95_s  = Get-P95 $allTimes["q5"]
        failures  = $failures
    }
}

function Parse-GoProducerStats([string]$appLabel) {
    <#
      Parses the LAST [stats] and [cycle] line per producer pod, then sums
      across pods. The Go simulator emits cumulative counters each minute
      (ok/err = since pod start, rps = rate over the last 60s), so summing
      every log line over-reports by 8-10x. Take last-per-pod, sum across pods.
    #>
    $result = @{
        observed_rps  = ""   # Producer-reported throughput in last 60s, summed across pods
        avg_cycle_s   = ""
        max_cycle_s   = ""
        saturated     = ""
        ok_delta      = ""   # Cumulative OK from last stats line per pod, summed
        err_delta     = ""
        error_rate    = ""
    }
    try {
        # Use space-separated jsonpath — embedded {"\n"} survives PS->kubectl
        # arg passing poorly on Windows (the inner double quotes get mangled),
        # which made kubectl return nothing and we reported "no pods found".
        $podsRaw = kubectl get pods -l "app=${appLabel}" -n $KafkaNamespace `
            -o jsonpath='{.items[*].metadata.name}' 2>$null
        $podNames = @(($podsRaw | Out-String) -split '\s+' | Where-Object { $_ -and $_.Trim() })
        if (-not $podNames -or $podNames.Count -eq 0) {
            Write-SubStep "  (no pods found for ${appLabel} — cannot parse producer stats)"
            return $result
        }

        $totalRps = 0.0; $totalOk = [int64]0; $totalErr = [int64]0
        $maxAvg = 0.0; $maxMax = 0.0; $anySaturated = $false

        foreach ($pod in $podNames) {
            $pod = $pod.Trim()
            # 30 lines easily covers the last [stats]/[cycle] pair even if there
            # are interspersed startup logs.
            $raw = kubectl logs $pod -n $KafkaNamespace --tail=30 2>&1

            $lastStats = ($raw | Select-String "\[stats\]" | Select-Object -Last 1)
            if ($lastStats -and ($lastStats.Line -match 'ok=(\d+)\s+err=(\d+)\s+total=(\d+)\s+rps=([\d.]+)')) {
                $totalOk  += [int64]$Matches[1]
                $totalErr += [int64]$Matches[2]
                $totalRps += [double]$Matches[4]
            }

            $lastCycle = ($raw | Select-String "\[cycle\]" | Select-Object -Last 1)
            if ($lastCycle -and ($lastCycle.Line -match 'avg=([\d.]+\w*)\s+max=([\d.]+\w*).*saturated=(\w+)')) {
                $avgStr = $Matches[1]; $maxStr = $Matches[2]; $sat = $Matches[3]
                $avgVal = 0.0; $maxVal = 0.0
                if     ($avgStr -match '^([\d.]+)ms$') { $avgVal = [double]$Matches[1] / 1000 }
                elseif ($avgStr -match '^([\d.]+)s$')  { $avgVal = [double]$Matches[1] }
                elseif ($avgStr -match '^([\d.]+)$')   { $avgVal = [double]$Matches[1] }
                if     ($maxStr -match '^([\d.]+)ms$') { $maxVal = [double]$Matches[1] / 1000 }
                elseif ($maxStr -match '^([\d.]+)s$')  { $maxVal = [double]$Matches[1] }
                elseif ($maxStr -match '^([\d.]+)$')   { $maxVal = [double]$Matches[1] }
                if ($avgVal -gt $maxAvg) { $maxAvg = $avgVal }
                if ($maxVal -gt $maxMax) { $maxMax = $maxVal }
                if ($sat -eq 'true') { $anySaturated = $true }
            }
        }

        $result.observed_rps = [math]::Round($totalRps, 0)
        $result.ok_delta     = $totalOk
        $result.err_delta    = $totalErr
        $result.error_rate   = if (($totalOk + $totalErr) -gt 0) {
            [math]::Round($totalErr / ($totalOk + $totalErr), 6)
        } else { 0 }
        $result.avg_cycle_s  = [math]::Round($maxAvg, 3)
        $result.max_cycle_s  = [math]::Round($maxMax, 3)
        $result.saturated    = if ($anySaturated) { "TRUE" } else { "FALSE" }
    } catch {
        Write-SubStep "  (Could not parse Go producer stats for ${appLabel}: $($_.Exception.Message))"
    }
    return $result
}

function Update-ExperimentMatrix {
    param(
        [int]$Homes,
        [string]$Phase,                # "nonbatched" or "batched"
        [int]$GoPods,
        [hashtable]$GoStats,
        [hashtable]$GranularProm,
        [hashtable]$CkptT0,
        [hashtable]$CkptT1,
        [hashtable]$AzSA,
        [hashtable]$TrinoBench
    )

    if (-not (Test-Path $ExperimentMatrixMd)) {
        Write-SubStep "  Experiment matrix file not found - skipping update."
        return
    }

    $runId = Get-RunId $Homes $Phase
    if (-not $runId) {
        Write-SubStep "  No tier mapping for $Homes homes - skipping matrix update."
        return
    }

    Write-Step "Updating experiment matrix for $runId ($Homes homes, $Phase)..."

    $homesPerPod = if ($GoPods -gt 0) { [math]::Floor($Homes / $GoPods) } else { $Homes }
    $expectedRps = $Homes * (1 + $PlugsPerHome)

    function Get-PodVal([hashtable]$podMap, [string]$podPattern, [double]$divisor = 1) {
        if (-not $podMap -or $podMap.Count -eq 0) { return "" }
        foreach ($key in $podMap.Keys) {
            if ($key -like $podPattern) {
                return [math]::Round($podMap[$key] / $divisor, 3)
            }
        }
        return ""
    }

    $flinkTmPods = @()
    if ($GranularProm["flink_cpu"]) {
        $flinkTmPods = @($GranularProm["flink_cpu"].Keys | Where-Object { $_ -like "flink-taskmanager*" } | Sort-Object)
    }

    $kafkaBrokers = @("kafka-0*", "kafka-1*")
    $kafka = @()
    foreach ($pattern in $kafkaBrokers) {
        $kafka += @{
            cpu    = Get-PodVal $GranularProm["kafka_cpu"] $pattern
            mem    = Get-PodVal $GranularProm["kafka_mem"] $pattern (1GB)
            netrx  = Get-PodVal $GranularProm["kafka_netrx"] $pattern (1MB)
            nettx  = Get-PodVal $GranularProm["kafka_nettx"] $pattern (1MB)
            pvc    = Get-PodVal $GranularProm["kafka_pvc"] "*kafka-0*" (1GB)
        }
    }
    if ($GranularProm["kafka_pvc"]) {
        $pvcKeys = @($GranularProm["kafka_pvc"].Keys | Sort-Object)
        for ($i = 0; $i -lt [math]::Min(2, $pvcKeys.Count); $i++) {
            $kafka[$i].pvc = [math]::Round($GranularProm["kafka_pvc"][$pvcKeys[$i]] / 1GB, 3)
        }
    }

    $jm = @{
        cpu   = Get-PodVal $GranularProm["flink_cpu"] "flink-jobmanager*"
        mem   = Get-PodVal $GranularProm["flink_mem"] "flink-jobmanager*" (1GB)
        netrx = Get-PodVal $GranularProm["flink_netrx"] "flink-jobmanager*" (1MB)
        nettx = Get-PodVal $GranularProm["flink_nettx"] "flink-jobmanager*" (1MB)
        fs    = Get-PodVal $GranularProm["flink_fs"] "flink-jobmanager*" (1GB)
    }

    $tms = @()
    for ($i = 0; $i -lt 2; $i++) {
        if ($i -lt $flinkTmPods.Count) {
            $pod = $flinkTmPods[$i]
            $tms += @{
                cpu   = Get-PodVal $GranularProm["flink_cpu"] "${pod}*"
                mem   = Get-PodVal $GranularProm["flink_mem"] "${pod}*" (1GB)
                netrx = Get-PodVal $GranularProm["flink_netrx"] "${pod}*" (1MB)
                nettx = Get-PodVal $GranularProm["flink_nettx"] "${pod}*" (1MB)
                fs    = Get-PodVal $GranularProm["flink_fs"] "${pod}*" (1GB)
            }
        } else {
            $tms += @{ cpu=""; mem=""; netrx=""; nettx=""; fs="" }
        }
    }

    $ckCompleted = $CkptT1.completed - $CkptT0.completed
    $ckFailed    = $CkptT1.failed    - $CkptT0.failed
    $ckRestored  = $CkptT1.restored  - $CkptT0.restored
    if ($ckCompleted -lt 0) {
        Write-Warning "Checkpoint completed delta is negative ($ckCompleted) in matrix update — using T1 absolute value"
        $ckCompleted = $CkptT1.completed
    }
    if ($ckFailed -lt 0) { $ckFailed = $CkptT1.failed }
    if ($ckRestored -lt 0) { $ckRestored = $CkptT1.restored }
    $failover    = if ($ckRestored -gt 0) { "Y" } else { "N" }

    $saTx   = if ($AzSA["transactions"])     { $AzSA["transactions"] }     else { "" }
    $saIn   = if ($AzSA["ingress_gib"])      { $AzSA["ingress_gib"] }      else { "" }
    $saOut  = if ($AzSA["egress_gib"])       { $AzSA["egress_gib"] }       else { "" }
    $saE2E  = if ($AzSA["e2e_latency_ms"])   { $AzSA["e2e_latency_ms"] }   else { "" }
    $saSrv  = if ($AzSA["server_latency_ms"]){ $AzSA["server_latency_ms"] } else { "" }
    $saAvl  = if ($AzSA["availability_pct"])  { $AzSA["availability_pct"] }  else { "" }
    $saCap  = if ($AzSA["used_capacity_gib"]) { $AzSA["used_capacity_gib"] } else { "" }

    $tQ1 = $TrinoBench.q1_p95_s
    $tQ2 = $TrinoBench.q2_p95_s
    $tQ3 = $TrinoBench.q3_p95_s
    $tQ4 = $TrinoBench.q4_p95_s
    $tQ5 = $TrinoBench.q5_p95_s
    $tFails = $TrinoBench.failures

    $cols = @(
        $runId, $Homes, $GoPods, $homesPerPod, $expectedRps,
        # Producer stats (single phase)
        $GoStats.observed_rps, $GoStats.avg_cycle_s,
        $GoStats.max_cycle_s, $GoStats.saturated,
        $GoStats.ok_delta, $GoStats.err_delta, $GoStats.error_rate,
        # Kafka
        $kafka[0].cpu, $kafka[0].mem, $kafka[0].netrx, $kafka[0].nettx, $kafka[0].pvc,
        $kafka[1].cpu, $kafka[1].mem, $kafka[1].netrx, $kafka[1].nettx, $kafka[1].pvc,
        # Flink JM
        $jm.cpu, $jm.mem, $jm.netrx, $jm.nettx, $jm.fs,
        # Flink TM-1 to TM-2 (cluster sized for 2 TaskManagers)
        $tms[0].cpu, $tms[0].mem, $tms[0].netrx, $tms[0].nettx, $tms[0].fs,
        $tms[1].cpu, $tms[1].mem, $tms[1].netrx, $tms[1].nettx, $tms[1].fs,
        # Checkpoints
        $ckCompleted, $ckFailed, $ckRestored, $failover,
        # Azure SA
        $saTx, $saIn, $saOut, $saE2E, $saSrv, $saAvl, $saCap,
        # Trino (5 queries)
        $tQ1, $tQ2, $tQ3, $tQ4, $tQ5, $tFails,
        ""
    )

    $newRow = "| " + ($cols -join " | ") + " |"

    $lines = Get-Content $ExperimentMatrixMd -Encoding utf8
    $pattern = "^\|\s*${runId}\s*\|"
    $replaced = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match $pattern) {
            $lines[$i] = $newRow
            $replaced = $true
            break
        }
    }

    if ($replaced) {
        $lines | Out-File -FilePath $ExperimentMatrixMd -Encoding utf8
        Write-SubStep "  Updated row $runId in experiment matrix."
    } else {
        Write-SubStep "  Could not find row $runId in matrix - skipping."
    }

    # Always keep the default (base) matrix file in sync
    if ($ExperimentMatrixMd -ne $DefaultMatrixMd -and (Test-Path $DefaultMatrixMd)) {
        $defaultLines = Get-Content $DefaultMatrixMd -Encoding utf8
        $defaultReplaced = $false
        for ($j = 0; $j -lt $defaultLines.Count; $j++) {
            if ($defaultLines[$j] -match $pattern) {
                $defaultLines[$j] = $newRow
                $defaultReplaced = $true
                break
            }
        }
        if ($defaultReplaced) {
            $defaultLines | Out-File -FilePath $DefaultMatrixMd -Encoding utf8
            Write-SubStep "  Also updated row $runId in default matrix."
        }
    }

    Upsert-MatrixWideCsvRow -rowValues $cols
    Write-SubStep "  Upserted row $runId in $(Join-Path $ResultsDir 'matrix-full.csv')."
}

# ─────────────────────────────────────────────────────────────
# Deploy data-processing pipeline with retry on Flink job failure
# Returns $true if Flink streaming job is stably RUNNING, $false otherwise
# ─────────────────────────────────────────────────────────────
function Invoke-DeployWithRetry([int]$maxAttempts = 3, [int]$healthGateTimeoutSec = 300) {
    $attemptTimeouts = @(60, 120, $healthGateTimeoutSec)
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        if ($attempt -gt 1) {
            Write-Step "RETRY ${attempt}/${maxAttempts}: Re-deploying data-processing pipeline..."
            # Delete the failed jobs so they can be re-created
            kubectl delete job paimon-bootstrap -n data-processing --ignore-not-found 2>$null | Out-Null
            kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
            # Cancel any lingering Flink jobs
            try {
                $jobsJson = Get-FlinkJobsJson
                $jobs = $jobsJson | ConvertFrom-Json -ErrorAction SilentlyContinue
                if ($jobs -and $jobs.jobs) {
                    foreach ($j in $jobs.jobs) {
                        if ($j.status -in @("RUNNING","CREATED","INITIALIZING","RECONCILING","FAILING")) {
                            kubectl exec -n data-processing deployment/flink-jobmanager -- `
                                curl -s -X PATCH "http://localhost:${FlinkRestPort}/jobs/$($j.id)?mode=cancel" 2>$null | Out-Null
                        }
                    }
                }
            } catch { }
            Start-Sleep -Seconds 10
            # Re-verify cluster readiness before retry
            Wait-FlinkClusterReady -timeoutSec 120
        }

        Deploy-DataProcessing

        $thisTimeout = $attemptTimeouts[[math]::Min($attempt - 1, $attemptTimeouts.Count - 1)]
        Write-SubStep "  Health gate timeout for attempt ${attempt}: ${thisTimeout}s"
        $flinkOk = Test-FlinkStreamingJobRunning -timeoutSec $thisTimeout -stabilityChecks 3 -stabilityIntervalSec 10
        if ($flinkOk) {
            return $true
        }
        Write-Warning "Flink streaming job not stably RUNNING on attempt ${attempt}/${maxAttempts}"
    }
    return $false
}

# ═════════════════════════════════════════════════════════════
#  MAIN LOOP — PER-TIER COMBINED: Nonbatched + Batched back-to-back
# ═════════════════════════════════════════════════════════════
$ExperimentMatrixMd = Resolve-ExperimentMatrixPath -explicitPath $ExperimentMatrixPath -fallbackPath $DefaultMatrixMd
Write-Step "Experiment matrix target: $ExperimentMatrixMd"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   LAKEHOUSE-ON-K8S EXPERIMENT RUNNER (COMBINED IMPROVED)         ║" -ForegroundColor Green
Write-Host "╠══════════════════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║  Tag            : $ExperimentTag" -ForegroundColor Green
Write-Host "║  Mode           : Per-tier combined (RunPhase=$RunPhase)" -ForegroundColor Green
Write-Host "║  Tiers          : $($Tiers -join ', ') homes" -ForegroundColor Green
Write-Host "║  Warm-up        : ${WarmupMinutes} min" -ForegroundColor Green
Write-Host "║  Run            : ${RunMinutes} min (per sub-phase)" -ForegroundColor Green
Write-Host "║  Flush cooldown : ${FlushCooldownSeconds}s" -ForegroundColor Green
Write-Host "║  Query suite    : Q1-Q5 (3 runs each, median reported, parallel)" -ForegroundColor Green
Write-Host "║  Results        : $ResultsDir" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

if (-not (Test-Path $ResultsDir)) {
    New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null
}

# Start a full transcript of console output for forensic / thesis traceability.
# Captures everything written to the host (Write-Host, Write-Warning, native command
# stdout, etc.) and writes it to a file inside $ResultsDir. Stopped at the end of
# the run (or whenever the PowerShell session ends).
$transcriptPath = Join-Path $ResultsDir "orchestrator-transcript.log"
try {
    Start-Transcript -Path $transcriptPath -Force | Out-Null
    Write-Host "Transcript active: $transcriptPath" -ForegroundColor DarkGray
} catch {
    Write-Warning ("Could not start transcript: {0}" -f $_.Exception.Message)
}

$csvPath = Join-Path $ResultsDir "summary.csv"

# Resolve storage account key once so all az storage commands use --account-key.
Initialize-StorageAccountKey

# Pre-run validation
Test-PreRunHealth | Out-Null

# ─────────────────────────────────────────────────────────────
#  PER-TIER LOOP
# ─────────────────────────────────────────────────────────────
$script:CurrentTierHomes = 0
$tierIndex = 0
foreach ($homes in $Tiers) {
    $tierIndex++
    $script:CurrentTierHomes = $homes
    $experimentLabel = "${homes}h"
    $nbYamlFile      = Join-Path $ExperimentsDir "go-iot-sim-${homes}h.yaml"
    $bYamlFile       = Join-Path $ExperimentsDir "go-batched-iot-sim-${homes}h.yaml"

    # Result files
    $nbResultFile    = Join-Path $ResultsDir "tier-${homes}h-nonbatched.txt"
    $nbT0File        = Join-Path $ResultsDir "tier-${homes}h-nonbatched-t0.txt"
    $bResultFile     = Join-Path $ResultsDir "tier-${homes}h-batched.txt"
    $bT0File         = Join-Path $ResultsDir "tier-${homes}h-batched-t0.txt"

    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor Green
    Write-Host "  TIER $tierIndex / $($Tiers.Count) :  $homes homes  ($experimentLabel)" -ForegroundColor Green
    Write-Host ("=" * 70) -ForegroundColor Green

    if ($RunPhase -ne "batched") {
    # ══════════════════════════════════════════════════════
    #  SUB-PHASE A: NONBATCHED PRODUCERS (full reset)
    # ══════════════════════════════════════════════════════
    Write-Host ""
    Write-Host ("─" * 70) -ForegroundColor Magenta
    Write-Host "  SUB-PHASE A — NONBATCHED  |  $homes homes" -ForegroundColor Magenta
    Write-Host ("─" * 70) -ForegroundColor Magenta

    # ── A1. Full environment reset + deploy pipeline ──────
    $healthGateTimeout = if ($homes -ge 10000) { 600 } else { 300 }
    if (-not $SkipCleanup) {
        Reset-Environment
    }
    $flinkOk = Invoke-DeployWithRetry -maxAttempts 3 -healthGateTimeoutSec $healthGateTimeout
    if (-not $flinkOk) {
        Write-Host "  SKIPPING tier $homes (nonbatched) — Flink streaming job not RUNNING after all retries" -ForegroundColor Red
        continue
    }

    # ── A2. Generate & deploy nonbatched producers ────────
    Write-Step "Generating nonbatched experiment YAML for $homes homes..."
    & $GenerateGoScript $homes -Registry $AcrRegistry
    if (-not (Test-Path $nbYamlFile)) {
        Write-Error "Nonbatched YAML file not generated: $nbYamlFile"
        continue
    }
    Write-Step "Deploying Go nonbatched IoT simulator pods..."
    kubectl apply -f $nbYamlFile 2>&1 | ForEach-Object { Write-SubStep $_ }
    Start-Sleep -Seconds 5
    Wait-ForPodReady "app=go-iot-sim-producer,experiment=${experimentLabel}" $KafkaNamespace 300

    # ── A3. Warm-up ───────────────────────────────────────
    Write-Step "Nonbatched warm-up: waiting ${WarmupMinutes} minutes..."
    $warmupEnd = (Get-Date).AddMinutes($WarmupMinutes)
    while ((Get-Date) -lt $warmupEnd) {
        $remaining = [math]::Ceiling(($warmupEnd - (Get-Date)).TotalSeconds)
        Write-Host ("`r    Warm-up remaining: {0}s  " -f $remaining) -NoNewline
        Start-Sleep -Seconds 10
    }
    Write-Host ""

    # ── A4. T0 baseline + checkpoint T0 (Flink RUNNING) ──
    Write-Step "Collecting T0 baseline metrics (nonbatched)..."
    $nbT0 = Collect-Metrics $homes "T0-baseline" "nonbatched"
    Write-MetricsToFile $nbT0 $nbT0File
    Write-SubStep "T0 Kafka total offset: $($nbT0['kafka_total_offset'])"
    Write-SubStep "T0 Paimon raw rows:    $($nbT0['trino_raw_count'])"

    # Verify Paimon has data after warmup — recover if Flink died
    if ($nbT0['trino_raw_count'] -eq "0" -or $nbT0['trino_raw_count'] -eq "") {
        Write-Host "  WARNING: Paimon raw rows at T0 is 0 — checking Flink job status..." -ForegroundColor Yellow
        $flinkStillOk = Test-FlinkStreamingJobRunning -timeoutSec 30 -stabilityChecks 1 -stabilityIntervalSec 5
        if (-not $flinkStillOk) {
            Write-Host "  Flink streaming job DIED during warmup — attempting recovery..." -ForegroundColor Red
            kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
            Start-Sleep -Seconds 5
            Wait-FlinkClusterReady -timeoutSec 120
            $recoveryOk = Invoke-DeployWithRetry -maxAttempts 2
            if (-not $recoveryOk) {
                Write-Host "  SKIPPING tier $homes (nonbatched) — Flink recovery failed" -ForegroundColor Red
                kubectl delete -f $nbYamlFile --ignore-not-found 2>$null | Out-Null
                continue
            }
            Write-Step "Post-recovery warmup: waiting 3 minutes..."
            $recoveryEnd = (Get-Date).AddMinutes(3)
            while ((Get-Date) -lt $recoveryEnd) {
                $remaining = [math]::Ceiling(($recoveryEnd - (Get-Date)).TotalSeconds)
                Write-Host ("`r    Recovery warmup remaining: {0}s  " -f $remaining) -NoNewline
                Start-Sleep -Seconds 10
            }
            Write-Host ""
            Write-Step "Re-collecting T0 baseline after Flink recovery..."
            $nbT0 = Collect-Metrics $homes "T0-baseline-recovered" "nonbatched"
            Write-MetricsToFile $nbT0 $nbT0File
            Write-SubStep "T0 Kafka total offset: $($nbT0['kafka_total_offset'])"
            Write-SubStep "T0 Paimon raw rows:    $($nbT0['trino_raw_count'])"
        }
    }

    # Fix 5: Skip sub-phase if T0 shows OOM indicators
    $t0RawVal = "$($nbT0['trino_raw_count'])"
    if ($t0RawVal -match "OOM_EXIT_137|exit code|terminated|container not found") {
        Write-Host "  SKIPPING tier $homes (nonbatched) — T0 collection failed with OOM/pod failure" -ForegroundColor Red
        kubectl delete -f $nbYamlFile --ignore-not-found 2>$null | Out-Null
        continue
    }

    $nbCkptT0 = Get-FlinkCheckpointCounts
    $nbFlinkJobId = $nbCkptT0.job_id
    Write-SubStep "Flink job ID for checkpoint tracking: $nbFlinkJobId"
    $nbRestartT0 = Get-PodRestartCounts
    $nbMeasureStartUtc = (Get-Date).ToUniversalTime()

    # ── A5. Steady-state run (30 min) with periodic Flink health checks ──
    # Tenmin aggregation schedule: fire at run-elapsed minutes 10, 20, ..., RunMinutes.
    # For the default RunMinutes=30 this is [10, 20, 30] — three in-run firings to
    # exercise streaming + batch coexistence on Flink slots (slot guard inside
    # Invoke-DuringRunTenmin skips a firing if a previous batch is still RUNNING).
    Write-Step "Nonbatched run: ${RunMinutes} minutes..."
    $runEnd = (Get-Date).AddMinutes($RunMinutes)
    $lastHealthCheck = Get-Date
    $healthCheckIntervalSec = 300
    $flinkDiedDuringRun = $false
    $tenminSchedule = @()
    for ($_m = 10; $_m -le $RunMinutes; $_m += 10) { $tenminSchedule += $_m }
    $tenminFiredAt = @{}
    while ((Get-Date) -lt $runEnd) {
        $remaining = [math]::Ceiling(($runEnd - (Get-Date)).TotalMinutes)
        Write-Host ("`r    Run remaining: ~{0} min  " -f $remaining) -NoNewline
        $elapsedMin = ((Get-Date).ToUniversalTime() - $nbMeasureStartUtc).TotalMinutes
        foreach ($fireAt in $tenminSchedule) {
            if (-not $tenminFiredAt.ContainsKey($fireAt) -and $elapsedMin -ge $fireAt) {
                $tenminFiredAt[$fireAt] = $true
                Write-Host ""
                Invoke-DuringRunTenmin "${experimentLabel}-nb" $fireAt | Out-Null
            }
        }
        if (((Get-Date) - $lastHealthCheck).TotalSeconds -ge $healthCheckIntervalSec) {
            $lastHealthCheck = Get-Date
            try {
                $jobsRaw = Get-FlinkJobsJson
                $jobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                $running = @()
                if ($jobs -and $jobs.jobs) {
                    $running = @($jobs.jobs | Where-Object { $_.status -eq "RUNNING" })
                }
                if ($running.Count -eq 0) {
                    Write-Host ""
                    Write-Host "  CRITICAL: Flink streaming job DIED during run — attempting recovery..." -ForegroundColor Red
                    # Cancel any residual Flink jobs before re-submitting
                    Write-SubStep "  Cancelling existing Flink jobs before recovery..."
                    try {
                        $allJobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                        if ($allJobs -and $allJobs.jobs) {
                            foreach ($fj in $allJobs.jobs) {
                                if ($fj.status -notin @("CANCELED","FINISHED")) {
                                    kubectl exec -n data-processing deployment/flink-jobmanager -- `
                                        curl -s -X PATCH "http://localhost:${FlinkRestPort}/jobs/$($fj.id)?mode=cancel" 2>$null | Out-Null
                                }
                            }
                        }
                    } catch { }
                    kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
                    Start-Sleep -Seconds 5
                    Wait-FlinkClusterReady -timeoutSec 120
                    $recoveryOk = Invoke-DeployWithRetry -maxAttempts 2 -healthGateTimeoutSec $healthGateTimeout
                    if ($recoveryOk) {
                        Write-Host "  Flink recovered — continuing run" -ForegroundColor Yellow
                        $nbFlinkJobId = (Get-FlinkCheckpointCounts).job_id
                    } else {
                        Write-Host "  Flink recovery FAILED — marking run as degraded" -ForegroundColor Red
                        $flinkDiedDuringRun = $true
                    }
                }
            } catch { }
        }
        Start-Sleep -Seconds 30
    }
    Write-Host ""

    # Catch-up: fire any tenmin schedule entries the loop missed due to sleep
    # granularity (typically the +RunMinutes one, since Floor of 29.9x = 29).
    # Skip if Flink died and recovery failed — firing batches at a dead cluster
    # would just produce errors.
    if (-not $flinkDiedDuringRun) {
        foreach ($fireAt in $tenminSchedule) {
            if (-not $tenminFiredAt.ContainsKey($fireAt)) {
                $tenminFiredAt[$fireAt] = $true
                Invoke-DuringRunTenmin "${experimentLabel}-nb" $fireAt | Out-Null
            }
        }
    }

    $nbRestartT1 = Get-PodRestartCounts
    $nbRestartDelta = [ordered]@{}
    foreach ($pod in $nbRestartT1.Keys) {
        $before = if ($nbRestartT0.Contains($pod)) { $nbRestartT0[$pod] } else { 0 }
        $delta = $nbRestartT1[$pod] - $before
        if ($delta -gt 0) { $nbRestartDelta[$pod] = $delta }
    }
    if ($nbRestartDelta.Count -gt 0) {
        Write-Warning "Pod restarts detected during nonbatched run:"
        foreach ($pod in $nbRestartDelta.Keys) { Write-Warning "  $pod : +$($nbRestartDelta[$pod])" }
    } else {
        Write-SubStep "No pod restarts during nonbatched run."
    }

    # ── A6. Parse nonbatched producer stats WHILE PODS ALIVE ──
    Write-Step "Parsing Go nonbatched producer stats (pods still running)..."
    $goStatsNonbatched = Parse-GoProducerStats "go-iot-sim-producer"
    Write-SubStep "Nonbatched producer RPS (last 60s): $($goStatsNonbatched.observed_rps)"
    Write-SubStep "Nonbatched avg cycle:               $($goStatsNonbatched.avg_cycle_s)s"
    Write-SubStep "Nonbatched saturated:               $($goStatsNonbatched.saturated)"
    Write-SubStep "Nonbatched cumulative OK / Errors:  $($goStatsNonbatched.ok_delta) / $($goStatsNonbatched.err_delta)"

    # ── A7. Collect T1 snapshot (producers still alive) ───
    Write-Step "Collecting T1 metrics snapshot (nonbatched, producers running)..."
    $nbT1 = Collect-Metrics $homes "T1-final" "nonbatched" -AfterTs $nbMeasureStartUtc.ToString("yyyy-MM-dd HH:mm:ss")

    # ── A8. Stop producers & flush cooldown ───────────────
    Write-Step "Stopping nonbatched producers and waiting ${FlushCooldownSeconds}s for Flink to flush..."
    kubectl delete -f $nbYamlFile --ignore-not-found 2>$null | Out-Null
    $cooldownEnd = (Get-Date).AddSeconds($FlushCooldownSeconds)
    while ((Get-Date) -lt $cooldownEnd) {
        $remaining = [math]::Ceiling(($cooldownEnd - (Get-Date)).TotalSeconds)
        Write-Host ("`r    Flush cooldown remaining: {0}s  " -f $remaining) -NoNewline
        Start-Sleep -Seconds 10
    }
    Write-Host ""

    # ── A9. Post-flush 30-minute aggregation ──────────────
    # Tenmin firings already happened in-run at +10/+20/+30; this step rolls up
    # the full window into sensor_data_30min for the per-tier summary metrics.
    Invoke-30minAggregation "${experimentLabel}-nb"
    Update-PostAggregationMetrics $nbT1
    # Persist T1 AFTER post-agg so the per-tier file reflects the final 30-min rollup.
    Write-MetricsToFile $nbT1 $nbResultFile

    # ── A10. Checkpoint T1 (Flink STILL RUNNING) ─────────
    Write-Step "Collecting Flink checkpoint counts (nonbatched T1)..."
    $nbCkptT1 = Get-FlinkCheckpointCounts -JobId $nbFlinkJobId
    Write-SubStep "Checkpoints completed (delta): $($nbCkptT1.completed - $nbCkptT0.completed)"
    Write-SubStep "Checkpoints failed (delta):    $($nbCkptT1.failed    - $nbCkptT0.failed)"
    Write-SubStep "Checkpoints restored (delta):  $($nbCkptT1.restored  - $nbCkptT0.restored)"

    # ── A11. Granular Prometheus (covers this 30-min run) ─
    $nbMeasureEndUtc = (Get-Date).ToUniversalTime()
    $nbElapsedMin = [math]::Ceiling(($nbMeasureEndUtc - $nbMeasureStartUtc).TotalMinutes)
    Write-Step "Collecting granular Prometheus averages (window: ${nbElapsedMin}m)..."
    $nbGranularProm = Collect-GranularPrometheus -windowMinutes $nbElapsedMin

    # ── A12. Azure Storage Account metrics ────────────────
    Write-Step "Collecting Azure Storage Account metrics (nonbatched window)..."
    $nbAzSA = Get-AzureStorageMetrics $nbMeasureStartUtc $nbMeasureEndUtc

    # ── A13. Trino benchmark ──────────────────────────────
    Write-Step "Running Trino benchmark queries (5 queries x 5 iterations)..."
    $nbTrinoBench = Run-TrinoBenchmark -iterations 5

    Write-Step "Persisting nonbatched phase artifacts..."
    Write-PhaseArtifacts -Homes $homes -Phase "nonbatched" -ProducerType "nonbatched" -GoStats $goStatsNonbatched -GranularProm $nbGranularProm -CkptT0 $nbCkptT0 -CkptT1 $nbCkptT1 -AzSA $nbAzSA -TrinoBench $nbTrinoBench -RestartDelta $nbRestartDelta

    # ── A14. Verify Paimon data ───────────────────────────
    if ($nbT1['trino_raw_count'] -eq "0" -or $nbT1['trino_raw_count'] -eq "") {
        Write-Host "  WARNING: Paimon raw rows at T1 is STILL 0 — data pipeline issue!" -ForegroundColor Red
    }

    # ── A15. Summary ──────────────────────────────────────
    $nbKafkaDelta = [int64]$nbT1["kafka_total_offset"] - [int64]$nbT0["kafka_total_offset"]
    $nbRawT1 = 0; $nbRawT0 = 0
    [int64]::TryParse(($nbT1["trino_raw_count"] -replace '[^\d]',''), [ref]$nbRawT1) | Out-Null
    [int64]::TryParse(($nbT0["trino_raw_count"] -replace '[^\d]',''), [ref]$nbRawT0) | Out-Null
    $nbPaimonDelta = $nbRawT1 - $nbRawT0
    $nbActualRps   = [math]::Round($nbKafkaDelta / ($RunMinutes * 60), 1)
    $nbExpectedRps = $homes * (1 + $PlugsPerHome)

    Write-Step "Tier $homes NONBATCHED summary:"
    Write-SubStep "Expected RPS:            $nbExpectedRps  ($homes homes x (1 general + $PlugsPerHome plugs) readings/sec)"
    Write-SubStep "Producer RPS (last 60s): $($goStatsNonbatched.observed_rps)  (sum of last [stats] line per pod, simulator-side)"
    Write-SubStep "Kafka ingest RPS:        $nbActualRps  (Kafka offset delta / $RunMinutes min)"
    Write-SubStep "Avg cycle / saturated:   $($goStatsNonbatched.avg_cycle_s)s / $($goStatsNonbatched.saturated)"
    Write-SubStep "Producer cumulative OK / Errors: $($goStatsNonbatched.ok_delta) / $($goStatsNonbatched.err_delta)"
    Write-SubStep "Kafka messages (delta):  $nbKafkaDelta"
    Write-SubStep "Paimon raw rows (delta): $nbPaimonDelta"
    Write-SubStep "Flink running jobs:      $($nbT1['flink_running_jobs'])"
    Write-SubStep "E2E latency:             $($nbT1['e2e_latency_seconds'])s"
    if ($flinkDiedDuringRun) {
        Write-Host "  WARNING: Flink died and recovery failed during this run — results are DEGRADED" -ForegroundColor Red
    }

    Append-SummaryCsv $nbT0 $nbT1 $csvPath

    # ── A16. Update nonbatched matrix row (fresh data) ────
    $goPods = [math]::Ceiling($homes / $HomesPerPod)
    Update-ExperimentMatrix `
        -Homes              $homes `
        -Phase              "nonbatched" `
        -GoPods             $goPods `
        -GoStats            $goStatsNonbatched `
        -GranularProm       $nbGranularProm `
        -CkptT0             $nbCkptT0 `
        -CkptT1             $nbCkptT1 `
        -AzSA               $nbAzSA `
        -TrinoBench         $nbTrinoBench

    Write-Host ""
    Write-Host "  SUB-PHASE A (nonbatched) COMPLETE for tier $homes." -ForegroundColor Green
    } # end sub-phase A

    if ($RunPhase -ne "nonbatched") {
    # ══════════════════════════════════════════════════════
    #  SUB-PHASE B: BATCHED PRODUCERS (full reset)
    # ══════════════════════════════════════════════════════
    Write-Host ""
    Write-Host ("─" * 70) -ForegroundColor Magenta
    Write-Host "  SUB-PHASE B — BATCHED  |  $homes homes" -ForegroundColor Magenta
    Write-Host ("─" * 70) -ForegroundColor Magenta

    # ── B1. Full environment reset + deploy pipeline ──────
    if (-not $SkipCleanup) {
        Reset-Environment
    }
    $flinkOk = Invoke-DeployWithRetry -maxAttempts 3 -healthGateTimeoutSec $healthGateTimeout
    if (-not $flinkOk) {
        Write-Host "  SKIPPING tier $homes (batched) — Flink streaming job not RUNNING after all retries" -ForegroundColor Red
        if ($tierIndex -lt $Tiers.Count) { Start-Sleep -Seconds 30 }
        continue
    }

    # ── B2. Generate & deploy batched producers ───────────
    Write-Step "Generating batched experiment YAML for $homes homes..."
    & $GenerateGoBatchedScript $homes -Registry $AcrRegistry
    if (-not (Test-Path $bYamlFile)) {
        Write-Error "Batched YAML file not generated: $bYamlFile"
        continue
    }
    Write-Step "Deploying Go batched IoT simulator pods..."
    kubectl apply -f $bYamlFile 2>&1 | ForEach-Object { Write-SubStep $_ }
    Start-Sleep -Seconds 5
    Wait-ForPodReady "app=go-iot-sim-producer-batched,experiment=${experimentLabel}" $KafkaNamespace 300

    # ── B3. Warm-up ───────────────────────────────────────
    Write-Step "Batched warm-up: waiting ${WarmupMinutes} minutes..."
    $warmupEnd = (Get-Date).AddMinutes($WarmupMinutes)
    while ((Get-Date) -lt $warmupEnd) {
        $remaining = [math]::Ceiling(($warmupEnd - (Get-Date)).TotalSeconds)
        Write-Host ("`r    Warm-up remaining: {0}s  " -f $remaining) -NoNewline
        Start-Sleep -Seconds 10
    }
    Write-Host ""

    # ── B4. T0 baseline + checkpoint T0 (Flink RUNNING) ──
    Write-Step "Collecting T0 baseline metrics (batched)..."
    $bT0 = Collect-Metrics $homes "T0-baseline" "batched"
    Write-MetricsToFile $bT0 $bT0File
    Write-SubStep "T0 Kafka total offset: $($bT0['kafka_total_offset'])"
    Write-SubStep "T0 Paimon raw rows:    $($bT0['trino_raw_count'])"

    # Verify Paimon has data after warmup — recover if Flink died
    if ($bT0['trino_raw_count'] -eq "0" -or $bT0['trino_raw_count'] -eq "") {
        Write-Host "  WARNING: Paimon raw rows at T0 is 0 — checking Flink job status..." -ForegroundColor Yellow
        $flinkStillOk = Test-FlinkStreamingJobRunning -timeoutSec 30 -stabilityChecks 1 -stabilityIntervalSec 5
        if (-not $flinkStillOk) {
            Write-Host "  Flink streaming job DIED during warmup — attempting recovery..." -ForegroundColor Red
            kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
            Start-Sleep -Seconds 5
            Wait-FlinkClusterReady -timeoutSec 120
            $recoveryOk = Invoke-DeployWithRetry -maxAttempts 2
            if (-not $recoveryOk) {
                Write-Host "  SKIPPING tier $homes (batched) — Flink recovery failed" -ForegroundColor Red
                kubectl delete -f $bYamlFile --ignore-not-found 2>$null | Out-Null
                if ($tierIndex -lt $Tiers.Count) { Start-Sleep -Seconds 30 }
                continue
            }
            Write-Step "Post-recovery warmup: waiting 3 minutes..."
            $recoveryEnd = (Get-Date).AddMinutes(3)
            while ((Get-Date) -lt $recoveryEnd) {
                $remaining = [math]::Ceiling(($recoveryEnd - (Get-Date)).TotalSeconds)
                Write-Host ("`r    Recovery warmup remaining: {0}s  " -f $remaining) -NoNewline
                Start-Sleep -Seconds 10
            }
            Write-Host ""
            Write-Step "Re-collecting T0 baseline after Flink recovery..."
            $bT0 = Collect-Metrics $homes "T0-baseline-recovered" "batched"
            Write-MetricsToFile $bT0 $bT0File
            Write-SubStep "T0 Kafka total offset: $($bT0['kafka_total_offset'])"
            Write-SubStep "T0 Paimon raw rows:    $($bT0['trino_raw_count'])"
        }
    }

    # Fix 5: Skip sub-phase if T0 shows OOM indicators
    $t0RawVal = "$($bT0['trino_raw_count'])"
    if ($t0RawVal -match "OOM_EXIT_137|exit code|terminated|container not found") {
        Write-Host "  SKIPPING tier $homes (batched) — T0 collection failed with OOM/pod failure" -ForegroundColor Red
        kubectl delete -f $bYamlFile --ignore-not-found 2>$null | Out-Null
        if ($tierIndex -lt $Tiers.Count) { Start-Sleep -Seconds 30 }
        continue
    }

    $bCkptT0 = Get-FlinkCheckpointCounts
    $bFlinkJobId = $bCkptT0.job_id
    Write-SubStep "Flink job ID for checkpoint tracking: $bFlinkJobId"
    $bRestartT0 = Get-PodRestartCounts
    $bMeasureStartUtc = (Get-Date).ToUniversalTime()

    # ── B5. Steady-state run (30 min) with periodic Flink health checks ──
    # See sub-phase A for tenmin schedule rationale.
    Write-Step "Batched run: ${RunMinutes} minutes..."
    $runEnd = (Get-Date).AddMinutes($RunMinutes)
    $lastHealthCheck = Get-Date
    $healthCheckIntervalSec = 300
    $flinkDiedDuringRun = $false
    $tenminSchedule = @()
    for ($_m = 10; $_m -le $RunMinutes; $_m += 10) { $tenminSchedule += $_m }
    $tenminFiredAt = @{}
    while ((Get-Date) -lt $runEnd) {
        $remaining = [math]::Ceiling(($runEnd - (Get-Date)).TotalMinutes)
        Write-Host ("`r    Run remaining: ~{0} min  " -f $remaining) -NoNewline
        $elapsedMin = ((Get-Date).ToUniversalTime() - $bMeasureStartUtc).TotalMinutes
        foreach ($fireAt in $tenminSchedule) {
            if (-not $tenminFiredAt.ContainsKey($fireAt) -and $elapsedMin -ge $fireAt) {
                $tenminFiredAt[$fireAt] = $true
                Write-Host ""
                Invoke-DuringRunTenmin "${experimentLabel}-b" $fireAt | Out-Null
            }
        }
        if (((Get-Date) - $lastHealthCheck).TotalSeconds -ge $healthCheckIntervalSec) {
            $lastHealthCheck = Get-Date
            try {
                $jobsRaw = Get-FlinkJobsJson
                $jobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                $running = @()
                if ($jobs -and $jobs.jobs) {
                    $running = @($jobs.jobs | Where-Object { $_.status -eq "RUNNING" })
                }
                if ($running.Count -eq 0) {
                    Write-Host ""
                    Write-Host "  CRITICAL: Flink streaming job DIED during run — attempting recovery..." -ForegroundColor Red
                    # Cancel any residual Flink jobs before re-submitting
                    Write-SubStep "  Cancelling existing Flink jobs before recovery..."
                    try {
                        $allJobs = $jobsRaw | ConvertFrom-Json -ErrorAction SilentlyContinue
                        if ($allJobs -and $allJobs.jobs) {
                            foreach ($fj in $allJobs.jobs) {
                                if ($fj.status -notin @("CANCELED","FINISHED")) {
                                    kubectl exec -n data-processing deployment/flink-jobmanager -- `
                                        curl -s -X PATCH "http://localhost:${FlinkRestPort}/jobs/$($fj.id)?mode=cancel" 2>$null | Out-Null
                                }
                            }
                        }
                    } catch { }
                    kubectl delete job flink-submit-kafka-to-paimon -n data-processing --ignore-not-found 2>$null | Out-Null
                    Start-Sleep -Seconds 5
                    Wait-FlinkClusterReady -timeoutSec 120
                    $recoveryOk = Invoke-DeployWithRetry -maxAttempts 2 -healthGateTimeoutSec $healthGateTimeout
                    if ($recoveryOk) {
                        Write-Host "  Flink recovered — continuing run" -ForegroundColor Yellow
                        $bFlinkJobId = (Get-FlinkCheckpointCounts).job_id
                    } else {
                        Write-Host "  Flink recovery FAILED — marking run as degraded" -ForegroundColor Red
                        $flinkDiedDuringRun = $true
                    }
                }
            } catch { }
        }
        Start-Sleep -Seconds 30
    }
    Write-Host ""

    # Catch-up: see sub-phase A for rationale.
    if (-not $flinkDiedDuringRun) {
        foreach ($fireAt in $tenminSchedule) {
            if (-not $tenminFiredAt.ContainsKey($fireAt)) {
                $tenminFiredAt[$fireAt] = $true
                Invoke-DuringRunTenmin "${experimentLabel}-b" $fireAt | Out-Null
            }
        }
    }

    $bRestartT1 = Get-PodRestartCounts
    $bRestartDelta = [ordered]@{}
    foreach ($pod in $bRestartT1.Keys) {
        $before = if ($bRestartT0.Contains($pod)) { $bRestartT0[$pod] } else { 0 }
        $delta = $bRestartT1[$pod] - $before
        if ($delta -gt 0) { $bRestartDelta[$pod] = $delta }
    }
    if ($bRestartDelta.Count -gt 0) {
        Write-Warning "Pod restarts detected during batched run:"
        foreach ($pod in $bRestartDelta.Keys) { Write-Warning "  $pod : +$($bRestartDelta[$pod])" }
    } else {
        Write-SubStep "No pod restarts during batched run."
    }

    # ── B6. Parse batched producer stats WHILE PODS ALIVE ──
    Write-Step "Parsing Go batched producer stats (pods still running)..."
    $goStatsBatched = Parse-GoProducerStats "go-iot-sim-producer-batched"
    Write-SubStep "Batched producer RPS (last 60s): $($goStatsBatched.observed_rps)"
    Write-SubStep "Batched avg cycle:               $($goStatsBatched.avg_cycle_s)s"
    Write-SubStep "Batched saturated:               $($goStatsBatched.saturated)"
    Write-SubStep "Batched cumulative OK / Errors:  $($goStatsBatched.ok_delta) / $($goStatsBatched.err_delta)"

    # ── B7. Collect T1 snapshot (producers still alive) ───
    Write-Step "Collecting T1 metrics snapshot (batched, producers running)..."
    $bT1 = Collect-Metrics $homes "T1-final" "batched" -AfterTs $bMeasureStartUtc.ToString("yyyy-MM-dd HH:mm:ss")

    # ── B8. Stop producers & flush cooldown ───────────────
    Write-Step "Stopping batched producers and waiting ${FlushCooldownSeconds}s for Flink to flush..."
    kubectl delete -f $bYamlFile --ignore-not-found 2>$null | Out-Null
    $cooldownEnd = (Get-Date).AddSeconds($FlushCooldownSeconds)
    while ((Get-Date) -lt $cooldownEnd) {
        $remaining = [math]::Ceiling(($cooldownEnd - (Get-Date)).TotalSeconds)
        Write-Host ("`r    Flush cooldown remaining: {0}s  " -f $remaining) -NoNewline
        Start-Sleep -Seconds 10
    }
    Write-Host ""

    # ── B9. Post-flush 30-minute aggregation ──────────────
    # Tenmin firings already happened in-run at +10/+20/+30; this step rolls up
    # the full window into sensor_data_30min for the per-tier summary metrics.
    Invoke-30minAggregation "${experimentLabel}-b"
    Update-PostAggregationMetrics $bT1
    # Persist T1 AFTER post-agg so the per-tier file reflects the final 30-min rollup.
    Write-MetricsToFile $bT1 $bResultFile

    # ── B10. Checkpoint T1 (Flink STILL RUNNING) ─────────
    Write-Step "Collecting Flink checkpoint counts (batched T1)..."
    $bCkptT1 = Get-FlinkCheckpointCounts -JobId $bFlinkJobId
    Write-SubStep "Checkpoints completed (delta): $($bCkptT1.completed - $bCkptT0.completed)"
    Write-SubStep "Checkpoints failed (delta):    $($bCkptT1.failed    - $bCkptT0.failed)"
    Write-SubStep "Checkpoints restored (delta):  $($bCkptT1.restored  - $bCkptT0.restored)"

    # ── B11. Granular Prometheus (covers this 30-min run) ─
    $bMeasureEndUtc = (Get-Date).ToUniversalTime()
    $bElapsedMin = [math]::Ceiling(($bMeasureEndUtc - $bMeasureStartUtc).TotalMinutes)
    Write-Step "Collecting granular Prometheus averages (window: ${bElapsedMin}m)..."
    $bGranularProm = Collect-GranularPrometheus -windowMinutes $bElapsedMin

    # ── B12. Azure Storage Account metrics ────────────────
    Write-Step "Collecting Azure Storage Account metrics (batched window)..."
    $bAzSA = Get-AzureStorageMetrics $bMeasureStartUtc $bMeasureEndUtc

    # ── B13. Trino benchmark ──────────────────────────────
    Write-Step "Running Trino benchmark queries (5 queries x 5 iterations)..."
    $bTrinoBench = Run-TrinoBenchmark -iterations 5

    Write-Step "Persisting batched phase artifacts..."
    Write-PhaseArtifacts -Homes $homes -Phase "batched" -ProducerType "batched" -GoStats $goStatsBatched -GranularProm $bGranularProm -CkptT0 $bCkptT0 -CkptT1 $bCkptT1 -AzSA $bAzSA -TrinoBench $bTrinoBench -RestartDelta $bRestartDelta

    # ── B14. Verify Paimon data ───────────────────────────
    if ($bT1['trino_raw_count'] -eq "0" -or $bT1['trino_raw_count'] -eq "") {
        Write-Host "  WARNING: Paimon raw rows at T1 is STILL 0 — data pipeline issue!" -ForegroundColor Red
    }

    # ── B15. Summary ──────────────────────────────────────
    $bKafkaDelta = [int64]$bT1["kafka_total_offset"] - [int64]$bT0["kafka_total_offset"]
    $bRawT1 = 0; $bRawT0 = 0
    [int64]::TryParse(($bT1["trino_raw_count"] -replace '[^\d]',''), [ref]$bRawT1) | Out-Null
    [int64]::TryParse(($bT0["trino_raw_count"] -replace '[^\d]',''), [ref]$bRawT0) | Out-Null
    $bPaimonDelta = $bRawT1 - $bRawT0
    $bActualRps   = [math]::Round($bKafkaDelta / ($RunMinutes * 60), 1)
    $bExpectedRps = $homes * (1 + $PlugsPerHome)

    Write-Step "Tier $homes BATCHED summary:"
    Write-SubStep "Expected RPS:            $bExpectedRps  ($homes homes x (1 general + $PlugsPerHome plugs) readings/sec)"
    Write-SubStep "Producer RPS (last 60s): $($goStatsBatched.observed_rps)  (sum of last [stats] line per pod, simulator-side)"
    Write-SubStep "Kafka ingest RPS:        $bActualRps  (Kafka offset delta / $RunMinutes min)"
    Write-SubStep "Avg cycle / saturated:   $($goStatsBatched.avg_cycle_s)s / $($goStatsBatched.saturated)"
    Write-SubStep "Producer cumulative OK / Errors: $($goStatsBatched.ok_delta) / $($goStatsBatched.err_delta)"
    Write-SubStep "Kafka messages (delta):  $bKafkaDelta"
    Write-SubStep "Paimon raw rows (delta): $bPaimonDelta"
    Write-SubStep "Flink running jobs:      $($bT1['flink_running_jobs'])"
    Write-SubStep "E2E latency:             $($bT1['e2e_latency_seconds'])s"
    if ($flinkDiedDuringRun) {
        Write-Host "  WARNING: Flink died and recovery failed during this run — results are DEGRADED" -ForegroundColor Red
    }

    Append-SummaryCsv $bT0 $bT1 $csvPath

    # ── B16. Update batched matrix row (fresh data) ───────
    Update-ExperimentMatrix `
        -Homes              $homes `
        -Phase              "batched" `
        -GoPods             $goPods `
        -GoStats            $goStatsBatched `
        -GranularProm       $bGranularProm `
        -CkptT0             $bCkptT0 `
        -CkptT1             $bCkptT1 `
        -AzSA               $bAzSA `
        -TrinoBench         $bTrinoBench

    Write-Host ""
    Write-Host "  SUB-PHASE B (batched) COMPLETE for tier $homes." -ForegroundColor Green
    } # end sub-phase B

    Write-Host "  Tier $homes COMPLETE (RunPhase=$RunPhase)." -ForegroundColor Green
    Write-Host ""

    if ($tierIndex -lt $Tiers.Count) {
        Write-Step "Cooldown: 30 seconds before next tier..."
        Start-Sleep -Seconds 30
    }
}

# ─────────────────────────────────────────────────────────────
# Final summary
# ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  ALL TIERS COMPLETE (COMBINED IMPROVED)                         ║" -ForegroundColor Green
Write-Host "╠══════════════════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║  Tiers: $($Tiers.Count) x ($RunPhase)" -ForegroundColor Green
Write-Host "║  Query suite: Q1 (count raw), Q2 (groupby raw)," -ForegroundColor Green
Write-Host "║               Q3 (window fn), Q4 (join), Q5 (percentile)" -ForegroundColor Green
Write-Host "║  Results in: $ResultsDir" -ForegroundColor Green
Write-Host "║  CSV:        $csvPath" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "Files generated:" -ForegroundColor White
Get-ChildItem $ResultsDir | ForEach-Object { Write-Host ("  " + $_.Name) -ForegroundColor Gray }
Write-Host ""

# Stop the transcript before any post-run actions (cluster shutdown).
# Wrapped in try/catch so a transcript error never aborts the run.
try {
    Stop-Transcript | Out-Null
    Write-Host "Transcript saved: $transcriptPath" -ForegroundColor DarkGray
} catch {
    # Transcript may already be stopped (e.g., session-level handler) — ignore.
}

if ($ClusterShutdown) {
    Write-Host ""
    Write-Host ">>> Stopping AKS cluster to save costs..." -ForegroundColor Yellow
    az aks stop --resource-group "rg-lakehouse-on-k8s-01" --name "aks-lakehouse-on-k8s-01" --no-wait
    Write-Host "  AKS stop command issued (--no-wait). Cluster will deallocate shortly." -ForegroundColor Green
}
