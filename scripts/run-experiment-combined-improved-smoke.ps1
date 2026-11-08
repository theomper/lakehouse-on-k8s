<#
.SYNOPSIS
  Reusable smoke-test wrapper for run-experiment-combined-improved.ps1.

.DESCRIPTION
  Runs a single-tier short experiment (nonbatched + batched) with an isolated
  matrix copy and optional post-run validation.
  Uses k8s/manifests and the acrlakehouseonk8s01 registry.

.EXAMPLE
  .\run-experiment-combined-improved-smoke.ps1

.EXAMPLE
  .\run-experiment-combined-improved-smoke.ps1 -Tier 50 -WarmupMinutes 1 -RunMinutes 5 -Validate

.EXAMPLE
  .\run-experiment-combined-improved-smoke.ps1 -Tier 10000 -RunMinutes 10 -SkipCleanup -Validate
#>

[CmdletBinding()]
param(
    [int]$Tier = 50,

    [int]$WarmupMinutes = 1,

    [int]$RunMinutes = 5,

    [int]$FlushCooldownSeconds = 30,

    [string]$ExperimentTag = ("smoke-" + (Get-Date -Format "yyyyMMdd_HHmmss")),

    [string]$MatrixTemplatePath = "",

    [string]$MatrixOutputPath = "",

    [switch]$SkipCleanup,

    [switch]$Validate,

    [switch]$AllowEmptyAzureMetrics,

    [switch]$AllowEmptyPrometheusSeries
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$RunnerScript = Join-Path $PSScriptRoot "run-experiment-combined-improved.ps1"
$ValidatorScript = Join-Path $PSScriptRoot "validate-experiment-combined-improved-results.ps1"
$ResultsRoot = Join-Path $RepoRoot "experiments\results"

if (-not (Test-Path $RunnerScript)) {
    throw "Runner script not found: $RunnerScript"
}

if (-not $MatrixTemplatePath) {
    $MatrixTemplatePath = Join-Path $ResultsRoot "experiment-matrix-lakehouse-on-k8s-combined-improved.md"
} elseif (-not [System.IO.Path]::IsPathRooted($MatrixTemplatePath)) {
    $MatrixTemplatePath = Join-Path $RepoRoot $MatrixTemplatePath
}

if (-not (Test-Path $MatrixTemplatePath)) {
    throw "Matrix template not found: $MatrixTemplatePath"
}

if (-not $MatrixOutputPath) {
    $MatrixOutputPath = Join-Path $ResultsRoot ("experiment-matrix-lakehouse-on-k8s-combined-improved-smoke-{0}.md" -f $ExperimentTag)
} elseif (-not [System.IO.Path]::IsPathRooted($MatrixOutputPath)) {
    $MatrixOutputPath = Join-Path $RepoRoot $MatrixOutputPath
}

Copy-Item -Path $MatrixTemplatePath -Destination $MatrixOutputPath -Force

$runParams = @{
    Tiers                = @($Tier)
    WarmupMinutes        = $WarmupMinutes
    RunMinutes           = $RunMinutes
    FlushCooldownSeconds = $FlushCooldownSeconds
    ExperimentTag        = $ExperimentTag
    ExperimentMatrixPath = $MatrixOutputPath
}

if ($SkipCleanup) {
    $runParams.SkipCleanup = $true
}

Write-Host ""
Write-Host "Running smoke experiment..." -ForegroundColor Cyan
Write-Host "  Tier: $Tier"
Write-Host "  Tag: $ExperimentTag"
Write-Host "  Matrix copy: $MatrixOutputPath"

& $RunnerScript @runParams

$resultsDir = Join-Path $ResultsRoot ("combined-improved-{0}" -f $ExperimentTag)

# ─────────────────────────────────────────────────────────────
# Post-run: Flink Prometheus metric discovery (backpressure debugging)
# Best done while Flink is still running (pipeline up after flush cooldown)
# ─────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Discovering Flink Prometheus metrics (backpressure diagnosis)..." -ForegroundColor Cyan

$promNs = "monitoring"
$diagFile = Join-Path $resultsDir "flink-prometheus-metric-discovery.txt"

try {
    # Discover Prometheus pod by label (StatefulSet pod, name varies by helm release)
    $promPod = ""
    try {
        $promPod = (kubectl get pods -n $promNs -l "app.kubernetes.io/name=prometheus" -o jsonpath='{.items[0].metadata.name}' 2>$null).Trim()
    } catch {
        $promPod = ""
    }
    if (-not $promPod) {
        throw "Could not locate a Prometheus pod in namespace '$promNs' (label app.kubernetes.io/name=prometheus)."
    }
    Write-Host "  Using Prometheus pod: $promPod"

    # 1. Find all Flink metric names exposed to Prometheus
    Write-Host "  Querying Prometheus for all flink_* metric names..."
    $encoded = [Uri]::EscapeDataString('count({__name__=~"flink_.*"}) by (__name__)')
    $raw = kubectl exec -n $promNs $promPod -c prometheus -- `
        wget -qO- "http://localhost:9090/api/v1/query?query=${encoded}" 2>&1
    $rawStr = ($raw -join "`n")
    $diagLines = @("=== Flink Prometheus Metric Discovery ===", "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')", "")

    $parsed = $rawStr | ConvertFrom-Json -ErrorAction SilentlyContinue
    if ($parsed -and $parsed.data -and $parsed.data.result) {
        $metricNames = @($parsed.data.result | ForEach-Object { $_.metric.__name__ }) | Sort-Object
        $diagLines += "Found $($metricNames.Count) flink_* metrics in Prometheus:"
        foreach ($m in $metricNames) { $diagLines += "  $m" }
    } else {
        $diagLines += "WARNING: No flink_* metrics found in Prometheus."
        $diagLines += "Raw response (first 2000 chars): $($rawStr.Substring(0, [math]::Min(2000, $rawStr.Length)))"
    }

    # 2. Attempt the specific backpressure/busy-time queries used by the main script
    $diagLines += ""
    $diagLines += "=== Targeted Backpressure Metric Queries ==="
    $targetQueries = @(
        @{ Label="consumer_lag_max"; Query='sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)' },
        @{ Label="busy_time_ms_per_sec"; Query='avg(flink_taskmanager_job_task_busyTimeMsPerSecond)' },
        @{ Label="backpressured_time_ms_per_sec"; Query='avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)' },
        @{ Label="idle_time_ms_per_sec"; Query='avg(flink_taskmanager_job_task_idleTimeMsPerSecond)' },
        @{ Label="records_in_per_sec"; Query='sum(flink_taskmanager_job_task_numRecordsInPerSecond)' },
        @{ Label="records_out_per_sec"; Query='sum(flink_taskmanager_job_task_numRecordsOutPerSecond)' }
    )
    foreach ($tq in $targetQueries) {
        $enc = [Uri]::EscapeDataString($tq.Query)
        $r = kubectl exec -n $promNs $promPod -c prometheus -- `
            wget -qO- "http://localhost:9090/api/v1/query?query=${enc}" 2>&1
        $rStr = (($r -join "`n")).Trim()
        $hasData = $rStr -match '"result":\[.+\]'
        $status = if ($hasData) { "HAS DATA" } else { "EMPTY" }
        $diagLines += "  [$status] $($tq.Label): $($tq.Query)"
        if ($hasData) {
            $diagLines += "    Response: $($rStr.Substring(0, [math]::Min(500, $rStr.Length)))"
        }
    }

    $diagLines | Out-File -FilePath $diagFile -Encoding utf8
    Write-Host "  Discovery results saved to: $diagFile" -ForegroundColor Green

    # Print summary
    if ($parsed -and $parsed.data -and $parsed.data.result) {
        $backpressureRelated = @($metricNames | Where-Object { $_ -match 'busy|backpress|idle|lag|record' })
        if ($backpressureRelated.Count -gt 0) {
            Write-Host "  Backpressure-related metrics found:" -ForegroundColor Yellow
            foreach ($m in $backpressureRelated) { Write-Host "    $m" }
        } else {
            Write-Host "  WARNING: No backpressure-related metrics found. Check Flink metrics.reporter config." -ForegroundColor Red
        }
    }
} catch {
    Write-Warning "Flink metric discovery failed: $_"
}

Write-Host ""
Write-Host "Smoke run completed." -ForegroundColor Green
Write-Host "  Results: $resultsDir"
Write-Host "  Matrix:  $MatrixOutputPath"

if ($Validate) {
    if (-not (Test-Path $ValidatorScript)) {
        throw "Validator script not found: $ValidatorScript"
    }

    $validateParams = @{
        ResultsDir = $resultsDir
        Tier       = $Tier
        MatrixPath = $MatrixOutputPath
    }
    if ($AllowEmptyAzureMetrics) {
        $validateParams.AllowEmptyAzureMetrics = $true
    }
    if ($AllowEmptyPrometheusSeries) {
        $validateParams.AllowEmptyPrometheusSeries = $true
    }

    Write-Host ""
    Write-Host "Running validation..." -ForegroundColor Cyan
    & $ValidatorScript @validateParams
    exit $LASTEXITCODE
}
