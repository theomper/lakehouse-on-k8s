<#
.SYNOPSIS
  Validates that combined-improved experiment metrics were captured and saved.

.DESCRIPTION
  Checks:
  - required result files per phase (nonbatched + batched)
  - JSON artifact structure and key metrics presence
  - matrix-full.csv rows/columns for the tier run IDs
  - optional matrix markdown row width consistency

  Exit code:
  - 0 = validation passed
  - 1 = validation failed

.EXAMPLE
  .\validate-experiment-combined-improved-results.ps1 -ResultsDir experiments\results\combined-improved-smoke-20260329_120000 -Tier 50

.EXAMPLE
  .\validate-experiment-combined-improved-results.ps1 -ExperimentTag smoke-20260329_120000 -Tier 50 -MatrixPath experiments\results\experiment-matrix-lakehouse-on-k8s-combined-improved-smoke-smoke-20260329_120000.md
#>

[CmdletBinding()]
param(
    [string]$ExperimentTag = "",

    [string]$ResultsDir = "",

    [int]$Tier = 50,

    [string]$MatrixPath = "",

    [switch]$AllowEmptyAzureMetrics,

    [switch]$AllowEmptyPrometheusSeries
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$issues = [System.Collections.Generic.List[string]]::new()

function Add-Issue([string]$message) {
    $script:issues.Add($message) | Out-Null
}

function Resolve-PathOrRepo([string]$inputPath, [string]$repoRoot) {
    if (-not $inputPath) { return "" }
    if ([System.IO.Path]::IsPathRooted($inputPath)) { return $inputPath }
    return (Join-Path $repoRoot $inputPath)
}

function Get-JsonObject([string]$path) {
    try {
        return (Get-Content -Path $path -Raw -Encoding utf8 | ConvertFrom-Json -ErrorAction Stop)
    } catch {
        Add-Issue ("Invalid JSON in {0}: {1}" -f $path, $_.Exception.Message)
        return $null
    }
}

function Require-Property([object]$obj, [string]$propName, [string]$context) {
    if ($null -eq $obj) {
        Add-Issue ("{0}: object is null" -f $context)
        return $false
    }
    $prop = $obj.PSObject.Properties[$propName]
    if (-not $prop) {
        Add-Issue ("{0}: missing property '{1}'" -f $context, $propName)
        return $false
    }
    return $true
}

function Get-MarkdownCols([string]$line) {
    if (-not $line) { return @() }
    $trimmed = $line.Trim()
    if (-not $trimmed.StartsWith("|")) { return @() }
    $parts = $trimmed -split '\|'
    if ($parts.Count -lt 3) { return @() }
    $first = 1
    $last = $parts.Count - 2
    return @($parts[$first..$last] | ForEach-Object { $_.Trim() })
}

if (-not $ResultsDir) {
    if (-not $ExperimentTag) {
        throw "Provide either -ResultsDir or -ExperimentTag."
    }
    $ResultsDir = Join-Path $RepoRoot ("experiments\results\combined-improved-{0}" -f $ExperimentTag)
} else {
    $ResultsDir = Resolve-PathOrRepo $ResultsDir $RepoRoot
}

if ($MatrixPath) {
    $MatrixPath = Resolve-PathOrRepo $MatrixPath $RepoRoot
}

if (-not (Test-Path $ResultsDir)) {
    throw "Results directory not found: $ResultsDir"
}

$tierIndex = @{
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

$runIds = @()
if ($tierIndex.ContainsKey($Tier)) {
    $idx = $tierIndex[$Tier]
    $runIds = @("N$idx", "B$idx")
} else {
    Add-Issue ("Tier '{0}' is not in tier mapping; matrix row checks skipped." -f $Tier)
}

# 1) Required files
$requiredFiles = @(
    "summary.csv",
    "matrix-full.csv"
)
foreach ($phase in @("nonbatched", "batched")) {
    $requiredFiles += @(
        ("tier-{0}h-{1}-t0.txt" -f $Tier, $phase),
        ("tier-{0}h-{1}.txt" -f $Tier, $phase),
        ("tier-{0}h-{1}-producer-stats.json" -f $Tier, $phase),
        ("tier-{0}h-{1}-granular-prometheus.json" -f $Tier, $phase),
        ("tier-{0}h-{1}-checkpoints.json" -f $Tier, $phase),
        ("tier-{0}h-{1}-azure-storage.json" -f $Tier, $phase),
        ("tier-{0}h-{1}-trino-benchmark.json" -f $Tier, $phase)
    )
}

foreach ($file in $requiredFiles) {
    $full = Join-Path $ResultsDir $file
    if (-not (Test-Path $full)) {
        Add-Issue ("Missing required file: {0}" -f $full)
    }
}

# 2) JSON content checks
$requiredPromKeys = @(
    "kafka_cpu", "kafka_mem", "kafka_netrx", "kafka_nettx", "kafka_pvc",
    "flink_cpu", "flink_mem", "flink_netrx", "flink_nettx", "flink_fs",
    "trino_cpu", "trino_mem"
)
$requiredProducerKeys = @("observed_rps", "avg_cycle_s", "max_cycle_s", "saturated", "ok_delta", "err_delta", "error_rate")
$requiredTrinoBenchKeys = @("q1_p95_s", "q2_p95_s", "q3_p95_s", "q4_p95_s", "q5_p95_s", "failures")
$requiredAzureKeys = @("transactions", "ingress_gib", "egress_gib", "e2e_latency_ms", "server_latency_ms", "availability_pct", "used_capacity_gib")

foreach ($phase in @("nonbatched", "batched")) {
    $promPath = Join-Path $ResultsDir ("tier-{0}h-{1}-granular-prometheus.json" -f $Tier, $phase)
    if (Test-Path $promPath) {
        $promObj = Get-JsonObject $promPath
        foreach ($k in $requiredPromKeys) {
            if (-not (Require-Property $promObj $k ("granular-prometheus ({0})" -f $phase))) { continue }
            $series = $promObj.$k
            $seriesCount = 0
            if ($null -ne $series) {
                $seriesCount = @($series.PSObject.Properties).Count
            }
            if (-not $AllowEmptyPrometheusSeries -and $seriesCount -eq 0) {
                Add-Issue ("granular-prometheus ({0}): key '{1}' has empty series" -f $phase, $k)
            }
        }
    }

    $producerPath = Join-Path $ResultsDir ("tier-{0}h-{1}-producer-stats.json" -f $Tier, $phase)
    if (Test-Path $producerPath) {
        $producerObj = Get-JsonObject $producerPath
        foreach ($k in $requiredProducerKeys) {
            if (-not (Require-Property $producerObj $k ("producer-stats ({0})" -f $phase))) { continue }
            $v = [string]$producerObj.$k
            if ([string]::IsNullOrWhiteSpace($v)) {
                Add-Issue ("producer-stats ({0}): '{1}' is empty" -f $phase, $k)
            }
        }
    }

    $ckptPath = Join-Path $ResultsDir ("tier-{0}h-{1}-checkpoints.json" -f $Tier, $phase)
    if (Test-Path $ckptPath) {
        $ckObj = Get-JsonObject $ckptPath
        if (Require-Property $ckObj "delta" ("checkpoints ({0})" -f $phase)) {
            $delta = $ckObj.delta
            foreach ($k in @("completed", "failed", "restored")) {
                if (-not (Require-Property $delta $k ("checkpoints.delta ({0})" -f $phase))) { continue }
                $v = [string]$delta.$k
                if ([string]::IsNullOrWhiteSpace($v)) {
                    Add-Issue ("checkpoints.delta ({0}): '{1}' is empty" -f $phase, $k)
                }
            }
        }
    }

    $trinoPath = Join-Path $ResultsDir ("tier-{0}h-{1}-trino-benchmark.json" -f $Tier, $phase)
    if (Test-Path $trinoPath) {
        $trinoObj = Get-JsonObject $trinoPath
        foreach ($k in $requiredTrinoBenchKeys) {
            if (-not (Require-Property $trinoObj $k ("trino-benchmark ({0})" -f $phase))) { continue }
            $v = [string]$trinoObj.$k
            if ([string]::IsNullOrWhiteSpace($v)) {
                Add-Issue ("trino-benchmark ({0}): '{1}' is empty" -f $phase, $k)
            }
        }
    }

    $azurePath = Join-Path $ResultsDir ("tier-{0}h-{1}-azure-storage.json" -f $Tier, $phase)
    if (Test-Path $azurePath) {
        $azureObj = Get-JsonObject $azurePath
        foreach ($k in $requiredAzureKeys) {
            if (-not (Require-Property $azureObj $k ("azure-storage ({0})" -f $phase))) { continue }
            if (-not $AllowEmptyAzureMetrics) {
                $v = [string]$azureObj.$k
                if ([string]::IsNullOrWhiteSpace($v)) {
                    Add-Issue ("azure-storage ({0}): '{1}' is empty" -f $phase, $k)
                }
            }
        }
    }
}

# 3) matrix-full.csv checks
# Schema (post-30-min-aggregation refactor): cluster runs 2 TaskManagers, so the
# matrix wide-CSV must include flink_tm_1_* and flink_tm_2_* columns and must
# NOT include flink_tm_3_* / flink_tm_4_* (which were dropped along with the
# hourly aggregation). The deprecation guard catches accidental schema drift.
$matrixCsv = Join-Path $ResultsDir "matrix-full.csv"
if (Test-Path $matrixCsv) {
    $csvRows = @(Import-Csv -Path $matrixCsv)
    $requiredCsvCols = @(
        "run_id", "observed_rps", "avg_cycle_s", "max_cycle_s", "saturated", "ok_delta", "errors_delta", "error_rate",
        "kafka_0_cpu_cores", "kafka_0_mem_gib", "kafka_0_net_rx_mib_s", "kafka_0_net_tx_mib_s", "kafka_0_pvc_gib",
        "kafka_1_cpu_cores", "kafka_1_mem_gib", "kafka_1_net_rx_mib_s", "kafka_1_net_tx_mib_s", "kafka_1_pvc_gib",
        "flink_jm_cpu_cores", "flink_jm_mem_gib", "flink_jm_net_rx_mib_s", "flink_jm_net_tx_mib_s", "flink_jm_fs_gib",
        "flink_tm_1_cpu_cores", "flink_tm_1_mem_gib", "flink_tm_1_net_rx_mib_s", "flink_tm_1_net_tx_mib_s", "flink_tm_1_fs_gib",
        "flink_tm_2_cpu_cores", "flink_tm_2_mem_gib", "flink_tm_2_net_rx_mib_s", "flink_tm_2_net_tx_mib_s", "flink_tm_2_fs_gib",
        "ckpt_completed_delta", "ckpt_failed_delta", "ckpt_restored_delta",
        "azure_sa_tx_sum", "azure_sa_ingress_gib", "azure_sa_egress_gib", "azure_sa_e2e_latency_ms", "azure_sa_server_latency_ms", "azure_sa_availability_pct", "azure_sa_capacity_gib",
        "trino_q1_p95_s", "trino_q2_p95_s", "trino_q3_p95_s", "trino_q4_p95_s", "trino_q5_p95_s", "trino_failures"
    )

    $deprecatedCsvCols = @(
        "flink_tm_3_cpu_cores", "flink_tm_3_mem_gib", "flink_tm_3_net_rx_mib_s", "flink_tm_3_net_tx_mib_s", "flink_tm_3_fs_gib",
        "flink_tm_4_cpu_cores", "flink_tm_4_mem_gib", "flink_tm_4_net_rx_mib_s", "flink_tm_4_net_tx_mib_s", "flink_tm_4_fs_gib"
    )

    foreach ($col in $requiredCsvCols) {
        if ($csvRows.Count -gt 0) {
            if (-not ($csvRows[0].PSObject.Properties.Name -contains $col)) {
                Add-Issue ("matrix-full.csv missing column '{0}'" -f $col)
            }
        } else {
            Add-Issue "matrix-full.csv has no data rows"
            break
        }
    }

    if ($csvRows.Count -gt 0) {
        foreach ($col in $deprecatedCsvCols) {
            if ($csvRows[0].PSObject.Properties.Name -contains $col) {
                Add-Issue ("matrix-full.csv has deprecated column '{0}' (cluster runs 2 TaskManagers; TM-3/TM-4 columns were dropped)" -f $col)
            }
        }
    }

    if ($AllowEmptyAzureMetrics) {
        $requiredCsvCols = @($requiredCsvCols | Where-Object { $_ -notlike "azure_sa_*" })
    }

    foreach ($runId in $runIds) {
        $row = @($csvRows | Where-Object { $_.run_id -eq $runId } | Select-Object -First 1)
        if ($row.Count -eq 0) {
            Add-Issue ("matrix-full.csv missing row for run_id '{0}'" -f $runId)
            continue
        }
        foreach ($col in $requiredCsvCols) {
            if ($col -eq "run_id") { continue }
            $v = [string]$row[0].$col
            if ([string]::IsNullOrWhiteSpace($v)) {
                Add-Issue ("matrix-full.csv row '{0}' has blank '{1}'" -f $runId, $col)
            }
        }
    }
}

# 3b) summary.csv schema check (per-phase rollup CSV produced by Append-SummaryCsv)
# Catches the case where the orchestrator's CSV write got out of sync with the
# 30-min aggregation refactor (column rename paimon_hourly_rows_t1 ->
# paimon_30min_rows_t1, distinct_homes_hourly -> distinct_homes_30min).
$summaryCsv = Join-Path $ResultsDir "summary.csv"
if (Test-Path $summaryCsv) {
    $summaryRows = @(Import-Csv -Path $summaryCsv)
    $requiredSummaryCols = @(
        "experiment_tag", "producer_type", "tier_homes",
        "kafka_total_delta", "actual_rps", "ingestion_ratio",
        "paimon_raw_rows_t1", "paimon_30min_rows_t1", "paimon_10min_rows_t1",
        "distinct_homes_raw", "distinct_homes_30min",
        "slo_kafka_vs_paimon_raw"
    )
    $deprecatedSummaryCols = @("paimon_hourly_rows_t1", "distinct_homes_hourly")

    if ($summaryRows.Count -eq 0) {
        Add-Issue "summary.csv has no data rows"
    } else {
        foreach ($col in $requiredSummaryCols) {
            if (-not ($summaryRows[0].PSObject.Properties.Name -contains $col)) {
                Add-Issue ("summary.csv missing column '{0}'" -f $col)
            }
        }
        foreach ($col in $deprecatedSummaryCols) {
            if ($summaryRows[0].PSObject.Properties.Name -contains $col) {
                Add-Issue ("summary.csv has deprecated column '{0}' (replaced by *_30min equivalent in the 30-min aggregation refactor)" -f $col)
            }
        }
    }
}

# 4) Matrix markdown row-width checks (optional)
if ($MatrixPath) {
    if (-not (Test-Path $MatrixPath)) {
        Add-Issue ("Matrix markdown file not found: {0}" -f $MatrixPath)
    } else {
        $matrixLines = Get-Content -Path $MatrixPath -Encoding utf8
        $headerLine = @($matrixLines | Where-Object { $_ -match '^\|\s*Run ID\s*\|' } | Select-Object -First 1)
        if ($headerLine.Count -eq 0) {
            Add-Issue ("Matrix markdown has no '| Run ID |' header: {0}" -f $MatrixPath)
        } else {
            $headerCols = Get-MarkdownCols $headerLine[0]
            foreach ($runId in $runIds) {
                $rowLine = @($matrixLines | Where-Object { $_ -match ("^\|\s*{0}\s*\|" -f [Regex]::Escape($runId)) } | Select-Object -First 1)
                if ($rowLine.Count -eq 0) {
                    Add-Issue ("Matrix markdown missing row '{0}'" -f $runId)
                    continue
                }
                $rowCols = Get-MarkdownCols $rowLine[0]
                if ($rowCols.Count -ne $headerCols.Count) {
                    Add-Issue ("Matrix markdown row '{0}' column count mismatch: header={1}, row={2}" -f $runId, $headerCols.Count, $rowCols.Count)
                }
            }
        }
    }
}

Write-Host ""
Write-Host ("Validation target: {0}" -f $ResultsDir) -ForegroundColor Cyan
if ($MatrixPath) {
    Write-Host ("Matrix checked:    {0}" -f $MatrixPath) -ForegroundColor Cyan
}

if ($issues.Count -gt 0) {
    Write-Host ""
    Write-Host ("FAILED: {0} issue(s) found." -f $issues.Count) -ForegroundColor Red
    foreach ($issue in $issues) {
        Write-Host (" - {0}" -f $issue) -ForegroundColor Red
    }
    exit 1
}

Write-Host ""
Write-Host "PASSED: all required metrics/artifacts were captured and saved." -ForegroundColor Green
exit 0
