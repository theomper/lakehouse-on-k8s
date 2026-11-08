<#
.SYNOPSIS
  Calibrate HomesPerPod for the Go producer.
.EXAMPLE
  .\run-go-calibration.ps1
  .\run-go-calibration.ps1 -Steps @(250, 500, 1000, 2000)
#>
param(
    [int[]]$Steps = @(250, 500, 750, 1000, 1250, 1500, 2000),
    [int]$WaitSettle = 60,
    [int]$WaitStats  = 120
)

$Namespace   = "data-ingest"
$DeployName  = "go-calibration"
$Template    = Join-Path $PSScriptRoot "go-calibration-pod.yaml"
$ResultsFile = Join-Path $PSScriptRoot "go-calibration-results.txt"

Write-Host "============================================="
Write-Host " Go Producer Calibration - HomesPerPod sizing"
Write-Host "============================================="
Write-Host "Steps: $($Steps -join ', ')"
Write-Host "Settle: ${WaitSettle}s, Stats window: ${WaitStats}s"
Write-Host "Results: $ResultsFile"
Write-Host ""

$header    = "{0,-10} {1,-10} {2,-12} {3,-12} {4,-12} {5,-10}" -f "Homes","RPS","CycleAvg","CycleMax","Interval","Saturated"
$separator = "{0,-10} {1,-10} {2,-12} {3,-12} {4,-12} {5,-10}" -f "-----","---","--------","--------","--------","---------"
$header    | Tee-Object -FilePath $ResultsFile
$separator | Tee-Object -FilePath $ResultsFile -Append

foreach ($homes in $Steps) {
    Write-Host "`n>>> Testing with $homes homes..."

    $patched = Join-Path $env:TEMP "go-calibration-${homes}h.yaml"
    (Get-Content $Template -Raw) -replace 'value: "PLACEHOLDER"', "value: `"$homes`"" | Set-Content $patched -Encoding utf8

    kubectl apply -f $patched -n $Namespace 2>$null
    Write-Host "    Deployed. Waiting ${WaitSettle}s for pod to settle..."
    Start-Sleep -Seconds $WaitSettle

    $pod = kubectl get pods -n $Namespace -l app=$DeployName `
        --field-selector=status.phase=Running `
        -o jsonpath='{.items[0].metadata.name}' 2>$null

    if (-not $pod) {
        Write-Host "    WARNING: No running pod found, skipping $homes homes"
        kubectl delete -f $patched -n $Namespace --ignore-not-found 2>$null
        continue
    }

    Write-Host "    Pod: $pod - collecting stats for ${WaitStats}s..."
    Start-Sleep -Seconds $WaitStats

    $logs = kubectl logs $pod -n $Namespace --tail=20 2>$null
    $statsLine = ($logs | Select-String '\[stats\]' | Select-Object -Last 1).ToString()
    $cycleLine = ($logs | Select-String '\[cycle\]' | Select-Object -Last 1).ToString()

    $rps      = if ($statsLine -match 'rps=(\d+)')        { $Matches[1] } else { "?" }
    $cavg     = if ($cycleLine -match 'avg=(\S+)')         { $Matches[1] } else { "?" }
    $cmax     = if ($cycleLine -match 'max=(\S+)')         { $Matches[1] } else { "?" }
    $interval = if ($cycleLine -match 'interval=(\S+)')    { $Matches[1] } else { "?" }
    $sat      = if ($cycleLine -match 'saturated=(\S+)')   { $Matches[1] } else { "?" }

    $row = "{0,-10} {1,-10} {2,-12} {3,-12} {4,-12} {5,-10}" -f $homes, $rps, $cavg, $cmax, $interval, $sat
    $row | Tee-Object -FilePath $ResultsFile -Append

    kubectl delete -f $patched -n $Namespace --ignore-not-found 2>$null
    Write-Host "    Cleaned up. Waiting 30s before next step..."
    Start-Sleep -Seconds 30
    Remove-Item $patched -ErrorAction SilentlyContinue
}

Write-Host "`n============================================="
Write-Host " Go calibration complete! Results: $ResultsFile"
Write-Host "============================================="
Write-Host "Pick the largest Homes where Saturated=false and CycleMax < 800ms."
Write-Host "Update HOMES_PER_POD in generate-go-experiment.sh / .ps1 accordingly."
