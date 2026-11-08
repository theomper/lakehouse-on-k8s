<#
.SYNOPSIS
  Calibrate HomesPerPod for the Python producer.
.EXAMPLE
  .\run-py-calibration.ps1
  .\run-py-calibration.ps1 -Steps @(25, 50, 100, 200)
#>
param(
    [int[]]$Steps = @(25, 50, 75, 100, 150, 200, 300),
    [int]$WaitSettle = 60,
    [int]$WaitStats  = 120
)

$Namespace   = "data-ingest"
$DeployName  = "py-calibration"
$Template    = Join-Path $PSScriptRoot "py-calibration-pod.yaml"
$ResultsFile = Join-Path $PSScriptRoot "py-calibration-results.txt"

Write-Host "================================================"
Write-Host " Python Producer Calibration - HomesPerPod sizing"
Write-Host "================================================"
Write-Host "Steps: $($Steps -join ', ')"
Write-Host "Settle: ${WaitSettle}s, Stats window: ${WaitStats}s"
Write-Host "Results: $ResultsFile"
Write-Host ""

$header    = "{0,-10} {1,-10} {2,-14} {3,-12} {4,-10}" -f "Homes","RPS","CycleDuration","Interval","Saturated"
$separator = "{0,-10} {1,-10} {2,-14} {3,-12} {4,-10}" -f "-----","---","-------------","--------","---------"
$header    | Tee-Object -FilePath $ResultsFile
$separator | Tee-Object -FilePath $ResultsFile -Append

foreach ($homes in $Steps) {
    Write-Host "`n>>> Testing with $homes homes..."

    $patched = Join-Path $env:TEMP "py-calibration-${homes}h.yaml"
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

    $rps      = if ($statsLine -match 'rps=(\d+)')           { $Matches[1] } else { "?" }
    $cdur     = if ($cycleLine -match 'duration=(\S+)')      { $Matches[1] } else { "?" }
    $interval = if ($cycleLine -match 'interval=(\S+)')      { $Matches[1] } else { "?" }
    $sat      = if ($cycleLine -match 'saturated=(\S+)')     { $Matches[1] } else { "?" }

    $row = "{0,-10} {1,-10} {2,-14} {3,-12} {4,-10}" -f $homes, $rps, $cdur, $interval, $sat
    $row | Tee-Object -FilePath $ResultsFile -Append

    kubectl delete -f $patched -n $Namespace --ignore-not-found 2>$null
    Write-Host "    Cleaned up. Waiting 30s before next step..."
    Start-Sleep -Seconds 30
    Remove-Item $patched -ErrorAction SilentlyContinue
}

Write-Host "`n================================================"
Write-Host " Python calibration complete! Results: $ResultsFile"
Write-Host "================================================"
Write-Host "Pick the largest Homes where Saturated=False and duration < 0.8s."
Write-Host "Update HOMES_PER_POD in generate-py-experiment.sh / .ps1 accordingly."
