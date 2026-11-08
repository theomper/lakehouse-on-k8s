# ============================================================
# restart-flink-env.ps1
#
# Restarts the Flink environment. It applies the Flink ConfigMap,
# restarts the Flink JobManager and TaskManager deployments, cleans
# up the bootstrap and processing jobs/cronjobs, and removes the
# IoT simulator deployment so everything can be re-created fresh.
# ============================================================

# ----------------------------------------------------------
# Resolve the repo root relative to this script's location so
# the script works correctly regardless of the caller's CWD.
# ----------------------------------------------------------
$repoRoot = Split-Path -Parent $PSScriptRoot

# ----------------------------------------------------------
# Paths / config. Storage account is a placeholder — replace
# with the Azure Storage account that holds your Paimon warehouse.
# ----------------------------------------------------------
$configmapFolder    = Join-Path $repoRoot "k8s\flink"
$storageAccountName = "stlakehouseonk8s01"
$simProducersPath   = "k8s\producers"
$dataProcessingPath = "k8s\manifests\data-processing.yaml"

Write-Host ""
Write-Host ">>> Storage account: $storageAccountName"
Write-Host ">>> ConfigMap      : $configmapFolder\flink-conf-configmap.yaml"
Write-Host ""

# ----------------------------------------------------------
# Step 0 – Delete the Paimon data from the "warehouse" blob
# container in the environment's Azure Storage account.
# This wipes all blobs under the "paimon/" virtual directory
# so the lake starts completely clean on the next bootstrap.
# Authentication is handled via the current Azure CLI session
# (run 'az login' first if needed).
# ----------------------------------------------------------
Write-Host "[0/8] Deleting Paimon blobs from 'warehouse' container in '$storageAccountName'..." -ForegroundColor Yellow
az storage blob delete-batch `
    --account-name $storageAccountName `
    --source warehouse `
    --pattern "paimon/*" `
    --auth-mode login

# ----------------------------------------------------------
# Step 1 – Apply the Flink ConfigMap.
# This is the only step that is environment/folder-aware.
# ----------------------------------------------------------
Write-Host "[1/9] Applying Flink ConfigMap..." -ForegroundColor Yellow
kubectl apply -f "$configmapFolder\flink-conf-configmap.yaml"

# ----------------------------------------------------------
# Step 2 – Apply the Flink PodMonitor so Prometheus scrapes
# Flink metrics (backpressure, checkpoint, throughput, etc.).
# ----------------------------------------------------------
$podMonitorPath = Join-Path $configmapFolder "flink-podmonitor.yaml"
if (Test-Path $podMonitorPath) {
    Write-Host "[2/9] Applying Flink PodMonitor for Prometheus scraping..." -ForegroundColor Yellow
    kubectl apply -f $podMonitorPath
} else {
    Write-Host "[2/9] Skipping PodMonitor - file not found: $podMonitorPath" -ForegroundColor DarkYellow
}

# ----------------------------------------------------------
# Step 3 – Restart the Flink TaskManager deployment so it
# picks up any ConfigMap changes.
# ----------------------------------------------------------
Write-Host "[3/9] Restarting Flink TaskManager deployment..." -ForegroundColor Yellow
kubectl rollout restart deployment flink-taskmanager -n data-processing

# ----------------------------------------------------------
# Step 4 – Restart the Flink JobManager deployment.
# ----------------------------------------------------------
Write-Host "[4/9] Restarting Flink JobManager deployment..." -ForegroundColor Yellow
kubectl rollout restart deployment flink-jobmanager -n data-processing

# ----------------------------------------------------------
# Step 5 – Delete the Paimon bootstrap Job so it can be
# re-applied on the next deployment cycle.
# ----------------------------------------------------------
Write-Host "[5/9] Deleting paimon-bootstrap job..." -ForegroundColor Yellow
kubectl delete job paimon-bootstrap -n data-processing

# ----------------------------------------------------------
# Step 6 – Delete the Kafka-to-Paimon Flink submit Job.
# ----------------------------------------------------------
Write-Host "[6/9] Deleting flink-submit-kafka-to-paimon job..." -ForegroundColor Yellow
kubectl delete job flink-submit-kafka-to-paimon -n data-processing

# ----------------------------------------------------------
# Step 7 – Delete the hourly aggregation CronJob.
# ----------------------------------------------------------
Write-Host "[7/9] Deleting hourly-aggregation cronjob..." -ForegroundColor Yellow
kubectl delete cronjob hourly-aggregation -n data-processing

# ----------------------------------------------------------
# Step 8 – Delete the 10-minute aggregation CronJob.
# ----------------------------------------------------------
Write-Host "[8/9] Deleting tenmin-aggregation cronjob..." -ForegroundColor Yellow
kubectl delete cronjob tenmin-aggregation -n data-processing

# ----------------------------------------------------------
# Step 9 – Remove the IoT simulator producer deployment so
# it is recreated cleanly in the next apply.
# ----------------------------------------------------------
Write-Host "[9/9] Deleting iot-sim-producer deployment..." -ForegroundColor Yellow
kubectl delete deployment iot-sim-producer -n data-ingest

# ----------------------------------------------------------
# Done – print next steps for the operator.
# ----------------------------------------------------------
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Restart complete" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host ""
Write-Host "  1. Start the IoT data generators by applying the manifests" -ForegroundColor White
Write-Host "     found under the '$simProducersPath' folder, e.g.:" -ForegroundColor Gray
Write-Host "       kubectl apply -f .\go-iot-sim-producers.yaml" -ForegroundColor DarkGray
Write-Host "       kubectl apply -f .\iot-sim-producers.yaml" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  2. Re-apply the data-processing manifest for this environment:" -ForegroundColor White
Write-Host "       kubectl apply -f .\data-processing.yaml" -ForegroundColor DarkGray
Write-Host "     Full path from repo root: $dataProcessingPath" -ForegroundColor Gray
Write-Host ""
