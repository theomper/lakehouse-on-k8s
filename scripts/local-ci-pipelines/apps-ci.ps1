# apps-ci.ps1
# Local equivalent of .github/workflows/apps-ci.yml
# Builds the Flink+Paimon+Kafka image, pushes to ACR, creates K8s secrets,
# deploys all manifests, Flink standalone, Trino and kube-prometheus-stack via Helm.
# Naming convention: "lakehouse-on-k8s" variant (matches deploy-azcli.ps1).
# Prerequisites: az login, kubectl configured, Helm installed.
# Usage: .\apps-ci.ps1

$ErrorActionPreference = "Stop"

# Ensure all relative paths resolve from the repo root
Push-Location (Join-Path $PSScriptRoot "..\..")
try {

# ---------------------------------------------------------------
# Naming convention - "lakehouse-on-k8s" variant
# ---------------------------------------------------------------
$resourceGroup   = "rg-lakehouse-on-k8s-01"
$aksCluster      = "aks-lakehouse-on-k8s-01"
$storageAccount  = "stlakehouseonk8s01"
$acrName         = "acrlakehouseonk8s01"
$acrLoginServer  = "$acrName.azurecr.io"

# K8s manifest / Helm paths
$manifestsDir    = "k8s/manifests"
$flinkStandalone = "k8s/flink"
$trinoValues     = "k8s/helm/trino-values.yaml"
$kpsValues       = "k8s/helm/kube-prometheus-stack-values.yaml"
$iotSimManifest  = "k8s/producers/iot-sim-producers.yaml"

# JAR versions (match CI workflow)
$paimonVersion          = "1.2.0"
$flinkVersion           = "1.20.2"
$flinkMajorVersion      = "1.20"
$kafkaConnectorVersion  = "3.3.0-1.20"
$preBundledHadoop       = "2.8.3-10.0"

# ---------------------------------------------------------------
# 1. Get AKS credentials
# ---------------------------------------------------------------
Write-Host "Getting AKS credentials for $aksCluster..." -ForegroundColor Cyan
az aks get-credentials --resource-group $resourceGroup --name $aksCluster --overwrite-existing

# ---------------------------------------------------------------
# 2. Create namespaces
# ---------------------------------------------------------------
Write-Host "Ensuring namespaces exist..." -ForegroundColor Cyan
foreach ($ns in @("data-ingest", "data-processing", "analytics", "monitoring")) {
    kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f -
}

# ---------------------------------------------------------------
# 3. Download Flink, Paimon, Hadoop & Kafka connector JARs
# ---------------------------------------------------------------
Write-Host "Downloading dependency JARs..." -ForegroundColor Cyan

$libDir = "docker/lib"
if (-not (Test-Path $libDir)) { New-Item -ItemType Directory -Path $libDir -Force | Out-Null }

$jars = @(
    @{
        Name = "flink-shaded-hadoop-2-uber-${preBundledHadoop}.jar"
        Url  = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${preBundledHadoop}/flink-shaded-hadoop-2-uber-${preBundledHadoop}.jar"
    },
    @{
        Name = "flink-azure-fs-hadoop-${flinkVersion}.jar"
        Url  = "https://repo1.maven.org/maven2/org/apache/flink/flink-azure-fs-hadoop/${flinkVersion}/flink-azure-fs-hadoop-${flinkVersion}.jar"
    },
    @{
        Name = "paimon-azure-${paimonVersion}.jar"
        Url  = "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-azure/${paimonVersion}/paimon-azure-${paimonVersion}.jar"
    },
    @{
        Name = "paimon-flink-${flinkMajorVersion}-${paimonVersion}.jar"
        Url  = "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-${flinkMajorVersion}/${paimonVersion}/paimon-flink-${flinkMajorVersion}-${paimonVersion}.jar"
    },
    @{
        Name = "flink-sql-connector-kafka-${kafkaConnectorVersion}.jar"
        Url  = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${kafkaConnectorVersion}/flink-sql-connector-kafka-${kafkaConnectorVersion}.jar"
    }
)

foreach ($jar in $jars) {
    $dest = Join-Path $libDir $jar.Name
    if (Test-Path $dest) {
        Write-Host "  [skip] $($jar.Name) already exists" -ForegroundColor DarkGray
    } else {
        Write-Host "  Downloading $($jar.Name)..."
        Invoke-WebRequest -Uri $jar.Url -OutFile $dest -UseBasicParsing
    }
}

# ---------------------------------------------------------------
# 4. Build and push Flink + Paimon + Kafka image (ACR Task - no local Docker needed)
# ---------------------------------------------------------------
Write-Host "`nBuilding Flink + Paimon + Kafka image via ACR Task..." -ForegroundColor Cyan
$commitHash  = (git rev-parse --short HEAD)
$imageBase   = "flink-paimon-kafka"
$imageLatest = "$acrLoginServer/${imageBase}:latest"

az acr build `
    --registry $acrName `
    --image "${imageBase}:${commitHash}" `
    --image "${imageBase}:latest" `
    --file docker/flink-paimon-kafka.Dockerfile `
    docker

# ---------------------------------------------------------------
# 5. Create/update Azure Storage secret in K8s
# ---------------------------------------------------------------
Write-Host "`nRetrieving storage account key and creating K8s secret..." -ForegroundColor Cyan
$accountKey = (az storage account keys list `
    --account-name $storageAccount `
    --resource-group $resourceGroup `
    --query "[0].value" -o tsv)

foreach ($ns in @("data-processing", "analytics")) {
    kubectl create secret generic azure-storage-secret `
        --from-literal=storage-account-key=$accountKey `
        -n $ns --dry-run=client -o yaml | kubectl apply -f -
}

# ---------------------------------------------------------------
# 6. Deploy K8s manifests
# ---------------------------------------------------------------
Write-Host "`nDeploying Kubernetes manifests..." -ForegroundColor Cyan

kubectl apply -f k8s/manifests/data-ingest.yaml -n data-ingest
kubectl apply -f "$manifestsDir/data-processing.yaml" -n data-processing

# Grafana dashboards -> ConfigMaps
kubectl -n analytics create configmap grafana-dashboard-iot-live-raw `
    --from-file=iot-live-raw.json=grafana/iot-live-raw.json `
    --dry-run=client -o yaml | kubectl apply -f -
kubectl -n analytics create configmap grafana-dashboard-iot-energy-kpis `
    --from-file=iot-energy-kpis.json=grafana/iot-energy-kpis.json `
    --dry-run=client -o yaml | kubectl apply -f -
kubectl -n analytics create configmap grafana-dashboard-iot-appliance-intel `
    --from-file=iot-appliance-intel.json=grafana/iot-appliance-intel.json `
    --dry-run=client -o yaml | kubectl apply -f -
kubectl -n analytics create configmap grafana-dashboard-iot-data-trust `
    --from-file=iot-data-trust.json=grafana/iot-data-trust.json `
    --dry-run=client -o yaml | kubectl apply -f -

# Label + folder annotation for Grafana sidecar
kubectl -n analytics label configmap grafana-dashboard-iot-live-raw grafana_dashboard=1 --overwrite
kubectl -n analytics label configmap grafana-dashboard-iot-energy-kpis grafana_dashboard=1 --overwrite
kubectl -n analytics label configmap grafana-dashboard-iot-appliance-intel grafana_dashboard=1 --overwrite
kubectl -n analytics label configmap grafana-dashboard-iot-data-trust grafana_dashboard=1 --overwrite
kubectl -n analytics annotate configmap grafana-dashboard-iot-live-raw grafana_folder="IoT" --overwrite
kubectl -n analytics annotate configmap grafana-dashboard-iot-energy-kpis grafana_folder="IoT" --overwrite
kubectl -n analytics annotate configmap grafana-dashboard-iot-appliance-intel grafana_folder="IoT" --overwrite
kubectl -n analytics annotate configmap grafana-dashboard-iot-data-trust grafana_folder="IoT" --overwrite

kubectl apply -f k8s/manifests/analytics.yaml -n analytics
kubectl apply -f k8s/manifests/monitoring.yaml -n monitoring
kubectl apply -f $iotSimManifest -n data-ingest

# Flink standalone
kubectl apply -f "$flinkStandalone/flink-conf-configmap.yaml" -n data-processing
kubectl apply -f "$flinkStandalone/flink-jobmanager-service.yaml" -n data-processing
kubectl apply -f "$flinkStandalone/flink-jobmanager-session-deployment-non-ha.yaml" -n data-processing
kubectl apply -f "$flinkStandalone/flink-taskmanager-session-deployment.yaml" -n data-processing

# ---------------------------------------------------------------
# 7. Ensure Helm is available, then deploy Trino
# ---------------------------------------------------------------
if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
    Write-Host "helm not found - installing via winget..." -ForegroundColor Yellow
    winget install --id Helm.Helm -e --accept-source-agreements --accept-package-agreements
    # Refresh PATH so the new binary is visible in this session
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("PATH", "User")
}

Write-Host "`nDeploying Trino via Helm (template + apply, services managed separately)..." -ForegroundColor Cyan
helm repo add trino https://trinodb.github.io/charts
helm repo update

# The Trino chart always emits Services with port 8080, which is blocked by
# azurepolicy-k8sazurev1serviceallowedports. Workaround: render the chart to
# YAML, strip Service documents, apply the rest, then apply headless services.
$trinoRendered = helm template trino-standalone trino/trino `
    --namespace analytics `
    -f $trinoValues

$trinoDocs = ($trinoRendered -split '(?m)^---\s*$') | Where-Object { $_.Trim() -ne '' }
$trinoNonSvc = $trinoDocs | Where-Object { $_ -notmatch '(?m)^\s*kind:\s*Service\b' }
($trinoNonSvc | ForEach-Object { "---`n$_" }) -join "`n" | kubectl apply -n analytics -f -

# Apply headless Trino services (no port declarations)
kubectl apply -f k8s/helm/trino-services.yaml -n analytics

# ---------------------------------------------------------------
# 8. Deploy Prometheus + Grafana (kube-prometheus-stack)
# ---------------------------------------------------------------
Write-Host "`nDeploying kube-prometheus-stack via Helm..." -ForegroundColor Cyan
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install kps prometheus-community/kube-prometheus-stack `
    --namespace monitoring `
    --create-namespace `
    -f $kpsValues

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
Write-Host "`n===== Application Deployment Complete =====" -ForegroundColor Green
Write-Host "Resource Group   : $resourceGroup"
Write-Host "AKS Cluster      : $aksCluster"
Write-Host "Storage Account  : $storageAccount"
Write-Host "ACR Login Server : $acrLoginServer"
Write-Host "Flink Image      : $imageLatest"

} finally { Pop-Location }
