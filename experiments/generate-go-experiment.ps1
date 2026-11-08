<#
.SYNOPSIS
  Generate a Go experiment deployment YAML.
.EXAMPLE
  .\generate-go-experiment.ps1 4000
#>
param(
    [Parameter(Mandatory=$true, Position=0)]
    [int]$TotalHomes,

    [string]$Registry = "acrlakehouseonk8s01.azurecr.io"
)

$HomesPerPod     = 3500
$MaxVCoresLimit  = 10

$OutDir      = $PSScriptRoot
$KfkBootstrap = "kafka-0.kafka-svc.data-ingest.svc.cluster.local:9092,kafka-1.kafka-svc.data-ingest.svc.cluster.local:9092"

$NumPods = [math]::Ceiling($TotalHomes / $HomesPerPod)
$File    = Join-Path $OutDir "go-iot-sim-${TotalHomes}h.yaml"

$VCoresLimit = $NumPods * 2
if ($VCoresLimit -gt $MaxVCoresLimit) {
    Write-Warning "${TotalHomes} homes → ${NumPods} pods → ${VCoresLimit} vCores limit (exceeds ${MaxVCoresLimit})"
}

$content = @()
$Start = 1

for ($p = 0; $p -lt $NumPods; $p++) {
    $End = $Start + $HomesPerPod - 1
    if ($End -gt $TotalHomes) { $End = $TotalHomes }
    $Actual = $End - $Start + 1

    $CpuReq = "500m"
    $CpuLim = "2"
    if ($Actual -le 100) { $CpuReq = "250m"; $CpuLim = "1" }

    if ($p -gt 0) { $content += "---" }

    $content += @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: go-iot-sim-${TotalHomes}h-pod${p}
  namespace: data-ingest
  labels:
    app: go-iot-sim-producer
    component: simulator
    experiment: "${TotalHomes}h"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: go-iot-sim-producer
      experiment: "${TotalHomes}h"
      pod-index: "${p}"
  template:
    metadata:
      labels:
        app: go-iot-sim-producer
        component: simulator
        experiment: "${TotalHomes}h"
        pod-index: "${p}"
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: producer
        image: ${Registry}/go_iot_sim_data_producer:latest
        imagePullPolicy: IfNotPresent
        env:
          - name: KFK_BOOTSTRAP_SERVERS
            value: "${KfkBootstrap}"
          - { name: PRODUCER_STREAM,     value: "both" }
          - { name: KFK_TOPIC_HOME,      value: "iot-sensor-general" }
          - { name: KFK_TOPIC_PLUG,      value: "iot-sensor-plug" }
          - { name: SIM_HOME_ID_START,   value: "${Start}" }
          - { name: SIM_HOME_ID_END,     value: "${End}" }
          - { name: SIM_PLUGS_PER_HOME,  value: "4" }
          - { name: SIM_SLEEP_SEC,       value: "1.0" }
        resources:
          requests: { cpu: "${CpuReq}", memory: "256Mi" }
          limits:   { cpu: "${CpuLim}", memory: "1Gi" }
"@

    $Start = $End + 1
}

$content | Out-File -FilePath $File -Encoding utf8

$Rps = $TotalHomes + $TotalHomes * 4
$VCoresReq = $NumPods * 0.5
$FileName = Split-Path $File -Leaf
Write-Host ('  [OK] ' + $File)
Write-Host ('    {0} homes, {1} pods' -f $TotalHomes, $NumPods)
Write-Host ('    vCores: {0} req, {1} limit' -f $VCoresReq, $VCoresLimit)
Write-Host ('    Estimated RPS: {0}' -f $Rps)
Write-Host ''
Write-Host ('Apply:    kubectl apply -f ' + $FileName)
Write-Host ('Teardown: kubectl delete -f ' + $FileName)
