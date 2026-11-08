#!/usr/bin/env bash
# Generate a Python experiment deployment YAML for a given number of homes.
# Usage: bash generate-py-experiment.sh <total_homes>

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <total_homes>"
  echo "  e.g.: $0 1500"
  exit 1
fi

TOTAL_HOMES=$1
HOMES_PER_POD=1000
MAX_VCORES_LIMIT=10

OUTDIR="$(cd "$(dirname "$0")" && pwd)"
REGISTRY="acrlakehouseonk8s01.azurecr.io"
KFK_BOOTSTRAP="kafka-0.kafka-svc.data-ingest.svc.cluster.local:9092,kafka-1.kafka-svc.data-ingest.svc.cluster.local:9092"

NUM_PODS=$(( (TOTAL_HOMES + HOMES_PER_POD - 1) / HOMES_PER_POD ))
FILE="$OUTDIR/py-iot-sim-${TOTAL_HOMES}h.yaml"
> "$FILE"

# Budget check: 2 vCores limit per pod
VCORES_LIMIT=$(( NUM_PODS * 2 ))
if (( VCORES_LIMIT > MAX_VCORES_LIMIT )); then
  echo "  ⚠ WARNING: ${TOTAL_HOMES} homes → ${NUM_PODS} pods → ${VCORES_LIMIT} vCores limit (exceeds ${MAX_VCORES_LIMIT})"
fi

START=1
for (( p=0; p<NUM_PODS; p++ )); do
  END=$(( START + HOMES_PER_POD - 1 ))
  if (( END > TOTAL_HOMES )); then END=$TOTAL_HOMES; fi

  if (( p > 0 )); then echo "---" >> "$FILE"; fi

  cat >> "$FILE" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: py-iot-sim-${TOTAL_HOMES}h-pod${p}
  namespace: data-ingest
  labels:
    app: iot-sim-producer
    component: simulator
    experiment: "${TOTAL_HOMES}h"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: iot-sim-producer
      experiment: "${TOTAL_HOMES}h"
      pod-index: "${p}"
  template:
    metadata:
      labels:
        app: iot-sim-producer
        component: simulator
        experiment: "${TOTAL_HOMES}h"
        pod-index: "${p}"
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: producer
        image: ${REGISTRY}/iot_sim_data_producer:latest
        imagePullPolicy: IfNotPresent
        env:
          - name: KFK_BOOTSTRAP_SERVERS
            value: "${KFK_BOOTSTRAP}"
          - { name: PRODUCER_STREAM,     value: "both" }
          - { name: KFK_TOPIC_HOME,      value: "iot-sensor-general" }
          - { name: KFK_TOPIC_PLUG,      value: "iot-sensor-plug" }
          - { name: SIM_HOME_ID_START,   value: "${START}" }
          - { name: SIM_HOME_ID_END,     value: "${END}" }
          - { name: SIM_PLUGS_PER_HOME,  value: "4" }
          - { name: SIM_SLEEP_SEC,       value: "1.0" }
        resources:
          requests: { cpu: "500m", memory: "256Mi" }
          limits:   { cpu: "2",    memory: "1Gi" }
EOF
  START=$(( END + 1 ))
done

RPS=$(( TOTAL_HOMES + TOTAL_HOMES * 4 ))
VCORES_REQ=$(echo "$NUM_PODS * 0.5" | bc)
echo "  ✓ $FILE"
echo "    ${TOTAL_HOMES} homes, ${NUM_PODS} pods"
echo "    vCores: ${VCORES_REQ} req, ${VCORES_LIMIT} limit"
echo "    Estimated RPS: ${RPS}"
echo ""
echo "Apply:    kubectl apply -f $(basename "$FILE")"
echo "Teardown: kubectl delete -f $(basename "$FILE")"
