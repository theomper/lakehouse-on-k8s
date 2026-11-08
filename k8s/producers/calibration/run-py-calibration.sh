#!/usr/bin/env bash
# Calibrate HomesPerPod for the Python producer.
# Usage: bash run-py-calibration.sh [step_list]
#   Default steps: 25 50 75 100 150 200 300

set -euo pipefail

STEPS="${1:-25 50 75 100 150 200 300}"
NAMESPACE="data-ingest"
DEPLOY_NAME="py-calibration"
TEMPLATE="$(cd "$(dirname "$0")" && pwd)/py-calibration-pod.yaml"
WAIT_SETTLE=60
WAIT_STATS=120
RESULTS_FILE="$(cd "$(dirname "$0")" && pwd)/py-calibration-results.txt"

echo "================================================"
echo " Python Producer Calibration — HomesPerPod sizing"
echo "================================================"
echo ""
echo "Steps: $STEPS"
echo "Settle: ${WAIT_SETTLE}s, Stats window: ${WAIT_STATS}s"
echo "Results: $RESULTS_FILE"
echo ""

printf "%-10s %-10s %-14s %-12s %-10s\n" \
  "Homes" "RPS" "CycleDuration" "Interval" "Saturated" \
  | tee "$RESULTS_FILE"
printf "%-10s %-10s %-14s %-12s %-10s\n" \
  "-----" "---" "-------------" "--------" "---------" \
  | tee -a "$RESULTS_FILE"

for HOMES in $STEPS; do
  echo ""
  echo ">>> Testing with $HOMES homes..."

  PATCHED="/tmp/py-calibration-${HOMES}h.yaml"
  sed "s/value: \"PLACEHOLDER\"/value: \"${HOMES}\"/" "$TEMPLATE" > "$PATCHED"

  kubectl apply -f "$PATCHED" -n "$NAMESPACE" 2>/dev/null
  echo "    Deployed. Waiting ${WAIT_SETTLE}s for pod to settle..."
  sleep "$WAIT_SETTLE"

  POD=$(kubectl get pods -n "$NAMESPACE" -l app="$DEPLOY_NAME" \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

  if [[ -z "$POD" ]]; then
    echo "    ⚠ No running pod found, skipping $HOMES homes"
    kubectl delete -f "$PATCHED" -n "$NAMESPACE" --ignore-not-found 2>/dev/null
    continue
  fi

  echo "    Pod: $POD — collecting stats for ${WAIT_STATS}s..."
  sleep "$WAIT_STATS"

  STATS_LINE=$(kubectl logs "$POD" -n "$NAMESPACE" --tail=20 2>/dev/null \
    | grep '\[stats\]' | tail -1 || echo "")
  CYCLE_LINE=$(kubectl logs "$POD" -n "$NAMESPACE" --tail=20 2>/dev/null \
    | grep '\[cycle\]' | tail -1 || echo "")

  RPS=$(echo "$STATS_LINE" | grep -oP 'rps=\K[0-9]+' || echo "?")
  CDUR=$(echo "$CYCLE_LINE" | grep -oP 'duration=\K[^ ]+' || echo "?")
  INTERVAL=$(echo "$CYCLE_LINE" | grep -oP 'interval=\K[^ ]+' || echo "?")
  SAT=$(echo "$CYCLE_LINE" | grep -oP 'saturated=\K[^ ]+' || echo "?")

  printf "%-10s %-10s %-14s %-12s %-10s\n" \
    "$HOMES" "$RPS" "$CDUR" "$INTERVAL" "$SAT" \
    | tee -a "$RESULTS_FILE"

  kubectl delete -f "$PATCHED" -n "$NAMESPACE" --ignore-not-found 2>/dev/null
  echo "    Cleaned up. Waiting 30s before next step..."
  sleep 30
  rm -f "$PATCHED"
done

echo ""
echo "================================================"
echo " Python calibration complete! Results: $RESULTS_FILE"
echo "================================================"
echo ""
echo "Pick the largest Homes where Saturated=False and duration < 0.8s."
echo "Update HOMES_PER_POD in generate-py-experiment.sh / .ps1 accordingly."
