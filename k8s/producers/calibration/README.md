# HomesPerPod Calibration

## Why calibrate?

Each producer pod has a CPU limit. Increasing the number of simulated homes
increases CPU and network load. At some point the producer can't complete a
full cycle within the 1-second interval — it becomes **saturated**.

**HomesPerPod** is the largest home count a single pod can sustain while keeping
cycle time under 80% of the interval (safety margin).

## What the producers log

Both producers print stats **once per minute**:

### Go producer
```
[stats] ok=2500 err=0 total=2500 rps=250
[cycle] homes=500 avg=12ms max=45ms interval=1.0s saturated=false
```

| Field | Meaning |
|-------|---------|
| `avg` | Average cycle time across all home goroutines |
| `max` | Worst-case cycle time across all homes |
| `saturated` | `true` if `max > 80% of interval` |

### Python producer
```
[stats] ok=250 err=0 total=250 rps=250
[cycle] homes=50 duration=0.320s interval=1.0s saturated=False
```

| Field | Meaning |
|-------|---------|
| `duration` | Time to complete one full loop (all homes, send + flush) |
| `saturated` | `True` if `duration > 80% of interval` |

## How to run

### Prerequisites

- `kubectl` configured for your AKS cluster
- Producer images pushed to ACR
- Kafka running in `data-ingest` namespace

### Go calibration

**Bash:**
```bash
# Default steps: 250 500 750 1000 1250 1500 2000
bash run-go-calibration.sh

# Custom steps
bash run-go-calibration.sh "250 500 1000 2000"
```

**PowerShell:**
```powershell
.\run-go-calibration.ps1

.\run-go-calibration.ps1 -Steps @(250, 500, 1000, 2000)
```

### Python calibration

**Bash:**
```bash
# Default steps: 25 50 75 100 150 200 300
bash run-py-calibration.sh

# Custom steps
bash run-py-calibration.sh "25 50 100 200"
```

**PowerShell:**
```powershell
.\run-py-calibration.ps1

.\run-py-calibration.ps1 -Steps @(25, 50, 100, 200)
```

## What happens per step

1. **Deploy** a single pod with `SIM_HOME_ID_START=1` and `SIM_HOME_ID_END=<step>`
2. **Wait 3 minutes** for stabilization (goroutines/threads warm up, TCP connections settle)
3. **Collect stats** for 2 minutes from pod logs
4. **Record** cycle metrics and rps
5. **Tear down** and wait 30s before next step

Total: ~5.5 minutes per step.

## Reading the results

### Go example (`go-calibration-results.txt`)

```
Homes      RPS        CycleAvg     CycleMax     Interval     Saturated
-----      ---        --------     --------     --------     ---------
250        1250       15ms         89ms         1.0s         false
500        2500       22ms         210ms        1.0s         false
750        3750       35ms         520ms        1.0s         false
1000       5000       48ms         780ms        1.0s         false
1250       6250       65ms         950ms        1.0s         true
```

**Result: HomesPerPod = 1000** (last row where `saturated=false` and `CycleMax < 800ms`)

### Python example (`py-calibration-results.txt`)

```
Homes      RPS        CycleDuration  Interval     Saturated
-----      ---        -------------  --------     ---------
25         125        0.085s         1.0s         False
50         250        0.190s         1.0s         False
75         375        0.350s         1.0s         False
100        500        0.620s         1.0s         False
150        750        0.950s         1.0s         True
```

**Result: HomesPerPod = 100** (last row where `saturated=False` and `duration < 0.8s`)

## After calibration

Update `HOMES_PER_POD` in the experiment generator scripts:

| File | Variable |
|------|----------|
| `generate-go-experiment.sh` | `HOMES_PER_POD=250` → your Go result |
| `generate-go-experiment.ps1` | `$HomesPerPod = 250` → your Go result |
| `generate-py-experiment.sh` | `HOMES_PER_POD=50` → your Python result |
| `generate-py-experiment.ps1` | `$HomesPerPod = 50` → your Python result |

Then regenerate experiment YAMLs:
```bash
bash ../generate-go-experiment.sh 2000
bash ../generate-py-experiment.sh 200
```

## Files

| File | Purpose |
|------|---------|
| `go-calibration-pod.yaml` | Go single-pod deployment template |
| `py-calibration-pod.yaml` | Python single-pod deployment template |
| `run-go-calibration.sh` | Bash Go calibration runner |
| `run-go-calibration.ps1` | PowerShell Go calibration runner |
| `run-py-calibration.sh` | Bash Python calibration runner |
| `run-py-calibration.ps1` | PowerShell Python calibration runner |
| `go-calibration-results.txt` | Go results (generated after a run) |
| `py-calibration-results.txt` | Python results (generated after a run) |
| `README.md` | This file |
