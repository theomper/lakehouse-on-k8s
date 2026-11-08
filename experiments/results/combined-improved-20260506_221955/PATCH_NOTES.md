## Summary.csv patch — 30-min rollup recovery

`summary.csv` was patched on 2026-05-08 to repair four columns that the orchestrator wrote as zeros despite the post-flush 30-min aggregation actually succeeding for every tier.

### What was wrong
In `scripts/run-experiment-combined-improved.ps1` the function
`Update-PostAggregationMetrics([hashtable]$t1Metrics)` received an
`OrderedDictionary` (created by `[ordered]@{}` in `Collect-Metrics`) under
Windows PowerShell 5.1. The `[hashtable]` parameter cast materialized a NEW
`Hashtable` copy, so updates to `$t1Metrics["..."]` only mutated the copy.
The caller's `$nbT1` / `$bT1` kept the pre-aggregation zeros, which then
flowed into `Append-SummaryCsv`. The orchestrator log reads from the
function-local copy, which is why it printed correct numbers
("Updated 30-min rows: 500, energy: 50455.5 kWh") even though summary.csv
was zero.

### What was patched
For all 20 rows (10 tiers × NB+B), these columns were updated using the
"Updated 30-min rows" / "energy" log lines from
`orchestrator-transcript.log`:

- `paimon_30min_rows_t1`  — restored from log
- `distinct_homes_30min` — set to `tier_homes` (1 row per (home, plug, dt))
- `total_energy_kwh`     — restored from log

### What is still missing
- `peak_power_w` — left as 0.0 (not printed in log)
- `avg_voltage_v` — left blank (not printed in log)

These two are recoverable only by re-running the affected tiers OR if the
`paimon.iot.sensor_data_30min` table on the cluster still holds this run's
final snapshot before the next run overwrites/extends it.

### Originals
The pre-patch file is preserved as
`summary.csv.original-with-zero-30min-bug`.

### Code fixes applied (same day)
1. `Update-PostAggregationMetrics` parameter changed to
   `[System.Collections.IDictionary]$t1Metrics` so the cast no longer copies.
2. Tier T1 file (`tier-NNh-XXX.txt`) is now written AFTER
   `Update-PostAggregationMetrics`, so the per-tier file reflects the final
   30-min rollup state instead of the pre-aggregation zeros.

The fix is in `scripts/run-experiment-combined-improved.ps1`.
