"""
Generate experiment-matrix Excel spreadsheet for May 06 run.
Sources: matrix-full.csv, summary.csv, per-tier JSON files.
"""
import json, csv, os, sys
from pathlib import Path
from collections import OrderedDict

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    print("ERROR: openpyxl not found. Run: pip install openpyxl")
    sys.exit(1)

RESULTS_DIR = Path(r"C:\path\to\lakehouse-on-k8s\experiments\results\combined-improved-20260506_221955")
OUTPUT = Path(r"C:\path\to\lakehouse-on-k8s\experiments\results\experiment-matrix-lakehouse-on-k8s-060526.xlsx")

TIERS = [50, 500, 1500, 3500, 5500, 7500, 10000, 12500, 15000, 17500]
TIER_NAMES = {50:'T1', 500:'T2', 1500:'T3', 3500:'T4', 5500:'T5',
              7500:'T6', 10000:'T7', 12500:'T8', 15000:'T9', 17500:'T10'}
RUN_ID = {'nonbatched': {50:'N1',500:'N2',1500:'N3',3500:'N4',5500:'N5',
                         7500:'N6',10000:'N7',12500:'N8',15000:'N9',17500:'N10'},
          'batched':    {50:'B1',500:'B2',1500:'B3',3500:'B4',5500:'B5',
                         7500:'B6',10000:'B7',12500:'B8',15000:'B9',17500:'B10'}}

def prom_val(s):
    if not s:
        return None
    try:
        if isinstance(s, str):
            d = json.loads(s)
        else:
            d = s
        r = d.get('data', {}).get('result', [])
        if r:
            v = r[0]['value'][1]
            return None if v in ('NaN','nan','Inf','+Inf','-Inf') else float(v)
    except Exception:
        pass
    return None

def sf(v, scale=1):
    if v is None or v == '' or str(v).strip() == '':
        return None
    try:
        return float(v) * scale
    except Exception:
        return None

def si(v):
    r = sf(v)
    return int(r) if r is not None else None

def read_json(fname):
    p = RESULTS_DIR / fname
    return json.loads(p.read_text(encoding='utf-8-sig')) if p.exists() else {}

def read_t1_kv(fname):
    """Parse tier-*-{phase}.txt into key→value dict (handles 'key = value' lines)."""
    p = RESULTS_DIR / fname
    if not p.exists():
        return {}
    d = {}
    for line in p.read_text(encoding='utf-8-sig', errors='ignore').splitlines():
        if '=' in line and not line.strip().startswith('─') and not line.strip().startswith('='):
            k, _, v = line.partition('=')
            d[k.strip()] = v.strip()
    return d

# ── Load summary.csv ─────────────────────────────────────────────────────────
summary = {}
with open(RESULTS_DIR / 'summary.csv', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        summary[(row['producer_type'], int(row['tier_homes']))] = row

# ── Load matrix-full.csv ─────────────────────────────────────────────────────
matrix = {}
with open(RESULTS_DIR / 'matrix-full.csv', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        matrix[row['run_id']] = row

# ── Build one record per (phase, tier) ───────────────────────────────────────
all_rows = []
for phase in ('nonbatched', 'batched'):
    for tier in TIERS:
        rid  = RUN_ID[phase][tier]
        s    = summary.get((phase, tier), {})
        m    = matrix.get(rid, {})
        prom = read_json(f"tier-{tier}h-{phase}-granular-prometheus.json")
        ckpt = read_json(f"tier-{tier}h-{phase}-checkpoints.json")
        az   = read_json(f"tier-{tier}h-{phase}-azure-storage.json")
        tbm  = read_json(f"tier-{tier}h-{phase}-trino-benchmark.json")
        prod = read_json(f"tier-{tier}h-{phase}-producer-stats.json")
        t1kv = read_t1_kv(f"tier-{tier}h-{phase}.txt")

        # ── Prometheus pod metrics ────────────────────────────────────────────
        def sum_dict(d):
            """Sum all values in a pod-keyed dict."""
            return sum(d.values()) if d else None

        def pod_val(d, key_part):
            """Get value for a pod whose name contains key_part."""
            for k, v in d.items():
                if key_part in k:
                    return v
            return None

        # Kafka
        kc = prom.get('kafka_cpu', {})
        km = prom.get('kafka_mem', {})
        kp = prom.get('kafka_pvc', {})
        krx= prom.get('kafka_netrx', {})
        ktx= prom.get('kafka_nettx', {})

        k0_cpu = pod_val(kc, 'kafka-0')
        k1_cpu = pod_val(kc, 'kafka-1')
        k0_mem_gib = (pod_val(km,'kafka-0') or 0) / 1024**3 or None
        k1_mem_gib = (pod_val(km,'kafka-1') or 0) / 1024**3 or None
        k0_pvc_gib = (pod_val(kp,'kafka-0') or 0) / 1024**3 or None
        k1_pvc_gib = (pod_val(kp,'kafka-1') or 0) / 1024**3 or None
        k0_rx = (pod_val(krx,'kafka-0') or 0) / 1024**2 or None
        k1_rx = (pod_val(krx,'kafka-1') or 0) / 1024**2 or None
        k0_tx = (pod_val(ktx,'kafka-0') or 0) / 1024**2 or None
        k1_tx = (pod_val(ktx,'kafka-1') or 0) / 1024**2 or None

        # Flink pods
        fc  = prom.get('flink_cpu', {})
        fm  = prom.get('flink_mem', {})
        frx = prom.get('flink_netrx', {})
        ftx = prom.get('flink_nettx', {})
        ffs = prom.get('flink_fs', {})

        jm_pods = sorted([k for k in fc if 'jobmanager' in k])
        tm_pods = sorted([k for k in fc if 'taskmanager' in k])
        jm = jm_pods[0] if jm_pods else None
        tm1= tm_pods[0] if len(tm_pods) > 0 else None
        tm2= tm_pods[1] if len(tm_pods) > 1 else None

        def fp(d, pod, scale=1):
            if pod and pod in d:
                return d[pod] * scale
            return None

        # Trino pods
        tc  = prom.get('trino_cpu', {})
        tmem= prom.get('trino_mem', {})
        coord = sorted([k for k in tc if 'coordinator' in k])
        workers= sorted([k for k in tc if 'worker' in k])
        c_pod = coord[0] if coord else None
        w1_pod= workers[0] if len(workers) > 0 else None
        w2_pod= workers[1] if len(workers) > 1 else None

        # Kafka consumer lag + Flink health from T1 .txt file (summary.csv JSON fields are unquoted)
        lag  = prom_val(t1kv.get('prom_kafka_consumer_lag_max',''))
        busy = prom_val(t1kv.get('prom_flink_busy_time_ms_per_sec',''))
        bp   = prom_val(t1kv.get('prom_flink_backpressured_time_ms_per_sec',''))
        flink_idle = prom_val(t1kv.get('prom_flink_idle_time_ms_per_sec',''))

        delta = ckpt.get('delta', {})
        failover = 'Y' if (delta.get('restored') or 0) > 0 else 'N'

        row = {
            # Identity
            'run_id':       rid,
            'phase':        'Nonbatched' if phase == 'nonbatched' else 'Batched',
            'tier':         TIER_NAMES[tier],
            'homes':        tier,
            'go_pods':      si(m.get('go_pods')),
            'homes_per_pod':si(m.get('homes_per_go_pod')),
            'expected_rps': si(m.get('expected_rps')),
            # Producer
            'prod_rps':     sf(prod.get('observed_rps')),
            'kafka_rps':    sf(s.get('actual_rps')),
            'ingest_ratio': sf(s.get('ingestion_ratio')),
            'avg_cycle_s':  sf(prod.get('avg_cycle_s')),
            'max_cycle_s':  sf(prod.get('max_cycle_s')),
            'saturated':    prod.get('saturated',''),
            'ok_delta':     si(prod.get('ok_delta')),
            'err_delta':    si(prod.get('err_delta')),
            'error_rate':   sf(prod.get('error_rate')),
            # Kafka brokers (Prometheus 30-min avg)
            'k0_cpu':   k0_cpu,
            'k0_mem':   k0_mem_gib,
            'k0_rx':    k0_rx,
            'k0_tx':    k0_tx,
            'k0_pvc':   k0_pvc_gib,
            'k1_cpu':   k1_cpu,
            'k1_mem':   k1_mem_gib,
            'k1_rx':    k1_rx,
            'k1_tx':    k1_tx,
            'k1_pvc':   k1_pvc_gib,
            # Kafka topic offsets
            'kafka_gen_delta':  si(s.get('kafka_general_delta')),
            'kafka_plug_delta': si(s.get('kafka_plug_delta')),
            'kafka_total_delta':si(s.get('kafka_total_delta')),
            'kafka_lag':        lag,
            # Flink JM
            'jm_cpu':   fp(fc, jm),
            'jm_mem':   fp(fm, jm, 1/1024**3),
            'jm_rx':    fp(frx,jm, 1/1024**2),
            'jm_tx':    fp(ftx,jm, 1/1024**2),
            'jm_fs':    fp(ffs,jm, 1/1024**3),
            # Flink TM-1
            'tm1_cpu':  fp(fc, tm1),
            'tm1_mem':  fp(fm, tm1, 1/1024**3),
            'tm1_rx':   fp(frx,tm1,1/1024**2),
            'tm1_tx':   fp(ftx,tm1,1/1024**2),
            'tm1_fs':   fp(ffs,tm1,1/1024**3),
            # Flink TM-2
            'tm2_cpu':  fp(fc, tm2),
            'tm2_mem':  fp(fm, tm2, 1/1024**3),
            'tm2_rx':   fp(frx,tm2,1/1024**2),
            'tm2_tx':   fp(ftx,tm2,1/1024**2),
            'tm2_fs':   fp(ffs,tm2,1/1024**3),
            # Flink pipeline health
            'flink_busy': busy,
            'flink_bp':   bp,
            'flink_idle': flink_idle,
            'flink_jobs': si(t1kv.get('flink_running_jobs') or s.get('flink_running_jobs')),
            'ckpt_done':  si(delta.get('completed')),
            'ckpt_fail':  si(delta.get('failed')),
            'ckpt_rest':  si(delta.get('restored')),
            'failover':   failover,
            # Paimon / SLO
            'raw_t0':   si(s.get('paimon_raw_rows_t0')),
            'raw_t1':   si(s.get('paimon_raw_rows_t1')),
            'raw_delta':si(s.get('paimon_raw_delta')),
            'tenmin_t1':si(s.get('paimon_10min_rows_t1')),
            'thirtymin_t1':si(s.get('paimon_30min_rows_t1')),
            'homes_raw': si(s.get('distinct_homes_raw')),
            'homes_30m': si(s.get('distinct_homes_30min')),
            'slo':      sf(s.get('slo_kafka_vs_paimon_raw')),
            # Azure Storage
            'az_tx':    si(az.get('transactions')),
            'az_in':    az.get('ingress_gib'),
            'az_out':   az.get('egress_gib'),
            'az_e2e':   az.get('e2e_latency_ms'),
            'az_srv':   az.get('server_latency_ms'),
            'az_avail': az.get('availability_pct'),
            'az_cap':   az.get('used_capacity_gib'),
            # E2E latency (s) from wall-clock vs MAX(ts) in raw
            'e2e_s':    sf(s.get('e2e_latency_seconds')),
            # Trino Q1-Q5 p95 (s) from trino-benchmark.json
            'q1_p95':   tbm.get('q1_p95_s'),
            'q2_p95':   tbm.get('q2_p95_s'),
            'q3_p95':   tbm.get('q3_p95_s'),
            'q4_p95':   tbm.get('q4_p95_s'),
            'q5_p95':   tbm.get('q5_p95_s'),
            # Trino Q1-Q5 median/min/max (ms) from summary.csv
            'q1_med':   sf(s.get('trino_q1_count_raw_median_ms')),
            'q1_min':   sf(s.get('trino_q1_count_raw_min_ms')),
            'q1_max':   sf(s.get('trino_q1_count_raw_max_ms')),
            'q2_med':   sf(s.get('trino_q2_raw_groupby_median_ms')),
            'q2_min':   sf(s.get('trino_q2_raw_groupby_min_ms')),
            'q2_max':   sf(s.get('trino_q2_raw_groupby_max_ms')),
            'q3_med':   sf(s.get('trino_q3_window_fn_median_ms')),
            'q3_min':   sf(s.get('trino_q3_window_fn_min_ms')),
            'q3_max':   sf(s.get('trino_q3_window_fn_max_ms')),
            'q4_med':   sf(s.get('trino_q4_join_median_ms')),
            'q4_min':   sf(s.get('trino_q4_join_min_ms')),
            'q4_max':   sf(s.get('trino_q4_join_max_ms')),
            'q5_med':   sf(s.get('trino_q5_percentile_median_ms')),
            'q5_min':   sf(s.get('trino_q5_percentile_min_ms')),
            'q5_max':   sf(s.get('trino_q5_percentile_max_ms')),
            'trino_fail':si(tbm.get('failures',0)),
            # Trino cluster resources (Prometheus 30-min avg)
            'tc_cpu':  fp(tc,  c_pod),
            'tw1_cpu': fp(tc,  w1_pod),
            'tw2_cpu': fp(tc,  w2_pod),
            'tc_mem':  fp(tmem,c_pod,  1/1024**3),
            'tw1_mem': fp(tmem,w1_pod, 1/1024**3),
            'tw2_mem': fp(tmem,w2_pod, 1/1024**3),
            # Domain metrics (from 30-min rollup; peak_power/avg_voltage unavailable due to bug)
            'energy_kwh':   sf(s.get('total_energy_kwh')),
            'peak_power_w': sf(s.get('peak_power_w')),   # 0 — bug
            'avg_voltage_v':sf(s.get('avg_voltage_v')) if s.get('avg_voltage_v') else None,
            'notes': m.get('notes',''),
        }
        all_rows.append(row)

# ── Column definitions ────────────────────────────────────────────────────────
# (section_key, header_text, data_key, number_format, col_width)
COLS = [
    # Identity — FROZEN (cols 1-7)
    ('id',  'Run ID',            'run_id',      '@',       7),
    ('id',  'Phase',             'phase',       '@',      12),
    ('id',  'Tier',              'tier',        '@',       6),
    ('id',  'Homes',             'homes',       '#,##0',   8),
    ('id',  'Go\nPods',          'go_pods',     '0',       7),
    ('id',  'Homes/\nPod',       'homes_per_pod','#,##0',  9),
    ('id',  'Expected\nRPS',     'expected_rps','#,##0',  10),
    # Producer performance
    ('prod','Producer\nRPS',     'prod_rps',    '#,##0.0',10),
    ('prod','Kafka\nRPS',        'kafka_rps',   '#,##0.0',10),
    ('prod','Ingestion\nRatio',  'ingest_ratio','0.0000', 10),
    ('prod','Avg Cycle\n(s)',    'avg_cycle_s', '0.000',  10),
    ('prod','Max Cycle\n(s)',    'max_cycle_s', '0.000',  10),
    ('prod','Saturated?',        'saturated',   '@',      10),
    ('prod','OK (Δ)',            'ok_delta',    '#,##0',  12),
    ('prod','Errors (Δ)',        'err_delta',   '#,##0',  10),
    ('prod','Error Rate',        'error_rate',  '0.000',  10),
    # Kafka brokers
    ('kafka','K0 CPU\n(cores)',   'k0_cpu',   '0.000',10),
    ('kafka','K0 Mem\n(GiB)',     'k0_mem',   '0.000',10),
    ('kafka','K0 Net RX\n(MiB/s)','k0_rx',    '0.000',12),
    ('kafka','K0 Net TX\n(MiB/s)','k0_tx',    '0.000',12),
    ('kafka','K0 PVC\n(GiB)',     'k0_pvc',   '0.000',10),
    ('kafka','K1 CPU\n(cores)',   'k1_cpu',   '0.000',10),
    ('kafka','K1 Mem\n(GiB)',     'k1_mem',   '0.000',10),
    ('kafka','K1 Net RX\n(MiB/s)','k1_rx',    '0.000',12),
    ('kafka','K1 Net TX\n(MiB/s)','k1_tx',    '0.000',12),
    ('kafka','K1 PVC\n(GiB)',     'k1_pvc',   '0.000',10),
    ('kafka','General\nΔ (msgs)', 'kafka_gen_delta', '#,##0',14),
    ('kafka','Plug\nΔ (msgs)',    'kafka_plug_delta','#,##0',14),
    ('kafka','Total Kafka\nΔ (msgs)','kafka_total_delta','#,##0',15),
    ('kafka','Consumer\nLag Max', 'kafka_lag', '#,##0',  12),
    # Flink JM
    ('fjm','JM CPU\n(cores)',    'jm_cpu',  '0.000',10),
    ('fjm','JM Mem\n(GiB)',      'jm_mem',  '0.000',10),
    ('fjm','JM Net RX\n(MiB/s)','jm_rx',   '0.000',12),
    ('fjm','JM Net TX\n(MiB/s)','jm_tx',   '0.000',12),
    ('fjm','JM FS\n(GiB)',       'jm_fs',   '0.0000',10),
    # Flink TM-1
    ('ftm1','TM-1 CPU\n(cores)',   'tm1_cpu', '0.000',11),
    ('ftm1','TM-1 Mem\n(GiB)',     'tm1_mem', '0.000',11),
    ('ftm1','TM-1 RX\n(MiB/s)',   'tm1_rx',  '0.000',12),
    ('ftm1','TM-1 TX\n(MiB/s)',   'tm1_tx',  '0.000',12),
    ('ftm1','TM-1 FS\n(GiB)',      'tm1_fs',  '0.0000',10),
    # Flink TM-2
    ('ftm2','TM-2 CPU\n(cores)',   'tm2_cpu', '0.000',11),
    ('ftm2','TM-2 Mem\n(GiB)',     'tm2_mem', '0.000',11),
    ('ftm2','TM-2 RX\n(MiB/s)',   'tm2_rx',  '0.000',12),
    ('ftm2','TM-2 TX\n(MiB/s)',   'tm2_tx',  '0.000',12),
    ('ftm2','TM-2 FS\n(GiB)',      'tm2_fs',  '0.0000',10),
    # Flink pipeline health
    ('fhlt','Busy\n(ms/s)',        'flink_busy', '0.00', 10),
    ('fhlt','Idle\n(ms/s)',        'flink_idle', '0.00', 10),
    ('fhlt','Backpress.\n(ms/s)',  'flink_bp',   '0.00', 12),
    ('fhlt','Running\nJobs',       'flink_jobs','0',     9),
    ('fhlt','Ckpt\nDone Δ',        'ckpt_done', '0',     9),
    ('fhlt','Ckpt\nFail Δ',        'ckpt_fail', '0',     9),
    ('fhlt','Ckpt\nRest. Δ',       'ckpt_rest', '0',    10),
    ('fhlt','Failover?',           'failover',  '@',     9),
    # Paimon / SLO
    ('paimon','Raw Rows\nT0',       'raw_t0',     '#,##0',13),
    ('paimon','Raw Rows\nT1',       'raw_t1',     '#,##0',13),
    ('paimon','Raw Rows\nΔ',        'raw_delta',  '#,##0',13),
    ('paimon','10min Rows\nT1',     'tenmin_t1',  '#,##0',13),
    ('paimon','30min Rows\nT1',     'thirtymin_t1','#,##0',13),
    ('paimon','Homes\n(raw)',        'homes_raw',  '#,##0',10),
    ('paimon','Homes\n(30min)',      'homes_30m',  '#,##0',12),
    ('paimon','SLO\n(Paimon/Kafka)','slo',         '0.0000',14),
    # Azure Storage
    ('az','SA Tx\n(sum)',          'az_tx',   '#,##0',11),
    ('az','SA Ingress\n(GiB)',     'az_in',   '0.000',12),
    ('az','SA Egress\n(GiB)',      'az_out',  '0.000',12),
    ('az','SA E2E\nLat (ms)',      'az_e2e',  '0.0',  12),
    ('az','SA Srv\nLat (ms)',      'az_srv',  '0.0',  12),
    ('az','SA Avail\n(%)',         'az_avail','0.00', 10),
    ('az','SA Capacity\n(GiB)',    'az_cap',  '0.000',13),
    # E2E latency
    ('lat','E2E Latency\n(s)',     'e2e_s',   '0.0',  12),
    # Trino Q1
    ('tq','Q1 p95 (s)',            'q1_p95',  '0.00',  9),
    ('tq','Q1 Median\n(ms)',       'q1_med',  '#,##0',11),
    ('tq','Q1 Min\n(ms)',          'q1_min',  '#,##0',10),
    ('tq','Q1 Max\n(ms)',          'q1_max',  '#,##0',10),
    # Trino Q2
    ('tq','Q2 p95 (s)',            'q2_p95',  '0.00',  9),
    ('tq','Q2 Median\n(ms)',       'q2_med',  '#,##0',11),
    ('tq','Q2 Min\n(ms)',          'q2_min',  '#,##0',10),
    ('tq','Q2 Max\n(ms)',          'q2_max',  '#,##0',10),
    # Trino Q3
    ('tq','Q3 p95 (s)',            'q3_p95',  '0.00',  9),
    ('tq','Q3 Median\n(ms)',       'q3_med',  '#,##0',11),
    ('tq','Q3 Min\n(ms)',          'q3_min',  '#,##0',10),
    ('tq','Q3 Max\n(ms)',          'q3_max',  '#,##0',10),
    # Trino Q4
    ('tq','Q4 p95 (s)',            'q4_p95',  '0.00',  9),
    ('tq','Q4 Median\n(ms)',       'q4_med',  '#,##0',11),
    ('tq','Q4 Min\n(ms)',          'q4_min',  '#,##0',10),
    ('tq','Q4 Max\n(ms)',          'q4_max',  '#,##0',10),
    # Trino Q5
    ('tq','Q5 p95 (s)',            'q5_p95',  '0.00',  9),
    ('tq','Q5 Median\n(ms)',       'q5_med',  '#,##0',11),
    ('tq','Q5 Min\n(ms)',          'q5_min',  '#,##0',10),
    ('tq','Q5 Max\n(ms)',          'q5_max',  '#,##0',10),
    ('tq','Trino\nFailures',       'trino_fail','0',  10),
    # Trino cluster resources
    ('tres','Coord CPU\n(cores)',  'tc_cpu',  '0.000',12),
    ('tres','Worker-1\nCPU',      'tw1_cpu', '0.000',12),
    ('tres','Worker-2\nCPU',      'tw2_cpu', '0.000',12),
    ('tres','Coord Mem\n(GiB)',   'tc_mem',  '0.000',12),
    ('tres','Worker-1\nMem (GiB)','tw1_mem', '0.000',13),
    ('tres','Worker-2\nMem (GiB)','tw2_mem', '0.000',13),
    # Domain metrics
    ('dom','Energy\n(kWh)',        'energy_kwh',   '#,##0.000',14),
    ('dom','Peak Power\n(W)†',     'peak_power_w', '#,##0.000',13),
    ('dom','Avg Voltage\n(V)†',    'avg_voltage_v','0.000',    13),
    # Notes
    ('misc', 'Notes',             'notes',        '@',        22),
]

SECTION_LABELS = {
    'id':    'Identity',
    'prod':  'Producer Performance',
    'kafka': 'Kafka Brokers',
    'fjm':   'Flink JobManager',
    'ftm1':  'Flink TaskManager-1',
    'ftm2':  'Flink TaskManager-2',
    'fhlt':  'Flink Pipeline Health',
    'paimon':'Paimon / SLO',
    'az':    'Azure Storage Account',
    'lat':   'E2E Latency',
    'tq':    'Trino Queries (Q1–Q5)',
    'tres':  'Trino Cluster Resources',
    'dom':   'Domain Metrics  † bug: 30-min rollup patch; peak_power/avg_voltage unrecoverable',
    'misc':  'Notes',
}

SECTION_COLORS = {
    'id':    'D8D8D8',
    'prod':  'BDD7EE',
    'kafka': 'FCE4D6',
    'fjm':   'C6EFCE',
    'ftm1':  'B8E6C0',
    'ftm2':  'AADDB8',
    'fhlt':  'EBFACC',
    'paimon':'FADADD',
    'az':    'CCF2FF',
    'lat':   'FFFACC',
    'tq':    'E8CCFF',
    'tres':  'DBC8FF',
    'dom':   'FFD6CC',
    'misc':  'E8E8E8',
}

# ── Build section spans ───────────────────────────────────────────────────────
spans = OrderedDict()
for i, (sec, *_) in enumerate(COLS):
    cn = i + 1
    if sec not in spans:
        spans[sec] = [cn, cn]
    else:
        spans[sec][1] = cn

FREEZE_AT_COL = 8  # freeze first 7 identity cols; col 8 = first scrollable

# ── Create workbook ───────────────────────────────────────────────────────────
wb = Workbook()
ws = wb.active
ws.title = 'Experiment Data'

thin  = Side(style='thin',   color='BBBBBB')
med   = Side(style='medium', color='888888')
bdr   = Border(left=thin, right=thin, top=thin, bottom=thin)
bdr_r = Border(left=thin, right=med,  top=thin, bottom=thin)

def fill(hex_color):
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type='solid')

# Row 1: section headers
for sec, (sc, ec) in spans.items():
    color = SECTION_COLORS.get(sec, 'EEEEEE')
    if sc < ec:
        ws.merge_cells(start_row=1, start_column=sc, end_row=1, end_column=ec)
    c = ws.cell(row=1, column=sc, value=SECTION_LABELS.get(sec, sec))
    c.font      = Font(bold=True, size=9)
    c.fill      = fill(color)
    c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    c.border    = bdr_r

# Row 2: column headers
for i, (sec, hdr, key, fmt, w) in enumerate(COLS):
    cn = i + 1
    c = ws.cell(row=2, column=cn, value=hdr)
    c.font      = Font(bold=True, size=8)
    c.fill      = fill(SECTION_COLORS.get(sec,'EEEEEE'))
    c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    # Thicker right border at section boundaries
    ec = spans[sec][1]
    c.border = bdr_r if cn == ec else bdr
    ws.column_dimensions[get_column_letter(cn)].width = w

# Row heights
ws.row_dimensions[1].height = 28
ws.row_dimensions[2].height = 36

# Data rows
for ri, rd in enumerate(all_rows):
    rn = ri + 3
    is_nb = rd['phase'] == 'Nonbatched'
    row_bg = 'F5F9FF' if is_nb else 'FFF9F5'

    for ci, (sec, hdr, key, fmt, w) in enumerate(COLS):
        cn = ci + 1
        v  = rd.get(key)
        c  = ws.cell(row=rn, column=cn, value=v)
        c.font      = Font(size=9)
        c.alignment = Alignment(horizontal='center', vertical='center')
        ec = spans[sec][1]
        c.border = bdr_r if cn == ec else bdr

        # Number format
        if fmt != '@' and v is not None:
            c.number_format = fmt

        # Background
        if cn < FREEZE_AT_COL:
            c.fill = fill('EDEDED')
        else:
            c.fill = fill(row_bg)

        # Highlight conditions
        if key == 'saturated' and v == 'TRUE':
            c.fill = fill('FFCCCC')
            c.font = Font(bold=True, size=9, color='CC0000')
        if key == 'failover' and v == 'Y':
            c.fill = fill('FFE0AA')
            c.font = Font(bold=True, size=9, color='994400')
        if key == 'ckpt_fail' and v and int(v) > 0:
            c.fill = fill('FFDDB8')
        if key == 'trino_fail' and v and int(v) > 0:
            c.fill = fill('FFCCCC')
            c.font = Font(bold=True, size=9, color='CC0000')
        if key == 'kafka_lag' and v and float(v) > 100000:
            c.fill = fill('FFE0AA')

    ws.row_dimensions[rn].height = 15

# Freeze panes: freeze rows 1-2 and cols 1-7
ws.freeze_panes = ws.cell(row=3, column=FREEZE_AT_COL)

# ── Glossary / footnote section ──────────────────────────────────────────────
# Each entry: (section_color_key, metric_label, explanation)
# Empty label = blank separator row within a section block
GLOSSARY = [
    # ── Identity ──────────────────────────────────────────────────────────────
    ('id', 'IDENTITY', None),
    ('id', 'Run ID',        'Unique run identifier: N = Nonbatched, B = Batched; number = tier (1–10).'),
    ('id', 'Phase',         'Producer sending strategy: Nonbatched sends each message individually (linger.ms=0); Batched groups messages before sending to reduce broker round-trips.'),
    ('id', 'Tier',          'Load tier T1–T10, corresponding to 50–17,500 simulated homes.'),
    ('id', 'Homes',         'Number of simulated smart homes in this run. Each home produces 1 general-meter + 4 plug messages per second = 5 msg/s.'),
    ('id', 'Go Pods',       'Number of Go IoT simulator pods deployed. Each pod handles up to 3,500 homes (calibration limit where avg cycle stays below 1 s).'),
    ('id', 'Homes/Pod',     'Homes assigned per pod = Homes ÷ Go Pods.'),
    ('id', 'Expected RPS',  'Theoretical message rate = Homes × 5 msg/s (1 general + 4 plugs).'),
    # ── Producer ──────────────────────────────────────────────────────────────
    ('prod', 'PRODUCER PERFORMANCE', None),
    ('prod', 'Producer RPS',   'Observed message rate as reported by the Go producer at the end of the run (records/s).'),
    ('prod', 'Kafka RPS',      'Message rate computed from the Kafka offset delta over the exact 30-min window: Δoffset ÷ 1800 s. More accurate than the producer self-report.'),
    ('prod', 'Ingestion Ratio','Kafka RPS ÷ Expected RPS. ~1.0 = on-target; >1 = producer runs slightly ahead of the 1-s interval; saturation appears as the ratio falling below 1.'),
    ('prod', 'Avg / Max Cycle (s)', 'Average and maximum time to complete one full production cycle (send all messages for all assigned homes). Saturation threshold: 1 s.'),
    ('prod', 'Saturated?',     'TRUE if any production cycle exceeded the 1-s interval target during the run, indicating the producer could not keep up with the target rate.'),
    ('prod', 'OK (Δ) / Errors (Δ)', 'Total successful / failed Kafka send operations over the 30-min measurement window (Δ from T0 to T1).'),
    ('prod', 'Error Rate',     'Errors ÷ (OK + Errors). Should be 0 in a healthy run.'),
    # ── Kafka ─────────────────────────────────────────────────────────────────
    ('kafka', 'KAFKA BROKERS', None),
    ('kafka', 'K0 / K1 CPU (cores)', 'Average CPU usage (cores) of each Kafka broker pod over the 30-min window, from Prometheus cAdvisor metrics (avg of 30-min rate samples). K0 and K1 are equivalent replicas; both shown for symmetry.'),
    ('kafka', 'K0 / K1 Mem (GiB)',   'Average resident memory usage (GiB) of each broker pod.'),
    ('kafka', 'K0 / K1 Net RX / TX (MiB/s)', 'Average network receive (RX) and transmit (TX) bandwidth per broker (MiB/s). RX ≈ producer ingest; TX ≈ Flink consumer traffic.'),
    ('kafka', 'K0 / K1 PVC (GiB)',   'Persistent Volume Claim used storage (GiB) per broker at end of run (Azure Premium SSD, 128 GiB provisioned each).'),
    ('kafka', 'General Δ / Plug Δ (msgs)', 'Kafka offset delta over 30 min for the iot-sensor-general topic (4 partitions, 1 msg/home/s) and iot-sensor-plug topic (6 partitions, 4 msgs/home/s) respectively.'),
    ('kafka', 'Total Kafka Δ (msgs)', 'Sum of General Δ + Plug Δ = total messages ingested into Kafka during the window.'),
    ('kafka', 'Consumer Lag Max',     'Maximum Kafka consumer group lag (records behind the latest offset) at T1, from Prometheus flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max. NaN = metric unavailable during Flink failover.'),
    # ── Flink ─────────────────────────────────────────────────────────────────
    ('fjm', 'FLINK JOBMANAGER', None),
    ('fjm', 'JM CPU / Mem / Net RX / TX / FS', 'Prometheus 30-min averages for the Flink JobManager pod (coordinator role; low CPU expected). FS = container filesystem usage (GiB).'),
    ('ftm1', 'FLINK TASKMANAGERS (TM-1, TM-2)', None),
    ('ftm1', 'TM-1 / TM-2 CPU / Mem / Net RX / TX / FS', 'Prometheus 30-min averages for each TaskManager pod (actual stream-processing workers). Two TMs with parallelism=4 give 8 slots total. TM pods are assigned alphabetically by pod name within each tier. FS = container filesystem.'),
    # ── Flink health ──────────────────────────────────────────────────────────
    ('fhlt', 'FLINK PIPELINE HEALTH', None),
    ('fhlt', 'Busy (ms/s)',       'Flink operator busy time per second at T1 (Prometheus snapshot). Max 1000 ms/s = fully loaded. High busy + high backpressure = CPU-bound bottleneck.'),
    ('fhlt', 'Idle (ms/s)',       'Flink operator idle time per second at T1. Idle + Busy + Backpressure ≈ 1000 ms/s. Low idle under high load indicates the operator is saturated.'),
    ('fhlt', 'Backpressure (ms/s)', 'Flink operator backpressure time per second at T1. High values mean downstream operators (e.g., Paimon writer) cannot absorb records fast enough and are stalling the pipeline.'),
    ('fhlt', 'Running Jobs',      'Number of Flink streaming jobs in RUNNING state at T1 (expected: 1, the raw-ingest job).'),
    ('fhlt', 'Ckpt Done Δ',       'Checkpoint completions during the 30-min window (T1 − T0). Checkpoint interval = 30 s, so ~60 completions expected for a healthy run.'),
    ('fhlt', 'Ckpt Fail Δ',       'Checkpoint failures during the window. Failures indicate resource pressure (memory/network) at the Paimon write step. >0 is a reliability warning.'),
    ('fhlt', 'Ckpt Restored Δ',   'Checkpoint restores (job recovery events) during the window. Each restore = a Flink job restart from the last successful checkpoint.'),
    ('fhlt', 'Failover?',         'Y if Ckpt Restored Δ > 0, i.e., the Flink job restarted at least once during the measurement window.'),
    # ── Paimon / SLO ──────────────────────────────────────────────────────────
    ('paimon', 'PAIMON / SLO', None),
    ('paimon', 'Raw Rows T0 / T1 / Δ', 'Row count in sensor_data_raw (bronze / Paimon) at the start (T0) and end (T1) of the 30-min window, and the delta. Queried via Trino SELECT COUNT(*).'),
    ('paimon', '10min Rows T1',   'Row count in sensor_data_10min (silver) at T1. Populated by three in-run batch jobs fired at run-elapsed +10/+20/+30 min. Each row = one (home, plug, 10-min bucket) aggregate.'),
    ('paimon', '30min Rows T1',   'Row count in sensor_data_30min (silver) at T1. Populated by a single post-flush batch job after the 120-s cooldown. One row per (home, plug) for the entire 30-min window.'),
    ('paimon', 'Homes (raw) / Homes (30min)', 'Distinct home_id values present in each table at T1, used as a completeness sanity check. Should equal the tier home count.'),
    ('paimon', 'SLO (Paimon/Kafka)', 'Data-integrity SLO: Paimon Raw Δ ÷ Total Kafka Δ. A value of ~1.0 confirms that Flink persisted all Kafka messages into Paimon. Values <1 indicate data loss; >1 may indicate prior-tier residual data.'),
    # ── Azure Storage ──────────────────────────────────────────────────────────
    ('az', 'AZURE STORAGE ACCOUNT', None),
    ('az', 'SA Tx (sum)',        'Total storage API transactions (sum) over the window from Azure Monitor. Includes Flink write calls and Trino read calls to ADLS Gen2.'),
    ('az', 'SA Ingress (GiB)',   'Total data written to ADLS Gen2 (GiB) — dominated by Flink Paimon writes (Parquet files + manifest/snapshot metadata).'),
    ('az', 'SA Egress (GiB)',    'Total data read from ADLS Gen2 (GiB) — dominated by Trino query scans of Parquet files.'),
    ('az', 'SA E2E Lat / Srv Lat (ms)', 'Azure Monitor SuccessE2ELatency and SuccessServerLatency (ms) averaged over the window. E2E includes client-side latency; Srv is server-side only.'),
    ('az', 'SA Avail (%)',       'Storage account availability percentage from Azure Monitor. 100% = no failed requests.'),
    ('az', 'SA Capacity (GiB)',  'Used storage capacity (GiB) at end of run. Sampled at PT1H granularity by Azure Monitor; value closest to T1 is used.'),
    # ── E2E Latency ───────────────────────────────────────────────────────────
    ('lat', 'E2E LATENCY', None),
    ('lat', 'E2E Latency (s)', 'Data freshness: wall-clock time at T1 minus MAX(ts) in sensor_data_raw, queried via Trino. Measures end-to-end pipeline lag from event timestamp to queryable data. Values ≫30 s indicate Flink was stalled (e.g., during checkpoint recovery at T9-NB: ~1,061 s).'),
    # ── Trino Queries ─────────────────────────────────────────────────────────
    ('tq', 'TRINO QUERIES (Q1–Q5)', None),
    ('tq', 'Iterations',   'Each query is executed 5 times per run. p95 (95th-percentile), median, min, and max latency are reported. A SELECT 1 warm-up is run before the timed suite to avoid cold-start JIT penalty.'),
    ('tq', 'Q1 — COUNT(*) on raw',  'SELECT COUNT(*) FROM sensor_data_raw. Full-table scan; baseline metric. Scales linearly with data volume. p95 grows from ~5 s (T1) to ~326 s (T10-NB).'),
    ('tq', 'Q2 — GROUP BY + multi-aggregate on raw', 'GROUP BY home_id, plug_id with AVG(voltage), AVG(current_amp), MAX(voltage × current_amp × power_factor), COUNT(*). Full-scan aggregation; scales with data volume.'),
    ('tq', 'Q3 — Window function (rolling avg) on raw', 'AVG(voltage) OVER (PARTITION BY home_id ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW). Computationally heavy: requires sorting and partitioning the full dataset. Fails (OOM) at T10-NB (~200 M rows).'),
    ('tq', 'Q4 — Cross-table JOIN raw ↔ 10min',       'Joins sensor_data_raw with sensor_data_10min on (home_id, plug_id, dt) to compute voltage drift (|AVG raw voltage − 10-min avg voltage|). Filters to fully-populated 10-min buckets (samples ≥ 80 % of max) for fair cross-tier comparison.'),
    ('tq', 'Q5 — Percentile / stddev on raw',          'approx_percentile(voltage, 0.50/0.95/0.99) and stddev(voltage) GROUP BY home_id. Uses HyperLogLog sketch; scales with volume. All five Q runs use paimon.iot.sensor_data_raw via the Trino–Paimon connector.'),
    ('tq', 'Q1–Q5 p95 (s)',    'p95 latency in seconds across the 5 iterations (1 value below which 95 % of runs fall).'),
    ('tq', 'Q1–Q5 Median / Min / Max (ms)', 'Median, minimum, and maximum execution time in milliseconds across the 5 iterations.'),
    ('tq', 'Trino Failures',   'Number of query executions that failed (timeout or OOM) across all 5 queries × 5 iterations. Q3 at T10-NB fails due to memory exhaustion on the window sort.'),
    # ── Trino Resources ───────────────────────────────────────────────────────
    ('tres', 'TRINO CLUSTER RESOURCES', None),
    ('tres', 'Coord / Worker-1 / Worker-2 CPU (cores)', 'Prometheus 30-min average CPU usage per Trino pod: 1 coordinator + 2 workers. Coordinator handles query planning and coordination; workers execute distributed scan/aggregation tasks.'),
    ('tres', 'Coord / Worker Mem (GiB)', 'Average JVM heap + off-heap memory per Trino pod (30-min avg). Each pod configured with 2 GB heap (G1GC). RSS may exceed heap due to native memory.'),
    # ── Domain Metrics ────────────────────────────────────────────────────────
    ('dom', 'DOMAIN METRICS', None),
    ('dom', 'Energy (kWh)',     'Total energy accumulated in sensor_data_30min.energy_kwh (sum across all homes/plugs in the 30-min window). Value patched from orchestrator log due to 30-min rollup bug in 2026-05-06 run; see PATCH_NOTES.md.'),
    ('dom', 'Peak Power (W) †',    '† UNRECOVERABLE — affected by 30-min rollup bug (zero values written). Would represent MAX(voltage × current_amp × power_factor) across all homes in the window.'),
    ('dom', 'Avg Voltage (V) †',   '† UNRECOVERABLE — affected by 30-min rollup bug (NULL values written). Would represent the mean voltage across all homes and plugs in the window.'),
]

# Write glossary
g_start = len(all_rows) + 4
ws.row_dimensions[g_start - 1].height = 8  # small gap

for gi, (sec, label, desc) in enumerate(GLOSSARY):
    rn = g_start + gi
    color = SECTION_COLORS.get(sec, 'EEEEEE')
    is_header = desc is None

    # Col A: metric label
    ca = ws.cell(row=rn, column=1, value=label)
    # Col B onwards: description (merged across several cols for readability)
    if desc is not None:
        ws.merge_cells(start_row=rn, start_column=2, end_row=rn, end_column=12)
        cb = ws.cell(row=rn, column=2, value=desc)
        cb.alignment = Alignment(vertical='center', wrap_text=True)
        cb.font = Font(size=8, color='222222')
        cb.fill = fill('FAFAFA')

    if is_header:
        ca.font = Font(bold=True, size=9, color='000000')
        ca.fill = fill(color)
        ca.alignment = Alignment(vertical='center')
        ws.row_dimensions[rn].height = 16
        # Extend header color across merged col range
        for c in range(2, 13):
            cell = ws.cell(row=rn, column=c)
            cell.fill = fill(color)
    else:
        ca.font = Font(bold=True, size=8, color='333333')
        ca.fill = fill(color)
        ca.alignment = Alignment(vertical='top', wrap_text=True)
        ws.row_dimensions[rn].height = 28

    ws.column_dimensions['A'].width = 42
    ws.column_dimensions['B'].width = 110

wb.save(str(OUTPUT))
print(f"Saved: {OUTPUT}")
print(f"Rows: {len(all_rows)}  Cols: {len(COLS)}")
