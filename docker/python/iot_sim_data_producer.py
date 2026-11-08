"""
IoT simulator that publishes:
  - Whole-home (mains) readings to KFK_TOPIC_HOME
  - Per-plug readings to KFK_TOPIC_PLUG

Why two topics?
  - Different schemas and rates
  - Independent scaling/retention/consumers
  - Cleaner downstream (Flink → Paimon) pipelines

CHANGES (requested):
1) IDs renamed to include topic prefix:
     home_id = "<KFK_TOPIC_HOME>/<base_id>"   e.g., "iot-sensor-general/42"
     plug_id = "<KFK_TOPIC_PLUG>/<index>"     e.g., "iot-sensor-plug/3"
2) Deterministic cadence: one reading per home and per plug each interval.
3) Per home, each interval: sum of plug powers equals the home power exactly.
   (Also for energy increments over the interval.)

Partitioning / ordering:
  - Home stream messages keyed by home_id  → stable per-home ordering.
  - Plug stream messages keyed by "home_id:plug_id" → stable per-plug ordering.
  - Kafka uses the key to map to partitions; this balances load while keeping
    order within each key.
"""

import os
import json
import time
import datetime
import random
import signal
import sys
import threading
from typing import Optional, Dict, List, Tuple
from kafka import KafkaProducer

# ------- SLO instrumentation ------------------------------------------------
import itertools

_sent_ok = itertools.count()
_sent_err = itertools.count()
_sent_total = itertools.count()
# We use itertools.count() as a thread-safe atomic incrementer (CPython GIL).
# Read current value with: next(_sent_ok) — but that also increments.
# So we track separately with plain ints protected by a lock.

_stats_lock = threading.Lock()
_stats = {"ok": 0, "err": 0, "total": 0}

def _record_ok():
    with _stats_lock:
        _stats["ok"] += 1
        _stats["total"] += 1

def _record_err():
    with _stats_lock:
        _stats["err"] += 1
        _stats["total"] += 1

def _get_stats():
    with _stats_lock:
        return dict(_stats)

# ------- Cycle-time instrumentation -----------------------------------------
_cycle_lock = threading.Lock()
_cycle_duration: float = 0.0  # last full-loop duration in seconds

def _record_cycle(duration: float):
    global _cycle_duration
    with _cycle_lock:
        _cycle_duration = duration

def _get_cycle() -> float:
    with _cycle_lock:
        return _cycle_duration

# ------- Env helpers (with backward-compatible fallbacks) --------------------
def _env(name: str, default: Optional[str] = None, *fallbacks: str) -> str:
    """
    Get env var by preferred name, or by any legacy fallback names, else default.
    """
    if name in os.environ:
        return os.environ[name]
    for fb in fallbacks:
        if fb in os.environ:
            return os.environ[fb]
    return default

def _env_int(name: str, default: int, *fallbacks: str) -> int:
    return int(_env(name, str(default), *fallbacks))

def _env_float(name: str, default: float, *fallbacks: str) -> float:
    return float(_env(name, str(default), *fallbacks))

# ------- Configuration (new names with legacy fallbacks) ---------------------
KFK_BOOTSTRAP_SERVERS = _env("KFK_BOOTSTRAP_SERVERS", "kafka:9092", "BOOTSTRAP")
KFK_TOPIC_HOME        = _env("KFK_TOPIC_HOME", "iot-sensor-general", "TOPIC_GENERAL", "TOPIC")
KFK_TOPIC_PLUG        = _env("KFK_TOPIC_PLUG", "iot-sensor-plug", "TOPIC_PLUG")

# Which streams to produce: "general" (home), "plug", or "both"
# (Behavior is now deterministic per interval, not random sampling.)
PRODUCER_STREAM       = _env("PRODUCER_STREAM", "both", "PRODUCER_MODE").lower()

# Interval / cadence controls
SIM_SLEEP_SEC         = _env_float("SIM_SLEEP_SEC", 1.0, "SLEEP_SEC")

# Cardinality controls
# SIM_HOME_IDS: CSV "42,17,99" (interpreted as base IDs; topic prefix is added)
SIM_HOME_IDS          = _env("SIM_HOME_IDS", "", "HOME_IDS")
SIM_HOME_COUNT        = _env_int("SIM_HOME_COUNT", 42, "HOME_COUNT")
SIM_PLUGS_PER_HOME    = _env_int("SIM_PLUGS_PER_HOME", 4, "PLUGS_PER_HOME")

# Range-based home ID assignment for multi-pod scaling
SIM_HOME_ID_START     = _env_int("SIM_HOME_ID_START", 0)
SIM_HOME_ID_END       = _env_int("SIM_HOME_ID_END", 0)

# Kafka producer tuning
KFK_LINGER_MS         = _env_int("KFK_LINGER_MS", 50, "LINGER_MS")
# KFK_FLUSH_EVERY retained for backward compatibility but not used in deterministic mode
KFK_FLUSH_EVERY       = _env_int("KFK_FLUSH_EVERY", 200, "FLUSH_EVERY")

# Reproducibility for tests / demos
SIM_RANDOM_SEED       = _env("SIM_RANDOM_SEED", None, "RANDOM_SEED")
if SIM_RANDOM_SEED:
    random.seed(SIM_RANDOM_SEED)

# ------- Identities: homes & plugs (topic-linked IDs) -----------------------
if SIM_HOME_IDS:
    base_home_ids = [h.strip() for h in SIM_HOME_IDS.split(",") if h.strip()]
elif SIM_HOME_ID_START > 0 and SIM_HOME_ID_END > 0 and SIM_HOME_ID_END >= SIM_HOME_ID_START:
    # Range-based: each pod gets a unique slice [start, end]
    base_home_ids = [str(i) for i in range(SIM_HOME_ID_START, SIM_HOME_ID_END + 1)]
else:
    # Use simple 1..N identifiers (no zero-padding to match example "<topic>/42")
    base_home_ids = [str(i) for i in range(1, SIM_HOME_COUNT + 1)]

# Final home_id: "<home-topic>/<base_id>"
homes: List[str] = [f"{KFK_TOPIC_HOME}/{hid}" for hid in base_home_ids]

# Final plug_id: "<plug-topic>/<index>"
plugs_by_home: Dict[str, List[str]] = {
    h: [f"{KFK_TOPIC_PLUG}/{i}" for i in range(1, SIM_PLUGS_PER_HOME + 1)] for h in homes
}

# ------- Kafka producer ------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KFK_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks=1,                     # leader-only — realistic for IoT
    retries=5,
    linger_ms=0,                # no batching — simulate real independent sends
)

# ------- Energy integration state (simple running integral) ------------------
# Energy accumulators (Wh) — sum of plug energies will track home energy per interval
energy_home: Dict[str, float] = {h: 0.0 for h in homes}
energy_plug: Dict[Tuple[str, str], float] = {}

# ------- Consistent per-home generation -------------------------------------
def make_home_and_plugs_consistent(home_id: str):
    """
    Generate one home reading and one reading per plug for that home such that:
      sum(plug power) == home power   (exact per interval)
    Energy integration uses SIM_SLEEP_SEC for Δt.

    Returns:
      home_val: dict
      plug_vals: List[dict]
    """
    # Draw shared voltage and PF for the home this tick
    voltage = round(random.normalvariate(230, 2), 1)  # EU nominal 230V ± noise
    pf = round(random.uniform(0.8, 1.0), 2)

    # Draw a total current, derive total power
    total_current = round(random.uniform(0.2, 20.0), 2)
    total_w = voltage * total_current * pf

    # Timestamp (UTC, ISO-8601 with ms)
    ts = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    # Split total_w across plugs with normalized random weights
    plugs = plugs_by_home[home_id]
    raw = [random.random() for _ in plugs]
    s = sum(raw) or 1.0
    weights = [r / s for r in raw]

    # --- Home record (energy integral) ---
    energy_home[home_id] += total_w * SIM_SLEEP_SEC / 3600.0
    home_val = {
        "home_id": home_id,
        "plug_id": None,
        "voltage": voltage,
        "current": round(total_current, 2),
        "power_factor": pf,
        "power_w": round(total_w, 2),
        "energy_wh": round(energy_home[home_id], 3),
        "timestamp": ts,
    }

    # --- Plug records (energies sum to home increment) ---
    plug_vals = []
    for plug_id, w in zip(plugs, weights):
        plug_w = total_w * w
        # derive plug current from W = V * I * pf
        denom = (voltage * pf)
        plug_current = plug_w / denom if denom else 0.0
        key = (home_id, plug_id)
        energy_plug[key] = energy_plug.get(key, 0.0) + plug_w * SIM_SLEEP_SEC / 3600.0

        plug_vals.append({
            "home_id": home_id,
            "plug_id": plug_id,
            "voltage": voltage,
            "current": round(plug_current, 2),
            "power_factor": pf,
            "power_w": round(plug_w, 2),
            "energy_wh": round(energy_plug[key], 3),
            "timestamp": ts,
        })

    return home_val, plug_vals

# ------- SLO stats reporter (background thread, once per minute) -------------
def _stats_reporter():
    last_total = 0
    last_time = time.time()
    while _running:
        time.sleep(60)
        if not _running:
            break
        s = _get_stats()
        now = time.time()
        dt = now - last_time
        rps = (s["total"] - last_total) / dt if dt > 0 else 0
        cycle_sec = _get_cycle()
        saturated = cycle_sec > SIM_SLEEP_SEC * 0.8
        print(f"[stats] ok={s['ok']} err={s['err']} total={s['total']} rps={rps:.0f}",
              flush=True)
        print(f"[cycle] homes={len(homes)} duration={cycle_sec:.3f}s interval={SIM_SLEEP_SEC}s saturated={saturated}",
              flush=True)
        last_total = s["total"]
        last_time = now

# ------- Graceful shutdown (flush on SIGTERM / SIGINT) -----------------------
_running = True
def _stop(*_):
    global _running
    _running = False

signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)

# Start stats reporter
_reporter = threading.Thread(target=_stats_reporter, daemon=True)
_reporter.start()

# ------- Helper: send with instrumentation -----------------------------------
def _send(topic, key, value):
    """Send a single message and track ok/err for SLO."""
    try:
        producer.send(topic, key=key, value=value)
        _record_ok()
    except Exception:
        _record_err()

# ------- Main loop: one reading per home & per plug each interval ------------ 
while _running:
    interval_start = time.time()

    if PRODUCER_STREAM == "both":
        for h in homes:
            home_val, plug_vals = make_home_and_plugs_consistent(h)
            _send(KFK_TOPIC_HOME, h, home_val)
            for pv in plug_vals:
                key = f"{h}:{pv['plug_id']}"
                _send(KFK_TOPIC_PLUG, key, pv)

    elif PRODUCER_STREAM == "general":
        for h in homes:
            home_val, _ = make_home_and_plugs_consistent(h)
            _send(KFK_TOPIC_HOME, h, home_val)

    elif PRODUCER_STREAM == "plug":
        for h in homes:
            _, plug_vals = make_home_and_plugs_consistent(h)
            for pv in plug_vals:
                key = f"{h}:{pv['plug_id']}"
                _send(KFK_TOPIC_PLUG, key, pv)

    producer.flush()

    # Record cycle time (send phase only, before sleep)
    _record_cycle(time.time() - interval_start)

    sleep_left = SIM_SLEEP_SEC - (time.time() - interval_start)
    if sleep_left > 0 and _running:
        time.sleep(sleep_left)

# Final stats & shutdown
s = _get_stats()
print(f"[final] ok={s['ok']} err={s['err']} total={s['total']}", flush=True)
producer.flush()
producer.close()
sys.stdout.flush()
