# Kubernetes Manifests & Configurations

This directory contains all Kubernetes deployments, configurations, and Helm
values for the lakehouse-on-k8s platform. Everything here targets a single
Azure Kubernetes Service (AKS) cluster.

---

## Directory Structure

### `/manifests` — Core Platform Components

| File | Purpose |
|------|---------|
| `data-ingest.yaml` | Kafka cluster (StatefulSet, Headless Service) in the `data-ingest` namespace |
| `data-processing.yaml` | Paimon bootstrap, Flink streaming ingest job, and aggregation CronJobs in `data-processing` |
| `analytics.yaml` | `analytics` namespace (Trino is installed via Helm) |
| `monitoring.yaml` | `monitoring` namespace hooks for the kube-prometheus-stack |
| `debug-tools.yaml` | Debug pods for cluster troubleshooting |

Deploy in order: `data-ingest` → `data-processing` → `analytics` → `monitoring`.

---

### `/flink` — Flink Session Cluster

Kubernetes manifests for the Flink Session cluster (non-HA).

| File | Purpose |
|------|---------|
| `flink-conf-configmap.yaml` | Flink configuration (RPC, task slots, memory, Prometheus reporter) |
| `flink-jobmanager-session-deployment-non-ha.yaml` | JobManager Deployment (1 replica) |
| `flink-taskmanager-session-deployment.yaml` | TaskManager Deployment |
| `flink-jobmanager-service.yaml` | Service exposing JobManager web UI and RPC |
| `flink-podmonitor.yaml` | PodMonitor so Prometheus scrapes Flink metrics |

**Key settings:**
- Memory: JobManager 1600m, TaskManager 8192m
- Task slots: 4 per TaskManager
- Parallelism: 4 (default)

Used for the streaming ingest job and the batch aggregation jobs running in Session Mode.

---

### `/flink-application` — Flink Application Mode

| File | Purpose |
|------|---------|
| `flink-app-job-and-rbac.yaml` | Application Mode job, RBAC, and entrypoint configuration |

Each Application Mode job runs its own JobManager and TaskManagers as a single
logical unit. Provided as an alternative to the Session-cluster ingestion path.

---

### `/producers` — IoT Data Producers

Kubernetes Deployments for synthetic IoT data generators.

| File | Purpose |
|------|---------|
| `go-iot-sim-producers.yaml` | Go-based nonbatched producer |
| `go-iot-sim-producers-batched.yaml` | Go-based batched producer |
| `iot-sim-producers.yaml` | Python-based producer |

**Environment variables:**
- `KFK_BOOTSTRAP_SERVERS`, `KFK_TOPIC_HOME`, `KFK_TOPIC_PLUG` — Kafka targets
- `SIM_HOME_ID_START`, `SIM_HOME_ID_END` — Non-overlapping home ID range per pod
- `SIM_SLEEP_SEC`, `KFK_LINGER_MS` — Throughput tuning

#### `producers/calibration/`

Used to determine the maximum number of simulated homes a single producer pod
can sustain without saturation (cycle time exceeding the 1 s target interval).
Results feed into the experiment generator scripts.

**Calibration workflow:**
1. Deploy a single pod at increasing home counts
2. Wait for stabilisation
3. Collect stats
4. Record RPS, cycle time, saturation flag

**Result used across experiments: HomesPerPod = 3500** (see `calibration/*-results-*.txt`).

---

### `/helm` — Helm Values

Helm chart `values.yaml` overrides for Prometheus/Grafana and Trino.

| File | Purpose |
|------|---------|
| `kube-prometheus-stack-values.yaml` | Prometheus, Grafana, AlertManager, node-exporter config |
| `trino-values.yaml` | Trino coordinator and worker configuration |
| `trino-services.yaml` | Extra Trino Services (e.g. LoadBalancer) |

---

## Typical Deployment Order

For a fresh cluster:

```bash
# 1. Core platform
kubectl apply -f k8s/manifests/data-ingest.yaml
kubectl wait --for=condition=ready pod -l app=kafka -n data-ingest --timeout=300s

# 2. Flink Session cluster
kubectl apply -f k8s/flink/
kubectl wait --for=condition=ready pod -l app=flink -n data-processing --timeout=300s

# 3. Analytics (Trino via Helm)
helm install trino trino/trino -n analytics --values k8s/helm/trino-values.yaml

# 4. Monitoring (Prometheus/Grafana via Helm)
helm install prometheus prometheus-community/kube-prometheus-stack \
  -n monitoring --values k8s/helm/kube-prometheus-stack-values.yaml

# 5. Data processing (Paimon bootstrap + Flink ingest + aggregation CronJobs)
kubectl apply -f k8s/manifests/data-processing.yaml

# 6. Data simulator
kubectl apply -f k8s/producers/go-iot-sim-producers.yaml
```

---

## Namespace Summary

| Namespace | Components | Purpose |
|-----------|-----------|---------|
| `data-ingest` | Kafka | Message broker for sensor streams |
| `data-processing` | Flink, CronJobs | Stream processing and aggregations |
| `analytics` | Trino | SQL query engine over Paimon |
| `monitoring` | Prometheus, Grafana | Observability and alerting |

## Common Tasks

### Redeploy Flink after config change
```bash
kubectl rollout restart deployment/flink-jobmanager -n data-processing
kubectl rollout restart deployment/flink-taskmanager -n data-processing
```

### Monitor Kafka topics
```bash
kubectl exec -it kafka-0 -n data-ingest -- /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

### Access Flink Web UI
```bash
kubectl port-forward svc/flink-jobmanager 8081:8081 -n data-processing
# Open http://localhost:8081
```

## Notes

- Kafka runs in the `data-ingest` namespace, Flink in `data-processing`.
- Azure storage credentials are referenced via a Kubernetes Secret
  (`azure-storage-secret`), not baked into any manifest.
- Producer pods use `terminationGracePeriodSeconds: 30` for clean Kafka
  connection shutdown.
