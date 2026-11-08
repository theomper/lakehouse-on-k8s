## Kubernetes Deployment Architecture

### Overview

The Data Lake system is deployed on an Azure Kubernetes Service (AKS) cluster. The architecture implements a streaming data lakehouse using the following core components:

- **Kafka (KRaft)** for data ingestion and streaming (no ZooKeeper).
- **Flink** (standalone session cluster) for real-time stream processing and batch aggregation.
- **Paimon** as the lakehouse storage layer on ADLS Gen2.
- **Trino** for querying data from Paimon tables.
- **Prometheus + Grafana** for monitoring, alerting, and visualization.
- **IoT Data Producers** (Go / Python) for simulated sensor data generation.

All infrastructure is provisioned via Terraform and application deployments are automated through GitHub Actions CI/CD workflows.

### Namespaces

| Namespace          | Components                                         |
|--------------------|-----------------------------------------------------|
| `data-ingest`      | Kafka (StatefulSet), IoT data producers (Deployments) |
| `data-processing`  | Flink JobManager & TaskManagers, Paimon SQL Jobs    |
| `analytics`        | Trino (Helm chart)                                  |
| `monitoring`       | Prometheus, Grafana (kube-prometheus-stack Helm chart) |
| `debug-tools`      | Network debugging utilities                         |

### Deployment Architecture

```plaintext
                          +--------------------+
                          |  Azure Load        |
                          |  Balancer          |
                          +---------+----------+
                                    |
              +---------------------+---------------------+
              |                     |                      |
     +--------+--------+  +--------+----------+  +--------+---------+
     |  Namespace:      |  |  Namespace:       |  |  Namespace:      |
     |  data-ingest     |  |  data-processing  |  |  analytics       |
     +---------+--------+  +---------+---------+  +---------+--------+
               |                     |                      |
   Kafka (KRaft, 2 brokers)   Flink Session Cluster    Trino (Helm)
     StatefulSet               JobManager (1 replica)    Coordinator + Workers
     128Gi PVC per broker      TaskManagers (2 replicas) Paimon catalog on ADLS
     Topics:                   3 slots/TM, EXACTLY_ONCE  LoadBalancer :8080
       iot-sensor-general      Paimon on ADLS Gen2
       iot-sensor-plug         CronJobs: 10min/30min/hourly agg
               |
   IoT Producers (Go/Python)
     Deployments

              +---------------------+
              |  Namespace:         |
              |  monitoring         |
              +----------+----------+
                         |
              Prometheus (50Gi, 15d retention)
              Grafana (Trino datasource, dashboards)
              Flink metrics (Prometheus reporter :9249)
```

### Data Pipeline Flow

```plaintext
IoT Data Producers (Go / Python)
        |
        v
Kafka Topics (data-ingest)
  |- iot-sensor-general
  |- iot-sensor-plug
        |
        v
Flink Streaming SQL Job (data-processing)
  |- Consumes from Kafka with event-time watermarks
  |- Writes to Paimon tables on ADLS Gen2
        |
        v
Paimon Tables (ADLS Gen2 - warehouse filesystem)
  |- sensor_data_raw (7-day TTL)
  |- sensor_data_10min (batch aggregation via CronJob)
  |- sensor_data_30min (batch aggregation via CronJob)
  |- sensor_data_hourly (batch aggregation via CronJob)
        |
        v
Trino SQL Queries (analytics)
        |
        v
Grafana Dashboards (monitoring)
```

### Component Details

#### Kafka

- **Deployment**: Raw Kubernetes manifests (StatefulSet, 2 replicas)
- **Image**: `doughgle/kafka-kraft:latest` (KRaft mode, no ZooKeeper)
- **Service**: Headless (`kafka-svc.data-ingest.svc.cluster.local`)
- **Storage**: 128Gi PVC per broker (`managed-csi` storage class)
- **Topics**: Created via init Job (`iot-sensor-general`, `iot-sensor-plug`), replication factor 2

#### Flink

- **Deployment**: Standalone session cluster via raw Kubernetes Deployments (not Flink Operator)
- **Image**: `acrlakehouseonk8s01.azurecr.io/flink-paimon-kafka:latest` (custom, includes Paimon + Kafka + Azure FS plugins)
- **JobManager**: 1 replica on `systempool`, Web UI on port 8081
- **TaskManagers**: 2 replicas on `userpool`, 3 task slots each, 8192m process memory
- **Checkpointing**: `EXACTLY_ONCE`, 60s interval, 600s timeout, local filesystem (`file:///tmp/flink-checkpoints`)
- **Metrics**: Prometheus reporter on port range 9249-9259
- **Jobs**:
  - `paimon-bootstrap` (batch) - Creates Paimon catalog and tables via SQL DDL
  - `flink-submit-kafka-to-paimon` (streaming) - Kafka-to-Paimon ingestion
  - `tenmin-aggregation` (CronJob) - 10-minute batch aggregation of raw data
  - `thirtymin-aggregation` (CronJob) - 30-minute batch aggregation of raw data (per-tier rollup used by the experiment loop)
  - `hourly-aggregation` (CronJob) - Hourly batch aggregation of raw data
  - The three aggregation CronJobs ship with `suspend: true`; the experiment orchestrator triggers each one with `kubectl create job --from=cronjob/<name>`

#### Paimon

- **Storage**: ADLS Gen2 (`abfs://warehouse@stlakehouseonk8s01.dfs.core.windows.net/paimon`)
- **Tables**: `sensor_data_raw` (7-day TTL), `sensor_data_10min`, `sensor_data_30min`, `sensor_data_hourly`
- **Managed via**: Flink SQL (DDL for table creation, streaming inserts, batch aggregation)

#### Trino

- **Deployment**: Helm chart (`trino`)
- **Image**: Custom (`trinodb/trino:440` + Paimon plugin from Apache Snapshots)
- **Topology**: Coordinator + 2-4 workers on `userpool` (coordinator included in scheduling)
- **Catalogs**: `paimon` (ADLS Gen2 native Azure filesystem)
- **Service**: LoadBalancer on port 8080

#### Prometheus + Grafana

- **Deployment**: Helm chart (`kube-prometheus-stack`)
- **Prometheus**: 50Gi storage, 15-day retention, scrapes Flink + kubelet + kube-state-metrics
- **Grafana**: 10Gi storage, LoadBalancer service, Trino datasource plugin
- **Dashboards**: four are provisioned by CI as ConfigMaps labelled `grafana_dashboard=1`, which the Grafana sidecar imports: IoT Live (RAW), IoT Energy KPIs, IoT Appliance Intelligence, and IoT Lakehouse Data Trust. Two further exports in `grafana/`, IoT Energy Overview (Hourly) and IoT Power Quality & Voltage (Hourly), read `sensor_data_hourly` and are not deployed by CI; import them manually if needed.

### Azure Infrastructure (Terraform)

| Resource               | Details                                                      |
|------------------------|--------------------------------------------------------------|
| **AKS Cluster**        | `aks-lakehouse-on-k8s-01`, West Europe                |
| **System Node Pool**   | 1 node, Standard_DS3_v2 (or D2s_v6)                         |
| **User Node Pool**     | 2 nodes, Standard_D8s_v3 (or E8s_v6)                        |
| **ADLS Gen2**          | `stlakehouseonk8s01`, HNS enabled, filesystems: `warehouse`, `checkpoints` |
| **ACR**                | `acrlakehouseonk8s01.azurecr.io`, Standard SKU             |
| **Resource Group**     | `rg-lakehouse-on-k8s-01`                               |

Terraform modules: `aks`, `adls`, `acr` (in `infra/modules/`).

### CI/CD (GitHub Actions)

| Workflow                   | Purpose                                             |
|----------------------------|------------------------------------------------------|
| `infra-ci.yml`             | Terraform plan & apply                               |
| `apps-ci.yml`              | Build images, deploy manifests & Helm charts         |
| `producer-ci.yml`          | Build and push IoT producer images to ACR            |
| `trino-paimon-image.yml`   | Build Trino + Paimon plugin image and push to ACR    |
| `infra-destroy.yml`        | Terraform destroy (cleanup on demand)                |

### Configuration

Azure resource names are parameterized through `infra/terraform.tfvars` and the
CI workflow `env:` blocks. All resource names in this repository
(`aks-lakehouse-on-k8s-01`, `stlakehouseonk8s01`, `acrlakehouseonk8s01`, …) are
placeholders — replace them with your own before deploying.
