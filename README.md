# DetectFlow OSS - Complete Deployment Guide

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [Local Deployment with Minikube](#3-local-deployment-with-minikube)
4. [Access URLs & Credentials](#4-access-urls--credentials)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. Project Overview

**DetectFlow** is a real-time SIEM detection pipeline platform by SOC Prime. It applies Sigma rules on streaming events from Apache Kafka using Apache Flink, providing sub-second threat detection.

 > **Note:** The deployment script and all configuration files in this repository contain placeholder data (mock secrets) and are intended for demonstration purposes only.

### Repositories

| Repository | Role |
| --- | --- |
| [`detectflow-main`](https://github.com/socprime/detectflow-main) | Main repo + K8s manifests |
| [`detectflow-backend`](https://github.com/socprime/detectflow-backend) | Admin Panel API |
| [`detectflow-ui`](https://github.com/socprime/detectflow-ui) | Admin Panel Web UI |
| [`detectflow-matchnode`](https://github.com/socprime/detectflow-matchnode) | Real-time Sigma matching engine |
| [`detectflow-parser`](https://github.com/socprime/detectflow-parser) | Event parsing library |

---

## 2. Tech Stack

### Infrastructure Dependencies

| Dependency | Version | Purpose | Required By |
| --- | --- | --- | --- |
| **Kubernetes** | 1.28+ | Container orchestration | All components |
| **PostgreSQL** | 14+ | Backend database (rules, pipelines, users) | Backend |
| **Apache Kafka** | 3.8+ | Event streaming & rule distribution | Backend, MatchNode |
| **Apache Flink** | 2.2 | Stream processing engine | MatchNode |
| **Flink Kubernetes Operator** | 1.13.0 | Manages Flink deployments as CRDs | MatchNode lifecycle |
| **cert-manager** | 1.17.2 | TLS certificate management for Flink operator | Flink Operator |

### Local Development Tools

| Tool | Purpose | Install |
| --- | --- | --- |
| **minikube** | Local K8s cluster | `brew install minikube` |
| **kubectl** | K8s CLI | `brew install kubectl` |
| **helm** | K8s package manager | `brew install helm` |
| **kcat** | Kafka CLI producer/consumer | `brew install kcat` |
| **docker** | Container runtime | `brew install --cask docker-desktop` |

## 3. Local Deployment with Minikube

### Prerequisites

- macOS with Homebrew
- Docker Desktop installed and running (**8+ GB memory** allocated in Docker Desktop → Settings → Resources)
- At least **4 CPU cores** available
- At least **40 GB disk space**

### Deployment Options

This repository includes two Minikube deployment scripts:

| Script | Purpose |
| --- | --- |
| `deploy-detectflow-minikube.sh` | Builds DetectFlow images locally from source repositories, then deploys them into Minikube. |
| `deploy-detectflow-minikube-prebuilt.sh` | Pulls pre-built images from the [`socprime/detectflow-oss`](https://hub.docker.com/r/socprime/detectflow-oss/tags) Docker Hub repository and deploys them directly. |

### Deploy from Source

```bash
git clone https://github.com/socprime/detectflow-one-click-local-deployment.git
cd detectflow-one-click-local-deployment
chmod +x deploy-detectflow-minikube.sh
./deploy-detectflow-minikube.sh
```

The script automatically clones all required repositories into a `detectflow-repos/` subdirectory, builds the required images locally and deploys the full stack.

### Deploy with Pre-built Images

```bash
git clone https://github.com/socprime/detectflow-one-click-local-deployment.git
cd detectflow-one-click-local-deployment
chmod +x deploy-detectflow-minikube-prebuilt.sh
./deploy-detectflow-minikube-prebuilt.sh
```

This variant skips local image builds and uses pre-built images published to Docker Hub.

### Updating Pre-built Image Versions

If you need to change the image versions used by the pre-built deployment, update these variables in `deploy-detectflow-minikube-prebuilt.sh`:

```bash
# Pre-built Docker Hub images
IMAGE_BACKEND="socprime/detectflow-oss:admin-panel-be-oss.v0.9.4"
IMAGE_UI="socprime/detectflow-oss:admin-panel-ui-oss.v0.9.4"
IMAGE_FLINK="socprime/detectflow-oss:flink-sigma-detector-oss.v0.9.4"
```

Before changing them, verify the available tags in the [`socprime/detectflow-oss`](https://hub.docker.com/r/socprime/detectflow-oss/tags) Docker Hub repository.

---

## 4. Access URLs & Credentials

After deployment, the script sets up port-forwards and prints access URLs:

| Service | URL | Notes |
| --- | --- | --- |
| **DetectFlow UI** | `http://localhost:4173` | Main admin panel |
| **DetectFlow Backend API** | `http://localhost:8000` | REST API |
| **Kafka UI** | `http://localhost:8080` | Kafka monitoring & management |
| **Kafka Broker** | `localhost:9094` | External listener for host access |
| **K8s Dashboard** | `http://localhost:8443` | Kubernetes cluster monitoring |

⚠️ **Important: Pipeline Runtime Settings**

Before running the deployment, check the Pipeline Runtime settings in your DetectFlow environment and set them according to your system resources and requirements.

**Settings to review:**

- Parallelism
- TaskManager CPU (cores)
- TaskManager Memory (MB)
- Window Size (sec)
- Checkpoint Interval (sec)
- Enable Autoscaling

These settings can significantly affect performance and resource usage. Adjust them as needed for your environment.

Refer to the screenshot below for an example configuration:

![Pipeline Runtime Settings](./images/pipeline-runtime.png)

Make sure to update these values before creating new pipelines.

### Kafka Broker Access

The deploy script configures Kafka with two listeners:

| Listener | Address | Purpose |
| --- | --- | --- |
| `PLAINTEXT` | `kafka:9092` | In-cluster clients (Backend, Flink, MatchNode) |
| `EXTERNAL` | `localhost:9094` | Host access via `kubectl port-forward` |

The `EXTERNAL` listener is port-forwarded automatically so you can produce/consume events from your host machine, for example:

```bash
# Produce events using kcat
cat <<'JSON' | jq -c . | kcat -P -b localhost:9094 -t windows-events
{
  "event_id": 4624,
  "timestamp": "2024-02-24T19:02:11Z",
  "channel": "Security",
  "computer": "FILESERVER01.domain.local",
  "target_user": "DOMAIN\\alice",
  "logon_type": 3,
  "logon_process": "NtLmSsp",
  "auth_package": "NTLM",
  "workstation_name": "WORKSTATION01",
  "source_network_address": "192.168.1.100",
  "source_port": 52144,
  "message": "An account was successfully logged on."
}
JSON

# Consume events
kcat -C -b localhost:9094 -t windows-events -o beginning
```

> **Note:** Always use `localhost:9094` (not `9092`) when connecting from outside the cluster. Port `9092` advertises `kafka:9092` in metadata, which is unresolvable on the host.

If the port-forward dies, restart it manually:

```bash
kubectl port-forward -n flink svc/kafka 9094:9094 &
```

### Default Login Credentials

| Field | Value |
| --- | --- |
| **Email** | `admin@soc.local` |
| **Password** | `admin` |

### Useful Commands After Deploy

```bash
# Check all pods
kubectl get pods -n flink

# Check services
kubectl get svc -n flink

# View backend logs
kubectl logs -n flink -l app.kubernetes.io/name=admin-panel-be -f

# View UI logs
kubectl logs -n flink -l app.kubernetes.io/name=admin-panel-ui -f

# View PostgreSQL logs
kubectl logs -n flink -l app=postgres -f

# View Kafka logs
kubectl logs -n flink -l app=kafka -f

# View Kafka UI logs
kubectl logs -n flink -l app=kafka-ui -f

# Restart port-forwards (if they die)
kubectl port-forward -n flink svc/admin-panel-be 8000:8000 &
kubectl port-forward -n flink svc/admin-panel-ui 4173:4173 &
kubectl port-forward -n flink svc/kafka-ui 8080:8080 &
kubectl port-forward -n flink svc/kafka 9094:9094 &
kubectl port-forward -n kubernetes-dashboard svc/kubernetes-dashboard 8443:80 &

# Stop the cluster (preserves data)
minikube stop

# Full reset and redeploy
minikube delete && ./deploy-detectflow-minikube.sh
```

---

## 5. Troubleshooting

### Pods stuck in Pending/CrashLoopBackOff

```bash
kubectl describe pod <pod-name> -n flink
kubectl logs <pod-name> -n flink
```

### Backend can't connect to PostgreSQL

- Ensure PostgreSQL pod is running: `kubectl get pods -n flink -l app=postgres`
- Check DATABASE_URL format: `postgresql+asyncpg://detectflow:detectflow@postgres:5432/detectflow`

### Backend can't connect to Kafka

- Ensure Kafka pod is running: `kubectl get pods -n flink -l app=kafka`
- Wait for Kafka to fully start (can take 30-60 seconds)
- Check bootstrap server: `kafka:9092`

### Image pull errors

- If you use `deploy-detectflow-minikube.sh`, images are built locally in Minikube. Ensure `imagePullPolicy: Never` is set.
- If you use `deploy-detectflow-minikube-prebuilt.sh`, confirm the image tags in the script exist in the [`socprime/detectflow-oss`](https://hub.docker.com/r/socprime/detectflow-oss/tags) Docker Hub repository.
- For locally built images, rebuild with: `minikube image build -t <image>:local <path>`

### Minikube out of resources

Increase Docker Desktop memory (Settings → Resources → at least 8 GB), then:

```bash
minikube delete
./deploy-detectflow-minikube.sh
```

### Flink Operator not starting

```bash
kubectl get pods -n flink-operator
kubectl logs -n flink-operator -l app.kubernetes.io/name=flink-kubernetes-operator
```

### cert-manager issues

```bash
kubectl get pods -n cert-manager
# Wait for all 3 pods to be Running before installing Flink operator
```
