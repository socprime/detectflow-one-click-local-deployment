#!/usr/bin/env bash
###############################################################################
# DetectFlow OSS - One-Click Local Deployment (Minikube, Pre-built Images)
#
# Deploys the full DetectFlow stack locally:
#   - PostgreSQL 14
#   - Apache Kafka 3.8 (KRaft, no ZooKeeper)
#   - cert-manager
#   - Apache Flink Kubernetes Operator 1.13.0
#   - DetectFlow Backend  (pre-built image from Docker Hub)
#   - DetectFlow UI       (pre-built image from Docker Hub)
#   - DetectFlow Flink Sigma Detector (pre-built image from Docker Hub)
#   - Kafka UI (kafbat/kafka-ui)
#   - Kubernetes Dashboard
#
# Prerequisites:
#   - macOS with Homebrew
#   - Docker Desktop running (8+ GB memory allocated)
#   - 4+ CPU cores, 40+ GB disk
#
# Usage:
#   chmod +x deploy-detectflow-minikube-prebuilt.sh
#   ./deploy-detectflow-minikube-prebuilt.sh
#
# Access after deploy:
#   UI:        http://localhost:4173
#   API:       http://localhost:8000
#   Kafka UI:  http://localhost:8080
#   Kafka Broker: localhost:9094 (external listener)
#   K8s Dash:  http://localhost:8443
#   Login:     admin@soc.local / admin
# Note:
#   The deployment script contain placeholder data (mock secrets) and are intended for demonstration purposes only.
###############################################################################
set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────
MINIKUBE_CPUS=4
MINIKUBE_MEMORY=8192
MINIKUBE_DISK="40g"
MINIKUBE_DRIVER="docker"
NAMESPACE="flink"
FLINK_OPERATOR_NS="flink-operator"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOS_DIR="${SCRIPT_DIR}/detectflow-repos"

# Pre-built Docker Hub images
IMAGE_BACKEND="socprime/detectflow-oss:admin-panel-be-oss.v0.9.4"
IMAGE_UI="socprime/detectflow-oss:admin-panel-ui-oss.v0.9.4"
IMAGE_FLINK="socprime/detectflow-oss:flink-sigma-detector-oss.v0.9.4"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

log()    { echo -e "${GREEN}[✓]${NC} $*"; }
info()   { echo -e "${BLUE}[i]${NC} $*"; }
warn()   { echo -e "${YELLOW}[!]${NC} $*"; }
error()  { echo -e "${RED}[✗]${NC} $*"; }
header() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

# ─── Step 0: Clone Repository ────────────────────────────────────────────────
header "Step 0/13: Cloning Configuration Repository"

if [ -d "${REPOS_DIR}/detectflow-main" ]; then
    log "detectflow-main already cloned in ${REPOS_DIR}"
else
    info "Cloning detectflow-main (Kubernetes config files)..."
    mkdir -p "${REPOS_DIR}"
    git clone --depth 1 "https://github.com/socprime/detectflow-main.git" "${REPOS_DIR}/detectflow-main"
    log "detectflow-main cloned"
fi

# ─── Step 1: Install CLI Tools ───────────────────────────────────────────────
header "Step 1/13: Installing CLI Tools"

install_if_missing() {
    local cmd=$1
    local pkg=${2:-$1}
    if command -v "$cmd" &>/dev/null; then
        log "$cmd already installed ($(command -v "$cmd"))"
    else
        info "Installing $pkg..."
        brew install "$pkg"
        log "$pkg installed"
    fi
}

install_if_missing minikube
install_if_missing kubectl
install_if_missing helm
install_if_missing kcat

# ─── Step 2: Start Minikube ──────────────────────────────────────────────────
header "Step 2/13: Starting Minikube Cluster"

if minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
    log "Minikube is already running"
    info "Current config: $(minikube config view 2>/dev/null || echo 'default')"
else
    info "Starting minikube with: ${MINIKUBE_CPUS} CPUs, ${MINIKUBE_MEMORY}MB RAM, ${MINIKUBE_DISK} disk"
    minikube start \
        --driver="${MINIKUBE_DRIVER}" \
        --cpus="${MINIKUBE_CPUS}" \
        --memory="${MINIKUBE_MEMORY}" \
        --disk-size="${MINIKUBE_DISK}" \
        --addons=storage-provisioner,default-storageclass
    log "Minikube started"
fi

# Verify cluster is accessible
kubectl cluster-info &>/dev/null || { error "Cannot connect to cluster"; exit 1; }
log "Cluster is accessible"

# Label the minikube node for PVC scheduling
info "Detecting minikube node name and applying label..."
MINIKUBE_NODE=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
kubectl label node "${MINIKUBE_NODE}" spec-node-ns-pod-disk=pvc-security --overwrite
log "Node '${MINIKUBE_NODE}' labeled with spec-node-ns-pod-disk=pvc-security"

# ─── Step 3: Deploy Kubernetes Dashboard ─────────────────────────────────────
header "Step 3/13: Deploying Kubernetes Dashboard"

DASHBOARD_NS="kubernetes-dashboard"

if minikube addons list 2>/dev/null | grep -E '^\| dashboard\b' | grep -q enabled; then
    log "Kubernetes Dashboard addon already enabled"
else
    info "Enabling minikube dashboard addon..."
    minikube addons enable dashboard
    minikube addons enable metrics-server
    log "Kubernetes Dashboard addon enabled"
fi

info "Waiting for dashboard pods to be ready..."
kubectl wait --for=condition=Available deployment/kubernetes-dashboard \
    -n "${DASHBOARD_NS}" --timeout=180s 2>/dev/null || \
kubectl wait --for=condition=Available deployment/kubernetes-dashboard-web \
    -n "${DASHBOARD_NS}" --timeout=180s 2>/dev/null || true
log "Kubernetes Dashboard is running"

# Start dashboard port-forward immediately so it's available during the rest of the deploy
pkill -f "kubectl port-forward.*svc/kubernetes-dashboard" 2>/dev/null || true
kubectl port-forward -n "${DASHBOARD_NS}" svc/kubernetes-dashboard 8443:80 &>/dev/null &
PF_DASH_PID=$!
sleep 1
log "Dashboard available at http://localhost:8443"

# ─── Step 4: Create Namespace ─────────────────────────────────────────────────
header "Step 4/13: Creating Namespace '${NAMESPACE}'"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
log "Namespace '${NAMESPACE}' ready"

# ─── Step 5: Deploy PostgreSQL ───────────────────────────────────────────────
header "Step 5/13: Deploying PostgreSQL 14"

info "Deploying PostgreSQL with database 'detectflow', user 'detectflow'"

kubectl apply -n "${NAMESPACE}" -f - <<'EOF'
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:14-alpine
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_DB
          value: detectflow
        - name: POSTGRES_USER
          value: detectflow
        - name: POSTGRES_PASSWORD
          value: detectflow
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        volumeMounts:
        - name: postgres-data
          mountPath: /var/lib/postgresql/data
        resources:
          requests:
            cpu: 250m
            memory: 256Mi
          limits:
            cpu: "1"
            memory: 512Mi
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "detectflow", "-d", "detectflow"]
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: postgres-data
        persistentVolumeClaim:
          claimName: postgres-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  type: ClusterIP
  ports:
  - port: 5432
    targetPort: 5432
  selector:
    app: postgres
EOF

info "Waiting for PostgreSQL to be ready..."
kubectl rollout status deployment/postgres -n "${NAMESPACE}" --timeout=120s
log "PostgreSQL deployed and ready"

# ─── Step 6: Deploy Apache Kafka (KRaft) ─────────────────────────────────────
header "Step 6/13: Deploying Apache Kafka 3.8 (KRaft Mode)"

info "Deploying single-node Kafka (no ZooKeeper)"

kubectl apply -n "${NAMESPACE}" -f - <<'EOF'
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kafka-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  labels:
    app: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: apache/kafka:3.8.1
        ports:
        - containerPort: 9092
          name: broker
        - containerPort: 9093
          name: controller
        - containerPort: 9094
          name: external
        env:
        # KRaft (no ZooKeeper) settings
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "1@localhost:9093"
        # Listeners - PLAINTEXT for in-cluster, EXTERNAL for host via port-forward
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_INTER_BROKER_LISTENER_NAME
          value: "PLAINTEXT"
        # Cluster ID (static for local dev)
        - name: CLUSTER_ID
          value: "MkU3OEVBNTcwNTJENDM2Qg"
        # Performance
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
          value: "1"
        - name: KAFKA_NUM_PARTITIONS
          value: "4"
        - name: KAFKA_LOG_RETENTION_HOURS
          value: "168"
        - name: KAFKA_LOG_DIRS
          value: "/var/lib/kafka/data"
        volumeMounts:
        - name: kafka-data
          mountPath: /var/lib/kafka/data
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: "2"
            memory: 2Gi
        readinessProbe:
          tcpSocket:
            port: 9092
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
      - name: kafka-data
        persistentVolumeClaim:
          claimName: kafka-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  labels:
    app: kafka
spec:
  type: ClusterIP
  ports:
  - name: broker
    port: 9092
    targetPort: 9092
  - name: external
    port: 9094
    targetPort: 9094
  - name: controller
    port: 9093
    targetPort: 9093
  selector:
    app: kafka
EOF

info "Waiting for Kafka to be ready..."
kubectl rollout status deployment/kafka -n "${NAMESPACE}" --timeout=180s
# Extra wait for Kafka broker to fully initialize
info "Waiting additional 20s for Kafka broker initialization..."
sleep 20
log "Kafka deployed and ready"

# ─── Step 7: Deploy Kafka UI ─────────────────────────────────────────────────
header "Step 7/13: Deploying Kafka UI"

info "Deploying kafbat/kafka-ui..."
kubectl apply -n "${NAMESPACE}" -f - <<'EOF'
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-ui-config
data:
  KAFKA_CLUSTERS_0_NAME: "detectflow-local"
  KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:9092"
  DYNAMIC_CONFIG_ENABLED: "true"
  AUTH_TYPE: "DISABLED"
  SERVER_PORT: "8080"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ui
  labels:
    app: kafka-ui
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-ui
  template:
    metadata:
      labels:
        app: kafka-ui
    spec:
      containers:
      - name: kafka-ui
        image: ghcr.io/kafbat/kafka-ui:latest
        ports:
        - name: http
          containerPort: 8080
        envFrom:
        - configMapRef:
            name: kafka-ui-config
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
          limits:
            cpu: "1"
            memory: 512Mi
        readinessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          failureThreshold: 10
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 30
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  labels:
    app: kafka-ui
spec:
  type: ClusterIP
  ports:
  - name: http
    port: 8080
    targetPort: http
  selector:
    app: kafka-ui
EOF

info "Waiting for Kafka UI to be ready..."
kubectl rollout status deployment/kafka-ui -n "${NAMESPACE}" --timeout=180s
log "Kafka UI deployed"

# ─── Step 8: Install cert-manager ────────────────────────────────────────────
header "Step 8/13: Installing cert-manager"

if kubectl get namespace cert-manager &>/dev/null; then
    log "cert-manager namespace exists, checking pods..."
    if kubectl get pods -n cert-manager -l app.kubernetes.io/instance=cert-manager 2>/dev/null | grep -q Running; then
        log "cert-manager already running"
    else
        warn "cert-manager namespace exists but pods not healthy, reinstalling..."
        kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
    fi
else
    info "Installing cert-manager v1.17.2..."
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
fi

info "Waiting for cert-manager pods to be ready..."
kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-cainjector -n cert-manager --timeout=120s
log "cert-manager ready"

# ─── Step 9: Install Flink Kubernetes Operator ──────────────────────────────
header "Step 9/13: Installing Flink Kubernetes Operator"

if helm list -n "${FLINK_OPERATOR_NS}" 2>/dev/null | grep -q flink-kubernetes-operator; then
    log "Flink Kubernetes Operator already installed"
else
    info "Adding Flink operator Helm repo..."
    helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/ 2>/dev/null || true
    helm repo update

    info "Installing Flink Kubernetes Operator v1.13.0..."
    helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
        --namespace "${FLINK_OPERATOR_NS}" \
        --create-namespace \
        --set webhook.create=true \
        --wait --timeout 120s
    log "Flink Kubernetes Operator installed"
fi

# Verify CRD is registered
info "Verifying FlinkDeployment CRD..."
kubectl get crd flinkdeployments.flink.apache.org &>/dev/null && \
    log "FlinkDeployment CRD registered" || \
    warn "FlinkDeployment CRD not found yet - operator may still be starting"

# ─── Step 10: Deploy Flink Infrastructure ────────────────────────────────────
header "Step 10/13: Deploying Flink Infrastructure (RBAC, ConfigMaps, PVCs)"

K8S_DIR="${REPOS_DIR}/detectflow-main/detectflow_kubernetes_config_files"

# Apply RBAC
kubectl apply -f "${K8S_DIR}/flink-rbac.yaml" -n "${NAMESPACE}"
log "Flink RBAC applied"

# Apply ConfigMap
kubectl apply -f "${K8S_DIR}/flink-configmap.yaml" -n "${NAMESPACE}"
log "Flink ConfigMap applied"

# Apply Secret (with local Kafka config)
kubectl apply -n "${NAMESPACE}" -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: flink-sigma-config
type: Opaque
stringData:
  KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  KAFKA_AUTH_METHOD: "PLAINTEXT"
EOF
log "Flink Secret applied (local Kafka)"

# Apply PVCs (with standard storage class for minikube)
kubectl apply -n "${NAMESPACE}" -f - <<'EOF'
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flink-checkpoints-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flink-ha-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 2Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: flink-savepoints-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
EOF
log "Flink PVCs applied (minikube-sized)"

# ─── Step 11: Deploy DetectFlow Backend ──────────────────────────────────────
header "Step 11/13: Deploying DetectFlow Backend"

info "Using pre-built image: ${IMAGE_BACKEND}"

kubectl apply -n "${NAMESPACE}" -f - <<EOF
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: admin-panel-metrics-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: admin-panel-be-config
data:
  KUBERNETES_NAMESPACE: "flink"
  FLINK_IMAGE: "${IMAGE_FLINK}"
  IMAGE_PULL_POLICY: "IfNotPresent"
  KAFKA_SIGMA_RULES_TOPIC: "sigma-rules"
  KAFKA_METRICS_TOPIC: "rule-statistics"
  KAFKA_METRICS_CONSUMER_GROUP: "admin-panel-dashboard"
  KAFKA_ACTIVITY_TOPIC: "etl-activity"
  KAFKA_ACTIVITY_CONSUMER_GROUP: "admin-panel-activity"
  DASHBOARD_BROADCAST_INTERVAL_SECONDS: "2.0"
  AUDIT_LOGS_RETENTION_DAYS: "7"
  TDM_API_BASE_URL: "https://api.tdm.socprime.com"
  TDM_HOSTNAME: "tdm.socprime.com"
  LOG_LEVEL: "INFO"
  KAFKA_DEFAULT_REPLICATION_FACTOR: "1"
  FLINK_CHECKPOINTS_PVC: "flink-checkpoints-pvc"
  FLINK_SAVEPOINTS_PVC: "flink-savepoints-pvc"
  FLINK_HA_PVC: "flink-ha-pvc"
---
apiVersion: v1
kind: Secret
metadata:
  name: admin-panel-be-secret
type: Opaque
stringData:
  DATABASE_URL: "postgresql+asyncpg://detectflow:detectflow@postgres:5432/detectflow"
  KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  KAFKA_AUTH_METHOD: "PLAINTEXT"
  JWT_SECRET_KEY: "local-dev-jwt-secret-key-change-in-production"
  JWT_REFRESH_SECRET_KEY: "local-dev-jwt-refresh-secret-key-change-in-production"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: admin-panel-be
  labels:
    app.kubernetes.io/name: admin-panel-be
    app.kubernetes.io/instance: admin-panel-be
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: admin-panel-be
      app.kubernetes.io/instance: admin-panel-be
  template:
    metadata:
      labels:
        app.kubernetes.io/name: admin-panel-be
        app.kubernetes.io/instance: admin-panel-be
    spec:
      serviceAccountName: default
      containers:
      - name: admin-panel-be
        image: ${IMAGE_BACKEND}
        imagePullPolicy: IfNotPresent
        ports:
        - name: http
          containerPort: 8000
        envFrom:
        - configMapRef:
            name: admin-panel-be-config
        - secretRef:
            name: admin-panel-be-secret
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: "2"
            memory: 1Gi
        volumeMounts:
        - name: pvc
          mountPath: /mnt/metrics
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          failureThreshold: 10
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 60
          periodSeconds: 30
      volumes:
      - name: pvc
        persistentVolumeClaim:
          claimName: admin-panel-metrics-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: admin-panel-be
  labels:
    app.kubernetes.io/name: admin-panel-be
spec:
  type: NodePort
  ports:
  - name: app
    port: 8000
    targetPort: http
    nodePort: 30800
  selector:
    app.kubernetes.io/name: admin-panel-be
    app.kubernetes.io/instance: admin-panel-be
EOF

info "Waiting for Backend to be ready (migrations + startup)..."
kubectl rollout status deployment/admin-panel-be -n "${NAMESPACE}" --timeout=300s
log "DetectFlow Backend deployed"

# ─── Step 12: Deploy DetectFlow UI ───────────────────────────────────────────
header "Step 12/13: Deploying DetectFlow UI"

info "Using pre-built image: ${IMAGE_UI}"

kubectl apply -n "${NAMESPACE}" -f - <<EOF
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: admin-panel-ui-config
data:
  VITE_API_BASE_URL: "/api/v1"
  VITE_API_PROXY_TARGET: "http://admin-panel-be:8000"
  VITE_PREVIEW_ALLOWED_HOSTS: "localhost,127.0.0.1,0.0.0.0"
  VITE_USE_MOCK_API: "false"
  VITE_USE_MOCK_AUTH: "false"
  VITE_ENVIRONMENT_STAGE: "local"
  VITE_SSE_ENABLED: "true"
  VITE_SSE_RECONNECT: "true"
  VITE_SSE_RECONNECT_INTERVAL: "3000"
  VITE_SSE_RECONNECT_ATTEMPTS: "10"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: admin-panel-ui
  labels:
    app.kubernetes.io/name: admin-panel-ui
    app.kubernetes.io/instance: admin-panel-ui
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: admin-panel-ui
      app.kubernetes.io/instance: admin-panel-ui
  template:
    metadata:
      labels:
        app.kubernetes.io/name: admin-panel-ui
        app.kubernetes.io/instance: admin-panel-ui
    spec:
      containers:
      - name: admin-panel-ui
        image: ${IMAGE_UI}
        imagePullPolicy: IfNotPresent
        ports:
        - name: http
          containerPort: 4173
        envFrom:
        - configMapRef:
            name: admin-panel-ui-config
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
          limits:
            cpu: "1"
            memory: 512Mi
        readinessProbe:
          tcpSocket:
            port: 4173
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: admin-panel-ui
  labels:
    app.kubernetes.io/name: admin-panel-ui
spec:
  type: NodePort
  ports:
  - name: app
    port: 4173
    targetPort: http
    nodePort: 30173
  selector:
    app.kubernetes.io/name: admin-panel-ui
    app.kubernetes.io/instance: admin-panel-ui
EOF

info "Waiting for UI to be ready..."
kubectl rollout status deployment/admin-panel-ui -n "${NAMESPACE}" --timeout=180s
log "DetectFlow UI deployed"

# ─── Step 13: Print Access Information ───────────────────────────────────────
header "Step 13/13: Deployment Complete!"

# On macOS with Docker driver, minikube IP is not routable from the host.
# Use port-forward instead to expose services on localhost.
info "Starting port-forwards to expose services on localhost..."

# Kill any existing port-forwards from previous runs
pkill -f "kubectl port-forward.*svc/admin-panel-be" 2>/dev/null || true
pkill -f "kubectl port-forward.*svc/admin-panel-ui" 2>/dev/null || true
pkill -f "kubectl port-forward.*svc/kafka-ui" 2>/dev/null || true
pkill -f "kubectl port-forward.*svc/kafka " 2>/dev/null || true
sleep 1

kubectl port-forward -n "${NAMESPACE}" svc/admin-panel-be 8000:8000 &>/dev/null &
PF_BE_PID=$!
kubectl port-forward -n "${NAMESPACE}" svc/admin-panel-ui 4173:4173 &>/dev/null &
PF_UI_PID=$!
kubectl port-forward -n "${NAMESPACE}" svc/kafka-ui 8080:8080 &>/dev/null &
PF_KAFKA_UI_PID=$!
kubectl port-forward -n "${NAMESPACE}" svc/kafka 9094:9094 &>/dev/null &
PF_KAFKA_PID=$!
sleep 2

echo ""
echo -e "${BOLD}DetectFlow OSS - Local Deployment${NC}"
echo -e "${BOLD}==================================${NC}"
echo ""
echo -e "  ${GREEN}All components deployed successfully!${NC}"
echo ""
echo -e "  ${CYAN}Access URLs:${NC}"
echo -e "    UI:           ${YELLOW}http://localhost:4173${NC}"
echo -e "    Backend API:  ${YELLOW}http://localhost:8000${NC}"
echo -e "    Kafka UI:     ${YELLOW}http://localhost:8080${NC}"
echo -e "    Kafka Broker: ${YELLOW}localhost:9094${NC}  (external listener)"
echo -e "    K8s Dashboard: ${YELLOW}http://localhost:8443${NC}"
echo ""
echo -e "  ${CYAN}Login Credentials:${NC}"
echo -e "    Email:        ${YELLOW}admin@soc.local${NC}"
echo -e "    Password:     ${YELLOW}admin${NC}"
echo ""
echo -e "  ${CYAN}Pod Status:${NC}"
kubectl get pods -n "${NAMESPACE}" 2>/dev/null
echo ""
echo -e "  ${CYAN}Useful Commands:${NC}"
echo "    kubectl get pods -n flink"
echo "    kubectl logs -n flink -l app.kubernetes.io/name=admin-panel-be -f"
echo "    minikube stop    (preserves data)"
echo "    minikube delete  (removes everything)"
echo ""
echo -e "  ${CYAN}Port-Forward PIDs:${NC}"
echo "    Backend:    ${PF_BE_PID}"
echo "    UI:         ${PF_UI_PID}"
echo "    Kafka UI:   ${PF_KAFKA_UI_PID}"
echo "    Kafka:      ${PF_KAFKA_PID}"
echo "    Dashboard:  ${PF_DASH_PID}  (started in Step 3)"
echo -e "    Stop with: ${YELLOW}kill ${PF_BE_PID} ${PF_UI_PID} ${PF_KAFKA_UI_PID} ${PF_KAFKA_PID} ${PF_DASH_PID}${NC}"
echo ""
