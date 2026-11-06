#!/usr/bin/env bash
###############################################################################
# DetectFlow Demo - One-Click Local Deployment (Minikube)
#
# Deploys the full DetectFlow stack locally:
#   - PostgreSQL
#   - Apache Kafka (KRaft, no ZooKeeper)
#   - Logstash (syslog/json/unstructured ingest -> Kafka)
#   - cert-manager
#   - Apache Flink Kubernetes Operator
#   - DetectFlow Backend  (public pre-built image from Docker Hub)
#   - DetectFlow UI       (public pre-built image from Docker Hub)
#   - DetectFlow Flink Sigma Detector (public pre-built image from Docker Hub)
#   - Kafka UI (kafbat/kafka-ui)
#   - Kubernetes Dashboard
#
# Prerequisites:
#   - Ubuntu 22.04 / 24.04 / 26.04
#   - Docker Engine (installed by the script if missing); run as a user in the 'docker' group
#   - 8 CPU cores, 16 GB RAM, 128 GB disk (defaults are sized for this; swap recommended)
#   - All tunables in .env (copy .env.example -> .env and edit)
#
# Usage:
#   cp .env.example .env   # then set SERVER_HOST to this server's IP or FQDN
#   chmod +x deploy-detectflow-minikube.sh
#   ./deploy-detectflow-minikube.sh
#
# Access after deploy (SERVER_HOST = server IP/FQDN from .env). With TLS on (the
# default) the web services are https on the same ports; set TLS_ENABLED=false for http.
#   UI:           https://SERVER_HOST:UI_PORT
#   API:          https://SERVER_HOST:API_PORT
#   Kafka UI:     https://SERVER_HOST:KAFKA_UI_PORT
#   Kafka Broker: SERVER_HOST:KAFKA_EXTERNAL_PORT (external listener, plaintext)
#   PostgreSQL:   127.0.0.1:POSTGRES_PORT on the server only (SSH tunnel / pgAdmin);
#                 POSTGRES_PF_ADDRESS=0.0.0.0 exposes it to the network (demo password)
#   K8s Dash:     https://SERVER_HOST:DASHBOARD_PORT
#   Login:        admin@soc.local / admin
#   Port-forwards are self-healing (survive drop / logout / reboot).
# Note:
#   The deployment script contains placeholder data (mock secrets) and is intended for demonstration purposes only.
###############################################################################
set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"

# Load .env (all tunables live there). Values below are fallback defaults.
# `set -a` stays on for the whole config block (until `set +a` before the colors), so
# every value - from .env OR a fallback default below - is exported and reaches
# envsubst. Without it a key missing from .env rendered EMPTY in the manifests even
# though the script had a default for it.
set -a
if [ -f "${SCRIPT_DIR}/.env" ]; then
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/.env"
fi

# Minikube
MINIKUBE_CPUS="${MINIKUBE_CPUS:-6}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-12288}"
MINIKUBE_DISK="${MINIKUBE_DISK:-100g}"
MINIKUBE_DRIVER="${MINIKUBE_DRIVER:-docker}"
MINIKUBE_CONTAINER_RUNTIME="${MINIKUBE_CONTAINER_RUNTIME:-containerd}"
NAMESPACE="${NAMESPACE:-flink}"
FLINK_OPERATOR_NS="${FLINK_OPERATOR_NS:-flink-operator}"

# Operator / add-on versions
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.1}"
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.15.0}"

# Network / remote access - SERVER_HOST is required (validated in validate_config).
SERVER_HOST="${SERVER_HOST:-}"
PF_ADDRESS="${PF_ADDRESS:-0.0.0.0}"
# PostgreSQL gets its own bind address: server-only by default because it uses the
# default demo credentials (reach it through an SSH tunnel, e.g. pgAdmin).
POSTGRES_PF_ADDRESS="${POSTGRES_PF_ADDRESS:-127.0.0.1}"

# Ports
UI_PORT="${UI_PORT:-4173}"
API_PORT="${API_PORT:-8000}"
KAFKA_UI_PORT="${KAFKA_UI_PORT:-8080}"
KAFKA_EXTERNAL_PORT="${KAFKA_EXTERNAL_PORT:-9094}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8443}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
DASHBOARD_NS="${DASHBOARD_NS:-kubernetes-dashboard}"

# TLS (per-port). ON by default: the UI is effectively HTTPS-only (its image ships a
# CSP that upgrades all assets to https), so serving it over plain HTTP yields a blank
# page. Traefik terminates TLS so UI_PORT/API_PORT/KAFKA_UI_PORT/DASHBOARD_PORT serve
# HTTPS. Set TLS_ENABLED=false only if you terminate TLS for the UI some other way.
TLS_ENABLED="${TLS_ENABLED:-true}"
TLS_MODE="${TLS_MODE:-selfsigned}"
TRAEFIK_VERSION="${TRAEFIK_VERSION:-34.1.0}"
TLS_CERT_FILE="${TLS_CERT_FILE:-}"
TLS_KEY_FILE="${TLS_KEY_FILE:-}"
TRAEFIK_NS="${TRAEFIK_NS:-traefik}"
TLS_SECRET_NAME="${TLS_SECRET_NAME:-df-tls}"
# LLM self-signed CA for the agent job (optional). LLM_CA_FILE = host path to a PEM
# CA bundle; when set, it is projected into the agent-job pod and trusted for
# LLM_BASE_URL. LLM_CA_MOUNT_PATH is the in-container path the backend reads
# (dirname = mount dir, basename = the Secret key). SSL_CERT_FILE is what the agent
# ConfigMap advertises to the backend - empty unless the CA is enabled, so the
# default/off case does not point the agent at a missing file. LLM_SSL_VERIFY
# defaults to "true" (verification on); set "false" only for testing.
# DetectFlow Agent (LLM) - defaults match .env.example so an older .env without the
# LLM block still renders a complete agent ConfigMap instead of empty values.
LLM_BASE_URL="${LLM_BASE_URL:-http://localhost:8000/v1/}"
LLM_MODEL="${LLM_MODEL:-model}"
LLM_API_KEY="${LLM_API_KEY:-}"
LLM_CALLS_PER_MINUTE="${LLM_CALLS_PER_MINUTE:-60}"
LLM_MAX_RETRIES="${LLM_MAX_RETRIES:-3}"
LLM_MAX_TOKENS="${LLM_MAX_TOKENS:-2048}"
LLM_PROMPT_CHAR_BUDGET="${LLM_PROMPT_CHAR_BUDGET:-24000}"
MAX_CONCURRENT_LLM_REQUESTS="${MAX_CONCURRENT_LLM_REQUESTS:-1}"
LLM_CA_FILE="${LLM_CA_FILE:-}"
LLM_CA_MOUNT_PATH="${LLM_CA_MOUNT_PATH:-/etc/ssl/agent-ca/ca.pem}"
LLM_SSL_VERIFY="${LLM_SSL_VERIFY:-true}"
if [ -n "${LLM_CA_FILE}" ]; then
    SSL_CERT_FILE="${LLM_CA_MOUNT_PATH}"
else
    SSL_CERT_FILE=""
fi

# Public pre-built Docker Hub images (defaults match .env.example; validated in validate_config).
IMAGE_BACKEND="${IMAGE_BACKEND:-socprime/detectflow-oss:admin-panel-be-oss.v2.3.2}"
IMAGE_UI="${IMAGE_UI:-socprime/detectflow-oss:admin-panel-ui-oss.v2.3.2}"
IMAGE_FLINK="${IMAGE_FLINK:-socprime/detectflow-oss:flink-sigma-detector-oss.v2.3.2}"
IMAGE_AGENT="${IMAGE_AGENT:-socprime/detectflow-oss:df-agent-oss.v2.3.2}"
IMAGE_CORRELATION="${IMAGE_CORRELATION:-socprime/detectflow-oss:df-correlation-oss.v2.3.2}"

# PostgreSQL resources + tuning
IMAGE_POSTGRES="${IMAGE_POSTGRES:-postgres:14-alpine}"
PG_CPU_REQUEST="${PG_CPU_REQUEST:-250m}"
PG_CPU_LIMIT="${PG_CPU_LIMIT:-2}"
PG_MEM_REQUEST="${PG_MEM_REQUEST:-512Mi}"
PG_MEM_LIMIT="${PG_MEM_LIMIT:-1Gi}"
PG_STORAGE="${PG_STORAGE:-20Gi}"
PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-256MB}"
PG_EFFECTIVE_CACHE_SIZE="${PG_EFFECTIVE_CACHE_SIZE:-768MB}"
PG_MAINTENANCE_WORK_MEM="${PG_MAINTENANCE_WORK_MEM:-128MB}"
PG_WORK_MEM="${PG_WORK_MEM:-8MB}"
PG_MAX_CONNECTIONS="${PG_MAX_CONNECTIONS:-100}"

# Kafka resources + tuning
IMAGE_KAFKA="${IMAGE_KAFKA:-apache/kafka:4.3.1}"
IMAGE_KAFKA_UI="${IMAGE_KAFKA_UI:-ghcr.io/kafbat/kafka-ui:v1.5.0}"
KAFKA_CPU_REQUEST="${KAFKA_CPU_REQUEST:-500m}"
KAFKA_CPU_LIMIT="${KAFKA_CPU_LIMIT:-2}"
KAFKA_MEM_REQUEST="${KAFKA_MEM_REQUEST:-1536Mi}"
KAFKA_MEM_LIMIT="${KAFKA_MEM_LIMIT:-2Gi}"
KAFKA_STORAGE="${KAFKA_STORAGE:-30Gi}"
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx1G -Xms1G}"
KAFKA_NUM_PARTITIONS="${KAFKA_NUM_PARTITIONS:-3}"
KAFKA_LOG_RETENTION_HOURS="${KAFKA_LOG_RETENTION_HOURS:-2}"
KAFKA_LOG_ROLL_MS="${KAFKA_LOG_ROLL_MS:-600000}"
KAFKA_NUM_NETWORK_THREADS="${KAFKA_NUM_NETWORK_THREADS:-3}"
KAFKA_NUM_IO_THREADS="${KAFKA_NUM_IO_THREADS:-8}"
KAFKA_SOCKET_SEND_BUFFER_BYTES="${KAFKA_SOCKET_SEND_BUFFER_BYTES:-1048576}"
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES="${KAFKA_SOCKET_RECEIVE_BUFFER_BYTES:-1048576}"
KAFKA_SOCKET_REQUEST_MAX_BYTES="${KAFKA_SOCKET_REQUEST_MAX_BYTES:-104857600}"

# Logstash (ingest -> Kafka) - production-sized
IMAGE_LOGSTASH="${IMAGE_LOGSTASH:-docker.elastic.co/logstash/logstash-oss:9.5.2}"
LOGSTASH_REPLICAS="${LOGSTASH_REPLICAS:-1}"
LOGSTASH_JAVA_OPTS="${LOGSTASH_JAVA_OPTS:--Xms512m -Xmx512m}"
LOGSTASH_CPU_REQUEST="${LOGSTASH_CPU_REQUEST:-250m}"
LOGSTASH_CPU_LIMIT="${LOGSTASH_CPU_LIMIT:-1}"
LOGSTASH_MEM_REQUEST="${LOGSTASH_MEM_REQUEST:-768Mi}"
LOGSTASH_MEM_LIMIT="${LOGSTASH_MEM_LIMIT:-1Gi}"
LOGSTASH_PIPELINE_WORKERS="${LOGSTASH_PIPELINE_WORKERS:-2}"
LOGSTASH_PIPELINE_BATCH_SIZE="${LOGSTASH_PIPELINE_BATCH_SIZE:-125}"
LOGSTASH_KAFKA_BOOTSTRAP="${LOGSTASH_KAFKA_BOOTSTRAP:-kafka:9092}"
LOGSTASH_SYSLOG_PORT="${LOGSTASH_SYSLOG_PORT:-10501}"
LOGSTASH_JSON_PORT="${LOGSTASH_JSON_PORT:-10510}"
LOGSTASH_TEXT_PORT="${LOGSTASH_TEXT_PORT:-10520}"
LOGSTASH_SYSLOG_NODEPORT="${LOGSTASH_SYSLOG_NODEPORT:-30501}"
LOGSTASH_JSON_NODEPORT="${LOGSTASH_JSON_NODEPORT:-30510}"
LOGSTASH_TEXT_NODEPORT="${LOGSTASH_TEXT_NODEPORT:-30520}"
set +a   # end of the auto-exported config block

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

# Reject unset, empty, or untouched "REPLACE_WITH_*" placeholder values from .env
# so the run fails fast with a clear message instead of deep inside an image pull.
require_value() {  # $1=var name, $2=value, $3=what to set it to
    local name="$1" val="$2" hint="$3"
    if [ -z "$val" ] || [[ "$val" == REPLACE_WITH_* ]]; then
        error "${name} is not set. Edit .env and set it ${hint}."
        return 1
    fi
}

validate_config() {
    local ok=0
    require_value "SERVER_HOST"   "${SERVER_HOST}"   "to this server's IP or FQDN" || ok=1
    require_value "IMAGE_BACKEND" "${IMAGE_BACKEND}" "to the backend image (repo/name:tag)" || ok=1
    require_value "IMAGE_UI"      "${IMAGE_UI}"      "to the UI image (repo/name:tag)" || ok=1
    require_value "IMAGE_FLINK"   "${IMAGE_FLINK}"   "to the Flink image (repo/name:tag)" || ok=1
    require_value "IMAGE_AGENT"       "${IMAGE_AGENT}"       "to the df-agent job image (repo/name:tag)" || ok=1
    require_value "IMAGE_CORRELATION" "${IMAGE_CORRELATION}" "to the df-correlation job image (repo/name:tag)" || ok=1
    if [ "${TLS_ENABLED}" = "true" ]; then
        if [ "${TLS_MODE}" = "provided" ]; then
            # First catch unset/empty (same style as SERVER_HOST/IMAGE_*), then the
            # set-but-missing-file case.
            if ! require_value "TLS_CERT_FILE" "${TLS_CERT_FILE}" "to the path of your cert PEM file (TLS_MODE=provided)"; then
                ok=1
            elif [ ! -r "${TLS_CERT_FILE}" ]; then
                error "TLS_CERT_FILE ('${TLS_CERT_FILE}') is set but missing or unreadable."; ok=1
            fi
            if ! require_value "TLS_KEY_FILE" "${TLS_KEY_FILE}" "to the path of your key PEM file (TLS_MODE=provided)"; then
                ok=1
            elif [ ! -r "${TLS_KEY_FILE}" ]; then
                error "TLS_KEY_FILE ('${TLS_KEY_FILE}') is set but missing or unreadable."; ok=1
            fi
        elif [ "${TLS_MODE}" != "selfsigned" ]; then
            error "TLS_MODE must be 'selfsigned' or 'provided' (got '${TLS_MODE}')."; ok=1
        fi
    fi
    case "${MINIKUBE_CONTAINER_RUNTIME}" in
        containerd|docker|cri-o) ;;
        *) error "MINIKUBE_CONTAINER_RUNTIME must be 'containerd', 'docker' or 'cri-o' (got '${MINIKUBE_CONTAINER_RUNTIME}')."; ok=1 ;;
    esac
    # LLM self-signed CA (optional): if LLM_CA_FILE is set it must be readable, and
    # the in-container mount path must live in a subdirectory (a path directly under
    # / would shadow the container root and is rejected by the backend).
    if [ -n "${LLM_CA_FILE}" ]; then
        if [ ! -r "${LLM_CA_FILE}" ]; then
            error "LLM_CA_FILE ('${LLM_CA_FILE}') is set but missing or unreadable."; ok=1
        fi
        case "${LLM_CA_MOUNT_PATH}" in
            /*/*) : ;;
            *) error "LLM_CA_MOUNT_PATH ('${LLM_CA_MOUNT_PATH}') must be an absolute path under a subdirectory (e.g. /etc/ssl/agent-ca/ca.pem)."; ok=1 ;;
        esac
    fi
    if [ "$ok" -ne 0 ]; then
        error "Fix the above in ${SCRIPT_DIR}/.env (copy .env.example -> .env) and re-run."
        exit 1
    fi
}

# Self-healing port-forwards (no root). Installs the supervisor into ~/.detectflow,
# generates the forward set from .env + TLS mode, adds a @reboot crontab hook, and
# starts everything detached. Each forward auto-restarts on drop and survives logout
# + reboot. Replaces the old backgrounded `kubectl port-forward ... &` calls.
setup_port_forwards() {
    local df_home="${HOME}/.detectflow"
    local sup_src="${SCRIPT_DIR}/scripts/df-forward-supervisor.sh"
    local sup="${df_home}/df-forward-supervisor.sh"
    local conf="${df_home}/forwards.conf"

    mkdir -p "${df_home}/pids"

    if [ ! -r "${sup_src}" ]; then
        warn "Supervisor not found at ${sup_src}; using plain background forwards (no self-heal)."
        _legacy_port_forwards
        return
    fi
    install -m 0755 "${sup_src}" "${sup}"

    # Forward set (name|namespace|service|ports|address), conditional on TLS_ENABLED
    # exactly like the old inline forwards.
    {
        echo "# Generated by deploy-detectflow-minikube.sh - do not edit by hand."
        echo "kafka|${NAMESPACE}|kafka|${KAFKA_EXTERNAL_PORT}:9094|${PF_ADDRESS}"
        echo "postgres|${NAMESPACE}|postgres|${POSTGRES_PORT}:5432|${POSTGRES_PF_ADDRESS}"
        echo "logstash|${NAMESPACE}|logstash|${LOGSTASH_SYSLOG_PORT}:${LOGSTASH_SYSLOG_PORT} ${LOGSTASH_JSON_PORT}:${LOGSTASH_JSON_PORT} ${LOGSTASH_TEXT_PORT}:${LOGSTASH_TEXT_PORT}|${PF_ADDRESS}"
        if [ "${TLS_ENABLED}" = "true" ]; then
            echo "traefik|${TRAEFIK_NS}|traefik|${UI_PORT}:${UI_PORT} ${API_PORT}:${API_PORT} ${KAFKA_UI_PORT}:${KAFKA_UI_PORT} ${DASHBOARD_PORT}:${DASHBOARD_PORT}|${PF_ADDRESS}"
        else
            echo "ui|${NAMESPACE}|admin-panel-ui|${UI_PORT}:4173|${PF_ADDRESS}"
            echo "api|${NAMESPACE}|admin-panel-be|${API_PORT}:8000|${PF_ADDRESS}"
            echo "kafka-ui|${NAMESPACE}|kafka-ui|${KAFKA_UI_PORT}:8080|${PF_ADDRESS}"
            echo "dashboard|${DASHBOARD_NS}|kubernetes-dashboard|${DASHBOARD_PORT}:80|${PF_ADDRESS}"
        fi
    } > "${conf}"

    # Reconcile: stop prior supervised loops + clear any legacy stray forwards, then start.
    "${sup}" stop >/dev/null 2>&1 || true
    pkill -f "kubectl port-forward" 2>/dev/null || true
    "${sup}" start

    # @reboot hook (per-user crontab, no root) so the whole stack returns after a
    # reboot. A boot script runs `minikube start` FIRST - on the docker driver a
    # container restart alone brings the container up but leaves kubelet/apiserver
    # stopped, so the cluster is dead and forwards would storm; `minikube start`
    # actually brings the control plane up. Then it starts the forwards. cron has a
    # minimal env, so the boot script pins PATH/KUBECONFIG (kubectl/minikube live in
    # /usr/local/bin, not cron's default PATH).
    if command -v crontab >/dev/null 2>&1; then
        local kubectl_bin kube_dir kubeconfig minikube_bin mk_force boot
        kubectl_bin="$(command -v kubectl || echo /usr/local/bin/kubectl)"
        kube_dir="$(dirname "${kubectl_bin}")"
        kubeconfig="${KUBECONFIG:-${HOME}/.kube/config}"
        minikube_bin="$(command -v minikube || echo /usr/local/bin/minikube)"
        mk_force=""
        [ "$(id -u)" -eq 0 ] && [ "${MINIKUBE_DRIVER}" = "docker" ] && mk_force="--force"
        boot="${df_home}/df-boot.sh"
        cat > "${boot}" <<EOF
#!/usr/bin/env bash
# Generated by deploy-detectflow-minikube.sh - restores the stack after a reboot.
export KUBECONFIG="${kubeconfig}"
export PATH="${kube_dir}:/usr/local/bin:/usr/bin:/bin"
echo "[\$(date '+%F %T')] boot: minikube start + forwards" >> "${df_home}/boot.log"
"${minikube_bin}" start ${mk_force} >> "${df_home}/boot.log" 2>&1
"${sup}" start
EOF
        chmod +x "${boot}"
        if ( crontab -l 2>/dev/null | grep -v -e 'df-forward-supervisor.sh' -e 'df-boot.sh'; echo "@reboot ${boot} >/dev/null 2>&1" ) | crontab -; then
            info "Boot hook installed (@reboot starts minikube + port-forwards)."
        else
            warn "Could not install @reboot crontab; stack will NOT auto-restore after reboot."
        fi
    else
        warn "crontab not found: skipping reboot hook (forwards still self-heal + survive logout)."
    fi

    # Give the forwards a moment to bind before the summary so status shows UP, not
    # the transient WAITING right after start.
    sleep 4
    "${sup}" status || true
}

# Print how to reach PostgreSQL. Its forward has its own bind address
# (POSTGRES_PF_ADDRESS): loopback by default -> reach it through an SSH tunnel;
# anything else exposes the demo-credential database, so warn about it.
print_postgres_access() {
    case "${POSTGRES_PF_ADDRESS}" in
        127.0.0.1|localhost|::1)
            echo -e "    PostgreSQL:    ${YELLOW}${POSTGRES_PF_ADDRESS}:${POSTGRES_PORT}${NC} on the server only  (db / user: detectflow)"
            echo -e "                   pgAdmin: SSH Tunnel host ${SERVER_HOST}, Connection host ${POSTGRES_PF_ADDRESS} port ${POSTGRES_PORT}"
            echo -e "                   (README: \"PostgreSQL Access (pgAdmin)\"; POSTGRES_PF_ADDRESS=0.0.0.0 exposes it)"
            ;;
        *)
            local host="${SERVER_HOST}"
            [ "${POSTGRES_PF_ADDRESS}" != "0.0.0.0" ] && host="${POSTGRES_PF_ADDRESS}"
            echo -e "    PostgreSQL:    ${YELLOW}${host}:${POSTGRES_PORT}${NC}  (db / user: detectflow, plaintext)"
            warn "PostgreSQL is reachable from the network with the default demo password."
            warn "Firewall port ${POSTGRES_PORT}, or set POSTGRES_PF_ADDRESS=127.0.0.1 and use an SSH tunnel."
            ;;
    esac
}

# Fallback used only if the supervisor script is missing: the previous behaviour
# (plain backgrounded forwards, no self-heal). Kept so a partial checkout still works.
_legacy_port_forwards() {
    pkill -f "kubectl port-forward" 2>/dev/null || true
    sleep 1
    if [ "${TLS_ENABLED}" != "true" ]; then
        kubectl port-forward --address "${PF_ADDRESS}" -n "${NAMESPACE}" svc/admin-panel-be "${API_PORT}:8000" &>/dev/null &
        kubectl port-forward --address "${PF_ADDRESS}" -n "${NAMESPACE}" svc/admin-panel-ui "${UI_PORT}:4173" &>/dev/null &
        kubectl port-forward --address "${PF_ADDRESS}" -n "${NAMESPACE}" svc/kafka-ui "${KAFKA_UI_PORT}:8080" &>/dev/null &
        kubectl port-forward --address "${PF_ADDRESS}" -n "${DASHBOARD_NS}" svc/kubernetes-dashboard "${DASHBOARD_PORT}:80" &>/dev/null &
    fi
    kubectl port-forward --address "${PF_ADDRESS}" -n "${NAMESPACE}" svc/kafka "${KAFKA_EXTERNAL_PORT}:9094" &>/dev/null &
    kubectl port-forward --address "${POSTGRES_PF_ADDRESS}" -n "${NAMESPACE}" svc/postgres "${POSTGRES_PORT}:5432" &>/dev/null &
    kubectl port-forward --address "${PF_ADDRESS}" -n "${NAMESPACE}" svc/logstash \
        "${LOGSTASH_SYSLOG_PORT}:${LOGSTASH_SYSLOG_PORT}" \
        "${LOGSTASH_JSON_PORT}:${LOGSTASH_JSON_PORT}" \
        "${LOGSTASH_TEXT_PORT}:${LOGSTASH_TEXT_PORT}" &>/dev/null &
    if [ "${TLS_ENABLED}" = "true" ]; then
        kubectl port-forward --address "${PF_ADDRESS}" -n "${TRAEFIK_NS}" svc/traefik \
            "${UI_PORT}:${UI_PORT}" "${API_PORT}:${API_PORT}" "${KAFKA_UI_PORT}:${KAFKA_UI_PORT}" "${DASHBOARD_PORT}:${DASHBOARD_PORT}" &>/dev/null &
    fi
    sleep 2
}

ARCH="$(dpkg --print-architecture)"   # amd64 | arm64

# Docker Engine + Compose plugin, official Ubuntu apt repo:
# https://docs.docker.com/engine/install/ubuntu/
install_docker() {
    if command -v docker &>/dev/null; then
        log "docker already installed ($(command -v docker))"
    else
        info "Installing Docker Engine (official apt repo)..."
        sudo apt-get update -qq
        sudo apt-get install -y ca-certificates curl
        sudo install -m 0755 -d /etc/apt/keyrings
        sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
        sudo chmod a+r /etc/apt/keyrings/docker.asc
        echo \
          "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
          $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
          sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update -qq
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo systemctl enable --now docker
        log "Docker Engine installed"
    fi

    # Non-root needs to be in the 'docker' group to reach the daemon socket.
    # (root accesses the socket directly - skip the group dance.)
    if [ "$(id -u)" -ne 0 ] && ! groups "$USER" | grep -qw docker; then
        info "Adding '$USER' to the 'docker' group..."
        sudo usermod -aG docker "$USER"
        warn "You were added to the 'docker' group. Log out/in (or run 'newgrp docker')"
        warn "and re-run this script so the group membership takes effect."
        exit 1
    fi

    if ! docker info &>/dev/null; then
        error "Cannot talk to Docker daemon. Is it running? ('sudo systemctl status docker')"
        error "If you were just added to the 'docker' group, log out/in and re-run."
        exit 1
    fi
    log "Docker is installed and reachable"
}

# Traefik ingress controller via the official Helm chart. ClusterIP service with
# one TLS entrypoint per service (values rendered from manifests/traefik/values.yaml);
# exposed to the host by a port-forward in the final step (matches every other svc).
install_traefik() {
    info "Adding Traefik Helm repo..."
    helm repo add traefik https://traefik.github.io/charts 2>/dev/null || true
    helm repo update
    info "Installing/upgrading Traefik ${TRAEFIK_VERSION}..."
    local vfile
    vfile="$(mktemp)"
    envsubst < "${MANIFESTS_DIR}/traefik/values.yaml" > "${vfile}"
    helm upgrade --install traefik traefik/traefik \
        --namespace "${TRAEFIK_NS}" --create-namespace \
        --version "${TRAEFIK_VERSION}" \
        --values "${vfile}" \
        --wait --timeout 180s
    rm -f "${vfile}"
    log "Traefik installed"
}

# Fail fast on missing/placeholder .env values before any install work.
validate_config

# ─── Step 0: Install Docker ──────────────────────────────────────────────────
header "Step 0/15: Install Docker"

# Docker must exist before the docker minikube driver.
install_docker

info "DetectFlow images are public on Docker Hub - no login needed."

# ─── Step 1: Install CLI Tools ───────────────────────────────────────────────
header "Step 1/15: Installing CLI Tools"

# NOTE: Ubuntu install methods below use official upstream sources. Swap any of
# these functions if you prefer a specific package source / pinned version.
# (Docker is installed earlier, in Step 0.)

install_kubectl() {
    if command -v kubectl &>/dev/null; then
        log "kubectl already installed ($(command -v kubectl))"; return
    fi
    info "Installing kubectl..."
    local ver
    ver="$(curl -L -s https://dl.k8s.io/release/stable.txt)"
    curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/${ver}/bin/linux/${ARCH}/kubectl"
    sudo install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl
    rm -f /tmp/kubectl
    log "kubectl ${ver} installed"
}

install_minikube() {
    if command -v minikube &>/dev/null; then
        log "minikube already installed ($(command -v minikube))"; return
    fi
    info "Installing minikube (binary)..."
    curl -Lo /tmp/minikube "https://storage.googleapis.com/minikube/releases/latest/minikube-linux-${ARCH}"
    chmod +x /tmp/minikube
    sudo mkdir -p /usr/local/bin/
    sudo install /tmp/minikube /usr/local/bin/minikube
    rm -f /tmp/minikube
    log "minikube installed"
}

install_helm() {
    if command -v helm &>/dev/null; then
        log "helm already installed ($(command -v helm))"; return
    fi
    info "Installing helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    log "helm installed"
}

install_kcat() {
    # The Kafka CLI's package/binary name differs by release: 'kcat' on Ubuntu 24.04+,
    # 'kafkacat' on 22.04 and older. Both live in the 'universe' component, which
    # minimal images may not have enabled. kcat is only used for manual Kafka/Logstash
    # testing, so a failure here is non-fatal - the deploy continues.
    if command -v kcat &>/dev/null; then
        log "kcat already installed ($(command -v kcat))"; return
    fi
    info "Installing kcat (Kafka CLI)..."
    sudo apt-get update -qq
    if ! sudo apt-get install -y kcat 2>/dev/null && ! sudo apt-get install -y kafkacat 2>/dev/null; then
        warn "kcat/kafkacat not in the enabled repos - enabling 'universe' and retrying..."
        sudo apt-get install -y software-properties-common 2>/dev/null || true
        sudo add-apt-repository -y universe 2>/dev/null || true
        sudo apt-get update -qq || true
        sudo apt-get install -y kcat 2>/dev/null || sudo apt-get install -y kafkacat 2>/dev/null || true
    fi
    # On 22.04 the package is 'kafkacat' and ships only that binary; symlink so the
    # 'kcat' name (used throughout the README) works on every release.
    if ! command -v kcat &>/dev/null && command -v kafkacat &>/dev/null; then
        sudo ln -sf "$(command -v kafkacat)" /usr/local/bin/kcat || true
    fi
    if command -v kcat &>/dev/null; then
        log "kcat installed ($(command -v kcat))"
    else
        warn "Could not install kcat/kafkacat (only needed for manual Kafka/Logstash testing) - skipping."
    fi
}

install_envsubst() {
    if command -v envsubst &>/dev/null; then
        log "envsubst already installed ($(command -v envsubst))"; return
    fi
    info "Installing envsubst (gettext-base)..."
    sudo apt-get update -qq
    sudo apt-get install -y gettext-base
    log "envsubst installed"
}

install_kubectl
install_minikube
install_helm
install_kcat
install_envsubst

# ─── Step 2: Start Minikube ──────────────────────────────────────────────────
header "Step 2/15: Starting Minikube Cluster"

if minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
    log "Minikube is already running"
    info "Current config: $(minikube config view 2>/dev/null || echo 'default')"
else
    info "Starting minikube with: ${MINIKUBE_CPUS} CPUs, ${MINIKUBE_MEMORY}MB RAM, ${MINIKUBE_DISK} disk, ${MINIKUBE_CONTAINER_RUNTIME} runtime"

    # The docker driver refuses to run as root unless --force is given.
    # Recommended: run this script as a normal user that is in the 'docker' group.
    FORCE_FLAG=""
    if [ "$(id -u)" -eq 0 ] && [ "${MINIKUBE_DRIVER}" = "docker" ]; then
        if [ "${MINIKUBE_FORCE:-false}" = "true" ]; then
            warn "Running as root: passing --force to minikube (docker driver as root)."
            FORCE_FLAG="--force"
        else
            error "minikube's docker driver should NOT run as root."
            error "Either:"
            error "  1) Run this script as a normal user in the 'docker' group (recommended), or"
            error "  2) Re-run with MINIKUBE_FORCE=true to force it (root + docker driver)."
            exit 1
        fi
    fi

    minikube start \
        --driver="${MINIKUBE_DRIVER}" \
        --container-runtime="${MINIKUBE_CONTAINER_RUNTIME}" \
        --cpus="${MINIKUBE_CPUS}" \
        --memory="${MINIKUBE_MEMORY}" \
        --disk-size="${MINIKUBE_DISK}" \
        --addons=storage-provisioner,default-storageclass \
        ${FORCE_FLAG}
    log "Minikube started"
fi

# Make the minikube container restart on boot (docker driver). This only brings the
# *container* back - on the docker driver kubelet/apiserver stay stopped until
# `minikube start` runs, which the @reboot hook (df-boot.sh, set up in Step 15) does
# before starting the port-forward supervisor.
if [ "${MINIKUBE_DRIVER}" = "docker" ]; then
    if docker update --restart=always minikube >/dev/null 2>&1; then
        log "minikube container set to restart on boot"
    else
        warn "Could not set minikube restart policy (the @reboot hook still runs 'minikube start')."
    fi
fi

# Verify cluster is accessible
kubectl cluster-info &>/dev/null || { error "Cannot connect to cluster"; exit 1; }
log "Cluster is accessible"

# The runtime is fixed when the cluster is created; --container-runtime does not change
# an existing one. Warn if it differs from MINIKUBE_CONTAINER_RUNTIME - typically a
# cluster created by minikube < v1.39.0, whose implicit default was docker.
check_container_runtime() {
    local actual
    actual="$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}' 2>/dev/null)"
    actual="${actual%%://*}"   # e.g. containerd://2.3.4 -> containerd
    [ -z "${actual}" ] && { warn "Could not detect the cluster's container runtime."; return 0; }
    if [ "${actual}" = "${MINIKUBE_CONTAINER_RUNTIME}" ]; then
        log "Container runtime: ${actual}"
        return 0
    fi
    warn "This cluster runs the '${actual}' container runtime, not '${MINIKUBE_CONTAINER_RUNTIME}' (MINIKUBE_CONTAINER_RUNTIME)."
    warn "It was set when the cluster was created and is NOT changed by re-running this script."
    if [ "${actual}" = "docker" ]; then
        warn "On the docker runtime kubelet cannot rotate container logs and logs an error every"
        warn "few seconds into the node's in-memory journal, which slowly eats cluster RAM."
    fi
    warn "To switch: back up your data, then 'minikube delete' and re-run this script."
    warn "minikube delete REMOVES all cluster data (PostgreSQL, Kafka, Flink volumes)."
}
check_container_runtime

# Label the minikube node for PVC scheduling
info "Detecting minikube node name and applying label..."
MINIKUBE_NODE=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
kubectl label node "${MINIKUBE_NODE}" spec-node-ns-pod-disk=pvc-security --overwrite
kubectl label node "${MINIKUBE_NODE}" app=detectflow-prod --overwrite
log "Node '${MINIKUBE_NODE}' labeled with spec-node-ns-pod-disk=pvc-security, app=detectflow-prod"

# ─── Step 3: Deploy Kubernetes Dashboard ─────────────────────────────────────
header "Step 3/15: Deploying Kubernetes Dashboard"

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

# Dashboard is exposed at Step 15 by the port-forward supervisor (plaintext when TLS
# is off; via Traefik/HTTPS when on). No early forward needed here.
if [ "${TLS_ENABLED}" = "true" ]; then
    info "TLS enabled - dashboard will be served over HTTPS via Traefik after Step 14 (port ${DASHBOARD_PORT})."
fi

# ─── Step 4: Create Namespace ────────────────────────────────────────────────
header "Step 4/15: Creating Namespace '${NAMESPACE}'"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
log "Namespace '${NAMESPACE}' ready"

# ─── Step 5: Deploy PostgreSQL ───────────────────────────────────────────────
header "Step 5/15: Deploying PostgreSQL"

info "Deploying PostgreSQL with database 'detectflow', user 'detectflow'"

for f in "${MANIFESTS_DIR}/postgres"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

info "Waiting for PostgreSQL to be ready..."
kubectl rollout status deployment/postgres -n "${NAMESPACE}" --timeout=120s
log "PostgreSQL deployed and ready"

# ─── Step 6: Deploy Apache Kafka (KRaft) ─────────────────────────────────────
header "Step 6/15: Deploying Apache Kafka (KRaft Mode)"

info "Deploying single-node Kafka (no ZooKeeper)"

for f in "${MANIFESTS_DIR}/kafka"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

info "Waiting for Kafka to be ready..."
kubectl rollout status deployment/kafka -n "${NAMESPACE}" --timeout=180s
# Extra wait for Kafka broker to fully initialize
info "Waiting additional 20s for Kafka broker initialization..."
sleep 20
log "Kafka deployed and ready"

# ─── Step 7: Deploy Logstash ─────────────────────────────────────────────────
header "Step 7/15: Deploying Logstash"

info "Deploying Logstash (syslog/json/unstructured inputs -> Kafka)"

for f in "${MANIFESTS_DIR}/logstash"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

info "Waiting for Logstash to be ready..."
kubectl rollout status deployment/logstash -n "${NAMESPACE}" --timeout=180s
log "Logstash deployed"

# ─── Step 8: Deploy Kafka UI ─────────────────────────────────────────────────
header "Step 8/15: Deploying Kafka UI"

info "Deploying kafbat/kafka-ui..."
for f in "${MANIFESTS_DIR}/kafka-ui"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

info "Waiting for Kafka UI to be ready..."
kubectl rollout status deployment/kafka-ui -n "${NAMESPACE}" --timeout=180s
log "Kafka UI deployed"

# ─── Step 9: Install cert-manager ────────────────────────────────────────────
header "Step 9/15: Installing cert-manager"

if kubectl get namespace cert-manager &>/dev/null; then
    log "cert-manager namespace exists, checking pods..."
    if kubectl get pods -n cert-manager -l app.kubernetes.io/instance=cert-manager 2>/dev/null | grep -q Running; then
        log "cert-manager already running"
    else
        warn "cert-manager namespace exists but pods not healthy, reinstalling..."
        kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    fi
else
    info "Installing cert-manager ${CERT_MANAGER_VERSION}..."
    kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
fi

info "Waiting for cert-manager pods to be ready..."
kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s
kubectl wait --for=condition=Available deployment/cert-manager-cainjector -n cert-manager --timeout=120s
log "cert-manager ready"

# ─── Step 10: Install Flink Kubernetes Operator ──────────────────────────────
header "Step 10/15: Installing Flink Kubernetes Operator"

if helm list -n "${FLINK_OPERATOR_NS}" 2>/dev/null | grep -q flink-kubernetes-operator; then
    log "Flink Kubernetes Operator already installed"
else
    info "Adding Flink operator Helm repo..."
    helm repo add flink-operator-repo "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" 2>/dev/null || true
    helm repo update

    info "Installing Flink Kubernetes Operator v${FLINK_OPERATOR_VERSION}..."
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

# ─── Step 11: Deploy Flink Infrastructure ────────────────────────────────────
header "Step 11/15: Deploying Flink Infrastructure (RBAC, ConfigMaps, PVCs)"

# Apply RBAC (ServiceAccount 'flink' for the JobManager/TaskManager pods the backend
# spawns via the operator, and the backend's own permissions).
envsubst < "${MANIFESTS_DIR}/flink/rbac.yaml" | kubectl apply -n "${NAMESPACE}" -f -
log "Flink RBAC applied"

# Apply ConfigMap
envsubst < "${MANIFESTS_DIR}/flink/configmap.yaml" | kubectl apply -n "${NAMESPACE}" -f -
log "Flink ConfigMap applied"

# Apply Secret (with local Kafka config)
envsubst < "${MANIFESTS_DIR}/flink/secret.yaml" | kubectl apply -n "${NAMESPACE}" -f -
log "Flink Secret applied (local Kafka)"

# Apply PVCs (with standard storage class for minikube)
envsubst < "${MANIFESTS_DIR}/flink/pvc.yaml" | kubectl apply -n "${NAMESPACE}" -f -
log "Flink PVCs applied (minikube-sized)"

# ─── Step 12: Deploy DetectFlow Backend ──────────────────────────────────────
header "Step 12/15: Deploying DetectFlow Backend"

info "Using pre-built image: ${IMAGE_BACKEND}"

for f in "${MANIFESTS_DIR}/backend"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

# Optional: inject the LLM self-signed CA into the agent Secret. The static
# detectflow-agent-secret.yaml (applied above) carries LLM_API_KEY only; when
# LLM_CA_FILE is set we rebuild the Secret to also hold the CA bundle under the
# key basename(LLM_CA_MOUNT_PATH), which the ConfigMap's SSL_CERT_FILE points at.
# The PEM lives in a file, never in .env. When LLM_CA_FILE is unset, apply above
# already reconciled the Secret back to LLM_API_KEY only (drops a stale ca key).
if [ -n "${LLM_CA_FILE}" ]; then
    ca_key="$(basename "${LLM_CA_MOUNT_PATH}")"
    info "Injecting LLM CA (${LLM_CA_FILE}) into agent Secret as key '${ca_key}'"
    kubectl create secret generic detectflow-agent-secrets \
        --namespace "${NAMESPACE}" \
        --from-literal=LLM_API_KEY="${LLM_API_KEY:-}" \
        --from-file="${ca_key}=${LLM_CA_FILE}" \
        --dry-run=client -o yaml | kubectl apply -n "${NAMESPACE}" -f -
fi

info "Waiting for Backend to be ready (migrations + startup)..."
kubectl rollout status deployment/admin-panel-be -n "${NAMESPACE}" --timeout=300s
log "DetectFlow Backend deployed"

# ─── Step 13: Deploy DetectFlow UI ───────────────────────────────────────────
header "Step 13/15: Deploying DetectFlow UI"

info "Using pre-built image: ${IMAGE_UI}"

for f in "${MANIFESTS_DIR}/frontend"/*.yaml; do echo '---'; envsubst < "$f"; done | kubectl apply -n "${NAMESPACE}" -f -

info "Waiting for UI to be ready..."
kubectl rollout status deployment/admin-panel-ui -n "${NAMESPACE}" --timeout=180s
log "DetectFlow UI deployed"

# ─── Step 14: Enable TLS via Traefik + Ingress (on by default) ───────────────
header "Step 14/15: TLS & Ingress"

if [ "${TLS_ENABLED}" != "true" ]; then
    info "TLS_ENABLED != true - skipping Traefik/ingress (services remain on plaintext port-forwards)."
else
    # df-tls is Traefik's default certificate and lives in the Traefik namespace
    # (where the default TLSStore from values.yaml references it). Create the ns first.
    kubectl create namespace "${TRAEFIK_NS}" --dry-run=client -o yaml | kubectl apply -f -

    if [ "${TLS_MODE}" = "provided" ]; then
        info "TLS_MODE=provided - loading cert from ${TLS_CERT_FILE}"
        # Drop any cert-manager Certificate from a previous selfsigned run (either
        # namespace) so it stops reconciling/overwriting df-tls with its own cert.
        kubectl delete certificate detectflow-tls -n "${TRAEFIK_NS}" --ignore-not-found
        kubectl delete certificate detectflow-tls -n "${NAMESPACE}" --ignore-not-found
        # Drop a secret previously owned by cert-manager so apply manages it cleanly
        # (avoids the harmless "missing last-applied-configuration annotation" warning).
        kubectl delete secret "${TLS_SECRET_NAME}" -n "${TRAEFIK_NS}" --ignore-not-found
        kubectl create secret tls "${TLS_SECRET_NAME}" \
            --cert="${TLS_CERT_FILE}" --key="${TLS_KEY_FILE}" \
            -n "${TRAEFIK_NS}" --dry-run=client -o yaml | kubectl apply -f -
        log "TLS secret '${TLS_SECRET_NAME}' loaded from files into ns ${TRAEFIK_NS}"
    else
        info "TLS_MODE=selfsigned - issuing cert via cert-manager"
        envsubst < "${MANIFESTS_DIR}/tls/clusterissuer.yaml" | kubectl apply -f -
        # SERVER_HOST SAN: IP -> ipAddresses; hostname -> dnsNames. The Certificate
        # is built inline (the SAN field is conditional, so a static template can't
        # be both valid YAML and schema-clean).
        if [[ "${SERVER_HOST}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            cert_san="  ipAddresses:
    - ${SERVER_HOST}"
        else
            cert_san="  dnsNames:
    - ${SERVER_HOST}"
        fi
        kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: detectflow-tls
  namespace: ${TRAEFIK_NS}
spec:
  secretName: ${TLS_SECRET_NAME}
  duration: 8760h
${cert_san}
  issuerRef:
    name: detectflow-ca-issuer
    kind: ClusterIssuer
    group: cert-manager.io
EOF
        info "Waiting for certificate to be issued..."
        kubectl wait --for=condition=Ready certificate/detectflow-tls -n "${TRAEFIK_NS}" --timeout=120s
        log "TLS secret '${TLS_SECRET_NAME}' issued by cert-manager into ns ${TRAEFIK_NS}"
    fi

    # Install Traefik AFTER df-tls exists so the default-cert TLSStore resolves on first start.
    install_traefik

    # IngressRoutes live in ${NAMESPACE} (alongside the backend services they target).
    envsubst < "${MANIFESTS_DIR}/ingress/ingressroutes.yaml" | kubectl apply -f -
    # Dashboard route lives in its own namespace (${DASHBOARD_NS}).
    envsubst < "${MANIFESTS_DIR}/ingress/ingressroute-dashboard.yaml" | kubectl apply -f -
    log "IngressRoutes applied - HTTPS on ports ${UI_PORT} (UI), ${API_PORT} (API), ${KAFKA_UI_PORT} (Kafka UI), ${DASHBOARD_PORT} (Dashboard)"
fi

# ─── Step 15: Print Access Information ───────────────────────────────────────
header "Step 15/15: Deployment Complete!"

# With the Docker driver the minikube IP is not directly routable, so services are
# exposed via port-forward bound to ${PF_ADDRESS} (0.0.0.0 = reachable from network);
# PostgreSQL binds ${POSTGRES_PF_ADDRESS} instead (127.0.0.1 = server only).
# The supervisor keeps them alive: auto-restart on drop, survive logout + reboot.
info "Setting up self-healing port-forwards (bind ${PF_ADDRESS}; PostgreSQL ${POSTGRES_PF_ADDRESS})..."
setup_port_forwards

echo ""
echo -e "${BOLD}DetectFlow Demo - Local Deployment${NC}"
echo -e "${BOLD}==================================${NC}"
echo ""
echo -e "  ${GREEN}All components deployed successfully!${NC}"
echo ""
# UI/API/Kafka-UI scheme follows TLS_ENABLED (https on the same ports when on).
if [ "${TLS_ENABLED}" = "true" ]; then WEB_SCHEME="https"; else WEB_SCHEME="http"; fi
echo -e "  ${CYAN}Access URLs (SERVER_HOST=${SERVER_HOST}):${NC}"
echo -e "    UI:            ${YELLOW}${WEB_SCHEME}://${SERVER_HOST}:${UI_PORT}${NC}"
echo -e "    Backend API:   ${YELLOW}${WEB_SCHEME}://${SERVER_HOST}:${API_PORT}${NC}"
echo -e "    Kafka UI:      ${YELLOW}${WEB_SCHEME}://${SERVER_HOST}:${KAFKA_UI_PORT}${NC}"
echo -e "    K8s Dashboard: ${YELLOW}${WEB_SCHEME}://${SERVER_HOST}:${DASHBOARD_PORT}${NC}"
echo -e "    Kafka Broker:  ${YELLOW}${SERVER_HOST}:${KAFKA_EXTERNAL_PORT}${NC}  (external listener, plaintext)"
echo -e "    Logstash:      ${YELLOW}${SERVER_HOST}:${LOGSTASH_SYSLOG_PORT}${NC} syslog, ${YELLOW}${LOGSTASH_JSON_PORT}${NC} json, ${YELLOW}${LOGSTASH_TEXT_PORT}${NC} unstructured (plaintext)"
print_postgres_access
if [ "${TLS_ENABLED}" = "true" ]; then
    echo -e "    ${BOLD}TLS enabled${NC} (${TLS_MODE}) - UI/API/Kafka-UI/Dashboard served over HTTPS on the ports above."
    if [ "${TLS_MODE}" = "selfsigned" ]; then
        echo -e "      ${YELLOW}Self-signed CA: browsers warn until you import it (cert SAN = ${SERVER_HOST}).${NC}"
    fi
fi
echo ""
echo -e "  ${CYAN}Login Credentials:${NC}"
echo -e "    Email:        ${YELLOW}admin@soc.local${NC}"
echo -e "    Password:     ${YELLOW}admin${NC}"
echo ""
echo -e "  ${CYAN}Pod Status:${NC}"
kubectl get pods -n "${NAMESPACE}" 2>/dev/null
echo ""
echo -e "  ${CYAN}Useful Commands:${NC}"
echo "    kubectl get pods -n ${NAMESPACE}"
echo "    kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=admin-panel-be -f"
echo "    minikube stop    (preserves data)"
echo "    minikube delete  (removes everything)"
echo ""
echo -e "  ${CYAN}Port-Forwards (self-healing, survive logout + reboot):${NC}"
echo "    Status:  ~/.detectflow/df-forward-supervisor.sh status"
echo "    Stop:    ~/.detectflow/df-forward-supervisor.sh stop"
echo "    Restart: ~/.detectflow/df-forward-supervisor.sh restart"
echo "    Log:     ~/.detectflow/forwards.log"
echo ""
