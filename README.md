# DetectFlow Demo - Complete Deployment Guide

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [Deployment (Ubuntu + Minikube)](#3-deployment-ubuntu--minikube)
4. [Access URLs & Credentials](#4-access-urls--credentials)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. Project Overview

**DetectFlow** is a real-time SIEM detection pipeline platform by SOC Prime. It applies Sigma rules on streaming events from Apache Kafka using Apache Flink, providing sub-second threat detection.

 > **Note:** The deployment script and all configuration files in this repository contain placeholder data (mock secrets) and are intended for demonstration purposes only.
 >
 > Image names are configurable in `.env` (`IMAGE_BACKEND`, `IMAGE_UI`, `IMAGE_FLINK`, `IMAGE_AGENT`, `IMAGE_CORRELATION`).
 >
 > **Current release: DetectFlow v2.3.2** - the public `socprime/detectflow-oss` images are preset in `.env.example`; keep all five image tags on the same release (versions noted elsewhere, e.g. "backend ≥ v2.2.0", are minimums for a feature).

---

## 2. Tech Stack

### Infrastructure Dependencies

| Dependency | Version | Purpose | Required By |
| --- | --- | --- | --- |
| **Kubernetes** | 1.28+ | Container orchestration | All components |
| **PostgreSQL** | 14+ | Backend database (rules, pipelines, users) | Backend |
| **Apache Kafka** | 3.8+ | Event streaming & rule distribution | Backend, Flink Sigma Detector |
| **Logstash** | 9.x (OSS) | Optional ingest: syslog / JSON / unstructured logs → Kafka, for sources that can't write to Kafka directly | Optional (ingest) |
| **Apache Flink** | 2.2 | Stream processing engine | Flink Sigma Detector |
| **Flink Kubernetes Operator** | 1.15.0 | Manages Flink deployments as CRDs | Flink Sigma Detector lifecycle |
| **cert-manager** | 1.21.1 | TLS certificate management (Flink operator; app TLS in `selfsigned` mode) | Flink Operator, TLS |
| **Traefik** | 34.x (chart) | Per-port TLS termination for HTTPS on the UI / API / Kafka UI / dashboard | TLS (on by default) |

### Tools (Ubuntu)

The deploy script installs any missing tools automatically (check-then-install). Install sources used:

| Tool | Purpose | Install method (auto) |
| --- | --- | --- |
| **docker** | Container runtime + driver | Official apt repo (`docker-ce`, `docker-compose-plugin`) - [docs](https://docs.docker.com/engine/install/ubuntu/) |
| **minikube** | Local K8s cluster | Official binary (latest release) → `/usr/local/bin/minikube` |
| **kubectl** | K8s CLI | Official binary from `dl.k8s.io` |
| **helm** | K8s package manager | Official `get-helm-3` script |
| **kcat** | Kafka CLI producer/consumer | `apt-get install kcat` (24.04+) or `kafkacat` (22.04); auto-enables `universe`, symlinks `kcat` on 22.04. Non-fatal - testing only |
| **envsubst** | Render `${VAR}` in `manifests/*.yaml` from `.env` | `apt-get install gettext-base` |

---

## 3. Deployment (Ubuntu + Minikube)

### Prerequisites

- **Ubuntu 22.04 / 24.04 / 26.04**
- Docker Engine running, with your user in the `docker` group (the script installs Docker if missing)
- Defaults are sized for an **8 CPU / 16 GB RAM / 128 GB disk** server (1-2 Flink pipelines). Add a few GB of **swap** - with none, a memory spike can hang the node. For more pipelines or traffic, use a bigger server and raise `MINIKUBE_MEMORY` and the component limits in `.env` together.

### Configuration (`.env`)

All tunables live in `.env`. Copy the template and edit before deploying:

```bash
cp .env.example .env
```

Key values to set:

| Variable | Meaning |
| --- | --- |
| `SERVER_HOST` | **Set to this server's IP/hostname.** Used for the Kafka external listener and printed URLs. For remote clients it **must not** be `localhost`. |
| `PF_ADDRESS` | port-forward bind address. `0.0.0.0` = reachable from the network (default). |
| `POSTGRES_PF_ADDRESS` | bind address for the PostgreSQL forward only. `127.0.0.1` (default) = on the server only, connect through an SSH tunnel - see [PostgreSQL Access (pgAdmin)](#postgresql-access-pgadmin); `0.0.0.0` = reachable from the network. |
| `MINIKUBE_CPUS` / `MINIKUBE_MEMORY` / `MINIKUBE_DISK` | Cluster sizing (default 6 / 12288 MB / 100g). Keep at least ~4 GB RAM for the host, and keep the sum of the component memory limits (`PG_MEM_LIMIT`, `KAFKA_MEM_LIMIT`, `LOGSTASH_MEM_LIMIT`, ...) well below `MINIKUBE_MEMORY`. |
| `MINIKUBE_CONTAINER_RUNTIME` | Container runtime inside the minikube node: `containerd` (default), `docker` or `cri-o`. Pinned so it doesn't depend on the installed minikube version. Applies only when the cluster is created - see [Cluster runs the docker container runtime](#cluster-runs-the-docker-container-runtime). |
| `IMAGE_BACKEND` / `IMAGE_UI` / `IMAGE_FLINK` / `IMAGE_AGENT` / `IMAGE_CORRELATION` | Public Docker Hub image tags for the DetectFlow services (`socprime/detectflow-oss`), preset to the current release in `.env.example`. **All five are required** - the script fails fast if any is empty. `IMAGE_AGENT` (df-agent) and `IMAGE_CORRELATION` (df-correlation) are the job images the backend launches. |
| `PG_*` | PostgreSQL resources + tuning. |
| `KAFKA_*` | Kafka resources, JVM heap, threads, partitions, retention. `KAFKA_LOG_RETENTION_HOURS` (default 2) works together with `KAFKA_LOG_ROLL_MS` (default 10 min): Kafka deletes only whole closed segments, so the roll interval makes old data actually go away on time. |
| `LLM_*` / `MAX_CONCURRENT_LLM_REQUESTS` | DetectFlow Agent (LLM) config - endpoint, model, API key, and rate/size tuning. Optional; see below. |

> `.env` is gitignored.

### DetectFlow Agent (LLM) configuration

The v2.1.0+ backend can launch **agent jobs** on Flink that call an LLM. Their
configuration is rendered from `.env` (via `envsubst`) into two manifests that the
deploy applies automatically with the rest of the backend:

- `manifests/backend/detectflow-agent-config-configmap.yaml` → ConfigMap `detectflow-agent-config`
- `manifests/backend/detectflow-agent-secret.yaml` → Secret `detectflow-agent-secrets`

The backend references these by name when it starts an agent job. Set the values in
`.env`:

| Variable | Meaning |
| --- | --- |
| `LLM_BASE_URL` | LLM API endpoint (OpenAI-compatible, e.g. `http://llm.internal/v1/`). |
| `LLM_MODEL` | Model name to request. |
| `LLM_API_KEY` | API key if the endpoint needs auth. Injected via the Secret's `stringData` at apply time - **never committed**. Leave empty if unused. |
| `LLM_CALLS_PER_MINUTE` / `LLM_MAX_RETRIES` / `LLM_MAX_TOKENS` / `LLM_PROMPT_CHAR_BUDGET` / `MAX_CONCURRENT_LLM_REQUESTS` | Rate, retry, and size tuning (safe defaults shipped). |

> Optional: if you are not using the LLM features, keep the defaults. The agent
> config/secret are still applied (empty key is valid), so no action is required.

#### Self-signed / internal LLM CA (optional, backend ≥ v2.2.0)

If `LLM_BASE_URL` is an `https://` endpoint with a self-signed or internal CA, the
agent must be told to trust it. Point `LLM_CA_FILE` at a PEM CA bundle (kept out of
git, like `TLS_CERT_FILE`); the deploy projects it into the `detectflow-agent-secrets`
Secret and sets `SSL_CERT_FILE` in the ConfigMap so the backend mounts and trusts it.

| Variable | Meaning |
| --- | --- |
| `LLM_CA_FILE` | Host path to the CA bundle (PEM). Empty = feature off (system trust store). The file is read at apply time and never committed. |
| `LLM_CA_MOUNT_PATH` | In-container path for the CA. Dirname = mount dir, basename = the Secret key. Must be under a subdirectory, not directly under `/`. Default `/etc/ssl/agent-ca/ca.pem`. |
| `LLM_SSL_VERIFY` | `true` (default, secure) or `false` (testing only — skips TLS verification). |

> The certificate **must carry a SAN** matching the LLM host/IP — a CN-only cert is
> rejected by Python's `ssl` (`IP address mismatch`). Verify with
> `openssl x509 -in ca.pem -noout -text | grep -A1 "Subject Alternative Name"`.

### Deploy

```bash
git clone https://github.com/socprime/detectflow-one-click-local-deployment.git
cd detectflow-one-click-local-deployment
git checkout v2.3.2               # one branch per DetectFlow release
cp .env.example .env          # then set SERVER_HOST (and any optional tunables)
chmod +x deploy-detectflow-minikube.sh

# Run as a normal user in the 'docker' group (recommended):
./deploy-detectflow-minikube.sh
```

The script installs missing tools (Docker, minikube, kubectl, helm, kcat, envsubst) and deploys the full stack. All Kubernetes manifests live in `manifests/` next to the script; `${VAR}` placeholders in them are rendered from `.env` via `envsubst` at apply time. Nothing is fetched from external git repos.

> **First run - `docker` group (one-time):** if your user isn't in the `docker` group yet, the script adds it and then **stops** - Linux only applies a new group to a fresh login session, so the current shell still can't reach the Docker socket. Activate the group and re-run once:
>
> ```bash
> newgrp docker                      # or: log out and back in
> ./deploy-detectflow-minikube.sh    # re-run; this time it proceeds past Docker
> ```
>
> On the second run the group is active and the deploy continues normally. (Running the script as `root` skips this entirely, but minikube's docker driver then needs `MINIKUBE_FORCE=true` - see below.)

**Running as root:** minikube's docker driver refuses root. Run as a normal user, or force it:

```bash
MINIKUBE_FORCE=true ./deploy-detectflow-minikube.sh
```

> **Note:** No images are built locally. All DetectFlow images are public on Docker Hub (`socprime/detectflow-oss`) - no Docker Hub login is needed.

---

## 4. Access URLs & Credentials

After deployment, the script sets up **self-healing port-forwards** (bound to `PF_ADDRESS`, default `0.0.0.0`; PostgreSQL alone binds `POSTGRES_PF_ADDRESS`, default `127.0.0.1`) and prints access URLs. A small supervisor (`~/.detectflow/df-forward-supervisor.sh`) keeps each forward alive - it auto-restarts within ~2s if a forward drops (pod restart, API blip), survives logout, and reports honest status (`UP`/`WAITING`/`DOWN`). After a **reboot** a `@reboot` crontab hook runs `~/.detectflow/df-boot.sh`, which brings the cluster back (`minikube start` - a container restart alone leaves kubelet/apiserver stopped) and then the forwards - all with no root and no re-running the deploy. Replace `SERVER_HOST` with the value from your `.env` (remote clients use the server IP; locally `localhost` works):

| Service | URL | Notes |
| --- | --- | --- |
| **DetectFlow UI** | `https://SERVER_HOST:4173` | Main admin panel |
| **DetectFlow Backend API** | `https://SERVER_HOST:8000` | REST API |
| **Kafka UI** | `https://SERVER_HOST:8080` | Kafka monitoring & management |
| **Kafka Broker** | `SERVER_HOST:9094` | External listener for remote access (plaintext) |
| **PostgreSQL** | `127.0.0.1:5432` on the server | Server-only by default - connect with pgAdmin over an SSH tunnel, see [PostgreSQL Access (pgAdmin)](#postgresql-access-pgadmin). db / user `detectflow` (plaintext) |
| **Logstash Ingest** | `SERVER_HOST:10501` (syslog), `:10510` (json), `:10520` (text) | Log ingestion → Kafka (plaintext) |
| **K8s Dashboard** | `https://SERVER_HOST:8443` | Kubernetes cluster monitoring |

> Ports are configurable in `.env` (`UI_PORT`, `API_PORT`, `KAFKA_UI_PORT`, `KAFKA_EXTERNAL_PORT`, `DASHBOARD_PORT`, `POSTGRES_PORT`, `LOGSTASH_SYSLOG_PORT`, `LOGSTASH_JSON_PORT`, `LOGSTASH_TEXT_PORT`).
>
> URLs are `https` because **TLS is on by default** (`selfsigned` - browsers warn until you import the CA, but the page loads). The Kafka broker, PostgreSQL and Logstash stay plaintext. Set `TLS_ENABLED=false` for plain `http` on the web ports (the UI will then be blank - see [Why the UI needs HTTPS](#why-the-ui-needs-https)). Details: [HTTPS / TLS](#https--tls-on-by-default-per-port) below.

### HTTPS / TLS (on by default, per-port)

**TLS is on by default (`selfsigned`)** because the DetectFlow **UI is effectively HTTPS-only** - its image ships a `Content-Security-Policy: … upgrade-insecure-requests` header, so served over plain HTTP the browser upgrades its assets to https and the page renders **blank**. With TLS on, Traefik terminates TLS so the **UI, API, Kafka UI, and K8s dashboard serve HTTPS on their existing port numbers** (`https://…:4173`, etc.) and the UI loads. Configure in `.env`:

```bash
TLS_ENABLED=true
TLS_MODE=selfsigned          # or: provided
# provided mode only:
TLS_CERT_FILE=/path/to/cert.pem
TLS_KEY_FILE=/path/to/key.pem
```

- **selfsigned** - cert-manager issues a cert from a self-signed CA, with `SERVER_HOST` as the SAN (IP or hostname). Browsers warn until you import the CA. Good for demo/LAN.
- **provided** - bring your own cert. Point `TLS_CERT_FILE` / `TLS_KEY_FILE` at your PEM files and set `SERVER_HOST` to a name the cert covers (for a wildcard `*.example.com`, use a sub-host like `df.example.com` - wildcards don't cover the apex) for valid, warning-free SSL.

When enabled, the UI/API/Kafka-UI/dashboard ports serve HTTPS **instead of** HTTP (you can't have both on one port); the Kafka broker and Logstash stay plaintext. Reach everything as `https://SERVER_HOST:<port>`:

| Service | URL |
| --- | --- |
| **DetectFlow UI** | `https://SERVER_HOST:4173` |
| **DetectFlow Backend API** | `https://SERVER_HOST:8000` |
| **Kafka UI** | `https://SERVER_HOST:8080` |
| **K8s Dashboard** | `https://SERVER_HOST:8443` |

> `TLS_ENABLED=false` → all ports stay plain HTTP. Only do this if you terminate TLS
> for the UI another way (e.g. an external load balancer / ingress in front) - the UI
> will be blank over plain HTTP otherwise (see [Why the UI needs HTTPS](#why-the-ui-needs-https)).

**How it works:** Traefik is installed via Helm with one TLS entrypoint per service. A single default certificate (`df-tls`, set as a `TLSStore` named `default` in the `traefik` namespace) is served on every entrypoint - the routes match on path only (no Host), so a default cert is what gets served. The deploy port-forwards Traefik on the UI/API/Kafka-UI/dashboard ports.

**Provided-cert requirements:**

- `TLS_CERT_FILE` should be the **full chain** (leaf + intermediates). Check with `grep -c 'BEGIN CERTIFICATE' cert.pem` (`1` = leaf only; append the intermediate(s) if clients report an incomplete chain).
- `TLS_KEY_FILE` must be an **unencrypted** PEM key (`head -1 key.pem` must not say `ENCRYPTED`; decrypt with `openssl rsa -in key.pem -out key.dec.pem`).
- The cert must cover `SERVER_HOST`. A wildcard `*.example.com` covers `detectflow.example.com` (one label) but **not** the apex `example.com`.
- Keep cert/key out of git - put them under `certs/` (gitignored).

**Trusting the self-signed CA** (selfsigned mode, to remove the browser warning) - export the CA once and import it into your OS/browser trust store:

```bash
kubectl get secret detectflow-ca-keypair -n cert-manager -o jsonpath='{.data.tls\.crt}' | base64 -d > detectflow-ca.crt
```

**Verify what's served on the wire** (replace host/port):

```bash
echo | openssl s_client -connect SERVER_HOST:4173 -servername SERVER_HOST 2>/dev/null \
  | openssl x509 -noout -issuer -ext subjectAltName
```

**Switching `selfsigned` ↔ `provided`:** just change `TLS_MODE` (+ cert paths) in `.env` and re-run. App pods are not restarted; only the `df-tls` secret swaps. If the browser still shows the old cert, force a reload (this restarts Traefik only, ~5s):

```bash
kubectl rollout restart deployment/traefik -n traefik
```

### Pipeline Runtime Settings (in the DetectFlow UI)

After the stack is deployed and you log in to the DetectFlow UI, review the **Pipeline Runtime** settings and tune them to your system resources **before you create or run a pipeline**.

**Settings to review:**

- Parallelism
- TaskManager CPU (cores)
- TaskManager Memory (MB)
- Window Size (sec)
- Checkpoint Interval (sec)

These settings significantly affect performance and resource usage. Each running pipeline
gets its own JobManager and TaskManager pods inside the minikube node, so the TaskManager
memory must fit next to everything else.

**On the default 8 CPU / 16 GB server** start with Parallelism `1`, TaskManager CPU `1`
and TaskManager Memory `1536` MB - that leaves room for 1-2 running pipelines. A
TaskManager that doesn't fit stays `Pending` (`Insufficient memory`); see
[Minikube out of resources](#minikube-out-of-resources).

The screenshot below shows the settings page with values for a larger server
(TaskManager 8 GB, parallelism 2) - scale them to your resources:

![Pipeline Runtime Settings](./images/pipeline-runtime.png)

### Kafka Broker Access

The deploy script configures Kafka with two listeners:

| Listener | Address | Purpose |
| --- | --- | --- |
| `PLAINTEXT` | `kafka:9092` | In-cluster clients (Backend, Flink, MatchNode) |
| `EXTERNAL` | `SERVER_HOST:9094` | Remote/host access via `kubectl port-forward` |

The `EXTERNAL` listener advertises `SERVER_HOST:9094` (from `.env`) and is port-forwarded on `0.0.0.0`, so clients on other machines can produce/consume. Example (replace `SERVER_HOST` with the server IP):

```bash
# Produce events using kcat
cat <<'JSON' | jq -c . | kcat -P -b SERVER_HOST:9094 -t windows-events
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
kcat -C -b SERVER_HOST:9094 -t windows-events -o beginning
```

> **Note:** Always use `SERVER_HOST:9094` (not `9092`) when connecting from outside the cluster. Port `9092` advertises `kafka:9092` in metadata, which is unresolvable outside the cluster. `SERVER_HOST` must be the server's real IP/hostname - not `localhost` - for remote clients to connect.

Port-forwards are supervised and self-heal automatically. If you ever need to poke them by hand:

```bash
~/.detectflow/df-forward-supervisor.sh status    # what's up / down
~/.detectflow/df-forward-supervisor.sh restart   # restart all forwards
```

### Logstash Ingest

The deploy script runs Logstash with three input listeners, port-forwarded on `PF_ADDRESS` (default `0.0.0.0`). Each input tags events with a `type` and writes them to a per-type Kafka topic `logstash-<type>`:

| Input | Port (`.env`) | Protocol / codec | Kafka output topic |
| --- | --- | --- | --- |
| **syslog** | `10501` (`LOGSTASH_SYSLOG_PORT`) | TCP/UDP, RFC3164 syslog | `logstash-syslog` |
| **json** | `10510` (`LOGSTASH_JSON_PORT`) | TCP, JSON lines | `logstash-json` |
| **unstructured** | `10520` (`LOGSTASH_TEXT_PORT`) | TCP, raw text | `logstash-unstructured` |

To test each input, send one event to its port. Replace `SERVER_HOST` with the server's IP/hostname. Each command opens a TCP connection through bash's built-in `/dev/tcp` and is wrapped in `bash -c` so it runs from any shell:

```bash
# syslog (RFC3164) on 10501
bash -c 'echo "<13>Feb 24 19:02:11 FILESERVER01 sshd[1234]: Accepted password for alice" > /dev/tcp/SERVER_HOST/10501'

# json on 10510
bash -c 'echo "{\"event_id\":4624,\"computer\":\"FILESERVER01\",\"message\":\"logon\"}" > /dev/tcp/SERVER_HOST/10510'

# unstructured text on 10520
bash -c 'echo "raw application log line without structure" > /dev/tcp/SERVER_HOST/10520'
```

Verify the events landed in Kafka (consume the matching topic with kcat):

```bash
kcat -C -b SERVER_HOST:9094 -t logstash-syslog        -o beginning -e
kcat -C -b SERVER_HOST:9094 -t logstash-json          -o beginning -e
kcat -C -b SERVER_HOST:9094 -t logstash-unstructured  -o beginning -e
```

> Logstash creates the `logstash-*` topics on first event (Kafka `auto.create.topics.enable` is on).

### PostgreSQL Access (pgAdmin)

The PostgreSQL forward listens on **`127.0.0.1:5432` on the server only**
(`POSTGRES_PF_ADDRESS=127.0.0.1`, the default), because the database uses the default
demo credentials. From your workstation, connect through an **SSH tunnel** -
[pgAdmin 4](https://www.pgadmin.org/) has one built in, so you don't run `ssh -L`
yourself. In pgAdmin, right-click **Servers → Register → Server…** and fill in:

#### General tab

| Field | Value |
| --- | --- |
| Name | any label, e.g. `DetectFlow (SERVER_HOST)` |

#### Connection tab

| Field | Value |
| --- | --- |
| Host name/address | `127.0.0.1` - as seen **from the server**, where the forward listens (not `SERVER_HOST`) |
| Port | `5432` (`POSTGRES_PORT`) |
| Maintenance database | `detectflow` |
| Username | `detectflow` - there is no `postgres` role; the image creates `detectflow` as the superuser |
| Password | `detectflow` (see `manifests/postgres/deployment.yaml`) |

#### SSH Tunnel tab

| Field | Value |
| --- | --- |
| Use SSH tunneling | Yes |
| Tunnel host | `SERVER_HOST` |
| Tunnel port | `22` |
| Username | your SSH user on the server |
| Authentication | *Password*, or *Identity file* for an SSH private key |

Save and connect - pgAdmin opens the SSH session and forwards to the server's
`127.0.0.1:5432`. Field reference:
[pgAdmin Server Dialog](https://www.pgadmin.org/docs/pgadmin4/latest/server_dialog.html).

> The server's sshd must allow TCP forwarding (`AllowTcpForwarding yes`, the Ubuntu
> default). On the server, `ss -ltn | grep :5432` should show `127.0.0.1:5432`.

**Other clients (psql, DBeaver, scripts…) - manual SSH tunnel:** open the tunnel
yourself, then point the client at your local end of it:

```bash
# keep this running; -N = no remote shell, just the forward
ssh -N -L 15432:127.0.0.1:5432 <ssh-user>@SERVER_HOST
```

Connect to `127.0.0.1:15432` (db / user / password `detectflow`), e.g.
`psql -h 127.0.0.1 -p 15432 -U detectflow detectflow`. Local port `15432` avoids a
clash with a PostgreSQL already running on your machine; any free port works.
Windows 10+ ships the same `ssh` client (PowerShell); in PuTTY use
*Connection → SSH → Tunnels* (Source port `15432`, Destination `127.0.0.1:5432`).

**Exposing PostgreSQL to the network (not recommended):** set `POSTGRES_PF_ADDRESS="0.0.0.0"`
in `.env` and re-run the deploy. pgAdmin then connects directly (Host = `SERVER_HOST`,
SSH Tunnel off). The deploy prints a warning: anyone who can reach the server can then
log in with the default demo password, so firewall port 5432 if you do this.

### Default Login Credentials

| Field | Value |
| --- | --- |
| **Email** | `admin@soc.local` |
| **Password** | `admin` |

### Useful Commands After Deploy

> Examples use the default namespace `flink`; substitute your `NAMESPACE` from `.env` if you changed it.

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

# View Logstash logs
kubectl logs -n flink -l app=logstash -f

# Port-forwards (self-healing, survive logout + reboot) - managed by the supervisor:
~/.detectflow/df-forward-supervisor.sh status
~/.detectflow/df-forward-supervisor.sh restart   # restart all
~/.detectflow/df-forward-supervisor.sh stop      # stop all
# Log: ~/.detectflow/forwards.log   Set: ~/.detectflow/forwards.conf (regenerated by the deploy)

# Stop the cluster (preserves data)
minikube stop

# Full reset and redeploy
minikube delete && ./deploy-detectflow-minikube.sh
```

### Scaling cluster CPU / RAM (after resizing the VM)

With the **docker driver** the minikube node is a Docker **container**, and its CPU/RAM
limits are the container's cgroup limits set when the container was **created**. Because
of that:

- **`minikube stop` + `start` does NOT resize** - `start` just restarts the *existing*
  container, so the creation-time limits stay.
- **`minikube config set cpus/memory`** only applies on a `minikube delete` + fresh
  `start` (it says so) - and `delete` destroys your data.
- The **data-safe** way is to change the container's limits in place with
  [`docker update`](https://docs.docker.com/reference/cli/docker/container/update/),
  then start.

**CPU:** by default minikube does **not** set a hard CPU limit (the container can already
use every host core), so if you added vCPUs to the VM there is usually **nothing to do** -
the cluster picks them up automatically. Only act if you previously pinned a CPU limit.

**RAM:** the container is capped at its creation-time `--memory`, so to let it use more
you must raise that limit:

Memory is set in **bytes**: `bytes = <GB> * 1073741824` (e.g. 26 GB = `26 * 1073741824`
= `27917287424` for a 32 GB VM; 56 GB = `60129542144` for a 64 GB VM - keep ~4-8 GB for
the host). `--memory-swap=-1` (unlimited swap) is
**required**, otherwise docker errors with *"Memory limit should be smaller than already
set memoryswap limit"*.

```bash
minikube stop

# raise RAM only (leave CPU unlimited - it already uses all host cores):
docker update --memory=27917287424 --memory-swap=-1 minikube

# OR raise RAM and pin a hard CPU cap at the same time:
docker update --memory=27917287424 --memory-swap=-1 --cpus=14 minikube

minikube start                       # docker start reuses the container; the new limits stick
                                     # running as root? add --force
```

Verify the **actual** limits (not `kubectl get node …capacity` / `nproc` / `free` - those
report the **host**, not the container's cgroup):

```bash
docker inspect minikube --format='CPU: {{.HostConfig.NanoCpus}} | RAM: {{.HostConfig.Memory}}'
# NanoCpus / 1000000000 = CPUs (0 = unlimited) ; RAM in bytes
kubectl -n flink get pvc             # all Bound = data intact
kubectl -n flink get pods            # all Running (a few restarts from the cluster restart are normal)
```

Your data survives - the container (and its data volume) is reused, not recreated. Only
`minikube delete` destroys it.

A bigger node only helps once the components may use it: raise the matching limits in
`.env` (`PG_MEM_LIMIT` / `PG_SHARED_BUFFERS`, `KAFKA_MEM_LIMIT` / `KAFKA_HEAP_OPTS`,
`LOGSTASH_MEM_LIMIT` / `LOGSTASH_JAVA_OPTS`, ...) and set `MINIKUBE_MEMORY` to the new size,
then re-run the deploy - it re-applies the manifests without touching the data.

**Disk (`--disk-size`) is different - it can NOT be resized this way.** Unlike CPU/RAM,
the disk is the size of the minikube data volume, fixed when the cluster is **created**.
`docker update`, `minikube stop/start`, and `minikube config set disk-size` do **not**
change it (config-set only applies on a fresh create). The only way to change disk size
is to **recreate** the cluster with a new `--disk-size`, which **destroys all data**
(PVCs included):

```bash
# 1. BACK UP first - this path is destructive
kubectl -n flink exec deploy/postgres -- \
  pg_dump -U detectflow -d detectflow --clean --if-exists > df-db-$(date +%F).sql
# copy the dump off the server

# 2. recreate with the new disk size (DESTROYS the cluster + all PVCs)
minikube delete
# set MINIKUBE_DISK in .env, then:
./deploy-detectflow-minikube.sh            # or: minikube start --disk-size=<size> ...

# 3. restore the database
kubectl -n flink exec -i deploy/postgres -- \
  psql -U detectflow -d detectflow < df-db-$(date +%F).sql
```

There's no data-safe in-place disk grow on the docker driver (a manual docker-volume
resize is fragile and unsupported - don't). Size the disk generously at first deploy to
avoid this. Docs:
[minikube start / `--disk-size`](https://minikube.sigs.k8s.io/docs/commands/start/) ·
[config](https://minikube.sigs.k8s.io/docs/commands/config/) ·
[persistent volumes](https://minikube.sigs.k8s.io/docs/handbook/persistent_volumes/).

---

## 5. Troubleshooting

### Pods stuck in Pending/CrashLoopBackOff

```bash
kubectl describe pod <pod-name> -n flink
kubectl logs <pod-name> -n flink
```

`Insufficient memory` / `Insufficient cpu` in the events means the node is full - see
[Minikube out of resources](#minikube-out-of-resources).

### Backend can't connect to PostgreSQL

- Ensure PostgreSQL pod is running: `kubectl get pods -n flink -l app=postgres`
- Check DATABASE_URL format: `postgresql+asyncpg://detectflow:detectflow@postgres:5432/detectflow`

### Backend can't connect to Kafka

- Ensure Kafka pod is running: `kubectl get pods -n flink -l app=kafka`
- Wait for Kafka to fully start (can take 30-60 seconds)
- Check bootstrap server: `kafka:9092`

### Image pull errors

- Check the image tags in `.env` exist on Docker Hub (`socprime/detectflow-oss`), e.g. `docker pull socprime/detectflow-oss:admin-panel-be-oss.v2.3.2`
- Look at the pod events: `kubectl describe pod <pod> -n flink`
- `toomanyrequests` = Docker Hub's anonymous pull rate limit; wait and re-run the deploy

### Minikube: "docker driver should not be used with root privileges" (DRV_AS_ROOT)

You ran the script as root. Either run as a normal user in the `docker` group:

```bash
sudo usermod -aG docker $USER   # then log out/in (or: newgrp docker)
./deploy-detectflow-minikube.sh # no sudo
```

…or force it (root + docker driver):

```bash
MINIKUBE_FORCE=true ./deploy-detectflow-minikube.sh
```

### Cluster runs the docker container runtime

Step 2 warns `This cluster runs the 'docker' container runtime, not 'containerd'`. The
runtime (how kubelet runs pods inside the minikube node - not the same as
`MINIKUBE_DRIVER`) is fixed when the cluster is created. Clusters created by minikube
older than v1.39.0 without an explicit runtime got `docker`; re-running the deploy does
not change it. Check it with:

```bash
kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}'
```

On the docker runtime kubelet cannot rotate container logs (`docker does not support
reopening container log files`). It logs that error every few seconds into the node's
in-memory journal (`/run/log/journal`), which grows by roughly 100 MB a day and is never
freed, so a cluster running for a week can run out of memory.

To move to containerd, back up your data first - **`minikube delete` removes all
cluster data** (PostgreSQL, Kafka, Flink volumes) - then:

```bash
minikube delete
./deploy-detectflow-minikube.sh
```

Until you can recreate the cluster, cap the node's journal to stop the growth:

```bash
docker exec minikube sh -c 'mkdir -p /etc/systemd/journald.conf.d && \
  printf "[Journal]\nRuntimeMaxUse=50M\n" > /etc/systemd/journald.conf.d/size.conf && \
  systemctl restart systemd-journald && journalctl --vacuum-size=50M'
```

### Cannot connect to the Docker daemon

You were just added to the `docker` group but the shell session predates it. Log out/in or run `newgrp docker`, then re-run. Verify with `docker info`.

### Minikube out of resources

Symptoms: pods stuck in `Pending` (`Insufficient memory` / `Insufficient cpu` in
`kubectl describe pod`), `OOMKilled` restarts, or a node that hangs with a very high load
average.

- **The defaults need ~8 CPU / 16 GB.** On a smaller host lower `MINIKUBE_CPUS` /
  `MINIKUBE_MEMORY` *and* the component limits (`PG_*`, `KAFKA_*`, `LOGSTASH_*`) in `.env`.
  Keep at least ~4 GB RAM for the host and the sum of the component limits well below
  `MINIKUBE_MEMORY`; add swap.
- **Each Flink pipeline needs ~2.5 GB** (JobManager + TaskManager). On 16 GB plan for 1-2
  running pipelines; for more, use a bigger server - see
  [Scaling cluster CPU / RAM](#scaling-cluster-cpu--ram-after-resizing-the-vm).
- The minikube CPU/RAM limits are fixed when the cluster is created. To shrink them on an
  existing cluster without losing data use `docker update` as in the scaling section; or
  recreate it (**destroys all data**):

```bash
minikube delete
./deploy-detectflow-minikube.sh
```

### Flink Operator not starting

```bash
kubectl get pods -n flink-operator
kubectl logs -n flink-operator -l app.kubernetes.io/name=flink-kubernetes-operator
```

### Flink jobs fail on CPUs without AVX2 (Polars)

The flink-sigma-detector image ships two Polars native builds:
`polars-runtime-32` (AVX2, fast) and `polars-runtime-compat` (SSE4.2). Polars
prefers compat when `POLARS_FORCE_PKG` is unset, so the image sets it to `32`. On
CPUs that lack AVX2 - older hardware such as Sandy Bridge, or a VM whose CPU model
is a generic `x86-64-v2` / `kvm64` - the AVX2 build cannot run, so switch to compat.

Check the CPU the cluster sees (on the server):

```bash
grep -q avx2 /proc/cpuinfo && echo "AVX2 present" || echo "no AVX2 - use compat"
```

If AVX2 is missing, uncomment the override in `manifests/flink/configmap.yaml`
(ConfigMap `flink-config`):

```yaml
  POLARS_FORCE_PKG: "compat"
```

then re-run the deploy (or apply just the ConfigMap:
`NAMESPACE=flink envsubst < manifests/flink/configmap.yaml | kubectl apply -f -`) and restart the affected pipelines in the UI, so their new
JobManager/TaskManager pods pick up the setting. On a VM, exposing the host CPU
model (e.g. `host-passthrough` in KVM/Proxmox) is an alternative that keeps the
faster AVX2 build.

### cert-manager issues

```bash
kubectl get pods -n cert-manager
# Wait for all 3 pods to be Running before installing Flink operator
```

### Why the UI needs HTTPS

The DetectFlow UI ships browser-security hardening: its image sends a
`Content-Security-Policy: … upgrade-insecure-requests` header (plus HSTS). This is a
deliberate security control - it tells browsers to only ever load the app's resources
over a secure connection, which prevents a page loaded over HTTPS from being downgraded
or having assets injected over plain HTTP.

The practical consequence: the UI is **meant to be served over HTTPS**. That is why
`TLS_ENABLED` defaults to `true` - the deploy puts Traefik in front and the UI works out
of the box. If you force plain HTTP (`TLS_ENABLED=false`) without terminating TLS
elsewhere, the browser upgrades the app's asset requests to `https` on a port that only
speaks HTTP, so they fail and the page renders blank - not a deploy bug, just the UI's
security policy doing its job without TLS. With `selfsigned` the browser warns until you
import the CA, but the page loads; use `TLS_MODE=provided` with a real cert for a clean,
warning-free experience.

### HTTPS / TLS (only when `TLS_ENABLED=true`)

**Browser shows "TRAEFIK DEFAULT CERT" instead of your cert.** Traefik is serving its built-in default. Check the default TLSStore points at `df-tls` and the secret holds the right cert, then reload Traefik:

```bash
kubectl get tlsstore default -n traefik -o jsonpath='{.spec.defaultCertificate.secretName}'; echo   # df-tls
kubectl get secret df-tls -n traefik -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -noout -issuer -ext subjectAltName
kubectl rollout restart deployment/traefik -n traefik
```

**`provided` cert keeps reverting to the self-signed one.** A leftover cert-manager `Certificate` is still reconciling the secret. The deploy deletes it automatically; to do it by hand:

```bash
kubectl delete certificate detectflow-tls -n traefik --ignore-not-found
kubectl delete certificate detectflow-tls -n flink  --ignore-not-found
```

**Connection refused on the HTTPS port.** The Traefik port-forward isn't running. It is supervised and should self-heal within ~2s; check and restart it:

```bash
~/.detectflow/df-forward-supervisor.sh status
~/.detectflow/df-forward-supervisor.sh restart
```

**Cert name-mismatch warning.** The cert's SAN doesn't cover the name you're using. Confirm `SERVER_HOST` matches the cert (`openssl … -ext subjectAltName`); for a wildcard use a sub-host, not the apex.

**`TLSStore … spec.defaultCertificate.secretName: Required value` during install.** `TLS_SECRET_NAME` wasn't rendered into the Traefik values - make sure you're running the current script (it `export`s the var for `envsubst`).
