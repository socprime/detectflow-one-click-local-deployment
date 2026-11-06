#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# DetectFlow port-forward supervisor (no root required).
#
# Keeps the deploy's `kubectl port-forward`s alive. Each forward runs in its own
# respawn loop inside a detached session, so it:
#   - restarts within ~2s when the forward drops (pod restart / API blip / reset),
#   - survives the shell that launched it (setsid → reparents to init on logout),
#   - is restarted on boot by a `@reboot` crontab entry (installed by the deploy).
#
# Config: $DF_HOME/forwards.conf, one forward per line, '#' comments allowed:
#     name|namespace|service|ports|address
#   where `ports` is a space-separated list of hostPort:targetPort pairs, e.g.
#     logstash|flink|logstash|10501:10501 10510:10510 10520:10520|0.0.0.0
#
# Usage: df-forward-supervisor.sh {start|stop|restart|status}
# ─────────────────────────────────────────────────────────────────────────────
set -u

DF_HOME="${DF_HOME:-$HOME/.detectflow}"
CONF="${DF_FORWARDS_CONF:-$DF_HOME/forwards.conf}"
PIDDIR="$DF_HOME/pids"
LOG="${DF_FORWARDS_LOG:-$DF_HOME/forwards.log}"
KUBECTL="${KUBECTL:-kubectl}"
# Default the kubeconfig so cron/@reboot (minimal env) still finds the cluster.
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

mkdir -p "$PIDDIR"

_log() { echo "[$(date '+%F %T')] $*" >>"$LOG" 2>/dev/null || true; }

# True if something is listening on ${2:-127.0.0.1}:$1 (uses bash /dev/tcp, no deps).
_port_listen() {
    (exec 3<>"/dev/tcp/${2:-127.0.0.1}/$1") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    return 1
}

# Iterate config lines → callback "$name" "$ns" "$svc" "$ports" "$addr"
_each_forward() {
    local cb="$1" name ns svc ports addr
    [ -r "$CONF" ] || { echo "No config: $CONF" >&2; return 1; }
    while IFS='|' read -r name ns svc ports addr; do
        case "$name" in ''|\#*) continue ;; esac
        "$cb" "$name" "$ns" "$svc" "$ports" "$addr"
    done < "$CONF"
}

_running() {  # $1=name → 0 if its loop is alive
    local pidfile="$PIDDIR/$1.pid" pid
    [ -f "$pidfile" ] || return 1
    pid="$(cat "$pidfile" 2>/dev/null)"
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

start_one() {
    local name="$1" ns="$2" svc="$3" ports="$4" addr="$5"
    local pidfile="$PIDDIR/$name.pid"
    if _running "$name"; then return 0; fi
    # Detached respawn loop. `ports` is passed unquoted so it word-splits into
    # separate kubectl args; the inner shell keeps them as "$@".
    # shellcheck disable=SC2086
    setsid bash -c '
        LOG="$1"; KUBECTL="$2"; name="$3"; ns="$4"; svc="$5"; addr="$6"; shift 6
        backoff=2
        while true; do
            echo "[$(date "+%F %T")] ($name) up: port-forward -n $ns svc/$svc $*" >>"$LOG" 2>&1
            start=$(date +%s)
            "$KUBECTL" port-forward --address "$addr" -n "$ns" "svc/$svc" "$@" >>"$LOG" 2>&1
            ran=$(( $(date +%s) - start ))
            # Reset backoff after a healthy run; grow it (cap 30s) on fast failures
            # (e.g. cluster/kubectl unreachable) so a down cluster does not spam the log.
            if [ "$ran" -ge 10 ]; then backoff=2; else backoff=$(( backoff * 2 )); [ "$backoff" -gt 30 ] && backoff=30; fi
            echo "[$(date "+%F %T")] ($name) dropped after ${ran}s; restarting in ${backoff}s" >>"$LOG" 2>&1
            sleep "$backoff"
        done
    ' _ "$LOG" "$KUBECTL" "$name" "$ns" "$svc" "$addr" $ports >/dev/null 2>&1 </dev/null &
    echo $! > "$pidfile"
    _log "($name) supervised (pid $(cat "$pidfile"))"
}

stop_one() {
    local name="$1" pidfile="$PIDDIR/$name.pid" pid
    [ -f "$pidfile" ] || return 0
    pid="$(cat "$pidfile" 2>/dev/null)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        # Negative pid = kill the whole session/process group (loop + kubectl child).
        kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
    fi
    rm -f "$pidfile"
    _log "($name) stopped"
}

status_one() {
    local name="$1" ns="$2" svc="$3" ports="$4" addr="$5" st
    local first="${ports%% *}"   # first hostPort:targetPort pair
    local hp="${first%%:*}"      # its hostPort, to probe
    local probe="127.0.0.1" label="$svc ($ports)"
    # A forward bound to a specific address (e.g. PostgreSQL on 127.0.0.1) is probed
    # there and shown with @address so "server-only" is visible at a glance.
    case "$addr" in ""|0.0.0.0) ;; *) probe="$addr"; label="$label @$addr" ;; esac
    if ! _running "$name"; then
        st="DOWN (not supervised)"
    elif _port_listen "$hp" "$probe"; then
        st="UP"
    else
        st="WAITING (port ${hp} not listening - cluster/kubectl down?)"
    fi
    printf "  %-10s %-34s %s\n" "$name" "$label" "$st"
}

cmd_start()  { _each_forward start_one; }
# Stop EVERY managed forward (all pidfiles), not just the current config - so a
# TLS on/off switch cleanly tears down forwards that are no longer in the config.
cmd_stop() {
    local pidfile name
    for pidfile in "$PIDDIR"/*.pid; do
        [ -e "$pidfile" ] || continue
        name="$(basename "$pidfile" .pid)"
        stop_one "$name"
    done
}
cmd_status() {
    echo "df-forward supervisor (DF_HOME=$DF_HOME):"
    _each_forward status_one || return 1
    if crontab -l 2>/dev/null | grep -qE 'df-boot\.sh|df-forward-supervisor\.sh'; then
        echo "  boot hook: @reboot crontab present"
    else
        echo "  boot hook: MISSING (reboot will not auto-restore the stack)"
    fi
}

case "${1:-status}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; cmd_start ;;
    status)  cmd_status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" >&2; exit 2 ;;
esac
