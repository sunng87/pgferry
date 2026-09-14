#!/usr/bin/env bash
# pgferry M0 end-to-end acceptance test.
#
# Requires in PATH (or override via env): initdb, pg_ctl, postgres, psql,
# pgbench, cargo. A nix dev shell with everything is available via:
#
#     nix develop -c scripts/e2e.sh
#
# The script is self-contained and CI-safe:
# - builds the pgferry binary
# - starts a throwaway postgres cluster (trust auth, ephemeral port, tmpdir)
# - starts pgferry against it
# - runs the M0 acceptance checks (fail-fast with context)
# - always tears both down
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d /tmp/pgferry-e2e.XXXXXX)"
PGDATA="$WORK/pgdata"
PGFERRY_LOG="$WORK/pgferry.log"
PG_LOG="$WORK/postgres.log"

PSQL="${PSQL_BIN:-psql}"
PGBENCH="${PGBENCH_BIN:-pgbench}"
INITDB="${INITDB_BIN:-initdb}"
PG_CTL="${PG_CTL_BIN:-pg_ctl}"

PGFERRY_PID=""
PG_STARTED=0

cleanup() {
    local rc=$?
    if [ -n "$PGFERRY_PID" ]; then
        kill "$PGFERRY_PID" 2>/dev/null || true
        wait "$PGFERRY_PID" 2>/dev/null || true
    fi
    if [ "$PG_STARTED" = "1" ]; then
        "$PG_CTL" -D "$PGDATA" stop -m immediate -l "$PG_LOG" >/dev/null 2>&1 || true
    fi
    if [ "${PG2_STARTED:-0}" = "1" ]; then
        "$PG_CTL" -D "$WORK/pgdata2" stop -m immediate >/dev/null 2>&1 || true
    fi
    if [ "${SH1_STARTED:-0}" = "1" ]; then
        "$PG_CTL" -D "$WORK/shard1" stop -m immediate >/dev/null 2>&1 || true
    fi
    if [ "${SH2_STARTED:-0}" = "1" ]; then
        "$PG_CTL" -D "$WORK/shard2" stop -m immediate >/dev/null 2>&1 || true
    fi
    rm -rf "$WORK"
    exit $rc
}
trap cleanup EXIT INT TERM

say()  { printf '\n=== %s ===\n' "$*"; }
pass() { printf 'PASS: %s\n' "$*"; }
die()  { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# Pick a probably-free port (nothing listening). TOCTOU is acceptable here;
# bind failures are caught by the startup retries below.
pick_port() {
    local port tries=0
    while [ "$tries" -lt 50 ]; do
        port=$(( (RANDOM % 20000) + 20000 ))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "$port"; return 0
        fi
        tries=$((tries + 1))
    done
    die "could not find a free port"
}

wait_psql() { # wait_psql <connstr> <label> [log]
    local i
    for i in $(seq 1 100); do
        if "$PSQL" "$1" -Atqc "select 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    die "$2 did not become ready; logs: $(tail -5 "${3:-$PGFERRY_LOG}" 2>/dev/null)"
}

say "build pgferry"
(cd "$ROOT" && cargo build -p pgferry-bin)
PGFERRY_BIN="$ROOT/target/debug/pgferry"
[ -x "$PGFERRY_BIN" ] || die "pgferry binary not found at $PGFERRY_BIN"

say "start throwaway postgres"
PG_PORT="$(pick_port)"
"$INITDB" -D "$PGDATA" -U postgres --no-locale -A trust >/dev/null
mkdir -p "$WORK/run"

# M4: TLS test material — a CA plus postgres/proxy leaf certs, and postgres
# SSL enabled (skipped when openssl is unavailable)
TLS_READY=0
if command -v openssl >/dev/null 2>&1; then
    openssl req -new -x509 -days 2 -nodes -subj "/CN=pgferry-test-ca" \
        -keyout "$WORK/ca.key" -out "$WORK/ca.crt" >/dev/null 2>&1 || true
    for name in pg proxy; do
        openssl req -new -nodes -subj "/CN=localhost" \
            -keyout "$WORK/$name.key" -out "$WORK/$name.csr" >/dev/null 2>&1 || true
        printf 'subjectAltName=DNS:localhost,IP:127.0.0.1' > "$WORK/$name.ext"
        openssl x509 -req -in "$WORK/$name.csr" -CA "$WORK/ca.crt" -CAkey "$WORK/ca.key" \
            -out "$WORK/$name.crt" -days 2 -extfile "$WORK/$name.ext" >/dev/null 2>&1 || true
        chmod 600 "$WORK/$name.key" 2>/dev/null || true
    done
    if [ -s "$WORK/ca.crt" ] && [ -s "$WORK/pg.crt" ] && [ -s "$WORK/proxy.crt" ]; then
        cat >> "$PGDATA/postgresql.conf" <<E_O_F
ssl = on
ssl_cert_file = '$WORK/pg.crt'
ssl_key_file = '$WORK/pg.key'
E_O_F
        TLS_READY=1
    fi
fi
"$PG_CTL" -D "$PGDATA" -l "$PG_LOG" \
    -o "-p $PG_PORT -k $WORK/run" start >/dev/null
PG_STARTED=1
PG_UP="host=127.0.0.1 port=$PG_PORT user=postgres dbname=postgres"
wait_psql "$PG_UP" "postgres"

say "start pgferry"
PROXY_PORT="$(pick_port)"
RUST_LOG=pgferry=info "$PGFERRY_BIN" \
    --listen "127.0.0.1:$PROXY_PORT" \
    --upstream "host=127.0.0.1 port=$PG_PORT" >"$PGFERRY_LOG" 2>&1 &
PGFERRY_PID=$!
PROXY_UP="host=127.0.0.1 port=$PROXY_PORT user=postgres dbname=postgres"
wait_psql "$PROXY_UP" "pgferry"

say "1. simple query passthrough"
VERSION="$("$PSQL" "$PROXY_UP" -Atqc "select current_setting('server_version')")"
[ -n "$VERSION" ] || die "no server_version through proxy"
pass "server_version=$VERSION"

say "2. session pooling: reuse upstream between sessions"
PID_A="$("$PSQL" "$PROXY_UP" -Atqc "select pg_backend_pid()")"
sleep 1   # let the checkin (sanitize + park) land
PID_B="$("$PSQL" "$PROXY_UP" -Atqc "select pg_backend_pid()")"
[ "$PID_A" = "$PID_B" ] || die "session did not reuse the pooled upstream: $PID_A vs $PID_B"
pass "backend pid reused: $PID_A"

say "3. COPY out through proxy (100k rows)"
"$PSQL" "$PROXY_UP" -qc \
    "\copy (select i::bigint, (i::bigint)*(i::bigint) from generate_series(1,100000) i) to '$WORK/copy_out.csv' csv" >/dev/null
LINES="$(wc -l <"$WORK/copy_out.csv")"
[ "$LINES" = "100000" ] || die "COPY out: expected 100000 rows, got $LINES"
pass "copied out $LINES rows"

say "4. COPY in through proxy"
"$PSQL" "$PROXY_UP" -qc "drop table if exists e2e_copy; create table e2e_copy(a bigint, b bigint)" >/dev/null
"$PSQL" "$PROXY_UP" -qc "\copy e2e_copy from '$WORK/copy_out.csv' csv" >/dev/null
COUNT="$("$PSQL" "$PROXY_UP" -Atqc "select count(*), sum(b) from e2e_copy")"
[ "$COUNT" = "100000|333338333350000" ] || die "COPY in roundtrip mismatch: $COUNT"
pass "copied in and verified: $COUNT"

say "5. LISTEN/NOTIFY passthrough"
NOTIFY_OUT="$("$PSQL" "$PROXY_UP" -Atc \
    "listen e2e; select pg_sleep(1); notify e2e, 'payload'; select pg_sleep(1);")"
echo "$NOTIFY_OUT" | grep -q 'Asynchronous notification "e2e" with payload "payload"' \
    || die "notification not delivered through proxy; got: $NOTIFY_OUT"
pass "notification delivered"

say "6. error recovery on the same session"
ERRREC_OUT="$("$PSQL" "$PROXY_UP" -c "select 1/0" -c "select 42" 2>&1 || true)"
echo "$ERRREC_OUT" | grep -q "division by zero" || die "expected first command to error; got: $ERRREC_OUT"
echo "$ERRREC_OUT" | grep -q "42" || die "second command did not run after error; got: $ERRREC_OUT"
pass "error then success on one session"

say "6b. pooling: abandoned transaction is rolled back + discarded"
# a session is hard-killed mid-transaction (SIGTERM: no Terminate sent) --
# checkin must ROLLBACK + DISCARD ALL before the backend is reused
PID_LEAK="$("$PSQL" "$PROXY_UP" -Atqc "begin; create temp table e2e_leak(a int); select pg_backend_pid();")"
sleep 1
timeout 2 "$PSQL" "$PROXY_UP" -qc "begin; select pg_sleep(30);" >/dev/null 2>&1 || true
sleep 1
PID_NEXT="$("$PSQL" "$PROXY_UP" -Atqc "select pg_backend_pid()")"
[ "$PID_LEAK" = "$PID_NEXT" ] || die "expected the pooled backend $PID_LEAK to be reused, got $PID_NEXT"
LEAK_COUNT="$("$PSQL" "$PROXY_UP" -Atqc "select count(*) from pg_tables where tablename='e2e_leak'")"
[ "$LEAK_COUNT" = "0" ] || die "temp table leaked across sessions; sanitize failed (count=$LEAK_COUNT)"
pass "reused $PID_LEAK; abandoned tx rolled back, temp table gone"

say "6c. pooling: startup parameter replay (application_name)"
PGAPPNAME=aaa "$PSQL" "$PROXY_UP" -Atqc "select 1" >/dev/null
sleep 1
APP_B="$(PGAPPNAME=bbb "$PSQL" "$PROXY_UP" -Atqc "select current_setting('application_name'), pg_backend_pid();")"
APP_B_NAME="${APP_B%|*}"
[ "$APP_B_NAME" = "bbb" ] || die "application_name not replayed on reuse: got '$APP_B'"
pass "application_name replayed: $APP_B_NAME"

say "7. protocol cancel (SIGINT during pg_sleep)"
CANCEL_START=$SECONDS
CANCEL_OUT="$(timeout --signal=INT 2 "$PSQL" "$PROXY_UP" -c "select pg_sleep(30);" 2>&1 || true)"
CANCEL_ELAPSED=$((SECONDS - CANCEL_START))
echo "$CANCEL_OUT" | grep -q "canceling statement due to user request" \
    || die "query was not cancelled ($CANCEL_ELAPSED s); got: $CANCEL_OUT"
[ "$CANCEL_ELAPSED" -lt 10 ] || die "cancel took ${CANCEL_ELAPSED}s; cancel request was not forwarded"
pass "cancelled after ${CANCEL_ELAPSED}s"

say "8. pgbench: init (DDL) + all protocol modes"
"$PGBENCH" -i -q -s 1 "$PROXY_UP" >/dev/null 2>&1 || die "pgbench init failed"
pass "pgbench initialized"
for MODE in simple extended prepared; do
    "$PGBENCH" -c 4 -j 2 -T 5 -M "$MODE" "$PROXY_UP" >"$WORK/pgbench-$MODE.out" 2>&1 \
        || die "pgbench -M $MODE failed: $(tail -3 "$WORK/pgbench-$MODE.out")"
    TPS="$(sed -n 's/^tps = \(.*\) (.*$/\1/p' "$WORK/pgbench-$MODE.out" | head -1)"
    pass "pgbench -M $MODE ok (tps=$TPS)"
done

say "9. pool cap: max_size=2 limits concurrent upstreams"
PROXY2_PORT="$(pick_port)"
cat >"$WORK/capped.toml" <<E_O_F
listen = "127.0.0.1:$PROXY2_PORT"

[pool]
max_size = 2

[route]
name = "capped"
upstream = "host=127.0.0.1 port=$PG_PORT"
E_O_F
RUST_LOG=pgferry=info "$PGFERRY_BIN" --config "$WORK/capped.toml" >"$WORK/pgferry2.log" 2>&1 &
PGFERRY2_PID=$!
PROXY2_UP="host=127.0.0.1 port=$PROXY2_PORT user=postgres dbname=postgres"
wait_psql "$PROXY2_UP" "pgferry (capped)"

CAP_PIDS=""
for i in 1 2 3 4; do
    "$PSQL" "$PROXY2_UP" -Atqc "select pg_sleep(4), pg_backend_pid();" \
        >"$WORK/cap-$i.out" 2>&1 &
    CAP_PIDS="$CAP_PIDS $!"
done
# wait only for the clients (a bare `wait` would block on the proxy too)
# shellcheck disable=SC2086
wait $CAP_PIDS
for i in 1 2 3 4; do
    [ -s "$WORK/cap-$i.out" ] || die "capped-pool client $i got no result: $(cat "$WORK/cap-$i.out")"
done
DISTINCT_PIDS="$(cat "$WORK"/cap-*.out | awk -F'|' '{print $2}' | sort -u | wc -l)"
[ "$DISTINCT_PIDS" = "2" ] || die "max_size=2 should yield exactly 2 distinct backends, got $DISTINCT_PIDS: $(cat "$WORK"/cap-*.out)"
kill "$PGFERRY2_PID" 2>/dev/null || true
wait "$PGFERRY2_PID" 2>/dev/null || true
pass "4 concurrent clients served by $DISTINCT_PIDS upstream backends"

say "10. interceptor gateway (examples/gateway)"
cargo build -p pgferry --example gateway >/dev/null
GATEWAY_BIN="$ROOT/target/debug/examples/gateway"
[ -x "$GATEWAY_BIN" ] || die "gateway example binary not found"
GW_PORT="$(pick_port)"
RUST_LOG=gateway=info,pgferry=info "$GATEWAY_BIN" \
    --listen "127.0.0.1:$GW_PORT" \
    --upstream "host=127.0.0.1 port=$PG_PORT" \
    --deny '^drop table' \
    --rewrite 'e2e-rewrite-me' 'rewritten-by-gateway' \
    --mask-col secret >"$WORK/gateway.log" 2>&1 &
GW_PID=$!
GW_UP="host=127.0.0.1 port=$GW_PORT user=postgres dbname=postgres"
wait_psql "$GW_UP" "gateway example"

# deny: SQLSTATE 42501 surfaces to the client
DENY_OUT="$("$PSQL" "$GW_UP" -v VERBOSITY=verbose -c "drop table e2e_nothing" 2>&1 || true)"
echo "$DENY_OUT" | grep -q "42501" || die "deny did not return 42501: $DENY_OUT"
pass "deny returns ERROR 42501"

# the session stays usable after a denial (error recovery)
STILL="$("$PSQL" "$GW_UP" -c "drop table e2e_nothing" -c "select 41 + 1" 2>&1 || true)"
echo "$STILL" | grep -q "42" || die "session broken after denial: $STILL"
pass "session usable after denial"

# rewrite: the query result reflects the rewritten SQL
REWRITTEN="$("$PSQL" "$GW_UP" -Atqc "select 'e2e-rewrite-me'")"
[ "$REWRITTEN" = "rewritten-by-gateway" ] || die "rewrite not applied: $REWRITTEN"
pass "rewrite applied"

# row masking by column name
MASKED="$("$PSQL" "$GW_UP" -Atqc "select 'classified' as secret")"
[ "$MASKED" = "***" ] || die "row not masked: $MASKED"
pass "row masked by column name"

kill "$GW_PID" 2>/dev/null || true
wait "$GW_PID" 2>/dev/null || true

say "11. transaction pooling: sessions share backends mid-transaction"
TX_PORT="$(pick_port)"
cat >"$WORK/tx.toml" <<E_O_F
listen = "127.0.0.1:$TX_PORT"

[pool]
mode = "transaction"
max_size = 2

[route]
name = "tx"
upstream = "host=127.0.0.1 port=$PG_PORT"
E_O_F
RUST_LOG=pgferry=info "$PGFERRY_BIN" --config "$WORK/tx.toml" >"$WORK/pgferry-tx.log" 2>&1 &
TX_PID=$!
TX_UP="host=127.0.0.1 port=$TX_PORT user=postgres dbname=postgres"
wait_psql "$TX_UP" "pgferry (tx mode)"

# 11a. explicit transactions on one session stay consistent across attach/detach
TX_SEQ="$("$PSQL" "$TX_UP" -Atqc "begin; select 10; commit; select 20; begin; select 30; commit; select 40;")"
EXPECTED="$(printf '10\n20\n30\n40')"
[ "$TX_SEQ" = "$EXPECTED" ] || die "tx sequence mismatch: $TX_SEQ"
pass "transaction sequence on one session"

# 11b. two sessions holding transactions + a third queued; <=2 backends
TX_PIDS=""
"$PSQL" "$TX_UP" -Atqc "begin; select pg_backend_pid(); select pg_sleep(2); commit;" \
    >"$WORK/tx-a.out" 2>&1 &
TX_PIDS="$TX_PIDS $!"
"$PSQL" "$TX_UP" -Atqc "begin; select pg_backend_pid(); select pg_sleep(2); commit;" \
    >"$WORK/tx-b.out" 2>&1 &
TX_PIDS="$TX_PIDS $!"
sleep 0.5
"$PSQL" "$TX_UP" -Atqc "select pg_backend_pid();" >"$WORK/tx-c.out" 2>&1 &
TX_PIDS="$TX_PIDS $!"
# shellcheck disable=SC2086
wait $TX_PIDS
for f in tx-a tx-b tx-c; do
    [ -s "$WORK/$f.out" ] || die "tx client $f got no output: $(cat "$WORK/$f.out")"
done
TX_PIDS_USED="$(grep -hE '^[0-9]+$' "$WORK"/tx-*.out | sort -u | wc -l)"
[ "$TX_PIDS_USED" -le 2 ] || die "tx pooling exceeded 2 backends: $TX_PIDS_USED"
pass "3 overlapping-transaction sessions served by $TX_PIDS_USED backends"

# 11c. protocol-level prepared statements in transaction mode:
# pgbench -M prepared prepares named statements once per session, then
# executes them per transaction — each transaction re-attaches, so the
# proxy must replay the statement registry. TPS must be nonzero.
"$PGBENCH" -c 4 -j 2 -T 5 -M prepared "$TX_UP" >"$WORK/pgbench-tx-prepared.out" 2>&1 \
    || die "pgbench -M prepared in tx mode failed: $(tail -3 "$WORK/pgbench-tx-prepared.out")"
TX_TPS="$(sed -n 's/^tps = \(.*\) (.*$/\1/p' "$WORK/pgbench-tx-prepared.out" | head -1)"
[ -n "$TX_TPS" ] || die "no tps from tx-mode pgbench"
pass "pgbench -M prepared through tx pooling (tps=$TX_TPS)"

# 11d. protocol cancel in transaction mode (SIGINT during pg_sleep)
TXC_START=$SECONDS
TXC_OUT="$(timeout --signal=INT 2 "$PSQL" "$TX_UP" -c "select pg_sleep(30);" 2>&1 || true)"
TXC_ELAPSED=$((SECONDS - TXC_START))
echo "$TXC_OUT" | grep -q "canceling statement due to user request" \
    || die "tx-mode cancel did not forward ($TXC_ELAPSED s): $TXC_OUT"
[ "$TXC_ELAPSED" -lt 10 ] || die "tx-mode cancel took ${TXC_ELAPSED}s"
pass "cancel in transaction mode (${TXC_ELAPSED}s)"

kill "$TX_PID" 2>/dev/null || true
wait "$TX_PID" 2>/dev/null || true

say "12. operational surface: TLS, admin console, metrics, shutdown"
OPS_PORT="$(pick_port)"
OPS_METRICS_PORT="$(pick_port)"
cat >"$WORK/ops.toml" <<E_O_F
listen = "127.0.0.1:$OPS_PORT"
admin_database = "pgferry_admin"
metrics_addr = "127.0.0.1:$OPS_METRICS_PORT"
shutdown_drain_secs = 10
E_O_F
if [ "$TLS_READY" = "1" ]; then
cat >>"$WORK/ops.toml" <<E_O_F

[server_tls]
cert = "$WORK/proxy.crt"
key = "$WORK/proxy.key"

[route]
name = "ops"
upstream = "host=127.0.0.1 port=$PG_PORT sslmode=require"
tls_ca = "$WORK/ca.crt"
E_O_F
else
cat >>"$WORK/ops.toml" <<E_O_F

[route]
name = "ops"
upstream = "host=127.0.0.1 port=$PG_PORT"
E_O_F
fi
RUST_LOG=pgferry=info "$PGFERRY_BIN" --config "$WORK/ops.toml" >"$WORK/pgferry-ops.log" 2>&1 &
OPS_PID=$!
OPS_UP="host=127.0.0.1 port=$OPS_PORT user=postgres dbname=postgres"

if [ "$TLS_READY" = "1" ]; then
    # 12a: full TLS chain — psql (verify-ca) → proxy (TLS termination) →
    # postgres (TLS, verify-ca). The TLS-enabled proxy accepts plaintext
    # clients too.
    wait_psql "$OPS_UP sslmode=verify-ca sslrootcert=$WORK/ca.crt" "pgferry (tls)" "$WORK/pgferry-ops.log"
    TLS_OUT="$("$PSQL" "$OPS_UP sslmode=require" -Atqc "select 'over-tls'")"
    [ "$TLS_OUT" = "over-tls" ] || die "tls chain query failed: $TLS_OUT"
    pass "psql →(TLS)→ pgferry →(TLS, verify-ca)→ postgres"

    PLAIN_OUT="$("$PSQL" "$OPS_UP sslmode=disable" -Atqc "select 'plaintext-ok'")"
    [ "$PLAIN_OUT" = "plaintext-ok" ] || die "plaintext client failed on tls proxy: $PLAIN_OUT"
    pass "plaintext client still accepted (TLS optional)"
else
    wait_psql "$OPS_UP" "pgferry (ops)" "$WORK/pgferry-ops.log"
    echo "SKIP: TLS checks (openssl unavailable)"
fi

# 12b: admin console (listener readiness already established above)
ADMIN_UP="host=127.0.0.1 port=$OPS_PORT user=postgres dbname=pgferry_admin"
POOLS_OUT="$("$PSQL" "$ADMIN_UP" -Atqc "show pools")"
echo "$POOLS_OUT" | grep -q "postgres" || die "show pools empty: $POOLS_OUT"
CLIENTS_OUT="$("$PSQL" "$ADMIN_UP" -Atqc "show clients")"
echo "$CLIENTS_OUT" | grep -q "pgferry_admin" || die "admin session not listed: $CLIENTS_OUT"
echo "$CLIENTS_OUT" | grep -q "postgres" || die "client session not listed: $CLIENTS_OUT"
STATS_OUT="$("$PSQL" "$ADMIN_UP" -Atqc "show stats")"
echo "$STATS_OUT" | grep -qE "^[a-z_]+\\|[a-z_]+\\|[0-9]+" || die "show stats malformed: $STATS_OUT"
ADMIN_ERR="$("$PSQL" "$ADMIN_UP" -c "drop table nope" 2>&1 || true)"
echo "$ADMIN_ERR" | grep -q "unknown admin command" || die "admin unknown command not rejected: $ADMIN_ERR"
pass "admin console: pools/clients/stats + unknown-command error"

# 12c: metrics endpoint
fetch_http() { # fetch_http <port> <path>
    exec 3<>"/dev/tcp/127.0.0.1/$1" || return 1
    printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3
    cat <&3
    exec 3<&- 3>&-
}
METRICS_BODY="$(fetch_http "$OPS_METRICS_PORT" /metrics)"
echo "$METRICS_BODY" | grep -q "pgferry_pool_checkouts_total" \
    || die "metrics endpoint missing pool metrics: $(echo "$METRICS_BODY" | head -5)"
echo "$METRICS_BODY" | grep -q "pgferry_sessions_active" \
    || die "metrics endpoint missing session metrics"
pass "prometheus /metrics endpoint"

# 12d: graceful shutdown — in-flight query drains, SIGTERM exits cleanly
DRAIN_OUT="$WORK/drain.out"
"$PSQL" "$OPS_UP" -Atqc "select pg_sleep(2), 'drained';" >"$DRAIN_OUT" 2>&1 &
DRAIN_PIDS=$!
sleep 0.5
kill -TERM "$OPS_PID"
wait "$OPS_PID"
OPS_RC=$?
[ "$OPS_RC" = "0" ] || die "graceful shutdown exited $OPS_RC"
# shellcheck disable=SC2086
wait $DRAIN_PIDS || true
grep -q "drained" "$DRAIN_OUT" || die "in-flight query did not drain: $(cat "$DRAIN_OUT")"
pass "graceful shutdown: in-flight query drained, exit 0"

say "13. routing & failover (two postgres instances)"
# second throwaway postgres as the secondary endpoint
PG2_PORT="$(pick_port)"
"$INITDB" -D "$WORK/pgdata2" -U postgres --no-locale -A trust >/dev/null
mkdir -p "$WORK/run2"
"$PG_CTL" -D "$WORK/pgdata2" -l "$WORK/postgres2.log" \
    -o "-p $PG2_PORT -k $WORK/run2" start >/dev/null
PG2_STARTED=1
PG2_UP="host=127.0.0.1 port=$PG2_PORT user=postgres dbname=postgres"
wait_psql "$PG2_UP" "postgres #2"

FAILOVER_PORT="$(pick_port)"
cat >"$WORK/failover.toml" <<E_O_F
listen = "127.0.0.1:$FAILOVER_PORT"

[pool]
mode = "transaction"
stale_after_secs = 1

[route]
name = "ha"

[[route.endpoints]]
id = "primary"
upstream = "host=127.0.0.1 port=$PG_PORT"

[[route.endpoints]]
id = "secondary"
upstream = "host=127.0.0.1 port=$PG2_PORT"
E_O_F
RUST_LOG=pgferry=info "$PGFERRY_BIN" --config "$WORK/failover.toml" >"$WORK/pgferry-ha.log" 2>&1 &
HA_PID=$!
FAILOVER_UP="host=127.0.0.1 port=$FAILOVER_PORT user=postgres dbname=postgres"
wait_psql "$FAILOVER_UP" "pgferry (ha)"

# 13a: traffic initially served by the primary (identify the instance by
# its listen port — every connection has its own backend pid)
PORT_BEFORE="$("$PSQL" "$FAILOVER_UP" -Atqc "select inet_server_port()")"
[ "$PORT_BEFORE" = "$PG_PORT" ] || die "expected initial traffic on primary (got port $PORT_BEFORE)"
pass "traffic initially on primary (port $PORT_BEFORE)"

# 13b: rw-split via the example gateway (reads on the replica by default) (reads on secondary by default)
GW2_PORT="$(pick_port)"
"$GATEWAY_BIN" \
    --listen "127.0.0.1:$GW2_PORT" \
    --endpoint "primary=host=127.0.0.1 port=$PG_PORT" \
    --endpoint "replica=host=127.0.0.1 port=$PG2_PORT" \
    --rw-split "primary" >"$WORK/gateway-rw.log" 2>&1 &
GW2_PID=$!
GW2_UP="host=127.0.0.1 port=$GW2_PORT user=postgres dbname=postgres"
for i in $(seq 1 40); do "$PSQL" "$GW2_UP" -Atqc "select 1" >/dev/null 2>&1 && break; sleep 0.3; done
RW_READ_PORT="$("$PSQL" "$GW2_UP" -Atqc "select inet_server_port()")"
RW_WRITE_PORT="$("$PSQL" "$GW2_UP" -Atqc "begin; create table if not exists e2e_rw(i int); commit; select inet_server_port();" | tail -1)"
[ "$RW_READ_PORT" = "$PG2_PORT" ] || die "read not on replica: port $RW_READ_PORT"
[ "$RW_WRITE_PORT" != "$PG2_PORT" ] || die "write on replica: port $RW_WRITE_PORT"
kill "$GW2_PID" 2>/dev/null || true
wait "$GW2_PID" 2>/dev/null || true
pass "rw-split: reads on replica (port $RW_READ_PORT), writes on primary (port $RW_WRITE_PORT)"


# 13c: kill the primary; traffic moves to the secondary
"$PG_CTL" -D "$PGDATA" stop -m immediate >/dev/null 2>&1
sleep 1
PORT_AFTER=""
for i in $(seq 1 20); do
    PORT_AFTER="$("$PSQL" "$FAILOVER_UP" -Atqc "select inet_server_port()" 2>/dev/null)" \
        && [ -n "$PORT_AFTER" ] && break
    sleep 0.5
done
[ -n "$PORT_AFTER" ] || die "no traffic after primary kill: $(tail -5 "$WORK/pgferry-ha.log")"
[ "$PORT_AFTER" = "$PG2_PORT" ] || die "traffic did not move to secondary (got port $PORT_AFTER)"
pass "traffic moved to secondary after primary kill (port $PORT_AFTER)"

# 13c: prepared statements survive the failover (protocol-level)
PREP_OK="$("$PSQL" "$FAILOVER_UP" -Atqc "select 'post-failover-ok'")"
[ "$PREP_OK" = "post-failover-ok" ] || die "query after failover failed: $PREP_OK"
pass "queries keep working after failover"

kill "$HA_PID" 2>/dev/null || true
wait "$HA_PID" 2>/dev/null || true

say "14. sharding: scatter-gather across two postgres shards"
SH1_PORT="$(pick_port)"
SH2_PORT="$(pick_port)"
"$INITDB" -D "$WORK/shard1" -U postgres --no-locale -A trust >/dev/null
"$INITDB" -D "$WORK/shard2" -U postgres --no-locale -A trust >/dev/null
mkdir -p "$WORK/shardrun"
"$PG_CTL" -D "$WORK/shard1" -l "$WORK/shard1.log" -o "-p $SH1_PORT -k $WORK/shardrun" start >/dev/null
"$PG_CTL" -D "$WORK/shard2" -l "$WORK/shard2.log" -o "-p $SH2_PORT -k $WORK/shardrun" start >/dev/null
SH1_STARTED=1
SH2_STARTED=1
SH1_UP="host=127.0.0.1 port=$SH1_PORT user=postgres dbname=postgres"
SH2_UP="host=127.0.0.1 port=$SH2_PORT user=postgres dbname=postgres"
wait_psql "$SH1_UP" "shard 1"
wait_psql "$SH2_UP" "shard 2"

# partitioned data: 3 rows on shard 1, 5 on shard 2
"$PSQL" "$SH1_UP" -qc "create table t(i int); insert into t select generate_series(1,3)" >/dev/null
"$PSQL" "$SH2_UP" -qc "create table t(i int); insert into t select generate_series(4,8)" >/dev/null

GW3_PORT="$(pick_port)"
"$GATEWAY_BIN" \
    --listen "127.0.0.1:$GW3_PORT" \
    --endpoint "shard1=host=127.0.0.1 port=$SH1_PORT" \
    --endpoint "shard2=host=127.0.0.1 port=$SH2_PORT" \
    --shard-all 'from t' >"$WORK/gateway-shard.log" 2>&1 &
GW3_PID=$!
GW3_UP="host=127.0.0.1 port=$GW3_PORT user=postgres dbname=postgres"
for i in $(seq 1 40); do "$PSQL" "$GW3_UP" -Atqc "select 1" >/dev/null 2>&1 && break; sleep 0.3; done

# 14a: Sum merge — count(*) across shards returns the total
TOTAL="$("$PSQL" "$GW3_UP" -Atqc "select count(*) from t")"
[ "$TOTAL" = "8" ] || die "scatter count(*) returned $TOTAL, expected 8"
pass "scatter count(*) merged: $TOTAL"

# 14b: Concat merge — full row set from both shards
ROWS="$("$PSQL" "$GW3_UP" -Atqc "select i from t" | sort -n | tr '\n' ',')"
[ "$ROWS" = "1,2,3,4,5,6,7,8," ] || die "scatter concat returned: $ROWS"
pass "scatter concat: all 8 rows from both shards"

# 14c: single-shard passthrough still works alongside scatter
PT="$("$PSQL" "$GW3_UP" -Atqc "select 'passthrough'")"
[ "$PT" = "passthrough" ] || die "passthrough broken: $PT"
pass "non-scatter queries pass through"

# 14d: dead shard → clean error (not a hang)
"$PG_CTL" -D "$WORK/shard1" stop -m immediate >/dev/null 2>&1
SHARD_ERR_START=$SECONDS
SHARD_ERR="$("$PSQL" "$GW3_UP" -c "select count(*) from t" 2>&1 || true)"
SHARD_ERR_ELAPSED=$((SECONDS - SHARD_ERR_START))
echo "$SHARD_ERR" | grep -qiE "error|fatal" \
    || die "expected a clean error from dead shard, got: $SHARD_ERR"
[ "$SHARD_ERR_ELAPSED" -lt 15 ] || die "scatter on dead shard took ${SHARD_ERR_ELAPSED}s (hang?)"
pass "dead shard surfaces clean error (${SHARD_ERR_ELAPSED}s)"

kill "$GW3_PID" 2>/dev/null || true
wait "$GW3_PID" 2>/dev/null || true

say "all e2e checks passed"
