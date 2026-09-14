# pgferry — a programmable PostgreSQL proxy & pooler on pgwire

Status: PLAN (no code yet)
Target: standalone workspace, depends on published `pgwire` crate (server-api + client-api features)

## 1. Goal

Build a **programmable PostgreSQL proxy**: first a correct, fast connection
pooler; then a plugin surface ("interceptors") that lets user code observe,
rewrite, deny, or answer protocol traffic without knowing wire-protocol
details.

Prior art / comparison points: PgBouncer (C, protocol-level pooler), pgcat
(Rust, pooler + router), odyssey, supersus-prox. Differentiator: embedding
first — the proxy is a *library* you configure in Rust code (Envoy-style),
with a thin binary wrapper for config-file deployment.

### Non-goals (for now)

- SQL parsing/query routing by semantics (may come later; interceptors can
  bring their own parser — e.g. `sqlparser` — via the message hook).
- High-availability/failover clustering, load balancing beyond round-robin.
- Replication/COPY-both streaming features beyond transparent passthrough.

## 2. What pgwire gives us (API inventory)

### server-api (downstream: we are the server)

| API | Role in pgferry |
|---|---|
| `tokio::negotiate_tls(tcp, acceptor) -> Framed<MaybeTls, PgWireMessageServerCodec<S>>` | **pub** — gives us a framed downstream socket with SSL/GSS negotiation handled, without adopting pgwire's handler loop. Core of our accept path. |
| `PgWireMessageServerCodec` (`Decoder`/`Encoder`) | decode `PgWireFrontendMessage`, encode `PgWireBackendMessage`. Decoder behavior is **gated on `client_info.state()`** (startup vs query phase) — our session loop must keep `DefaultClient` state in sync. |
| `messages::*` (Startup, Authentication, ParameterStatus, BackendKeyData, Query, Parse/Bind/Describe/Execute/Close/Sync/Flush, Copy*, ErrorResponse, ReadyForQuery, NegotiateProtocolVersion, CancelRequest, Terminate…) | the full typed message vocabulary for both directions. |
| server handler traits (`StartupHandler`, `SimpleQueryHandler`, …) | **not used for the pump path**; potentially reused later for the admin console (virtual `SHOW POOLS` backend). |
| `api::auth` server handlers + `AuthSource` | reuse for optional downstream auth (SCRAM server with auth_file, v2). |
| `ConnectionManager` / `ConnectionHandle` | reference design for our cancel router (we need a variant keyed by *virtual* pid/secret → session). |

### client-api (upstream: we are the client)

| API | Role in pgferry |
|---|---|
| `Config` (libpq-style connstring parser, sslmode, timeouts, TCP keepalive) | upstream route configuration; pool key is derived from the resolved startup params. |
| `PgWireClient::connect(config, StartupHandler, TlsConnector)` | full upstream handshake incl. **cleartext / MD5 / SCRAM-SHA-256 client auth** (`DefaultStartupHandler`) and protocol negotiation. Caches `server_parameters` + upstream `BackendKeyData` (pid/secret). |
| `PgWireClient` as `Sink<PgWireFrontendMessage>` + `Stream<Item = PgWireBackendMessage>` | **after connect it's a plain message pipe** — this is the pump's upstream half. COPY/Notice/ParameterStatus all flow through untouched (the handler-API gaps don't apply, we never use `simple_query`/`extended_query` for forwarding). |
| `PgWireClient::cancel()` | opens a second upstream connection and sends CancelRequest with the *real* upstream pid/secret. Note: takes `&self` but the pump holds `&mut` — see §7 workaround. |

### Notable gaps in pgwire (workaround = local, no pgwire changes needed in v1)

- client-api has no COPY / notice API — **verified this is handler-layer only**:
  the client query handlers (`SimpleQueryHandler::on_message`,
  `ExtendedQueryClient`) reject Copy variants with `UnexpectedMessage`, but
  the codec layer handles all of them — `PgWireBackendMessage::decode` has
  arms for all six backend Copy messages (`CopyData` `d`, `CopyFail` `f`,
  `CopyDone` `c`, `CopyInResponse` `G`, `CopyOutResponse` `H`,
  `CopyBothResponse` `W`, src/messages/mod.rs:526–533) and
  `PgWireFrontendMessage` encodes/decodes `CopyData`/`CopyDone`/`CopyFail`.
  Since the pump only uses `PgWireClient` as a raw `Sink`+`Stream` after
  `connect()` and never calls the query-handler APIs, COPY passes through.
  M0's acceptance test includes `\copy` as the empirical proof.
- `PgWireClient` cannot be split into owned sink/stream halves (needed for
  concurrent cancel while pumping); workaround: `tokio::sync::Mutex` around
  cancel, or spawn a cancel task owning a clone path. Upstream improvement
  candidate (§9).
- Server codec's `DefaultClient` state gating means our loop must set
  `AwaitingStartup → ReadyForQuery` correctly or startup packets misparse.

## 3. Core architecture decision: message pump (not handler-level proxy)

Two ways to build a proxy on pgwire:

**A. Handler-level** — implement server handler traits (`do_query` etc.),
forward via client query API, return `Response`s.
Rejected: every response is materialized through `Response`/`QueryResponse`
(plumbing streaming rows across handler boundaries is awkward), COPY is
unsupported by client-api, and the pooler loses sight of protocol flow that
the default handler machinery hides (Sync recovery, portal lifecycle).

**B. Message pump (chosen)** — pgwire is used as:

```
                 typed messages, interceptor hooks fire here
   psql ──┐      ▼                                        ▼      ┌─ postgres
         │  PgWireMessageServerCodec              PgWireMessageClientCodec
         └─► Framed<MaybeTls, …> ──► [ session state machine ] ──► PgWireClient ──┘
   downstream (server-api)                     pool leases upstream (client-api)
```

- Downstream: our own accept loop → `negotiate_tls` → decode frontend msgs.
- Upstream: `PgWireClient::connect` (auth done here) → then treat it purely
  as `Sink`+`Stream`.
- The session loop pumps messages both directions, invoking interceptors and
  the state tracker on every message, keeping a lease on the pool.

Why: full protocol fidelity with zero buffering (COPY-in/out, portal
suspension, notices, Listen/Notify all "just work" by passthrough), a single
interception point that sees everything, and the state observations
(ReadyForQuery txn byte, Parse/Close, ParameterStatus) fall out naturally —
exactly the signals a pooler needs.

### Session loop sketch

```
loop {
    select! {
        downstream_msg = downstream.next() => match downstream_msg {
            Terminate | EOF   => return upstream to pool (DISCARD ALL) and exit,
            CancelRequest     => (handled pre-startup on a dedicated conn; routed via CancelRouter),
            msg               => interceptors.frontend(&mut ctx, &mut msg)?
                                  state_tracker.observe_frontend(&msg);
                                  if forwarding: upstream.send(msg)
        },
        upstream_msg = upstream.next() => match upstream_msg {
            msg => state_tracker.observe_backend(&msg);   // txn status, stmt cache, params
                   interceptors.backend(&mut ctx, &mut msg)?
                   downstream.send(msg)
        },
    }
}
```

## 4. Components

### 4.1 `listener` + `session`
- TCP listener (multi-addr later), optional TLS acceptor, startup timeout.
- Per-connection tokio task; owns `Framed<MaybeTls, PgWireMessageServerCodec<String>>`
  (via `negotiate_tls`) + `SessionCtx`.

### 4.2 downstream startup (`startup.rs`)
1. Read `Startup` (or SSL negotiation handled by `negotiate_tls` already;
   CancelRequest short-circuits to CancelRouter).
2. Protocol version policy (v1): if client requests > 3.0 → reply
   `NegotiateProtocolVersion` pinning **3.0**; keep `DefaultClient` state in
   sync. (3.2 support later — see §8.)
3. Downstream auth policy (v1): `trust`, or cleartext against a configured
   credential. (v2: SCRAM server with auth_file via pgwire server `auth`.)
4. Resolve route + acquire lease **lazily** (session mode: now; transaction
   mode: on first need) — connecting upstream uses `PgWireClient::connect`
   with a `Config` built from route config + client's user/db/options.
5. Send downstream: `Authentication::Ok`, replay upstream's cached
   `server_parameters` (filtered) as `ParameterStatus`, **our own**
   `BackendKeyData` (virtual pid + secret, registered in CancelRouter),
   `ReadyForQuery`.

### 4.3 pool (`pool.rs`)
- Key: `(route_id, user, database, options-normalized)`; one `Pool` per key,
  global registry. Bounded by `max_client_conn` (listener semaphore) and
  `default_pool_size`/`max_pool_size` per key.
- Entry = `PgWireClient` + liveness metadata (last used, generation).
- Checkout: idle first, else new connect, respecting size cap (waiters queue).
- Checkin: if session used it beyond startup → `DISCARD ALL` (+ drain reply)
  before parking; on error → destroy.
- Hygiene: idle timeout sweeper, server max lifetime, replace-on-error with
  one transparent reconnect **only when session state is provably clean**
  (v1: reconnect only between client sessions or between transactions in tx
  mode).
- Modes: `Session` (M1), `Transaction` (M3).

### 4.4 state tracker (`state.rs`) — the pooler brain
Observes messages; feeds pool + admin metrics + interceptors:
- **Transaction state**: from `ReadyForQuery` status byte (I/T/E) —
  authoritative for tx-mode attach/detach and failed-transaction handling.
- **Prepared statements**: record `Parse { name, sql, type_oids }` (client's
  names), invalidate on `Close(S)` / connection error. On tx-mode attach to a
  fresh upstream: silently re-`Parse` them (no reply expected client-side
  since we generate the Parse ourselves and swallow `ParseComplete`).
  Mirrors PgBouncer ≥ 1.21 statement tracking.
- **Session GUCs**: remember `ParameterStatus` deltas vs upstream baseline;
  on attach replay as `SET`s (best effort, documented limitation: GUCs
  changed via non-parameter-reporting paths need `pool_mode=session`).
- Extended-protocol phases (Parse→Bind→Execute, Sync recovery) tracked
  loosely for logging/rewriting correctness.

### 4.5 interceptors (`intercept.rs`) — the programmable surface

One trait, phase-based hooks over the session / query-cycle lifecycle,
raw message hooks as escape hatch (full design in §M2):

- `on_query` (request_filter): validate / deny / rewrite / respond early;
- `on_row` + `RowSchema`/`DataRowMut`: value-level row rewriting (schema
  tracked by the pump; re-encode fixes framing);
- `on_cycle_end` (logging phase): tag/rows/latency/error telemetry;
- `on_frontend`/`on_backend`: raw `&mut` message hooks, `Action::Forward`
  after in-place mutation (allocation-free);
- `upstream()` + `on_upstream_error` (M5): routing + retry policy.

`QueryCtx<Ctx>` carries user/db/route, the associated per-session `Ctx`
state (pingora's CTX; typed store via pgwire `SessionExtensions`), latency
timers, and per-session counters.

- **Injected-error rule**: when an interceptor denies a query in the extended
  protocol, the session (not upstream) must emulate error recovery: send
  ErrorResponse, then swallow downstream messages until `Sync`, then reply
  ReadyForQuery ourselves. (This is the one state-machine behavior we must
  own downstream — pgwire's `AwaitingSync` handling is the reference.)

### 4.6 cancel router (`cancel.rs`)
- Virtual `(pid, secret)` per session (our own `BackendKeyData` downstream).
- On CancelRequest conn (arrives as a fresh TCP conn carrying a CancelRequest
  instead of Startup): look up session → if it holds an upstream lease, call
  that `PgWireClient::cancel()` (real upstream keys are cached inside it) →
  close cancel conn. No lease / no query in flight → no-op close (matches
  PgBouncer semantics).

### 4.7 binary + config (`bin/`)
- The shipped binary is a **reference composition** (like pingora's
  example load balancer), not the product: minimal infra config only.
- `pgferry.toml` holds infra (pingora `ServerConf` model): listeners, TLS,
  daemon/upgrade, runtime; pool sizing may live here. Routes, endpoints,
  interceptors: never config — users compose those in their own binary
  (`Proxy::builder().service(...)`), depending on `pgferry` as a library.

## 5. Project layout (workspace)

```
pgferry/
├── Cargo.toml            # [workspace]
├── crates/
│   ├── pgferry/          # library: listener, session, pool, state, intercept, cancel
│   └── pgferry-bin/      # reference binary: infra-level TOML config only
├── examples/
│   ├── deny-list.rs      # QueryHook: deny DROP TABLE
│   ├── rewrite.rs        # QueryHook: SQL rewrite
│   └── audit.rs          # log SQL + command tags + row counts + latency
└── tests/                # integration: fake upstream = pgwire server-api test double
```

Dependencies: `pgwire = { features = ["server-api-aws-lc-rs", "client-api-aws-lc-rs"] }`,
tokio, bytes, futures, async-trait, thiserror, tracing; (bin: serde/figment or toml, clap).

## Roadmap drivers: the three target features

pgferry's end goals drive the architecture:

1. **opt-in message modification** — the interceptor API (M2);
2. **auto failover** — multi-endpoint routes, health checking, runtime
   topology updates, safe retry, state replay on relink;
3. **partition & sharding** — query-level routing with result merge
   (scatter-gather).

### Layered design (separate seams, composable)

All seams are code-driven: the interceptor trait is the user-facing
surface; routing is an interceptor hook (`upstream()`), and the machinery
below it is library-internal:

| Layer | Concern | Serves |
|---|---|---|
| `Interceptor` (phase hooks: `on_query`, `upstream()`, `on_row`, `on_cycle_end`, raw `on_frontend`/`on_backend`) | transformation + routing decisions | (1), (2), (3) |
| Built-in library types (`EndpointGroup`, `RwSplit`, `ShardSet`, retry policies) | embeddable helpers the hooks call | (2), (3) |
| Execution engine (`ExecutionPlan`, `ResultSetMerger`) | core-internal: per-cycle dispatch + scatter-gather composition | (3) |
| Pool + state replay | connection mgmt, SET/Parse replay on relink | (2), (3), M3 |

```rust
// routing hook on the interceptor (pingora's upstream_peer)
async fn upstream(&self, ctx: &mut QueryCtx<Self::Ctx>) -> EndpointId {
    self.split.select(ctx.query().read_write())   // built-in as a field
}

// core-internal: what the session does with the cycle
pub enum ExecutionPlan {
    Passthrough { endpoint: EndpointId },               // today's pump, per cycle
    Scatter { legs: Vec<ShardQuery>, merge: MergePolicy },
    Local(Response),                                     // Reply generalized
}

// core-internal: scatter-gather response composition, driven by the
// `scatter_gather` helper M6 ships
pub trait ResultSetMerger: Send + Sync {
    fn on_row_description(&mut self, leg: LegId, desc: RowDescription) -> Option<RowDescription>;
    fn on_row(&mut self, leg: LegId, row: DataRow) -> Option<DataRow>;
    fn on_complete(&mut self, leg: LegId, tag: Tag) -> Tag;
    fn on_error(&mut self, leg: LegId, err: ErrorInfo) -> MergeErrorAction; // fail-fast cancels other legs
}
```

### The cycle-oriented session loop (sharding prerequisite)

Sharding breaks "one session = one upstream = one pump loop": a client
query fans out to N legs and the client sees one RowDescription + merged
DataRows + one summed CommandComplete + RFQ. The session loop therefore
becomes cycle-oriented:

```
loop {
    read next query cycle   // simple: Query→RFQ; extended: Sync-delimited batch
    plan = router.plan(ctx) // single routing point for ALL features
    match plan {
        Passthrough{..} => forward verbatim (per-cycle form of today's pump),
        Scatter{..}    => drive N legs on shard pools, run merger, emit,
        Local(resp)    => emit locally,
    }
}
```

**The query-cycle boundary is the universal extension point**: interceptor
deny-recovery (M2), statement tracking (M3), safe-retry classification
(failover), and query routing (sharding) all key off the same boundary
detection. Designed once in the session, reused by every feature.

### Verified compatibility with M0–M2 + planned evolutions

No rework required; evolutions land with the features that need them:

1. `RouteConfig.upstream: String` → `Vec<EndpointConfig>`; `PoolKey` gains
   an `EndpointId` dimension; factories capture endpoints (failover).
2. `RouteRegistry` becomes dynamic (watch/admin-updatable) for runtime
   topology changes (Patroni/DCS-driven promotion).
3. Per-endpoint health checker (Alive/Suspect/Dead), consulted at checkout
   and on runtime errors; M1's `settle`/probe machinery is the seed.
4. Safe-retry classification falls out of existing `awaiting_response` +
   forwarded counters: retry on another endpoint only if nothing executed.
5. Extended-protocol scatter-gather uses M3's statement registry per leg
   (per-shard Parse/Bind/Execute); simple protocol first.
6. Multi-shard transactions v1: `Scatter` only outside explicit
   transactions or read-only; single-shard cycles stay `Passthrough`
   (session-pinned, transactionally sound). 2PC is a later explicit feature.
7. Cancel fan-out in composed mode routes through the session task (it
   owns all legs); the CancelRouter already targets sessions, not
   connections.

### Programmable framework: all code-driven (the pingora model)

pgferry is a **framework**, not a configurable product. Composition
happens in Rust code; there is no plugin registry selected by config and
no routing/interceptor logic in config files.

- **The extension point is the interceptor** — phase-based hooks over the
  session / query-cycle lifecycle, pingora's `ProxyHttp` analogue
  (compile-time trait objects; no dynamic loading, no ABI, no sandbox
  tiers).
- **Built-ins are embeddable library types that user code calls** — the
  `LoadBalancer<RoundRobin>`-as-a-field pattern. Failover, RW-split and
  sharding ship as library types and helpers composed into the user's
  interceptor, never as config-selected engines.
- **Config is infra-only** (pingora's `ServerConf` model): listener, TLS,
  runtime, daemon/upgrade — pool sizing may live there (pingora ships
  `upstream_keepalive_pool_size` in `ServerConf`). Routes, endpoints,
  interceptors: never config.
- The shipped `pgferry` binary is a reference composition / demo (like
  pingora's example load balancer), not the product.

Composition is code, one surface:

```rust
struct AppProxy {
    split: RwSplit,                 // built-in library type as a field
    deny: DenyList,
    mask: PiiMask,
}

#[async_trait]
impl Interceptor for AppProxy {
    type Ctx = AppCtx;
    fn new_ctx(&self) -> Self::Ctx { AppCtx::default() }

    async fn on_query(&self, ctx: &mut QueryCtx<Self::Ctx>, q: &mut QueryCycle) -> Action {
        // validate / deny / rewrite / respond early (request_filter)
    }
    async fn upstream(&self, ctx: &mut QueryCtx<Self::Ctx>) -> EndpointId {
        // routing hook (upstream_peer): user code decides, may call
        self.split.select(q.read_write())        // ...built-ins like this
    }
    async fn on_upstream_error(&self, ctx: &mut QueryCtx<Self::Ctx>, e: &UpstreamError) -> RetryDecision {
        // error policy: mark retry-able → framework re-enters `upstream()`
    }
    async fn on_cycle_end(&self, ctx: &mut QueryCtx<Self::Ctx>, r: &CycleReport) {
        // telemetry (logging phase): tag, rows, latency, error
    }
}

let proxy = Proxy::builder()
    .listen("0.0.0.0:6432")?         // infra
    .tls(server_cert_key)?            // infra
    .service(HttpProxyService::new(AppProxy { split, deny, mask }))
    .build()?;
```

Because there is no plugin boundary, interceptor hooks keep the ergonomic
zero-cost form: `&mut` message + `Action::Forward` (mutation in place).

#### Insights from pingora (cloudflare/pingora, HTTP-proxy analogue)

Reviewed 2026-09; confirms and refines several choices:

- **Phase-based hooks on one trait** (`ProxyHttp`: request_filter →
  upstream_peer → response_filter → logging, ~30 methods): pgferry's M2
  should include an `on_cycle_end` hook (their `logging` phase) carrying
  tag/rows/latency/error — the audit/telemetry endpoint.
- **`type CTX` + `new_ctx()`** per-request state threaded through hooks —
  same as our `SessionCtx`; use pgwire `SessionExtensions` for typed user
  state.
- **Error hooks as routing policy**: `fail_to_connect`/
  `error_while_proxy` mark errors retry-able → framework re-enters
  `upstream_peer`; retry-safety default (idempotent only) lives in an
  overridable default impl. Adopt this shape for M5: `on_upstream_error
  → RetryDecision`, framework re-routes; our safe-retry classification
  becomes the default impl.
- **Body filter shape** `(&mut Option<Bytes>, end_of_stream)` — candidate
  for later `CopyData` filters.
- **Opt-in complexity with loud defaults** (cache key callback panics
  unless overridden) — pattern for future pgferry features that are easy
  to misuse.
- **Built-ins are embeddable library types** (`LoadBalancer<RoundRobin>`
  as a field the user's hook calls), *not* config-selected engines; config
  is infra-only (threads, daemon, upgrade socket, keepalive pool size).
  pgferry adopts this model wholesale — all code-driven, no TOML routing;
  the shipped binary is a reference composition, not the product.

## 6. Milestones (each independently testable with `psql` / pgbench)

**M0 — transparent pump (no pool). — ✅ DONE**
Accept → single upstream per client → bidirectional passthrough with
tracing. Verified by `scripts/e2e.sh` against real PostgreSQL 18: simple
queries, \copy both directions (100k rows), LISTEN/NOTIFY, error recovery
on one session, protocol cancel (virtual keys → upstream cancel, 30s query
cut to 2s), pgbench in simple/extended/prepared modes (~7-9k TPS vs ~9-14k
direct, debug build). In-repo integration tests (`cargo test`) use a
pgwire-based fake upstream — no external dependencies.

Two bugs found & fixed during M0, both worth remembering for later
milestones:

1. **Cancel silently failing**: the virtual `(pid, secret_key)` must be
   stored on the downstream client via `set_pid_and_secret_key` *before*
   `finish_authentication` — it builds `BackendKeyData` from the client
   state. Registering a different pair in the `CancelRouter` than the one
   sent downstream means cancels miss and queries run to completion.
2. **Nagle-induced stalls in the extended protocol**: pgwire's client
   `connect_socket` didn't set `TCP_NODELAY`; each pipelined
   Parse/Bind/Execute stalled behind delayed ACKs → 13 TPS vs 8600 direct.
   Fixed in the local pgwire checkout (`src/tokio/client.rs` — upstream PR
candidate, see §9) plus upstream-flush batching in the pump (flush only on
   Query/Sync/Flush/Terminate/Copy*).

**M1 — session pooling. — ✅ DONE**
Pool keyed by (route, user, database): semaphore-bounded `max_size`, LIFO
idle stack, checkout probe of stale connections, sweeper for idle timeout +
max lifetime, `max_client_conn` session cap. Checkin sanitizes: cancel
in-flight query → `ROLLBACK` if needed → `DISCARD ALL`; tracked startup
params (`application_name`, `client_encoding`) are replayed via `SET` on
checkout. Verified by `scripts/e2e.sh`: backend pid reuse across sessions,
abandoned transaction rolled back + temp table gone on reuse,
`application_name` replay, and 4 concurrent clients served by exactly 2
backends at `max_size=2`. All M0 checks still pass through the pool;
pgbench unchanged (~6-9k TPS debug build).

Three pool-specific lessons found & fixed during M1 (see §7 risk list —
these expand it):

1. **In-flight queries outlive the client.** A session killed mid-query
   leaves the backend *executing*; the sanitize `Sync` blocks behind it
   for the query's full runtime. Checkin must `cancel()` first — exactly
   what pgbouncer does.
2. **The double-ReadyForQuery shift (nastiest bug class of the milestone).**
   After a cancel, the server emits `ErrorResponse`+`ReadyForQuery` on its
   own; layering our sanitize `Sync` on top queues a *second*
   `ReadyForQuery`. The drain consumes only the first, shifting every later
   response cycle by one — and the tail of our own `DISCARD ALL` response
   leaked into the *next session* (client saw `DISCARD ALL` as its query
   tag). Fix (`settle`): after cancel, drain the query's own terminal
   messages WITHOUT sending Sync; only fall back to Sync when nothing
   arrives (client died between Flush and Sync). Rule: **every response
   cycle on a pooled connection must be consumed exactly — a stray or
   missing terminal message desynchronizes the next session.**
3. **Tracked startup parameters must NOT go into the upstream connect
   Config.** Otherwise each pooled connection's post-`DISCARD ALL`
   baseline is the first session's startup packet, and "fresh" vs
   "reused" connections behave differently. Baseline = server defaults;
   per-session values applied with `SET` at checkout.

**M2 — interceptor API. — ✅ DONE** The extension point, pingora-shaped: one trait,
phase-based hooks over the session / query-cycle lifecycle, with raw
message hooks as the escape hatch. All code-driven composition — no
registry, no config selection.

```rust
#[async_trait]
pub trait Interceptor: Send + Sync {
    /// Per-session state threaded through every hook (pingora's `CTX`).
    type Ctx: Send + Default;
    fn new_ctx(&self) -> Self::Ctx { Self::Ctx::default() }

    /// Query-cycle start (request_filter): validate / deny / rewrite /
    /// respond early. `QueryCycle` exposes the SQL (Query or tracked
    /// Parse/Bind), statement/portal names, and protocol flavor.
    async fn on_query(&self, ctx: &mut QueryCtx<Self::Ctx>, q: &mut QueryCycle) -> Action;

    /// Cycle telemetry (logging phase): tag, row count, latency, error.
    async fn on_cycle_end(&self, ctx: &mut QueryCtx<Self::Ctx>, r: &CycleReport) {}

    /// Raw message escape hatch — full power incl. data payloads (SQL
    /// rewrite in `Query`/`Parse`, `Bind` param scrubbing, `CopyData`
    /// chunk-level transforms, `DataRow` raw rewriting, tag rewriting).
    /// Mutation in place + `Action::Forward`; allocation-free hot path.
    async fn on_frontend(&self, ctx: &mut QueryCtx<Self::Ctx>, msg: &mut PgWireFrontendMessage) -> Action { Action::Forward }
    async fn on_backend (&self, ctx: &mut QueryCtx<Self::Ctx>, msg: &mut PgWireBackendMessage) -> Action { Action::Forward }

    // `upstream()` routing + `on_upstream_error` retry policy hooks
    // arrive with M5 (pingora's upstream_peer / fail_to_connect).
}

pub enum Action {
    Forward,                     // includes in-place mutation of msg
    Reply(PgWireBackendMessage), // answer locally (cache / synthetic rows)
    Deny(ErrorInfo),
    Close,
}
```

- **Value-level sugar** for the common payload case — row rewriting. The
  pump tracks the session's current `RowDescription` (schema + per-column
  format) and feeds a higher-level hook on the same trait:

  ```rust
  async fn on_row(&self, ctx: &mut QueryCtx<Self::Ctx>, schema: &RowSchema, row: &mut DataRowMut) -> RowAction;
  // DataRowMut: get(i)/set(i)/drop_row() on decoded text values;
  // re-encoding fixes message lengths — framing cannot be corrupted.
  ```

  Decoding is opt-in (pump skips it when no interceptor overrides `on_row`).
  Still streaming: one row per call, no buffering.

  Documented limits: binary-format columns exposed read-only initially
  (full binary re-encode later); `CopyData` rewriting is chunk-level only
  (rows can span chunks — row-level COPY filtering would need explicit
  reassembly); dropped/added rows desync `CommandComplete` counts unless
  the interceptor opts into `adjust_command_counts` (never silent);
  `RowDescription` rewrite allowed with "you own consistency" contract.

Built-in interceptors ship as library types composed into user code
(`DenyList`, `Audit`, `Rewrite`); implement one example proxy embedding
them. Acceptance: examples run; deny returns proper `ERROR` and connection
stays usable for both simple and extended protocol (injected-error
recovery: ErrorResponse + swallow-until-Sync + ReadyForQuery, emulated by
the session, not upstream).

**M2 amendments & lessons (landed):**

- `Action::Reply` carries `Vec<PgWireBackendMessage>` — a full local
  answer needs RowDescription+rows+CommandComplete, not one message.
- `Deny` is honored from `on_query` only: from raw hooks mid-cycle it
  could not preserve extended-cycle consistency (there it logs + closes).
- DenyList/Rewrite/Audit live in `intercept::builtin`; the example gateway
  (`crates/pgferry/examples/gateway.rs`) composes them into one custom
  interceptor struct — the intended pattern.
- Bug found during M2 (same class as the M1 double-RFQ lesson — framing
  integrity): `DataRowMut::parse` originally consumed the row bytes and
  only wrote them back when modified, so an *unmodified* row was
  forwarded with an empty body (psql: "insufficient data in D message").
  Fix: parse as ranges into the original bytes; rebuild only on mutation
  (zero-copy passthrough). Regression test:
  `mask_rows_preserves_unmasked_columns`.

**M3 — transaction pooling. — ✅ DONE**
`PoolMode::Transaction` (config `[pool] mode = "transaction"`): lease on
the first message of a query cycle, release at `ReadyForQuery(Idle)`.
Session-state layer: per-session named-statement registry re-Parse'd on
attach, GUC overrides replayed via `SET`, `Sync` answered locally while
detached, `Close` of a detached statement answered locally. Dirty-flag
detach (DISCARD ALL only when statements/GUCs were replayed); pool
baseline-param snapshot feeds downstream startup in tx mode. Verified:
pool-sharing across sessions mid-transaction (3 overlapping txs on 2
backends), statement replay across attach (pgbench -M prepared, ~3.5k
TPS), cancel in tx mode, plus integration tests with an in-process fake
upstream.

Bugs found & fixed during M3 (recorded for the pattern book):

1. **Record-then-attach double-Parse**: registering a new named statement
   before `ensure_attached` made the attach replay re-Parse the very
   statement about to be forwarded → 42P05 "already exists". Fix: replay
   the old registry first, record the new statement after attach.
2. **Dirty flag reset on mid-transaction RFQ**: a `ReadyForQuery` with
   status T (in-transaction) reset `lease_dirty` even though no detach
   happened, so the final idle detach skipped `DISCARD ALL` and the next
   session's replay collided (42P05). Fix: reset only when a detach
   actually occurred. (Lesson: lifecycle flags belong to the lease, not
   the loop iteration — reset them in the same place that consumes them.)
3. Process: `cargo build -p pgferry` builds only the library — the
   `pgferry` binary is `pgferry-bin` and did not relink; a stale binary
   made an already-fixed bug look unfixed. Use `cargo build --workspace`
   (the e2e script does).

**M4 — operational surface. — ✅ DONE**
- **TLS**: downstream termination (`[server_tls]` cert/key → rustls
  acceptor through `negotiate_tls`; plaintext clients still accepted) and
  upstream TLS (`tls_ca` for verified chains, `tls_insecure` for
  require-style). Verified e2e with a generated CA: full chain
  psql→(TLS)→pgferry→(TLS, CA-verified)→postgres; a wrong CA fails with
  `UnknownIssuer`.
- **Admin console** (`admin_database = "pgferry_admin"`): sessions on that
  database serve `SHOW POOLS`/`SHOW CLIENTS`/`SHOW STATS`/`SHOW HELP`
  locally — the first real user of the `ExecutionPlan::Local` pattern.
  Session registry (`RuntimeState`) feeds `SHOW CLIENTS` and metrics.
- **Prometheus `/metrics`** (`metrics_addr`): hand-rolled HTTP, zero extra
  deps; session counters + per-pool counters/gauges (checkouts, reused,
  created, checkins, dropped, evicted, probed, active, idle, waiting,
  max_size).
- **Graceful shutdown**: SIGINT/SIGTERM → stop accepting → sessions end at
  the next cycle boundary (in-flight queries complete; verified with a
  pg_sleep under SIGTERM) → drain window (default 30s) → abort remainder.
  Lesson encoded: a shutdown branch that breaks the pump immediately kills
  in-flight queries — drain must be cycle-boundary aware
  (`awaiting_response`/COPY state), not task-abort based.

**M5 — routing foundations & auto failover. — ✅ DONE** All code-driven
(pingora model): the routing and error-policy hooks live on the
interceptor; built-ins are embeddable library values.

- **Hooks**: `upstream(ctx, RoutingInfo) -> EndpointId` (routing per
  attach), `on_upstream_error(ctx, UpstreamError) -> RetryDecision`
  (Relink/Close), `on_upstream_connected(ctx, endpoint)` (health
  successes). `RoutingInfo` carries the route's endpoints, session
  identity, and the SQL being routed (Bind/Execute attaches resolve it
  from the statement registry).
- **Built-ins** (`routing.rs`): `EndpointGroup` (passive health:
  failure threshold → Dead, revive cooldown, first-alive/round-robin
  selection), `RwSplit` (read/write classification — leading-keyword
  default or custom classifier — reads to replicas, writes pinned to the
  primary), `FailoverRouter` (auto-wired by the builder for multi-endpoint
  routes without a custom service).
- **Failover semantics**: connect/probe failures at attach retry through
  the policy (seamless, no client-visible error); mid-lease breaks relink
  and answer the interrupted cycle with a synthesized `08006` (queries are
  never replayed — non-idempotent by nature); a first-cycle `Parse` lost
  to a dead connection is healed by registry replay on the next attach.
  Session state (named statements, GUC overrides) is mode-agnostic since
  M5 and replays on every attach — surviving both transaction detach and
  failover relink.
- **Internals**: `RouteConfig.endpoints: Vec<EndpointConfig>`,
  `PoolKey` gains the endpoint id (pools per endpoint — `SHOW POOLS`
  lists them), per-endpoint connect factories; tx-mode startup probes the
  baseline through the routed endpoint with the same retry loop.
- Verified: 4 integration tests (connect-failure failover, mid-session
  relink with session survival, statement replay across relink, RW-split
  by instance marker) + e2e §13 against two real postgres instances:
  traffic on primary → RW-split (reads on replica by port) → kill
  primary → traffic on secondary → queries keep working.

**M6 — sharding: scatter-gather. — ✅ DONE** Scatter as an `Action`
returned from `on_query` — the user describes WHAT (`ScatterRequest`:
legs + `MergePolicy`), the session owns the machinery (fan-out, merge,
fail-fast, protocol composition).

- `Action::Scatter(ScatterRequest)` executes N legs concurrently on the
  endpoints' pools and answers the cycle with ONE client-visible result:
  first leg's `RowDescription`, merged rows, single
  `CommandComplete`/`ReadyForQuery`. This is the roadmap's
  `ExecutionPlan::Scatter` — `Reply` generalized to N legs — and the
  cycle-oriented dispatch in miniature (the scatter arm inside the pump's
  on_query handling is a local execution mode, no session-loop refactor
  needed).
- `MergePolicy::Concat` (one result set, leg order, total row count) and
  `MergePolicy::Sum` (numeric first column summed — `count(*)`/`sum()`
  aggregates).
- `ShardSet` helper: `broadcast(sql, merge, tag)`, `shard_for(key)` (SipHash
  key→shard for single-shard routing via the `upstream()` hook).
- Fail-fast: the first failed leg aborts the rest (dropping their futures
  closes the sockets — backends cancel server-side on disconnect); clean
  legs' connections are checked in; the client sees the shard's
  `ErrorResponse`. The session-lease leg (on the currently attached
  endpoint) reuses the session lease — avoiding self-deadlock on small
  pools.
- v1 restrictions honored: simple protocol + idle transaction state only
  (enforced with `0A000` errors); legs run one read-only simple query each
  and check in clean unless a `ParameterStatus` was seen.
- Verified: 4 integration tests (Sum 3+5=8, Concat both markers,
  shard-failure cleanliness, shard_for consistency) + e2e §14 against two
  real postgres shards with partitioned data: `count(*)` → 8, full row
  concat 1..8, passthrough alongside, dead shard → clean error in 0s.

**Later.** Multi-shard transactions (2PC); extended-protocol scatter
(per-leg statement registry, row streaming as legs complete);
row-ordered merge (ORDER BY push-down + k-way merge); per-pool statement
`max_prepared_statements` hygiene; protocol 3.2 passthrough (secret key
shape mapping in cancel router); row-level COPY reassembly for filtered
COPY streams; active health probes for `EndpointGroup`.

## 7. Known subtleties & risks (encode as tests)

1. **Server codec state gating** — `PgWireMessageServerCodec` decodes
   startup packets only while `DefaultClient.state` is `AwaitingStartup*`;
   the pump must transition state correctly or framing breaks.
2. **ParameterStatus replay** — upstream startup params are swallowed by
   `PgWireClient::connect` into `server_parameters`; downstream clients
   never see them unless we replay at our startup. Filter (`user`-ish keys,
   credentials) but keep `server_version`, `DateStyle`, `client_encoding`…
3. **Cancel concurrency** — `PgWireClient::cancel(&self)` vs pump's `&mut`;
   wrap upstream handle so cancel goes through a channel/task, not borrow.
4. **DISCARD ALL ordering** — checkin must send it and *consume* the reply
   (ReadyForQuery) before parking the conn; on any error destroy.
5. **Extended-protocol half-states** — Bind before Sync, suspended portals:
   passthrough is safe, but *tx-mode detach decisions* must only happen on
   `ReadyForQuery` (never mid-cycle).
6. **Virtual BackendKeyData** — never leak upstream pid/secret downstream;
   map both directions in cancel router.
7. **Pool exhaustion + cancel race** — a queued waiter must be cancellable
   (client disconnect while waiting for checkout).
8. **Protocol pinning** — v1 pins 3.0 downstream via NegotiateProtocolVersion
   and 3.0 upstream (client Config default). Unpinning requires secret-key
   shape mapping (3.2 uses 32-byte keys).

## 8. Test strategy

- **Unit**: state tracker transitions (message sequences → attach/detach
  decisions); pool checkout/checkin lifecycle; interceptor action
  application.
- **Integration (in-repo)**: fake "postgres" implemented with pgwire
  server-api (echo server exposing `pg_backend_pid()`, injectable errors,
  delay control) — no external dependency, deterministic.
- **E2E (optional, env-gated)**: real postgres via docker/nix; psql, pgbench
  simple + extended + transaction modes; JDBC/rust-postgres clients for
  prepared-statement coverage.

## 9. Expected pgwire feedback loop (candidate upstream PRs)

Found while building; #1 is already implemented in the local pgwire
checkout during M0:

1. ✅ **`TCP_NODELAY` on client connections** (`src/tokio/client.rs`,
   `connect_socket`) — without it, pipelined extended-protocol messages
   stall behind delayed ACKs (13 TPS vs 8600 TPS through the proxy).
   pgwire's own test suite passes with the change.
2. `PgWireClient::split()` or owned sink/stream halves (proxy needs
   concurrent cancel + pump).
3. Public `connect_framed` returning the framed upstream *without* running
   startup (for proxies that terminate downstream auth differently, or raw
   byte-forwarding mode).
4. Reusable "startup parameter replay" helper (server-side), since every
   proxy built this way will write one.
5. Possibly: server-side helper for injected-error recovery (AwaitingSync
   emulation) as a public utility.
```

That's the plan. Now let me present a summary in chat.
