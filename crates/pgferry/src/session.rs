//! Per-connection session: downstream startup, upstream lease management,
//! and the bidirectional message pump.
//!
//! Pooling modes:
//!
//! - **Session** (M1): lease an upstream connection for the whole session.
//! - **Transaction** (M3): lease on the first message of a query cycle and
//!   release when the session returns to `ReadyForQuery(Idle)`. A session
//!   state layer makes this transparent: named prepared statements are
//!   tracked per session and re-`Parse`d on re-attach, GUC overrides
//!   (observed via `ParameterStatus`) are replayed with `SET`, and a bare
//!   `Sync` while detached is answered locally.
//!
//! M2's interceptor dispatch is independent of lease state — hooks fire
//! the same in both modes.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::{FutureExt as _, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::Instrument as _;

use pgwire::api::ClientInfo as _;
use pgwire::api::PidSecretKeyGenerator as _;
use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::auth::{finish_authentication, save_startup_parameters_to_metadata};
use pgwire::api::client::ClientInfo as _;
use pgwire::error::ErrorInfo;
use pgwire::messages::extendedquery::{ParseComplete, Sync as FrontendSync};
use pgwire::messages::response::TransactionStatus;
use pgwire::messages::startup::{NegotiateProtocolVersion, SecretKey, Startup};
use pgwire::messages::{
    PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion, SslNegotiationMetaMessage,
};
use pgwire::tokio::server::{MaybeTls, PgWireMessageServerCodec, negotiate_tls};
use pgwire::tokio::tokio_rustls::rustls;

use crate::ProxyShared;
use crate::config::{EndpointConfig, RouteConfig};
use crate::intercept::SessionInterceptor;
use crate::intercept::{
    Action, CycleError, CycleKind, CycleReport, DataRowMut, QueryCycle, RetryDecision, RoutingInfo,
    RowAction, RowSchema, UpstreamError,
};
use crate::pool::{ConnectFactory, Hygiene, PoolKey, PoolMode, UpstreamLease};
use crate::upstream;

/// Downstream socket type after SSL/GSS negotiation.
pub(crate) type Downstream = Framed<MaybeTls, PgWireMessageServerCodec<String>>;

/// Startup parameters relevant to the proxy.
#[derive(Debug)]
pub(crate) struct SessionParams {
    pub(crate) user: String,
    pub(crate) database: Option<String>,
    /// `replication` in the startup packet: streaming replication is not
    /// supported by pgferry.
    replication: bool,
}

impl SessionParams {
    fn from_startup(startup: &Startup) -> Self {
        SessionParams {
            user: startup.parameters.get("user").cloned().unwrap_or_default(),
            database: startup.parameters.get("database").cloned(),
            replication: startup
                .parameters
                .get("replication")
                .map(|v| v == "1" || v == "true" || v == "database" || v == "on")
                .unwrap_or(false),
        }
    }
}

/// Serve one accepted TCP connection to completion. `client_permit` caps
/// concurrent sessions (released when the task ends).
pub async fn run_session(
    socket: TcpStream,
    addr: SocketAddr,
    shared: Arc<ProxyShared>,
    _client_permit: tokio::sync::OwnedSemaphorePermit,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;

    // TLS termination (M4): the acceptor handles the SSL/GSS request dance
    // and upgrades when configured; without one, TLS is refused and the
    // socket stays plaintext.
    let Some(mut downstream) = negotiate_tls::<String>(socket, shared.tls_acceptor.clone()).await?
    else {
        // client attempted direct TLS negotiation but no acceptor configured
        return Ok(());
    };

    // First message is either a Startup or a CancelRequest (the latter on a
    // dedicated connection per protocol).
    let startup = match downstream.next().await {
        Some(Ok(PgWireFrontendMessage::Startup(startup))) => startup,
        Some(Ok(PgWireFrontendMessage::CancelRequest(cancel))) => {
            let cancelled = shared
                .router
                .cancel(cancel.pid, cancel.secret_key.to_bytes());
            tracing::debug!(%addr, pid = cancel.pid, cancelled, "cancel request");
            downstream.close().await?;
            return Ok(());
        }
        Some(Ok(other)) => {
            let error_info = ErrorInfo::new(
                "FATAL".to_owned(),
                "08P01".to_owned(),
                format!("pgferry: expected Startup, got {other:?}"),
            );
            downstream
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            downstream.close().await?;
            return Ok(());
        }
        Some(Err(error)) => {
            tracing::warn!(%addr, %error, "startup decode error");
            return Ok(());
        }
        None => return Ok(()),
    };

    let params = SessionParams::from_startup(&startup);

    // Protocol version policy (M0): pin protocol 3.0 downstream, matching the
    // upstream connection (pgwire client Config defaults to 3.0). Clients
    // asking for a newer minor version (3.2, or the 3.9999 probe) are
    // negotiated down; older major versions are refused.
    match (startup.protocol_number_major, startup.protocol_number_minor) {
        (3, 0) => downstream.set_protocol_version(ProtocolVersion::PROTOCOL3_0),
        (3, minor) => {
            tracing::debug!(%addr, requested_minor = minor, "negotiating down to protocol 3.0");
            // minor-only form of the newest supported minor; understood by
            // both older and PostgreSQL 18+ clients
            downstream
                .send(PgWireBackendMessage::NegotiateProtocolVersion(
                    NegotiateProtocolVersion::new(0, vec![]),
                ))
                .await?;
            downstream.set_protocol_version(ProtocolVersion::PROTOCOL3_0);
        }
        (major, minor) => {
            let error_info = ErrorInfo::new(
                "FATAL".to_owned(),
                "08P01".to_owned(),
                format!("pgferry: unsupported protocol version {major}.{minor}"),
            );
            downstream
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            downstream.close().await?;
            return Ok(());
        }
    }

    // Persist startup parameters into the connection metadata (mirrors
    // pgwire's own server startup handling).
    save_startup_parameters_to_metadata(&mut downstream, &startup);

    // Virtual cancel keys presented to the downstream client. They must be
    // stored on the client BEFORE finish_authentication: it reads
    // `pid_and_secret_key()` to build the BackendKeyData message.
    let (pid, secret_key) = shared.pid_secret.generate(&downstream);
    downstream.set_pid_and_secret_key(pid, secret_key.clone());

    let span = tracing::info_span!(
        "session",
        pid,
        user = %params.user,
        db = params.database.as_deref().unwrap_or(""),
        client = %addr,
        route = %shared.route.name,
        mode = ?shared.pool_config.mode,
    );
    run_session_ready(downstream, shared, addr, pid, secret_key, params, startup)
        .instrument(span)
        .await
}

/// Session continuation once the downstream startup packet is validated:
/// refuse unsupported features, resolve the pool, complete downstream
/// authentication (trust), then pump.
async fn run_session_ready(
    mut downstream: Downstream,
    shared: Arc<ProxyShared>,
    addr: SocketAddr,
    pid: i32,
    secret_key: SecretKey,
    params: SessionParams,
    startup: Startup,
) -> std::io::Result<()> {
    let started = Instant::now();
    tracing::info!("session starting");

    if params.replication {
        send_fatal(
            &mut downstream,
            "0A000",
            "pgferry: replication connections are not supported",
        )
        .await?;
        return Ok(());
    }
    if params.user.is_empty() {
        send_fatal(
            &mut downstream,
            "28000",
            "pgferry: no user specified in startup packet",
        )
        .await?;
        return Ok(());
    }

    // Register the session in the runtime (admin console / metrics).
    let info = Arc::new(crate::runtime::SessionInfo {
        pid,
        addr,
        user: params.user.clone(),
        database: params.database.clone().unwrap_or_default(),
        is_admin: shared
            .admin_database
            .as_ref()
            .is_some_and(|db| params.database.as_deref() == Some(db.as_str())),
        attached: std::sync::atomic::AtomicBool::new(false),
        started: Instant::now(),
    });
    let _session_guard = shared.runtime.register(info.clone());

    // M4: admin console — serve locally instead of leasing upstreams.
    if info.is_admin {
        return crate::admin::run_admin_session(downstream, shared, info, pid, secret_key).await;
    }

    // M1: downstream auth is trust. The upstream connection authenticates
    // for real with the route's credentials.
    let transaction_mode = matches!(shared.pool_config.mode, PoolMode::Transaction);

    // Per-attach state (also drives the initial session-mode attach).
    let mut factories: std::collections::HashMap<String, ConnectFactory> =
        std::collections::HashMap::new();
    let mut hooks = shared.interceptor.clone().map(SessionHooks::new);
    let mut initial_lease: Option<UpstreamLease> = None;
    let mut lease_dirty = false;
    let mut current_endpoint: Option<String> = None;
    let registry = StatementRegistry::default();
    let guc_overrides: GucOverrides = GucOverrides::default();

    // Session mode attaches now (routed) and replays the connection's
    // parameters to the client; transaction mode attaches lazily at the
    // first query cycle and uses the routed pool's baseline snapshot for
    // the downstream handshake.
    let server_parameters = if transaction_mode {
        match baseline_params_routed(&mut hooks, &shared, &params, &mut factories).await {
            Ok(params) => params,
            Err(error) => {
                tracing::error!(%error, "upstream baseline probe failed");
                send_fatal(
                    &mut downstream,
                    "08006",
                    &format!("pgferry: failed to connect to upstream: {error}"),
                )
                .await?;
                return Ok(());
            }
        }
    } else {
        if !ensure_attached(
            &mut downstream,
            &shared,
            &params,
            &mut factories,
            &registry,
            &guc_overrides,
            &startup.parameters,
            &mut hooks,
            &mut initial_lease,
            &mut lease_dirty,
            &mut current_endpoint,
            None,
        )
        .await
        {
            return Ok(());
        }
        info.attached
            .store(true, std::sync::atomic::Ordering::Relaxed);
        initial_lease
            .as_ref()
            .map(|l| l.conn.server_parameters().clone())
            .unwrap_or_default()
    };
    drop(registry);
    drop(guc_overrides);

    // Complete downstream startup: AuthenticationOk, replay of the upstream
    // server parameters, our virtual BackendKeyData and ReadyForQuery.
    let provider = UpstreamParameterProvider {
        parameters: server_parameters,
    };
    finish_authentication(&mut downstream, &provider).await?;
    downstream.set_transaction_status(TransactionStatus::Idle);

    let cancel_rx = shared.router.register(pid, secret_key.to_bytes());

    let outcome = pump(
        &mut downstream,
        &shared,
        &params,
        initial_lease,
        cancel_rx,
        hooks,
        transaction_mode,
        startup.parameters.clone(),
        info,
        shared.shutdown.clone(),
    )
    .await;

    shared.router.unregister(pid, secret_key.to_bytes());
    tracing::info!(
        elapsed = ?started.elapsed(),
        frontend = outcome.frontend,
        backend = outcome.backend,
        end = ?outcome.end,
        "session ended"
    );

    let _ = downstream.close().await;

    // Session end: anything still leased goes back through the full
    // sanitize path (cancel in-flight, rollback, DISCARD ALL).
    match (outcome.end, outcome.lease) {
        (End::ClientGone, Some(lease)) => lease.checkin(outcome.hygiene).await,
        (End::ClientGone, None) => {}
        (End::UpstreamBroken(reason), Some(lease)) => lease.destroy(&reason),
        (End::UpstreamBroken(_), None) => {}
    }

    Ok(())
}

/// Why the pump ended — decides whether the upstream connection is returned
/// to the pool or destroyed.
#[derive(Debug)]
enum End {
    /// Downstream terminated, EOFed, or sent undecodable bytes. The upstream
    /// connection is healthy (checkin sanitizes any leftover state).
    ClientGone,
    /// The upstream connection broke (send error, decode error, or EOF).
    /// Destroy it.
    UpstreamBroken(String),
}

/// Observable outcome of the pump for the session.
struct PumpOutcome {
    frontend: u64,
    backend: u64,
    end: End,
    hygiene: Hygiene,
    /// The session's lease at pump end, if any (transaction mode may end
    /// detached).
    lease: Option<UpstreamLease>,
}

/// Per-session named prepared statement registry (transaction pooling).
///
/// Named statements live on whatever upstream connection the session is
/// currently attached to. On detach the connection is reset; on the next
/// attach the registry is replayed (re-`Parse`d). A client `Parse` of a
/// registered name is answered locally (the statement is already prepared
/// on the connection after replay), like pgbouncer's statement tracking.
#[derive(Debug, Default)]
struct StatementRegistry {
    /// name → (sql, type_oids)
    statements: HashMap<String, (String, Vec<u32>)>,
}

impl StatementRegistry {
    fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    fn record_parse(&mut self, name: &str, sql: String, type_oids: Vec<u32>) {
        self.statements.insert(name.to_owned(), (sql, type_oids));
    }

    fn lookup(&self, name: &str) -> Option<&(String, Vec<u32>)> {
        self.statements.get(name)
    }

    fn record_close(&mut self, name: &str) {
        self.statements.remove(name);
    }

    /// Re-prepare every registered statement on a (freshly clean) upstream
    /// connection: Parse×N then Sync, draining to ReadyForQuery.
    async fn replay(&self, conn: &mut pgwire::tokio::client::PgWireClient) -> Result<(), ()> {
        if self.statements.is_empty() {
            return Ok(());
        }
        for (name, (sql, type_oids)) in &self.statements {
            let parse = pgwire::messages::extendedquery::Parse::new(
                Some(name.clone()),
                sql.clone(),
                type_oids.clone(),
            );
            conn.feed(PgWireFrontendMessage::Parse(parse))
                .await
                .map_err(|_| ())?;
        }
        conn.send(PgWireFrontendMessage::Sync(FrontendSync::new()))
            .await
            .map_err(|_| ())?;

        // expect ParseComplete × N, then ReadyForQuery
        while let Some(item) = conn.next().await {
            match item.map_err(|_| ())? {
                PgWireBackendMessage::ReadyForQuery(ready) => {
                    conn.set_transaction_status(ready.status);
                    return Ok(());
                }
                PgWireBackendMessage::ParseComplete(_) => {}
                PgWireBackendMessage::ParameterStatus(ps) => {
                    conn.set_server_parameter(ps.name, ps.value);
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    let info = pgwire::error::ErrorInfo::from(error);
                    tracing::warn!(
                        code = %info.code,
                        message = %info.message,
                        "statement replay failed on re-attach"
                    );
                    return Err(());
                }
                _ => {}
            }
        }
        Err(())
    }
}

/// Per-session GUC overrides (transaction pooling), captured from
/// `ParameterStatus` messages so a `SET` survives re-attach to a different
/// connection. Reported GUCs only — see the `PoolMode::Transaction` docs.
type GucOverrides = HashMap<String, String>;

/// Per-session interceptor state: the (type-erased) interceptor, its
/// per-session `Ctx`, and cached capability flags.
struct SessionHooks {
    interceptor: Arc<dyn SessionInterceptor>,
    ctx: Box<dyn std::any::Any + Send>,
    wants_rows: bool,
    adjust_counts: bool,
    /// Current result-set schema, tracked from RowDescription when
    /// `wants_rows` is set.
    schema: RowSchema,
    /// Active cycle telemetry.
    cycle: CycleTracking,
}

impl SessionHooks {
    fn new(interceptor: Arc<dyn SessionInterceptor>) -> Self {
        let wants_rows = interceptor.wants_rows();
        let adjust_counts = interceptor.adjust_command_counts();
        SessionHooks {
            ctx: interceptor.new_session_ctx(),
            interceptor,
            wants_rows,
            adjust_counts,
            schema: RowSchema::default(),
            cycle: CycleTracking::default(),
        }
    }
}

struct CycleTracking {
    active: bool,
    sql: Option<String>,
    kind: CycleKind,
    started: Instant,
    tag: Option<String>,
    rows: u64,
    /// Rows dropped by on_row since the last CommandComplete (for count
    /// adjustment).
    dropped_pending: u64,
    /// Total rows dropped in the cycle (for the report).
    dropped_total: u64,
    error: Option<CycleError>,
}

impl Default for CycleTracking {
    fn default() -> Self {
        CycleTracking {
            active: false,
            sql: None,
            kind: CycleKind::Simple,
            started: Instant::now(),
            tag: None,
            rows: 0,
            dropped_pending: 0,
            dropped_total: 0,
            error: None,
        }
    }
}

impl CycleTracking {
    fn start(&mut self, sql: Option<String>, kind: CycleKind) {
        self.active = true;
        self.sql = sql;
        self.kind = kind;
        self.started = Instant::now();
        self.tag = None;
        self.rows = 0;
        self.dropped_pending = 0;
        self.dropped_total = 0;
        self.error = None;
    }

    fn finish(&mut self) -> CycleReport {
        let report = CycleReport {
            sql: self.sql.take(),
            kind: std::mem::replace(&mut self.kind, CycleKind::Simple),
            tag: self.tag.take(),
            rows: self.rows,
            rows_dropped: self.dropped_total,
            error: self.error.take(),
            latency: self.started.elapsed(),
        };
        self.active = false;
        report
    }
}

/// Transaction-mode startup parameters: route once (the `upstream()` hook),
/// probe that endpoint's pool for its baseline snapshot, retrying through
/// the error policy like an attach.
async fn baseline_params_routed(
    hooks: &mut Option<SessionHooks>,
    shared: &Arc<ProxyShared>,
    params: &SessionParams,
    factories: &mut std::collections::HashMap<String, ConnectFactory>,
) -> Result<BTreeMap<String, String>, String> {
    let attempts = shared.route.endpoints.len().saturating_mul(2).max(2);
    for _ in 0..attempts {
        let endpoint_id = dispatch_upstream(hooks, &shared.route.endpoints, params, None).await;
        let Some(endpoint) = shared.route.endpoint(&endpoint_id) else {
            return Err(format!("unknown upstream endpoint {endpoint_id:?}"));
        };
        let factory = factories
            .entry(endpoint.id.clone())
            .or_insert_with(|| make_factory_for(endpoint, &shared.route, params))
            .clone();
        let key = PoolKey {
            route: shared.route.name.clone(),
            endpoint: endpoint.id.clone(),
            user: params.user.clone(),
            database: params.database.clone(),
        };
        let pool = shared
            .pools
            .get_or_create(key, shared.pool_config.clone(), factory);
        match pool.baseline_params().await {
            Ok(map) => {
                dispatch_on_upstream_connected(hooks, &endpoint.id).await;
                return Ok(map);
            }
            Err(error) => {
                let decision = dispatch_on_upstream_error(
                    hooks,
                    UpstreamError::Connect {
                        endpoint: endpoint.id.clone(),
                        message: error.to_string(),
                    },
                )
                .await;
                if matches!(decision, RetryDecision::Close) {
                    return Err(error.to_string());
                }
            }
        }
    }
    Err("all upstream endpoints failed".to_owned())
}

/// Attach a lease for the session's next query cycle, through the routing
/// hook: `upstream()` picks an endpoint, its pool is resolved (created on
/// first use), and the connection carries the session state (startup
/// parameters, GUC overrides, named-statement registry). Connect/replay
/// failures consult `on_upstream_error` — `Relink` re-enters routing
/// (failover), `Close` (or exhausting attempts) is fatal for the session.
#[allow(clippy::too_many_arguments)]
async fn ensure_attached(
    downstream: &mut Downstream,
    shared: &Arc<ProxyShared>,
    params: &SessionParams,
    factories: &mut std::collections::HashMap<String, ConnectFactory>,
    registry: &StatementRegistry,
    guc_overrides: &GucOverrides,
    startup_parameters: &BTreeMap<String, String>,
    hooks: &mut Option<SessionHooks>,
    lease_slot: &mut Option<UpstreamLease>,
    lease_dirty: &mut bool,
    current_endpoint: &mut Option<String>,
    sql_hint: Option<&str>,
) -> bool {
    if lease_slot.is_some() {
        return true;
    }

    let attempts = shared.route.endpoints.len().saturating_mul(2).max(2);
    for _ in 0..attempts {
        let endpoint_id = dispatch_upstream(hooks, &shared.route.endpoints, params, sql_hint).await;
        let Some(endpoint) = shared.route.endpoint(&endpoint_id) else {
            tracing::error!(%endpoint_id, "routing hook returned an unknown endpoint");
            send_attach_fatal(
                downstream,
                &format!("pgferry: unknown upstream endpoint {endpoint_id:?}"),
            )
            .await;
            return false;
        };

        let factory = factories
            .entry(endpoint.id.clone())
            .or_insert_with(|| make_factory_for(endpoint, &shared.route, params))
            .clone();
        let key = PoolKey {
            route: shared.route.name.clone(),
            endpoint: endpoint.id.clone(),
            user: params.user.clone(),
            database: params.database.clone(),
        };
        let pool = shared
            .pools
            .get_or_create(key, shared.pool_config.clone(), factory);

        let mut lease = match pool.checkout().await {
            Ok(lease) => lease,
            Err(error) => {
                let decision = dispatch_on_upstream_error(
                    hooks,
                    UpstreamError::Connect {
                        endpoint: endpoint.id.clone(),
                        message: error.to_string(),
                    },
                )
                .await;
                match decision {
                    RetryDecision::Close => {
                        send_attach_fatal(
                            downstream,
                            &format!("pgferry: failed to connect to upstream: {error}"),
                        )
                        .await;
                        return false;
                    }
                    RetryDecision::Relink => continue,
                }
            }
        };

        // per-session startup parameters (application_name, client_encoding)
        if let Err(error) = lease.sync_startup_parameters(startup_parameters).await {
            tracing::warn!(%error, "attach: startup parameter sync failed");
            lease.destroy("attach: parameter sync failed");
            let decision = dispatch_on_upstream_error(
                hooks,
                UpstreamError::Connect {
                    endpoint: endpoint.id.clone(),
                    message: format!("parameter sync: {error}"),
                },
            )
            .await;
            if matches!(decision, RetryDecision::Close) {
                send_attach_fatal(
                    downstream,
                    &format!("pgferry: failed to prepare upstream connection: {error}"),
                )
                .await;
                return false;
            }
            continue;
        }

        // replay GUC overrides (`SET`s from earlier cycles — M3 state that
        // also survives failover relinks)
        let mut guc_failure = None;
        for (name, value) in guc_overrides {
            let current = lease
                .conn
                .server_parameters()
                .get(name)
                .cloned()
                .unwrap_or_default();
            if &current != value {
                let escaped = value.replace('\'', "''");
                if lease
                    .conn
                    .simple_query(
                        pgwire::api::client::query::DefaultSimpleQueryHandler::new(),
                        &format!("SET {name} = '{escaped}'"),
                    )
                    .await
                    .is_err()
                {
                    tracing::warn!(%name, "attach: GUC replay failed");
                    guc_failure = Some(name.clone());
                    break;
                }
                *lease_dirty = true;
            }
        }
        if let Some(name) = guc_failure {
            lease.destroy("attach: GUC replay failed");
            let decision = dispatch_on_upstream_error(
                hooks,
                UpstreamError::Connect {
                    endpoint: endpoint.id.clone(),
                    message: format!("GUC replay: SET {name}"),
                },
            )
            .await;
            if matches!(decision, RetryDecision::Close) {
                send_attach_fatal(
                    downstream,
                    &format!("pgferry: failed to replay session state: SET {name}"),
                )
                .await;
                return false;
            }
            continue;
        }

        // replay named prepared statements (registry — also M3 state,
        // ungated in M5 so session-mode relinks keep statements too)
        if !registry.is_empty() && registry.replay(&mut lease.conn).await.is_err() {
            lease.destroy("attach: statement replay failed");
            let decision = dispatch_on_upstream_error(
                hooks,
                UpstreamError::Connect {
                    endpoint: endpoint.id.clone(),
                    message: "prepared statement replay failed".to_owned(),
                },
            )
            .await;
            if matches!(decision, RetryDecision::Close) {
                send_attach_fatal(downstream, "pgferry: failed to replay prepared statements")
                    .await;
                return false;
            }
            continue;
        }
        if !registry.is_empty() {
            *lease_dirty = true;
        }

        dispatch_on_upstream_connected(hooks, &endpoint.id).await;
        tracing::debug!(upstream_pid = lease.upstream_pid(), endpoint = %endpoint.id, "upstream attached");
        *current_endpoint = Some(endpoint.id.clone());
        *lease_slot = Some(lease);
        return true;
    }

    send_attach_fatal(downstream, "pgferry: all upstream endpoints failed").await;
    false
}

async fn send_attach_fatal(downstream: &mut Downstream, message: &str) {
    use futures::SinkExt as _;
    let info = ErrorInfo::new("FATAL".to_owned(), "08006".to_owned(), message.to_owned());
    let _ = downstream
        .send(PgWireBackendMessage::ErrorResponse(info.into()))
        .await;
}

/// Route via the interceptor's `upstream()` hook (or the first endpoint
/// when no interceptor is registered).
async fn dispatch_upstream(
    hooks: &mut Option<SessionHooks>,
    endpoints: &[EndpointConfig],
    params: &SessionParams,
    sql: Option<&str>,
) -> String {
    let info = RoutingInfo::new(endpoints, &params.user, params.database.as_deref(), sql);
    match hooks {
        Some(h) => h.interceptor.upstream(h.ctx.as_mut(), &info).await,
        None => info.default_endpoint().to_owned(),
    }
}

async fn dispatch_on_upstream_error(
    hooks: &mut Option<SessionHooks>,
    error: UpstreamError,
) -> RetryDecision {
    match hooks {
        Some(h) => {
            h.interceptor
                .on_upstream_error(h.ctx.as_mut(), &error)
                .await
        }
        None => RetryDecision::Relink,
    }
}

async fn dispatch_on_upstream_connected(hooks: &mut Option<SessionHooks>, endpoint: &str) {
    if let Some(h) = hooks {
        h.interceptor
            .on_upstream_connected(h.ctx.as_mut(), endpoint)
            .await;
    }
}

/// Answer the interrupted cycle after an upstream connection was lost:
/// `ErrorResponse(08006)` and — for extended-protocol cycles — swallow
/// until the client's `Sync`, then `ReadyForQuery`. Returns false when the
/// downstream is gone.
async fn synthesize_upstream_lost(
    downstream: &mut Downstream,
    hooks: &mut Option<SessionHooks>,
    extended_cycle: bool,
    swallow_until_sync: &mut bool,
) -> bool {
    use futures::SinkExt as _;
    let info = ErrorInfo::new(
        "ERROR".to_owned(),
        "08006".to_owned(),
        "upstream connection lost".to_owned(),
    );
    if downstream
        .send(PgWireBackendMessage::ErrorResponse(info.into()))
        .await
        .is_err()
    {
        return false;
    }
    if extended_cycle {
        *swallow_until_sync = true;
    } else {
        if send_ready_for_query(downstream, TransactionStatus::Idle)
            .await
            .is_err()
        {
            return false;
        }
        downstream.set_transaction_status(TransactionStatus::Idle);
        if let Some(h) = hooks.as_mut()
            && h.cycle.active
        {
            let report = h.cycle.finish();
            h.cycle.error = Some(CycleError {
                code: "08006".to_owned(),
                message: "upstream connection lost".to_owned(),
            });
            h.interceptor.on_cycle_end(h.ctx.as_mut(), &report).await;
        }
    }
    true
}

/// Detach: the session is at ReadyForQuery(Idle) with nothing in flight.
/// Returns the connection to the pool (DISCARD ALL iff the lease was
/// dirtied). Transaction mode only. Returns whether a detach happened.
async fn detach_if_idle(
    lease_slot: &mut Option<UpstreamLease>,
    status: TransactionStatus,
    lease_dirty: bool,
    transaction_mode: bool,
) -> bool {
    if !transaction_mode {
        return false;
    }
    if !matches!(status, TransactionStatus::Idle) {
        return false;
    }
    if let Some(lease) = lease_slot.take() {
        let upstream_pid = lease.upstream_pid();
        lease.checkin_detached(lease_dirty).await;
        tracing::debug!(upstream_pid, dirty = lease_dirty, "upstream detached");
        return true;
    }
    false
}

/// The bidirectional message pump.
#[allow(clippy::too_many_arguments)]
async fn pump(
    downstream: &mut Downstream,
    shared: &Arc<ProxyShared>,
    params: &SessionParams,
    mut lease: Option<UpstreamLease>,
    mut cancel_rx: tokio::sync::mpsc::Receiver<()>,
    mut hooks: Option<SessionHooks>,
    transaction_mode: bool,
    startup_parameters: BTreeMap<String, String>,
    info: Arc<crate::runtime::SessionInfo>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> PumpOutcome {
    // per-endpoint connect factories + the currently attached endpoint
    let mut factories: std::collections::HashMap<String, ConnectFactory> =
        std::collections::HashMap::new();
    let mut current_endpoint: Option<String> = lease.as_ref().map(|_| {
        shared
            .route
            .endpoints
            .first()
            .map(|e| e.id.clone())
            .unwrap_or_default()
    });
    // frontend messages forwarded since the last ReadyForQuery (mid-cycle
    // detection for failover decisions)
    let mut fed_since_rfq: u32 = 0;
    // whether the in-flight cycle is extended-protocol (swallow-until-Sync
    // recovery after synthesized errors)
    let mut cycle_is_extended = false;
    let mut frontend: u64 = 0; // client → server messages forwarded
    let mut backend: u64 = 0; // server → client messages forwarded
    let mut hygiene = Hygiene::default();
    let end;

    // transaction-mode session state
    let mut registry = StatementRegistry::default();
    let mut guc_overrides: GucOverrides = HashMap::new();
    let mut lease_dirty = false;
    // injected-error recovery (interceptor deny, duplicate-statement deny):
    // swallow frontend messages until Sync. Pump-local: works with or
    // without an interceptor.
    let mut swallow_until_sync = false;
    // graceful shutdown: finish the current query cycle, then end
    let mut draining = false;

    'pump: loop {
        tokio::select! {
            msg = downstream.next() => {
                match msg {
                    None => {
                        tracing::debug!("downstream EOF");
                        end = End::ClientGone;
                        break 'pump;
                    }
                    Some(Err(error)) => {
                        tracing::warn!(%error, "downstream decode error");
                        end = End::ClientGone;
                        break 'pump;
                    }
                    Some(Ok(PgWireFrontendMessage::Terminate(_))) => {
                        tracing::debug!("downstream terminate");
                        end = End::ClientGone;
                        break 'pump;
                    }
                    Some(Ok(mut msg)) => {
                        tracing::trace!(kind = msg_kind_frontend(&msg), "→ upstream");

                        // ---- injected-error recovery: swallow until Sync ----
                        if swallow_until_sync {
                            if let PgWireFrontendMessage::Sync(_) = msg {
                                swallow_until_sync = false;
                                let status = lease
                                    .as_ref()
                                    .map(|l| l.conn.transaction_status())
                                    .unwrap_or(TransactionStatus::Idle);
                                if send_ready_for_query(downstream, status).await.is_err() {
                                    end = End::ClientGone;
                                    break 'pump;
                                }
                                if let Some(h) = hooks.as_mut() {
                                    let report = h.cycle.finish();
                                    h.interceptor.on_cycle_end(h.ctx.as_mut(), &report).await;
                                }
                            } else {
                                tracing::trace!(
                                    kind = msg_kind_frontend(&msg),
                                    "swallowed (deny recovery)"
                                );
                            }
                            continue;
                        }

                        // ---- local protocol handling (transaction mode) ----
                        // A Sync while detached has nothing to synchronize:
                        // answer it locally.
                        if transaction_mode
                            && lease.is_none()
                            && let PgWireFrontendMessage::Sync(_) = msg
                        {
                            if send_ready_for_query(
                                downstream,
                                downstream.transaction_status(),
                            )
                            .await
                            .is_err()
                            {
                                end = End::ClientGone;
                                break 'pump;
                            }
                            if let Some(h) = hooks.as_mut()
                                && h.cycle.active
                            {
                                let report = h.cycle.finish();
                                h.interceptor.on_cycle_end(h.ctx.as_mut(), &report).await;
                            }
                            continue;
                        }
                        // Flush while detached: nothing to flush.
                        if transaction_mode
                            && lease.is_none()
                            && let PgWireFrontendMessage::Flush(_) = msg
                        {
                            continue;
                        }
                        // Close of a named statement while detached: the
                        // statement is not on any connection; answer
                        // CloseComplete locally and drop it from the
                        // registry so it is not replayed.
                        if transaction_mode
                            && lease.is_none()
                            && let PgWireFrontendMessage::Close(close) = &msg
                        {
                            if let Some(name) = &close.name {
                                registry.record_close(name);
                            }
                            let ok = PgWireBackendMessage::CloseComplete(
                                pgwire::messages::extendedquery::CloseComplete::new(),
                            );
                            if downstream.send(ok).await.is_err() {
                                end = End::ClientGone;
                                break 'pump;
                            }
                            continue;
                        }

                        // ---- on_query phase (Query / Parse) ----
                        let cycle_input = cycle_input_of(&msg);
                        if let Some(kind) = cycle_input.as_ref().map(|c| c.kind.clone()) {
                            cycle_is_extended = matches!(kind, CycleKind::Extended { .. });
                        }
                        if let Some(input) = cycle_input.clone()
                            && hooks.is_some()
                        {
                            let kind = input.kind.clone();
                            let hooks_ref = hooks.as_mut().unwrap();
                            let mut cycle =
                                QueryCycle::new(input.sql.clone(), kind.clone());
                            let action = hooks_ref
                                .interceptor
                                .on_query(hooks_ref.ctx.as_mut(), &mut cycle)
                                .await;
                            let rewritten = cycle.take_rewritten();
                            match action {
                                Action::Forward => {
                                    apply_rewrite(&mut msg, rewritten);
                                    hooks_ref.cycle.start(cycle_sql_of(&msg), kind);
                                }
                                Action::Deny(info) => {
                                    hooks_ref.cycle.start(Some(input.sql), kind.clone());
                                    hooks_ref.cycle.error = Some(CycleError::from_info(&info));
                                    let response =
                                        PgWireBackendMessage::ErrorResponse((*info).into());
                                    if downstream.send(response).await.is_err() {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                    match kind {
                                        CycleKind::Simple => {
                                            if send_ready_for_query(
                                                downstream,
                                                lease
                                                    .as_ref()
                                                    .map(|l| l.conn.transaction_status())
                                                    .unwrap_or(TransactionStatus::Idle),
                                            )
                                            .await
                                            .is_err()
                                            {
                                                end = End::ClientGone;
                                                break 'pump;
                                            }
                                            let report = hooks_ref.cycle.finish();
                                            hooks_ref
                                                .interceptor
                                                .on_cycle_end(hooks_ref.ctx.as_mut(), &report)
                                                .await;
                                        }
                                        CycleKind::Extended { .. } => {
                                            swallow_until_sync = true;
                                        }
                                    }
                                    continue;
                                }
                                Action::Scatter(request) => {
                                    // restrictions: simple protocol, idle
                                    // transaction state
                                    let mut scatter_error = None;
                                    if !matches!(kind, CycleKind::Simple) {
                                        scatter_error = Some(ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            "0A000".to_owned(),
                                            "pgferry scatter: simple protocol only".to_owned(),
                                        ));
                                    } else if !matches!(
                                        downstream.transaction_status(),
                                        TransactionStatus::Idle
                                    ) {
                                        scatter_error = Some(ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            "0A000".to_owned(),
                                            "pgferry scatter: not allowed inside a transaction"
                                                .to_owned(),
                                        ));
                                    }

                                    hooks_ref.cycle.start(Some(input.sql), kind.clone());
                                    let result = match scatter_error {
                                        Some(info) => Err(crate::scatter::ScatterFailure {
                                            error: Box::new(info),
                                            session_lease_broken: false,
                                        }),
                                        None => {
                                            crate::scatter::execute_scatter(
                                                downstream,
                                                shared,
                                                params,
                                                &mut factories,
                                                &request,
                                                &mut lease,
                                                &mut current_endpoint,
                                                &startup_parameters,
                                            )
                                            .await
                                        }
                                    };

                                    match result {
                                        Ok(rows) => {
                                            hooks_ref.cycle.rows = rows;
                                            hooks_ref.cycle.tag =
                                                Some(format!("{} {rows}", request.tag));
                                            if send_ready_for_query(
                                                downstream,
                                                downstream.transaction_status(),
                                            )
                                            .await
                                            .is_err()
                                            {
                                                end = End::ClientGone;
                                                break 'pump;
                                            }
                                            let report = hooks_ref.cycle.finish();
                                            hooks_ref
                                                .interceptor
                                                .on_cycle_end(hooks_ref.ctx.as_mut(), &report)
                                                .await;
                                        }
                                        Err(failure) => {
                                            if failure.session_lease_broken
                                                && let Some(l) = lease.take()
                                            {
                                                l.destroy("scatter: session shard failed");
                                            }
                                            current_endpoint = None;
                                            info.attached.store(
                                                false,
                                                std::sync::atomic::Ordering::Relaxed,
                                            );
                                            hooks_ref.cycle.error =
                                                Some(CycleError::from_info(&failure.error));
                                            if downstream
                                                .send(PgWireBackendMessage::ErrorResponse(
                                                    (*failure.error).into(),
                                                ))
                                                .await
                                                .is_err()
                                            {
                                                end = End::ClientGone;
                                                break 'pump;
                                            }
                                            if send_ready_for_query(
                                                downstream,
                                                TransactionStatus::Idle,
                                            )
                                            .await
                                            .is_err()
                                            {
                                                end = End::ClientGone;
                                                break 'pump;
                                            }
                                            downstream
                                                .set_transaction_status(TransactionStatus::Idle);
                                            let report = hooks_ref.cycle.finish();
                                            hooks_ref
                                                .interceptor
                                                .on_cycle_end(hooks_ref.ctx.as_mut(), &report)
                                                .await;
                                        }
                                    }
                                    continue;
                                }
                                Action::Reply(messages) => {
                                    hooks_ref.cycle.start(Some(input.sql), kind.clone());
                                    for m in messages {
                                        match &m {
                                            PgWireBackendMessage::DataRow(_) => {
                                                hooks_ref.cycle.rows += 1;
                                            }
                                            PgWireBackendMessage::CommandComplete(cc) => {
                                                hooks_ref.cycle.tag = Some(cc.tag.clone());
                                            }
                                            _ => {}
                                        }
                                        if downstream.send(m).await.is_err() {
                                            end = End::ClientGone;
                                            break 'pump;
                                        }
                                    }
                                    if send_ready_for_query(
                                        downstream,
                                        lease
                                            .as_ref()
                                            .map(|l| l.conn.transaction_status())
                                            .unwrap_or(TransactionStatus::Idle),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                    match kind {
                                        CycleKind::Simple => {
                                            let report = hooks_ref.cycle.finish();
                                            hooks_ref
                                                .interceptor
                                                .on_cycle_end(hooks_ref.ctx.as_mut(), &report)
                                                .await;
                                        }
                                        CycleKind::Extended { .. } => {
                                            swallow_until_sync = true;
                                        }
                                    }
                                    continue;
                                }
                                Action::Close => {
                                    end = End::ClientGone;
                                    break 'pump;
                                }
                            }
                        }

                        // ---- named-statement registry (transaction mode) ----
                        // A Parse of a registered name is already prepared
                        // on the attached connection (replayed at attach);
                        // answer ParseComplete locally instead of erroring
                        // with "prepared statement already exists".
                        //
                        // Ordering matters: the new statement is recorded
                        // only AFTER ensure_attached — the attach replays the
                        // registry, and a new statement must not be replayed
                        // and then forwarded twice (42P05 "already exists").
                        // (statement tracking is mode-agnostic since M5:
                        // session-mode relinks replay the registry too)
                        let mut pending_parse: Option<(String, String, Vec<u32>)> = None;
                        if let PgWireFrontendMessage::Parse(parse) = &msg
                            && let Some(name) = &parse.name
                            && !name.is_empty()
                        {
                            match registry.lookup(name) {
                                Some((known_sql, _)) if known_sql == &parse.query => {
                                    let ok = PgWireBackendMessage::ParseComplete(ParseComplete::new());
                                    if downstream.send(ok).await.is_err() {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                    continue;
                                }
                                Some(_) => {
                                    // same name, different SQL: protocol error
                                    let info = ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "42P05".to_owned(),
                                        format!(
                                            "prepared statement \"{name}\" already exists (pgferry)"
                                        ),
                                    );
                                    if downstream
                                        .send(PgWireBackendMessage::ErrorResponse(info.into()))
                                        .await
                                        .is_err()
                                    {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                    swallow_until_sync = true;
                                    continue;
                                }
                                None => {
                                    pending_parse = Some((
                                        name.clone(),
                                        parse.query.clone(),
                                        parse.type_oids.clone(),
                                    ));
                                }
                            }
                        }

                        // ---- attach (transaction mode, lazy; routed) ----
                        let sql_hint = cycle_input
                            .as_ref()
                            .map(|c| c.sql.clone())
                            .or_else(|| bind_statement_sql(&msg, &registry));
                        if !ensure_attached(
                            downstream,
                            shared,
                            params,
                            &mut factories,
                            &registry,
                            &guc_overrides,
                            &startup_parameters,
                            &mut hooks,
                            &mut lease,
                            &mut lease_dirty,
                            &mut current_endpoint,
                            sql_hint.as_deref(),
                        )
                        .await
                        {
                            end = End::ClientGone;
                            break 'pump;
                        }

                        // record the new statement (it forwards next; the
                        // registry replays it on future attaches)
                        if let Some((name, sql, type_oids)) = pending_parse {
                            registry.record_parse(&name, sql, type_oids);
                            lease_dirty = true;
                        }
                        info.attached.store(true, std::sync::atomic::Ordering::Relaxed);

                        // ---- raw frontend hook ----
                        let action = match hooks.as_mut() {
                            Some(h) => {
                                h.interceptor
                                    .on_frontend(h.ctx.as_mut(), &mut msg)
                                    .await
                            }
                            None => Action::Forward,
                        };
                        match action {
                            Action::Forward => {}
                            Action::Reply(messages) => {
                                for m in messages {
                                    if downstream.send(m).await.is_err() {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                }
                                continue;
                            }
                            Action::Deny(_) | Action::Scatter(_) => {
                                tracing::warn!(
                                    "Deny/Scatter from on_frontend is not supported; closing session"
                                );
                                end = End::ClientGone;
                                break 'pump;
                            }
                            Action::Close => {
                                end = End::ClientGone;
                                break 'pump;
                            }
                        }

                        // These messages start a server-side response cycle
                        // (ended by ReadyForQuery); if the session ends
                        // before that arrives, a query may still be running
                        // and checkin must cancel it.
                        if matches!(
                            msg,
                            PgWireFrontendMessage::Query(_)
                                | PgWireFrontendMessage::Sync(_)
                                | PgWireFrontendMessage::Flush(_)
                        ) {
                            hygiene.awaiting_response = true;
                        }
                        // Flush only on messages that can trigger a response
                        // (Query/Sync/Flush/Terminate/Copy*): pipelined
                        // Parse/Bind/Describe/Execute are buffered into one
                        // write. Responses only ever follow the flushing
                        // kinds, so this is transparent to the client.
                        let sent = if triggers_response(&msg) {
                            lease.as_mut().unwrap().conn.send(msg).await
                        } else {
                            lease.as_mut().unwrap().conn.feed(msg).await
                        };
                        if sent.is_err() {
                            // failover: consult the error policy; Relink
                            // destroys the lease and continues the session
                            let endpoint = current_endpoint.clone().unwrap_or_default();
                            let decision = dispatch_on_upstream_error(
                                &mut hooks,
                                UpstreamError::ConnectionLost {
                                    endpoint,
                                    message: "send failed".to_owned(),
                                },
                            )
                            .await;
                            match decision {
                                RetryDecision::Close => {
                                    end = End::UpstreamBroken("upstream send failed".to_owned());
                                    break 'pump;
                                }
                                RetryDecision::Relink => {
                                    tracing::warn!("upstream send failed; relinking");
                                    if let Some(l) = lease.take() {
                                        l.destroy("send failed");
                                    }
                                    current_endpoint = None;
                                    info.attached
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    if hygiene.awaiting_response || fed_since_rfq > 0 {
                                        if !synthesize_upstream_lost(
                                            downstream,
                                            &mut hooks,
                                            cycle_is_extended,
                                            &mut swallow_until_sync,
                                        )
                                        .await
                                        {
                                            end = End::ClientGone;
                                            break 'pump;
                                        }
                                        hygiene.awaiting_response = false;
                                        fed_since_rfq = 0;
                                        frontend += 1; // the failed message was answered
                                    } else {
                                        // first pipelined message of a cycle:
                                        // a Parse is healed by registry replay
                                        // on the next attach
                                        frontend += 1;
                                    }
                                    continue;
                                }
                            }
                        }
                        fed_since_rfq += 1;
                        frontend += 1;
                    }
                }
            }
            msg = async {
                match lease.as_mut() {
                    Some(l) => Some(l.conn.next().await),
                    None => None,
                }
            }, if lease.is_some() => {
                let Some(msg) = msg else { continue };
                match msg {
                    None => {
                        // failover: connection lost; consult the error
                        // policy. Relink destroys the lease, answers an
                        // interrupted query with a synthesized error, and
                        // keeps the session alive.
                        let endpoint = current_endpoint.clone().unwrap_or_default();
                        let decision = dispatch_on_upstream_error(
                            &mut hooks,
                            UpstreamError::ConnectionLost {
                                endpoint,
                                message: "upstream closed unexpectedly".to_owned(),
                            },
                        )
                        .await;
                        match decision {
                            RetryDecision::Close => {
                                end = End::UpstreamBroken("upstream EOF".to_owned());
                                break 'pump;
                            }
                            RetryDecision::Relink => {
                                tracing::warn!("upstream lost; relinking");
                                if let Some(l) = lease.take() {
                                    l.destroy("upstream lost");
                                }
                                current_endpoint = None;
                                info.attached
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                if hygiene.awaiting_response
                                    && !synthesize_upstream_lost(
                                        downstream,
                                        &mut hooks,
                                        cycle_is_extended,
                                        &mut swallow_until_sync,
                                    )
                                    .await
                                {
                                    end = End::ClientGone;
                                    break 'pump;
                                }
                                hygiene.awaiting_response = false;
                                fed_since_rfq = 0;
                                continue 'pump;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        tracing::warn!(%error, "upstream decode error");
                        end = End::UpstreamBroken(format!("upstream error: {error}"));
                        break 'pump;
                    }
                    Some(Ok(mut msg)) => {
                        let upstream_status;
                        {
                            let conn = &mut lease.as_mut().unwrap().conn;
                            // Track session state for pool hygiene.
                            match &msg {
                                PgWireBackendMessage::ReadyForQuery(ready) => {
                                    conn.set_transaction_status(ready.status);
                                    downstream.set_transaction_status(ready.status);
                                    hygiene.awaiting_response = false;
                                    hygiene.copy = crate::pool::CopyCleanup::None;
                                    upstream_status = ready.status;
                                    fed_since_rfq = 0;
                                }
                                PgWireBackendMessage::ParameterStatus(ps) => {
                                    conn.set_server_parameter(
                                        ps.name.clone(),
                                        ps.value.clone(),
                                    );
                                    // track SETs so they survive re-attach
                                    // (transaction detach and failover relink)
                                    guc_overrides.insert(ps.name.clone(), ps.value.clone());
                                    if transaction_mode {
                                        lease_dirty = true;
                                    }
                                    upstream_status = conn.transaction_status();
                                }
                                PgWireBackendMessage::CopyInResponse(_)
                                | PgWireBackendMessage::CopyBothResponse(_) => {
                                    hygiene.copy = crate::pool::CopyCleanup::ClientOwesCopy;
                                    upstream_status = conn.transaction_status();
                                }
                                PgWireBackendMessage::CopyOutResponse(_) => {
                                    hygiene.copy = crate::pool::CopyCleanup::ServerStreamsCopy;
                                    upstream_status = conn.transaction_status();
                                }
                                _ => {
                                    upstream_status = conn.transaction_status();
                                }
                            }
                        }

                        // ---- schema / telemetry / on_row phase ----
                        let mut row_dropped = false;
                        if let Some(hooks_ref) = hooks.as_mut() {
                            match &msg {
                                PgWireBackendMessage::RowDescription(rd) => {
                                    if hooks_ref.wants_rows {
                                        hooks_ref.schema =
                                            RowSchema::from_row_description(rd);
                                    }
                                }
                                PgWireBackendMessage::CommandComplete(cc) => {
                                    hooks_ref.cycle.tag = Some(cc.tag.clone());
                                }
                                PgWireBackendMessage::ErrorResponse(er) => {
                                    hooks_ref.cycle.error =
                                        Some(cycle_error_of(er));
                                }
                                _ => {}
                            }

                            if hooks_ref.wants_rows
                                && let PgWireBackendMessage::DataRow(dr) = &mut msg
                            {
                                match DataRowMut::parse(dr) {
                                    Ok(mut row) => {
                                        let action = hooks_ref
                                            .interceptor
                                            .on_row(
                                                hooks_ref.ctx.as_mut(),
                                                &hooks_ref.schema,
                                                &mut row,
                                            )
                                            .await;
                                        match action {
                                            RowAction::Keep => {
                                                row.apply();
                                                hooks_ref.cycle.rows += 1;
                                            }
                                            RowAction::Drop => {
                                                row_dropped = true;
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        tracing::debug!(
                                            %error,
                                            "DataRow parse failed; forwarding raw"
                                        );
                                        hooks_ref.cycle.rows += 1;
                                    }
                                }
                            } else if let PgWireBackendMessage::DataRow(_) = &msg {
                                hooks_ref.cycle.rows += 1;
                            }

                            if row_dropped {
                                hooks_ref.cycle.dropped_total += 1;
                                hooks_ref.cycle.dropped_pending += 1;
                            }

                            // ---- optional CommandComplete count adjustment ----
                            if let PgWireBackendMessage::CommandComplete(cc) = &mut msg
                                && hooks_ref.adjust_counts
                                && hooks_ref.cycle.dropped_pending > 0
                            {
                                cc.tag = adjust_tag(&cc.tag, hooks_ref.cycle.dropped_pending);
                            }
                            if let PgWireBackendMessage::CommandComplete(_) = &msg {
                                hooks_ref.cycle.dropped_pending = 0;
                            }
                        }

                        if row_dropped {
                            continue;
                        }

                        tracing::trace!(kind = msg_kind_backend(&msg), "← downstream");

                        // ---- raw backend hook ----
                        let action = match hooks.as_mut() {
                            Some(h) => {
                                h.interceptor
                                    .on_backend(h.ctx.as_mut(), &mut msg)
                                    .await
                            }
                            None => Action::Forward,
                        };

                        let mut skip_forward = false;
                        match action {
                            Action::Forward => {}
                            Action::Reply(messages) => {
                                for m in messages {
                                    if downstream.send(m).await.is_err() {
                                        end = End::ClientGone;
                                        break 'pump;
                                    }
                                }
                                skip_forward = true;
                            }
                            Action::Deny(_) | Action::Scatter(_) => {
                                tracing::warn!(
                                    "Deny/Scatter from on_backend is not supported; closing session"
                                );
                                end = End::ClientGone;
                                break 'pump;
                            }
                            Action::Close => {
                                end = End::ClientGone;
                                break 'pump;
                            }
                        }

                        if !skip_forward {
                            // cycle end: ReadyForQuery forwarded → logging phase
                            let is_ready_for_query =
                                matches!(msg, PgWireBackendMessage::ReadyForQuery(_));
                            let mut cycle_done = None;
                            if is_ready_for_query
                                && let Some(hooks_ref) = hooks.as_mut()
                                && hooks_ref.cycle.active
                            {
                                cycle_done = Some(hooks_ref.cycle.finish());
                            }
                            if downstream.send(msg).await.is_err() {
                                tracing::warn!("downstream send failed");
                                end = End::ClientGone;
                                break 'pump;
                            }
                            backend += 1;
                            if let Some(report) = cycle_done
                                && let Some(hooks_ref) = hooks.as_mut()
                            {
                                hooks_ref
                                    .interceptor
                                    .on_cycle_end(hooks_ref.ctx.as_mut(), &report)
                                    .await;
                            }

                            if is_ready_for_query && draining {
                                tracing::info!("session ending: drained after shutdown");
                                end = End::ClientGone;
                                break 'pump;
                            }

                            // transaction pooling: release the lease when the
                            // session is idle again. NOTE: the dirty flag is
                            // reset only when a detach actually happened — a
                            // mid-transaction ReadyForQuery (status T) must
                            // not clear it, or the final idle detach would
                            // skip DISCARD and leak statements/GUCs onto the
                            // pooled connection.
                            if is_ready_for_query
                                && detach_if_idle(
                                    &mut lease,
                                    upstream_status,
                                    lease_dirty,
                                    transaction_mode,
                                )
                                .await
                            {
                                lease_dirty = false;
                                info.attached.store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                        } else {
                            backend += 1;
                        }
                    }
                }
            }
            _ = cancel_rx.recv() => {
                // Transaction mode: when detached there is no query to
                // cancel (the cancel is a no-op, like pgbouncer).
                if let Some(lease_ref) = lease.as_ref() {
                    tracing::info!("cancel requested");
                    if let Err(error) = lease_ref.conn.cancel().await {
                        tracing::warn!(%error, "cancel failed");
                    }
                } else {
                    tracing::debug!("cancel requested while detached; no-op");
                }
            }
            _ = shutdown_rx.changed() => {
                // graceful shutdown: end now when idle; mid-cycle, let the
                // current query finish (the backend arm ends the session at
                // the cycle's ReadyForQuery)
                if hygiene.awaiting_response || hygiene.copy != crate::pool::CopyCleanup::None {
                    tracing::info!("shutdown signal: draining current query cycle");
                    draining = true;
                } else {
                    tracing::info!("session ending: shutdown signal");
                    end = End::ClientGone;
                    break 'pump;
                }
            }
        }
    }

    // On UpstreamBroken the connection is dead; on ClientGone it goes back
    // to the pool — do NOT send Terminate upstream (that would close the
    // session). Checkin's sanitize handles all state cleanup.

    PumpOutcome {
        frontend,
        backend,
        end,
        hygiene,
        lease,
    }
}

/// Cycle-start input extracted from a frontend message.
#[derive(Clone)]
struct CycleInput {
    sql: String,
    kind: CycleKind,
}

/// `Some` when `msg` starts a query cycle (`Query` in the simple protocol,
/// `Parse` in the extended protocol — where SQL becomes visible).
fn cycle_input_of(msg: &PgWireFrontendMessage) -> Option<CycleInput> {
    match msg {
        PgWireFrontendMessage::Query(q) => Some(CycleInput {
            sql: q.query.clone(),
            kind: CycleKind::Simple,
        }),
        PgWireFrontendMessage::Parse(p) => Some(CycleInput {
            sql: p.query.clone(),
            kind: CycleKind::Extended {
                statement: p.name.clone(),
            },
        }),
        _ => None,
    }
}

/// Write a rewritten SQL back into the cycle-starting message.
fn apply_rewrite(msg: &mut PgWireFrontendMessage, rewritten: Option<String>) {
    match (msg, rewritten) {
        (PgWireFrontendMessage::Query(q), Some(sql)) => q.query = sql,
        (PgWireFrontendMessage::Parse(p), Some(sql)) => p.query = sql,
        _ => {}
    }
}

/// Resolve the SQL a Bind/Execute attach is executing, from the statement
/// registry (routing hints for mid-cycle attaches).
fn bind_statement_sql(msg: &PgWireFrontendMessage, registry: &StatementRegistry) -> Option<String> {
    if let PgWireFrontendMessage::Bind(bind) = msg {
        let name = bind.statement_name.as_deref().unwrap_or("");
        if !name.is_empty()
            && let Some((sql, _)) = registry.lookup(name)
        {
            return Some(sql.clone());
        }
    }
    None
}

/// The SQL text of a (possibly rewritten) cycle-starting message.
fn cycle_sql_of(msg: &PgWireFrontendMessage) -> Option<String> {
    match msg {
        PgWireFrontendMessage::Query(q) => Some(q.query.clone()),
        PgWireFrontendMessage::Parse(p) => Some(p.query.clone()),
        _ => None,
    }
}

/// Snapshot an `ErrorResponse` for a `CycleReport` without consuming it.
fn cycle_error_of(er: &pgwire::messages::response::ErrorResponse) -> CycleError {
    let mut code = String::new();
    let mut message = String::new();
    for (key, value) in &er.fields {
        match *key {
            b'C' => code = value.clone(),
            b'M' => message = value.clone(),
            _ => {}
        }
    }
    CycleError { code, message }
}

/// Subtract `dropped` from the trailing row count of a command tag
/// (`"SELECT 5"` → `"SELECT 3"`); tags without a trailing count are
/// returned unchanged.
fn adjust_tag(tag: &str, dropped: u64) -> String {
    match tag.rsplit_once(' ') {
        Some((head, count)) if !count.is_empty() && count.bytes().all(|b| b.is_ascii_digit()) => {
            let n: u64 = count.parse().unwrap_or(0);
            format!("{head} {}", n.saturating_sub(dropped))
        }
        _ => tag.to_owned(),
    }
}

async fn send_ready_for_query(
    downstream: &mut Downstream,
    status: TransactionStatus,
) -> Result<(), std::io::Error> {
    downstream
        .send(PgWireBackendMessage::ReadyForQuery(
            pgwire::messages::response::ReadyForQuery::new(status),
        ))
        .await
}

/// Factory creating new upstream connections for one endpoint's pool.
/// The TLS connector is built once per endpoint.
pub(crate) fn make_factory_for(
    endpoint: &EndpointConfig,
    route: &RouteConfig,
    params: &SessionParams,
) -> ConnectFactory {
    let connstring = endpoint.upstream.clone();
    let password = endpoint.password.clone().or_else(|| route.password.clone());
    let tls = endpoint.tls.clone().unwrap_or_else(|| route.tls.clone());
    let user = params.user.clone();
    let database = params.database.clone();
    let tls_connector = build_upstream_connector(&tls);
    Arc::new(move || {
        let connstring = connstring.clone();
        let user = user.clone();
        let database = database.clone();
        let password = password.clone();
        let tls_connector = tls_connector.clone();
        async move {
            upstream::connect_parts(
                &connstring,
                &user,
                database.as_deref(),
                password.as_deref(),
                tls_connector,
            )
            .await
        }
        .boxed()
    })
}

/// Build the upstream TLS connector from the route's TLS config:
/// CA file → verified against it; `insecure` → accepted unverifiable;
/// neither → `None` (plaintext; connstrings requiring TLS will fail
/// with a clear pgwire error).
fn build_upstream_connector(
    tls: &crate::config::UpstreamTlsConfig,
) -> Option<pgwire::tokio::TlsConnector> {
    use pgwire::tokio::tokio_rustls::rustls;
    use std::io::BufReader;

    if tls.insecure {
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        config.alpn_protocols = vec![b"postgresql".to_vec()];
        return Some(pgwire::tokio::TlsConnector::from(Arc::new(config)));
    }

    let ca_path = tls.ca.as_ref()?;
    let mut roots = rustls::RootCertStore::empty();
    let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(ca_path).ok()?))
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    for cert in certs {
        roots.add(cert).ok()?;
    }
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Some(pgwire::tokio::TlsConnector::from(Arc::new(config)))
}

/// Insecure certificate verifier (`tls_insecure = true`).
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// Frontend messages after which the server may send a response without
/// further input. Everything else (Parse/Bind/Describe/Execute/Close in the
/// extended protocol) is safely pipelined without flushing.
fn triggers_response(msg: &PgWireFrontendMessage) -> bool {
    matches!(
        msg,
        PgWireFrontendMessage::Query(_)
            | PgWireFrontendMessage::Sync(_)
            | PgWireFrontendMessage::Flush(_)
            | PgWireFrontendMessage::Terminate(_)
            | PgWireFrontendMessage::CopyData(_)
            | PgWireFrontendMessage::CopyDone(_)
            | PgWireFrontendMessage::CopyFail(_)
    )
}

/// Replays upstream startup parameters to the downstream client.
///
/// `PgWireClient::connect` caches the upstream's startup `ParameterStatus`
/// messages in [`PgWireClient::server_parameters`]; downstream clients would
/// never see them without this replay.
#[derive(Debug)]
struct UpstreamParameterProvider {
    parameters: std::collections::BTreeMap<String, String>,
}

impl ServerParameterProvider for UpstreamParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: pgwire::api::ClientInfo,
    {
        Some(
            self.parameters
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }
}

async fn send_fatal(downstream: &mut Downstream, code: &str, message: &str) -> std::io::Result<()> {
    let error_info = ErrorInfo::new("FATAL".to_owned(), code.to_owned(), message.to_owned());
    downstream
        .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
        .await?;
    downstream.close().await?;
    Ok(())
}

fn msg_kind_frontend(msg: &PgWireFrontendMessage) -> &'static str {
    match msg {
        PgWireFrontendMessage::Startup(_) => "Startup",
        PgWireFrontendMessage::CancelRequest(_) => "CancelRequest",
        PgWireFrontendMessage::PasswordMessageFamily(_) => "Password",
        PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(_)) => {
            "SslRequest"
        }
        PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresGss(_)) => {
            "GssRequest"
        }
        PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None) => "SslNegotiation",
        PgWireFrontendMessage::Query(_) => "Query",
        PgWireFrontendMessage::Parse(_) => "Parse",
        PgWireFrontendMessage::Bind(_) => "Bind",
        PgWireFrontendMessage::Describe(_) => "Describe",
        PgWireFrontendMessage::Execute(_) => "Execute",
        PgWireFrontendMessage::Close(_) => "Close",
        PgWireFrontendMessage::Flush(_) => "Flush",
        PgWireFrontendMessage::Sync(_) => "Sync",
        PgWireFrontendMessage::PortalSuspended(_) => "PortalSuspended",
        PgWireFrontendMessage::Terminate(_) => "Terminate",
        PgWireFrontendMessage::CopyData(_) => "CopyData",
        PgWireFrontendMessage::CopyDone(_) => "CopyDone",
        PgWireFrontendMessage::CopyFail(_) => "CopyFail",
    }
}

fn msg_kind_backend(msg: &PgWireBackendMessage) -> &'static str {
    match msg {
        PgWireBackendMessage::SslResponse(_) => "SslResponse",
        PgWireBackendMessage::GssEncResponse(_) => "GssEncResponse",
        PgWireBackendMessage::Authentication(_) => "Authentication",
        PgWireBackendMessage::ParameterStatus(_) => "ParameterStatus",
        PgWireBackendMessage::BackendKeyData(_) => "BackendKeyData",
        PgWireBackendMessage::NegotiateProtocolVersion(_) => "NegotiateProtocolVersion",
        PgWireBackendMessage::ParseComplete(_) => "ParseComplete",
        PgWireBackendMessage::BindComplete(_) => "BindComplete",
        PgWireBackendMessage::CloseComplete(_) => "CloseComplete",
        PgWireBackendMessage::PortalSuspended(_) => "PortalSuspended",
        PgWireBackendMessage::CommandComplete(_) => "CommandComplete",
        PgWireBackendMessage::EmptyQueryResponse(_) => "EmptyQueryResponse",
        PgWireBackendMessage::ReadyForQuery(_) => "ReadyForQuery",
        PgWireBackendMessage::ErrorResponse(_) => "ErrorResponse",
        PgWireBackendMessage::NoticeResponse(_) => "NoticeResponse",
        PgWireBackendMessage::NotificationResponse(_) => "NotificationResponse",
        PgWireBackendMessage::ParameterDescription(_) => "ParameterDescription",
        PgWireBackendMessage::RowDescription(_) => "RowDescription",
        PgWireBackendMessage::DataRow(_) => "DataRow",
        PgWireBackendMessage::NoData(_) => "NoData",
        PgWireBackendMessage::CopyData(_) => "CopyData",
        PgWireBackendMessage::CopyFail(_) => "CopyFail",
        PgWireBackendMessage::CopyDone(_) => "CopyDone",
        PgWireBackendMessage::CopyInResponse(_) => "CopyInResponse",
        PgWireBackendMessage::CopyOutResponse(_) => "CopyOutResponse",
        PgWireBackendMessage::CopyBothResponse(_) => "CopyBothResponse",
    }
}
