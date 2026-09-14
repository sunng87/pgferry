//! Upstream connection pool — session pooling mode (M1).
//!
//! One [`Pool`] per [`PoolKey`] (route + user + database), managed by a
//! shared [`PoolManager`]. A session *checks out* an upstream connection at
//! startup and *checks it in* when the client disconnects:
//!
//! - checkout: pop the most-recently parked idle connection (probing it if
//!   it has been idle beyond `stale_after`), else connect a new one, waiting
//!   on a semaphore while the pool is at `max_size`;
//! - checkin: sanitize the connection back to a neutral state (close any
//!   open extended-query cycle / COPY, `ROLLBACK` if a transaction was left
//!   open by a disconnecting client, `DISCARD ALL`), then park it;
//! - a background sweeper closes connections idle beyond `idle_timeout` or
//!   older than `max_lifetime`.
//!
//! The semaphore models "connections out": idle connections hold no permit,
//! so the sweeper can close them without permit bookkeeping. A session
//! waiting for a permit cancels cleanly when its client disconnects (the
//! acquire future is dropped).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::{SinkExt as _, StreamExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use pgwire::api::client::ClientInfo as _;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::error::PgWireClientError;
use pgwire::messages::copy::CopyFail;
use pgwire::messages::extendedquery::Sync as FrontendSync;
use pgwire::messages::response::TransactionStatus;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::client::PgWireClient;

/// How long checkin sanitization may take before the connection is dropped.
const SANITIZE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a checkout liveness probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for a canceled query's terminal messages before
/// falling back to sending `Sync`. The cancel's `ErrorResponse` +
/// `ReadyForQuery` arrive within a roundtrip; only the rare
/// "client died between Flush and Sync" case produces nothing at all.
const CANCEL_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// Startup-packet parameters replayed onto a reused connection with `SET`.
///
/// These must NOT be baked into the upstream connect `Config`: every pooled
/// connection's post-`DISCARD ALL` baseline then stays the server default,
/// so "fresh connection" and "reused connection" behave identically.
pub const TRACKED_STARTUP_PARAMETERS: &[&str] = &["application_name", "client_encoding"];

/// Identifies a group of interchangeable upstream connections — one pool
/// per (route, endpoint, user, database).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub route: String,
    pub endpoint: String,
    pub user: String,
    pub database: Option<String>,
}

/// Pooling mode: when upstream connections are leased relative to the
/// downstream session lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolMode {
    /// One upstream connection per downstream session (M1 behavior).
    #[default]
    Session,
    /// Lease on first query of a cycle; release when the session returns to
    /// `ReadyForQuery(Idle)` (transaction end). Many client sessions share
    /// fewer upstream connections (M3).
    ///
    /// Unsupported in transaction mode (documented): `CREATE TEMP TABLE`,
    /// `LISTEN`/`NOTIFY` persistence semantics, and `SET`s of non-reported
    /// GUCs (e.g. `search_path`).
    Transaction,
}

/// Pool tuning knobs.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Pooling mode (session vs transaction).
    pub mode: PoolMode,
    /// Maximum number of upstream connections (idle + checked out) per pool.
    pub max_size: usize,
    /// Idle connections are closed by the sweeper after this long.
    pub idle_timeout: Duration,
    /// Connections older than this are closed (at checkin or by the sweeper).
    pub max_lifetime: Duration,
    /// Idle connections older than this are probed on checkout.
    pub stale_after: Duration,
    /// Sweeper period.
    pub sweep_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            mode: PoolMode::Session,
            max_size: 16,
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(3600),
            stale_after: Duration::from_secs(30),
            sweep_interval: Duration::from_secs(15),
        }
    }
}

/// Creates new upstream connections for a pool. Captures the route config
/// and the pool key's user/database.
pub type ConnectFactory =
    Arc<dyn Fn() -> BoxFuture<'static, Result<PgWireClient, PgWireClientError>> + Send + Sync>;

/// Hygiene state the pump observed when it ended — decides how checkin
/// returns the connection to a neutral state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Hygiene {
    /// The server owes responses ending in `ReadyForQuery`: a `Query` or
    /// `Sync`/`Flush` was forwarded but the completing `ReadyForQuery` was
    /// not seen before the session ended — i.e. a query may still be
    /// *executing* on the backend. Checkin cancels it first, like pgbouncer.
    ///
    /// Conservatively true is safe: canceling an idle backend is a no-op.
    pub awaiting_response: bool,
    /// COPY sub-protocol state (see [`CopyCleanup`]).
    pub copy: CopyCleanup,
}

/// COPY sub-protocol state at pump end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyCleanup {
    /// No COPY in progress.
    #[default]
    None,
    /// The server is waiting for CopyData/CopyDone/CopyFail from us
    /// (CopyInResponse / CopyBothResponse seen, not yet completed): abort
    /// with CopyFail.
    ClientOwesCopy,
    /// The server is streaming CopyData to us (CopyOutResponse seen, not
    /// yet completed): drain until the command completes.
    ServerStreamsCopy,
}

/// Basic pool counters (cumulative since startup).
#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub checkouts: AtomicU64,
    pub reused: AtomicU64,
    pub created: AtomicU64,
    pub checked_in: AtomicU64,
    pub dropped: AtomicU64,
    pub probed: AtomicU64,
    pub evicted: AtomicU64,
    /// Connections currently checked out.
    pub active: AtomicI64,
    /// Sessions currently waiting for a connection (pool at capacity).
    pub waiting: AtomicI64,
}

struct Parked {
    conn: PgWireClient,
    idle_since: Instant,
    created_at: Instant,
}

struct PoolInner {
    key: PoolKey,
    config: PoolConfig,
    permits: Arc<Semaphore>,
    /// LIFO stack of idle connections (most recently parked on top).
    idle: Mutex<Vec<Parked>>,
    /// Baseline startup parameters snapshot (post-DISCARD state of pooled
    /// connections), replayed to downstream clients at session startup in
    /// transaction mode — where no connection is leased yet.
    baseline_params: Mutex<Option<std::collections::BTreeMap<String, String>>>,
    connect: ConnectFactory,
    metrics: PoolMetrics,
}

/// A shared pool for one [`PoolKey`].
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

/// A checked-out upstream connection. Holds the pool permit; returning it
/// via [`UpstreamLease::checkin`] parks it, [`UpstreamLease::destroy`]
/// closes it — either way the permit is released on drop.
pub struct UpstreamLease {
    pub conn: PgWireClient,
    created_at: Instant,
    _permit: OwnedSemaphorePermit,
    pool: Arc<PoolInner>,
}

impl std::fmt::Debug for UpstreamLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamLease")
            .field("created_at", &self.created_at)
            .finish()
    }
}

impl Pool {
    pub(crate) fn new(key: PoolKey, config: PoolConfig, connect: ConnectFactory) -> Self {
        let permits = Arc::new(Semaphore::new(config.max_size.max(1)));
        Pool {
            inner: Arc::new(PoolInner {
                key,
                config,
                permits,
                idle: Mutex::new(Vec::new()),
                baseline_params: Mutex::new(None),
                connect,
                metrics: PoolMetrics::default(),
            }),
        }
    }

    pub fn key(&self) -> &PoolKey {
        &self.inner.key
    }

    pub fn metrics(&self) -> &PoolMetrics {
        &self.inner.metrics
    }

    /// Number of idle (parked) connections.
    pub fn idle_len(&self) -> usize {
        self.inner.idle.lock().unwrap().len()
    }

    /// Configured maximum number of upstream connections.
    pub fn max_size(&self) -> usize {
        self.inner.config.max_size
    }

    fn pop_idle(&self) -> Option<Parked> {
        self.inner.idle.lock().unwrap().pop()
    }

    /// The pool's baseline startup parameters (post-DISCARD state).
    ///
    /// Transaction-pooling sessions need startup parameters for the
    /// downstream client before any connection is leased. The snapshot is
    /// captured at checkin (after sanitize, so it reflects the clean
    /// baseline); the first session through an empty pool does a probe
    /// checkout to establish it.
    pub async fn baseline_params(
        &self,
    ) -> Result<std::collections::BTreeMap<String, String>, PgWireClientError> {
        if let Some(params) = self.inner.baseline_params.lock().unwrap().clone() {
            return Ok(params);
        }
        // cold pool: probe one connection for its startup parameters,
        // then park it for real use
        let lease = self.checkout().await?;
        let params = lease.conn.server_parameters().clone();
        lease.checkin(Hygiene::default()).await;
        Ok(params)
    }

    /// Take an upstream connection: idle-first (probing stale ones), else
    /// connect a new one. Waits while the pool is at `max_size`.
    pub async fn checkout(&self) -> Result<UpstreamLease, PgWireClientError> {
        // One permit per connection-out; cancellable by dropping the future.
        self.inner.metrics.waiting.fetch_add(1, Ordering::Relaxed);
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        self.inner.metrics.waiting.fetch_sub(1, Ordering::Relaxed);

        while let Some(parked) = self.pop_idle() {
            let age = parked.created_at.elapsed();
            if age > self.inner.config.max_lifetime {
                self.inner.metrics.evicted.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(pool = %self.inner.key.route, user = %self.inner.key.user, ?age, "closing expired idle connection");
                continue; // dropped
            }

            let mut conn = parked.conn;
            if parked.idle_since.elapsed() > self.inner.config.stale_after {
                self.inner.metrics.probed.fetch_add(1, Ordering::Relaxed);
                if let Err(error) = probe(&mut conn).await {
                    self.inner.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%error, "stale idle connection failed probe, dropping");
                    continue;
                }
            }

            return Ok(self.finish_checkout(conn, parked.created_at, permit));
        }

        // No idle connection available: connect a new one.
        let conn = (self.inner.connect)().await.inspect_err(|_| {
            self.inner.metrics.dropped.fetch_add(1, Ordering::Relaxed);
        })?;
        self.inner.metrics.created.fetch_add(1, Ordering::Relaxed);
        Ok(self.finish_checkout(conn, Instant::now(), permit))
    }

    fn finish_checkout(
        &self,
        conn: PgWireClient,
        created_at: Instant,
        permit: OwnedSemaphorePermit,
    ) -> UpstreamLease {
        self.inner.metrics.checkouts.fetch_add(1, Ordering::Relaxed);
        self.inner.metrics.reused.fetch_add(1, Ordering::Relaxed);
        self.inner.metrics.active.fetch_add(1, Ordering::Relaxed);
        UpstreamLease {
            conn,
            created_at,
            _permit: permit,
            pool: Arc::clone(&self.inner),
        }
    }
}

impl UpstreamLease {
    /// Transaction-mode detach: the session is at `ReadyForQuery(Idle)`
    /// with nothing in flight, so the connection is known-clean — no
    /// settle/cancel/rollback needed. `DISCARD ALL` runs only when the
    /// session dirtied the connection during this lease (statements
    /// replayed, GUCs set, parameter changes seen) — the fast path skips it.
    ///
    /// Everything else (lifetime expiry, metrics, parking) matches
    /// [`UpstreamLease::checkin`].
    pub async fn checkin_detached(self, dirty: bool) {
        let Self {
            mut conn,
            created_at,
            _permit,
            pool,
        } = self;
        pool.metrics.active.fetch_sub(1, Ordering::Relaxed);

        if dirty {
            // clear replayed statements, session GUCs, etc.
            let cleaned = match timeout(SANITIZE_TIMEOUT, async {
                conn.simple_query(DefaultSimpleQueryHandler::new(), "DISCARD ALL")
                    .await
            })
            .await
            {
                Ok(Ok(_)) => true,
                Ok(Err(error)) => {
                    tracing::debug!(%error, "detach DISCARD failed, dropping connection");
                    false
                }
                Err(_) => {
                    tracing::debug!("detach DISCARD timed out, dropping connection");
                    false
                }
            };
            if !cleaned {
                pool.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                return; // conn dropped
            }
        }

        // refresh the baseline snapshot from the (clean) connection
        {
            let params = conn.server_parameters().clone();
            *pool.baseline_params.lock().unwrap() = Some(params);
        }

        let expired = created_at.elapsed() > pool.config.max_lifetime;
        if !expired {
            let idle_count = {
                let mut idle = pool.idle.lock().unwrap();
                idle.push(Parked {
                    conn,
                    idle_since: Instant::now(),
                    created_at,
                });
                idle.len()
            };
            pool.metrics.checked_in.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                idle = idle_count,
                "connection parked (transaction-mode detach)"
            );
        } else {
            pool.metrics.evicted.fetch_add(1, Ordering::Relaxed);
            // conn dropped → closed
        }
    }

    /// The backend pid of the upstream connection (for logs/metrics).
    pub fn upstream_pid(&self) -> i32 {
        self.conn.process_id()
    }

    /// Replay tracked startup-packet parameters (`application_name`,
    /// `client_encoding`) that differ from the connection's cached state.
    /// Responses are consumed upstream-side; the downstream client sees
    /// nothing. Must run before the session's first forwarded message.
    pub async fn sync_startup_parameters(
        &mut self,
        startup_parameters: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), PgWireClientError> {
        for name in TRACKED_STARTUP_PARAMETERS {
            let Some(desired) = startup_parameters.get(*name) else {
                continue;
            };
            let current = self
                .conn
                .server_parameters()
                .get(*name)
                .cloned()
                .unwrap_or_default();
            if &current != desired {
                set_parameter(&mut self.conn, name, desired).await?;
            }
        }
        Ok(())
    }

    /// Return the connection to the pool. Sanitizes it back to a neutral
    /// state first; a connection that cannot be sanitized (or is past
    /// `max_lifetime`) is closed instead of parked. `hygiene` describes
    /// what the pump observed when the session ended.
    pub async fn checkin(self, hygiene: Hygiene) {
        let Self {
            mut conn,
            created_at,
            _permit,
            pool,
        } = self;
        pool.metrics.active.fetch_sub(1, Ordering::Relaxed);

        let sanitized = match timeout(SANITIZE_TIMEOUT, sanitize(&mut conn, hygiene)).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::debug!(%error, "checkin sanitize failed, dropping connection");
                false
            }
            Err(_) => {
                tracing::debug!("checkin sanitize timed out, dropping connection");
                false
            }
        };

        let expired = created_at.elapsed() > pool.config.max_lifetime;
        if sanitized && !expired {
            let idle_count = {
                let mut idle = pool.idle.lock().unwrap();
                idle.push(Parked {
                    conn,
                    idle_since: Instant::now(),
                    created_at,
                });
                idle.len()
            };
            pool.metrics.checked_in.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(idle = idle_count, "connection parked");
        } else {
            if sanitized {
                pool.metrics.evicted.fetch_add(1, Ordering::Relaxed);
            } else {
                pool.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            }
            // conn dropped → socket closed
        }
    }

    /// Close the connection instead of returning it to the pool.
    pub fn destroy(self, reason: &str) {
        tracing::debug!(%reason, "connection destroyed");
        self.pool.metrics.dropped.fetch_add(1, Ordering::Relaxed);
        self.pool.metrics.active.fetch_sub(1, Ordering::Relaxed);
        // fields drop: conn closes, permit releases
    }
}

/// Registry of live pools, keyed by [`PoolKey`].
#[derive(Clone, Default)]
pub struct PoolManager {
    pools: Arc<RwLock<HashMap<PoolKey, Pool>>>,
}

impl PoolManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the pool for `key`, creating it with `config`/`connect` on first
    /// use. Existing pools keep their original config and factory.
    pub fn get_or_create(&self, key: PoolKey, config: PoolConfig, connect: ConnectFactory) -> Pool {
        if let Some(pool) = self.pools.read().unwrap().get(&key) {
            return pool.clone();
        }
        let mut pools = self.pools.write().unwrap();
        pools
            .entry(key.clone())
            .or_insert_with(|| Pool::new(key, config, connect))
            .clone()
    }

    /// All live pools (for the sweeper).
    pub fn pools(&self) -> Vec<Pool> {
        self.pools.read().unwrap().values().cloned().collect()
    }
}

/// Periodically close idle connections past `idle_timeout` or
/// `max_lifetime`, and log pool metrics.
pub async fn run_sweeper(manager: PoolManager, config: PoolConfig) {
    let interval = config.sweep_interval;
    tracing::debug!(?interval, "pool sweeper started");
    loop {
        tokio::time::sleep(interval).await;
        for pool in manager.pools() {
            pool.sweep();
        }
    }
}

impl Pool {
    fn sweep(&self) {
        let now = Instant::now();
        let mut expired = Vec::new();
        {
            let mut idle = self.inner.idle.lock().unwrap();
            idle.retain(|parked| {
                let idle_too_long =
                    now.duration_since(parked.idle_since) > self.inner.config.idle_timeout;
                let too_old =
                    now.duration_since(parked.created_at) > self.inner.config.max_lifetime;
                if idle_too_long || too_old {
                    expired.push(());
                    false
                } else {
                    true
                }
            });
        }
        if !expired.is_empty() {
            self.inner
                .metrics
                .evicted
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
            tracing::info!(
                pool = %self.inner.key.route,
                user = %self.inner.key.user,
                evicted = expired.len(),
                checkouts = self.inner.metrics.checkouts.load(Ordering::Relaxed),
                reused = self.inner.metrics.reused.load(Ordering::Relaxed),
                created = self.inner.metrics.created.load(Ordering::Relaxed),
                checked_in = self.inner.metrics.checked_in.load(Ordering::Relaxed),
                dropped = self.inner.metrics.dropped.load(Ordering::Relaxed),
                "pool sweep"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// connection hygiene helpers
// ---------------------------------------------------------------------------

/// Liveness probe for a parked connection: a bare `Sync` must be answered
/// with `ReadyForQuery`.
async fn probe(conn: &mut PgWireClient) -> Result<(), PgWireClientError> {
    timeout(PROBE_TIMEOUT, async {
        conn.send(PgWireFrontendMessage::Sync(FrontendSync::new()))
            .await?;
        drain_until_ready(conn).await
    })
    .await
    .map_err(|_| PgWireClientError::UnexpectedEOF)? // timeout: treat as broken
}

/// Stop whatever the server is doing and consume its terminal messages.
///
/// The ordering here is subtle: a cancel makes the server emit
/// `ErrorResponse` + `ReadyForQuery` for the aborted cycle on its own — so
/// we drain WITHOUT sending `Sync` first. A stray `Sync` on top would
/// queue a SECOND `ReadyForQuery`, shifting every later response cycle and
/// leaking the tail of our own `ROLLBACK`/`DISCARD ALL` into the next
/// session. Only when nothing arrives (client died between `Flush` and
/// `Sync`, nothing running) do we send `Sync` to elicit the
/// `ReadyForQuery` ourselves.
async fn settle(
    conn: &mut PgWireClient,
    send_copy_fail: bool,
    awaiting: bool,
) -> Result<(), PgWireClientError> {
    if send_copy_fail {
        conn.send(PgWireFrontendMessage::CopyFail(CopyFail::new(
            "pgferry: client disconnected during COPY".to_owned(),
        )))
        .await?;
    }
    if awaiting {
        // Cancel a (probably) running query on a side connection. No-op
        // when nothing is running.
        let _ = conn.cancel().await;
    }

    match tokio::time::timeout(CANCEL_DRAIN_TIMEOUT, drain_until_ready(conn)).await {
        Ok(result) => result,
        // nothing pending on the connection: close any open extended-query
        // cycle explicitly
        Err(_) => close_cycle(conn).await,
    }
}

/// Send `Sync` (harmless when no extended-query cycle is open) and consume
/// everything up to and including `ReadyForQuery`.
async fn close_cycle(conn: &mut PgWireClient) -> Result<(), PgWireClientError> {
    conn.send(PgWireFrontendMessage::Sync(FrontendSync::new()))
        .await?;
    drain_until_ready(conn).await
}

async fn drain_until_ready(conn: &mut PgWireClient) -> Result<(), PgWireClientError> {
    while let Some(item) = conn.next().await {
        match item? {
            PgWireBackendMessage::ReadyForQuery(ready) => {
                conn.set_transaction_status(ready.status);
                return Ok(());
            }
            PgWireBackendMessage::ParameterStatus(ps) => {
                conn.set_server_parameter(ps.name, ps.value);
            }
            _ => {}
        }
    }
    Err(PgWireClientError::UnexpectedEOF)
}

/// Return a connection to a neutral state after a session ended:
/// 1. stop anything still executing: cancel an in-flight query, or abort a
///    COPY the server is waiting on,
/// 2. close any open extended-query cycle (`Sync` + drain),
/// 3. `ROLLBACK` if the disconnecting client left a transaction open,
/// 4. `DISCARD ALL` (temp tables, `LISTEN`s, session GUCs, prepared stmts).
async fn sanitize(conn: &mut PgWireClient, hygiene: Hygiene) -> Result<(), PgWireClientError> {
    let Hygiene {
        awaiting_response,
        copy,
    } = hygiene;

    if copy == CopyCleanup::ClientOwesCopy {
        // A COPY the server is waiting on and/or a query still executing:
        // abort + drain (see `settle` for the Sync ordering).
        settle(conn, true, awaiting_response).await?;
    } else if awaiting_response {
        // A query is (probably) still executing — e.g. the client vanished
        // mid-`pg_sleep`. Cancel it on a side connection and consume its
        // terminal messages.
        settle(conn, false, true).await?;
    } else {
        close_cycle(conn).await?;
    }

    if conn.transaction_status() != TransactionStatus::Idle {
        conn.simple_query(DefaultSimpleQueryHandler::new(), "ROLLBACK")
            .await?;
    }
    conn.simple_query(DefaultSimpleQueryHandler::new(), "DISCARD ALL")
        .await?;
    Ok(())
}

/// `SET` a session parameter and update the cached value.
async fn set_parameter(
    conn: &mut PgWireClient,
    name: &str,
    value: &str,
) -> Result<(), PgWireClientError> {
    let escaped = value.replace('\'', "''");
    conn.simple_query(
        DefaultSimpleQueryHandler::new(),
        &format!("SET {name} = '{escaped}'"),
    )
    .await?;
    conn.set_server_parameter(name.to_owned(), value.to_owned());
    Ok(())
}
