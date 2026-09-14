//! Routing built-ins (M5): embeddable library types the interceptor's
//! [`upstream`](crate::Interceptor::upstream) hook calls — the
//! `LoadBalancer`-as-a-field pattern from pingora.
//!
//! - [`EndpointGroup`]: endpoint set with passive health tracking
//!   (Alive/Dead via failure threshold, revival after a cooldown) and
//!   selection helpers (first-alive, round-robin, by-role).
//! - [`RwSplit`]: read/write classification of SQL steering reads to
//!   replicas and writes to the primary.
//! - [`FailoverRouter`]: the default interceptor auto-wired when a route
//!   has multiple endpoints and no custom service is registered.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::config::{EndpointConfig, EndpointId};
use crate::intercept::{Interceptor, RetryDecision, RoutingInfo, UpstreamError};

/// Default failures before an endpoint is marked dead.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 2;
/// Default time after which a dead endpoint is retried.
pub const DEFAULT_REVIVE_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Health {
    Alive { failures: u32 },
    Dead { since: Instant },
}

/// A health-checked set of endpoints. Clone shares state (all clones see
/// the same health).
///
/// ```ignore
/// struct Gateway { group: EndpointGroup }
///
/// #[async_trait]
/// impl Interceptor for Gateway {
///     type Ctx = ();
///     async fn upstream(&self, _ctx: &mut (), info: &RoutingInfo<'_>) -> EndpointId {
///         self.group.select_first_alive(info).unwrap_or_else(|| info.default_endpoint().into())
///     }
///     async fn on_upstream_error(&self, _ctx: &mut (), e: &UpstreamError) -> RetryDecision {
///         self.group.report_failure(e.endpoint());
///         RetryDecision::Relink
///     }
///     async fn on_upstream_connected(&self, _ctx: &mut (), ep: &str) {
///         self.group.report_success(ep);
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct EndpointGroup {
    inner: std::sync::Arc<Mutex<HashMap<String, Health>>>,
    failure_threshold: u32,
    revive_after: Duration,
}

impl EndpointGroup {
    /// Track health for the given endpoint ids.
    pub fn new<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        EndpointGroup {
            inner: std::sync::Arc::new(Mutex::new(
                ids.into_iter()
                    .map(|id| (id.into(), Health::Alive { failures: 0 }))
                    .collect(),
            )),
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            revive_after: DEFAULT_REVIVE_AFTER,
        }
    }

    /// From a route's endpoints.
    pub fn from_endpoints(endpoints: &[EndpointConfig]) -> Self {
        Self::new(endpoints.iter().map(|e| e.id.clone()))
    }

    /// Set the consecutive-failure threshold before an endpoint is dead.
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold.max(1);
        self
    }

    /// Set how long a dead endpoint stays dead before being retried.
    pub fn with_revive_after(mut self, after: Duration) -> Self {
        self.revive_after = after;
        self
    }

    /// Whether `id` is currently considered alive (dead endpoints count as
    /// alive again once the revive cooldown passed).
    pub fn is_alive(&self, id: &str) -> bool {
        let map = self.inner.lock().unwrap();
        match map.get(id) {
            None => true, // untracked endpoints are assumed alive
            Some(Health::Alive { .. }) => true,
            Some(Health::Dead { since }) => since.elapsed() >= self.revive_after,
        }
    }

    /// Record a failure: after `failure_threshold` consecutive failures the
    /// endpoint is marked dead for `revive_after`.
    pub fn report_failure(&self, id: &str) {
        let mut map = self.inner.lock().unwrap();
        let entry = map
            .entry(id.to_owned())
            .or_insert(Health::Alive { failures: 0 });
        match entry {
            Health::Alive { failures } => {
                *failures += 1;
                if *failures >= self.failure_threshold {
                    tracing::warn!(endpoint = %id, failures = *failures, "endpoint marked dead");
                    *entry = Health::Dead {
                        since: Instant::now(),
                    };
                }
            }
            Health::Dead { since } => {
                // already dead: keep the clock fresh
                *since = Instant::now();
            }
        }
    }

    /// Record a success: clears the failure count (and revives the dead).
    pub fn report_success(&self, id: &str) {
        let mut map = self.inner.lock().unwrap();
        if let Some(entry) = map.get_mut(id) {
            *entry = Health::Alive { failures: 0 };
        }
    }

    /// First alive endpoint (configuration order), or `None` when all are
    /// dead. The route's configured order defines priority — put the
    /// primary first.
    pub fn select_first_alive(&self, info: &RoutingInfo<'_>) -> Option<EndpointId> {
        info.endpoints()
            .iter()
            .find(|e| self.is_alive(&e.id))
            .map(|e| e.id.clone())
    }

    /// Round-robin among alive endpoints (approximate: a shared counter is
    /// not worth a lock per pick; we jitter by elapsed nanos).
    pub fn select_round_robin(&self, info: &RoutingInfo<'_>) -> Option<EndpointId> {
        let alive: Vec<&EndpointConfig> = info
            .endpoints()
            .iter()
            .filter(|e| self.is_alive(&e.id))
            .collect();
        if alive.is_empty() {
            return None;
        }
        let idx = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0))
            % alive.len();
        Some(alive[idx].id.clone())
    }
}

/// Read/write preference for [`RwSplit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPreference {
    /// All traffic to the primary.
    PrimaryOnly,
    /// Reads to the first alive replica; writes to the primary.
    ReplicaFirst,
}

/// Classifies SQL as read or write and routes accordingly: reads to a
/// replica (when one is alive), writes to the primary. For transaction
/// pooling (routing happens per attach); in session mode the decision is
/// per session.
///
/// The default classifier treats leading `SELECT`/`SHOW` (optionally
/// wrapped in whitespace/comments) as reads; override with
/// [`RwSplit::with_classifier`].
#[derive(Clone)]
pub struct RwSplit {
    group: EndpointGroup,
    primary: EndpointId,
    preference: ReadPreference,
    classifier: std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync>,
}

impl RwSplit {
    pub fn new(group: EndpointGroup, primary: impl Into<String>) -> Self {
        RwSplit {
            group,
            primary: primary.into(),
            preference: ReadPreference::ReplicaFirst,
            classifier: std::sync::Arc::new(default_is_read),
        }
    }

    pub fn with_read_preference(mut self, preference: ReadPreference) -> Self {
        self.preference = preference;
        self
    }

    /// Replace the read classifier (`true` = read).
    pub fn with_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.classifier = std::sync::Arc::new(f);
        self
    }

    /// Route SQL to an endpoint: reads → first alive non-primary,
    /// writes → primary (even when marked dead — writes have nowhere else
    /// to go).
    pub fn route_sql(&self, info: &RoutingInfo<'_>, sql: &str) -> EndpointId {
        let is_read = (self.classifier)(sql);
        if !is_read || self.preference == ReadPreference::PrimaryOnly {
            return self.primary.clone();
        }
        info.endpoints()
            .iter()
            .find(|e| e.id != self.primary && self.group.is_alive(&e.id))
            .map(|e| e.id.clone())
            .unwrap_or_else(|| self.primary.clone())
    }

    /// Route by an explicit read/write decision.
    pub fn route(&self, info: &RoutingInfo<'_>, is_read: bool) -> EndpointId {
        if !is_read || self.preference == ReadPreference::PrimaryOnly {
            return self.primary.clone();
        }
        info.endpoints()
            .iter()
            .find(|e| e.id != self.primary && self.group.is_alive(&e.id))
            .map(|e| e.id.clone())
            .unwrap_or_else(|| self.primary.clone())
    }
}

impl std::fmt::Debug for RwSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwSplit")
            .field("primary", &self.primary)
            .field("preference", &self.preference)
            .finish()
    }
}

fn default_is_read(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    // strip leading SQL comments ("-- ...\n" and "/* ... */")
    let stripped = strip_comments(trimmed);
    let first = stripped
        .split(|c: char| c.is_whitespace() || c == '(')
        .find(|s| !s.is_empty())
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(first.as_str(), "SELECT" | "SHOW" | "TABLE")
}

fn strip_comments(sql: &str) -> &str {
    let mut s = sql;
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split_once('\n').map(|(_, r)| r).unwrap_or("");
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.split_once("*/").map(|(_, r)| r).unwrap_or("");
        } else {
            return s;
        }
    }
}

/// The default routing interceptor: first-alive with passive health
/// tracking and always-relink error policy. Auto-wired by
/// [`crate::ProxyBuilder`] when a route has multiple endpoints and no
/// custom service is registered; also usable explicitly.
#[derive(Debug, Clone)]
pub struct FailoverRouter {
    group: EndpointGroup,
}

impl FailoverRouter {
    pub fn new(group: EndpointGroup) -> Self {
        FailoverRouter { group }
    }

    /// From a route's endpoints.
    pub fn from_endpoints(endpoints: &[EndpointConfig]) -> Self {
        Self::new(EndpointGroup::from_endpoints(endpoints))
    }

    /// Shared health state (for introspection/tests).
    pub fn group(&self) -> &EndpointGroup {
        &self.group
    }
}

#[async_trait]
impl Interceptor for FailoverRouter {
    type Ctx = ();

    async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> EndpointId {
        self.group
            .select_first_alive(info)
            .unwrap_or_else(|| info.default_endpoint().to_owned())
    }

    async fn on_upstream_error(
        &self,
        _ctx: &mut Self::Ctx,
        error: &UpstreamError,
    ) -> RetryDecision {
        self.group.report_failure(error.endpoint());
        RetryDecision::Relink
    }

    async fn on_upstream_connected(&self, _ctx: &mut Self::Ctx, endpoint: &str) {
        self.group.report_success(endpoint);
    }
}
