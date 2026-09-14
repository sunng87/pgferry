//! Proxy configuration.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::pool::PoolConfig;

/// Configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid listen address {0:?}: {1}")]
    ListenAddr(String, std::net::AddrParseError),
    #[error("missing required configuration: {0}")]
    Missing(&'static str),
    #[error("invalid TLS configuration: {0}")]
    Tls(String),
}

/// Downstream TLS termination (listener level).
#[derive(Debug, Clone)]
pub struct ServerTlsConfig {
    /// PEM certificate chain path.
    pub cert: PathBuf,
    /// PEM private key path.
    pub key: PathBuf,
}

/// Upstream TLS (client side).
#[derive(Debug, Clone, Default)]
pub struct UpstreamTlsConfig {
    /// Verify the upstream against this PEM CA file. When `None` and
    /// `insecure` is false, no TLS connector is configured (plaintext, or
    /// an error if the connstring requires TLS).
    pub ca: Option<PathBuf>,
    /// Skip certificate verification (`sslmode=require`-style, no validation).
    pub insecure: bool,
}

/// Identifies one upstream endpoint of a route (routing decisions and
/// pool keys carry it).
pub type EndpointId = String;

/// One upstream endpoint: a routing target with its own connection
/// parameters and pool.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Endpoint name — the value [`crate::Interceptor::upstream`]
    /// returns and pool keys carry.
    pub id: EndpointId,
    /// libpq-style connection string, e.g. `"host=127.0.0.1 port=5432"`.
    /// `user` and `dbname` come from the downstream client's startup
    /// message.
    pub upstream: String,
    /// Password override for this endpoint; falls back to the route's.
    pub password: Option<String>,
    /// TLS override for this endpoint; falls back to the route's.
    pub tls: Option<UpstreamTlsConfig>,
}

/// A single upstream route with one or more endpoints.
///
/// Multiple endpoints enable routing and failover (M5): the interceptor's
/// `upstream()` hook picks an endpoint per attach; connection failures
/// re-enter routing.
#[derive(Debug, Clone)]
pub struct RouteConfig {
    /// Route name, used in logs.
    pub name: String,
    /// Upstream endpoints. The first is the default routing target.
    pub endpoints: Vec<EndpointConfig>,
    /// Default password for upstream authentication (endpoint override
    /// wins).
    pub password: Option<String>,
    /// Default upstream TLS configuration (endpoint override wins).
    pub tls: UpstreamTlsConfig,
}

impl RouteConfig {
    pub fn endpoint(&self, id: &str) -> Option<&EndpointConfig> {
        self.endpoints.iter().find(|e| e.id == id)
    }
}

/// Top-level proxy configuration.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub listen_addr: SocketAddr,
    pub route: RouteConfig,
    /// Upstream pool tuning.
    pub pool: PoolConfig,
    /// Cap on concurrent downstream sessions.
    pub max_client_conn: usize,
    /// When set, clients whose startup `database` matches get the admin
    /// console (`SHOW POOLS` / `SHOW CLIENTS` / `SHOW STATS`) instead of an
    /// upstream connection.
    pub admin_database: Option<String>,
    /// Prometheus `/metrics` endpoint address.
    pub metrics_addr: Option<SocketAddr>,
    /// Downstream TLS termination.
    pub tls_server: Option<ServerTlsConfig>,
    /// How long graceful shutdown waits for sessions to drain before
    /// aborting them.
    pub shutdown_drain: Duration,
}
