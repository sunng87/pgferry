//! pgferry — a programmable PostgreSQL proxy and connection pooler built on
//! [pgwire](https://github.com/sunng87/pgwire).
//!
//! pgferry speaks the PostgreSQL wire protocol on both sides: it serves
//! downstream clients with pgwire's server API, and connects to upstream
//! PostgreSQL with pgwire's client API. The core is a **message pump**: after
//! authenticating upstream, the session forwards typed wire protocol messages
//! in both directions, observing (and, in later milestones, intercepting)
//! every message.
//!
//! # M0 scope
//!
//! Transparent passthrough proxy, one upstream connection per client session:
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use pgferry::Proxy;
//!
//! let proxy = Proxy::builder()
//!     .listen("127.0.0.1:6432")?
//!     .upstream("host=127.0.0.1 port=5432")
//!     .password_env("UPSTREAM_PASSWORD")
//!     .build()?;
//! proxy.run().await?;
//! # Ok(())
//! # }
//! ```

pub mod admin;
pub mod cancel;
pub mod config;
pub mod intercept;
pub mod metrics;
pub mod pool;
pub mod proxy;
pub mod routing;
pub mod runtime;
pub mod scatter;
pub mod session;
pub mod upstream;

pub use cancel::CancelRouter;
pub use config::{EndpointConfig, EndpointId};
pub use config::{ProxyConfig, RouteConfig};
pub use intercept::{
    Action, ColumnMeta, CycleKind, CycleReport, DataRowMut, Interceptor, NoopInterceptor,
    QueryCycle, RetryDecision, RoutingInfo, RowAction, RowError, RowSchema, UpstreamError,
};
pub use pool::{Pool, PoolConfig, PoolKey, PoolManager, PoolMode, UpstreamLease};
pub use proxy::{Proxy, ProxyBuilder, ProxyServer, ProxyShared};
pub use routing::{EndpointGroup, FailoverRouter, ReadPreference, RwSplit};
pub use scatter::{MergePolicy, ScatterLeg, ScatterRequest, ShardSet};

/// Errors produced by pgferry.
#[derive(Debug, thiserror::Error)]
pub enum PgFerryError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream connection error: {0}")]
    Upstream(#[from] pgwire::error::PgWireClientError),
    #[error("wire protocol error: {0}")]
    Protocol(#[from] pgwire::error::PgWireError),
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
}
