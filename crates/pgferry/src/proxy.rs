//! Proxy entrypoint: builder, listener, accept loop, pool sweeper.

use std::net::SocketAddr;
use std::str::FromStr as _;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use pgwire::api::RandomPidSecretKeyGenerator;

use crate::cancel::CancelRouter;
use crate::config::{ConfigError, ProxyConfig, RouteConfig};
use crate::pool::{PoolConfig, PoolManager, run_sweeper};
use crate::session::run_session;

/// State shared by the accept loop and every session task.
pub struct ProxyShared {
    pub route: RouteConfig,
    pub pool_config: PoolConfig,
    pub pools: PoolManager,
    pub router: CancelRouter,
    pub pid_secret: Arc<RandomPidSecretKeyGenerator>,
    pub(crate) interceptor: Option<Arc<dyn crate::intercept::SessionInterceptor>>,
    /// Downstream TLS acceptor (termination).
    pub tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    /// Session registry + counters (admin console, metrics).
    pub runtime: Arc<crate::runtime::RuntimeState>,
    /// Admin-console database name, when enabled.
    pub admin_database: Option<String>,
    /// Shutdown signal for graceful drain.
    pub shutdown: watch::Receiver<bool>,
}

/// Load a PEM cert chain + key into a rustls [`TlsAcceptor`].
fn build_tls_acceptor(
    config: &crate::config::ServerTlsConfig,
) -> std::io::Result<pgwire::tokio::TlsAcceptor> {
    use pgwire::tokio::tokio_rustls::rustls;
    use pgwire::tokio::tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
    use std::io::BufReader;

    let cert_path = &config.cert;
    let certs: Vec<CertificateDer> = certs(&mut BufReader::new(std::fs::File::open(cert_path)?))
        .collect::<Result<_, _>>()
        .map_err(|e| std::io::Error::other(format!("reading {cert_path:?}: {e}")))?;

    let load_key = || -> std::io::Result<PrivateKeyDer> {
        let key_file = std::fs::File::open(&config.key)?;
        let mut reader = BufReader::new(key_file);
        let mut keys: Vec<PrivateKeyDer> = pkcs8_private_keys(&mut reader)
            .map(|key| key.map(PrivateKeyDer::from))
            .collect::<Result<_, _>>()
            .map_err(|e| std::io::Error::other(format!("pkcs8 key: {e}")))?;
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
        let mut reader = BufReader::new(std::fs::File::open(&config.key)?);
        let mut keys: Vec<PrivateKeyDer> = rsa_private_keys(&mut reader)
            .map(|key| key.map(PrivateKeyDer::from))
            .collect::<Result<_, _>>()
            .map_err(|e| std::io::Error::other(format!("rsa key: {e}")))?;
        keys.pop()
            .ok_or_else(|| std::io::Error::other("no private key found in key file"))
    };
    let key = load_key()?;

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| std::io::Error::other(format!("tls config: {e}")))?;
    Ok(pgwire::tokio::TlsAcceptor::from(Arc::new(server_config)))
}

/// A configured, not-yet-running proxy.
#[derive(Clone)]
pub struct Proxy {
    config: ProxyConfig,
    interceptor: Option<Arc<dyn crate::intercept::SessionInterceptor>>,
}

/// Builder for [`Proxy`].
#[derive(Clone, Default)]
pub struct ProxyBuilder {
    listen_addr: Option<SocketAddr>,
    route: Option<RouteConfig>,
    pool_config: Option<PoolConfig>,
    max_client_conn: usize,
    interceptor: Option<Arc<dyn crate::intercept::SessionInterceptor>>,
    admin_database: Option<String>,
    metrics_addr: Option<SocketAddr>,
    tls_server: Option<crate::config::ServerTlsConfig>,
    shutdown_drain: Option<std::time::Duration>,
}

impl Proxy {
    pub fn builder() -> ProxyBuilder {
        ProxyBuilder::default()
    }

    pub fn config(&self) -> &ProxyConfig {
        &self.config
    }

    /// Bind the listener and serve until [`ProxyServer::shutdown`] is called.
    ///
    /// Returns once shutdown completes. Use [`Proxy::serve`] to get a handle
    /// with the bound address (useful with `port 0` in tests).
    pub async fn run(&self) -> std::io::Result<()> {
        self.serve().await?.wait().await
    }

    /// Bind the listener and spawn the accept loop, pool sweeper and
    /// metrics endpoint in the background.
    pub async fn serve(&self) -> std::io::Result<ProxyServer> {
        let listener = TcpListener::bind(self.config.listen_addr).await?;
        let local_addr = listener.local_addr()?;

        let tls_acceptor = match &self.config.tls_server {
            Some(tls) => Some(build_tls_acceptor(tls)?),
            None => None,
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = Arc::new(ProxyShared {
            route: self.config.route.clone(),
            pool_config: self.config.pool.clone(),
            pools: PoolManager::new(),
            router: CancelRouter::new(),
            pid_secret: Arc::new(RandomPidSecretKeyGenerator::default()),
            interceptor: self.interceptor.clone(),
            tls_acceptor,
            runtime: Arc::new(crate::runtime::RuntimeState::new()),
            admin_database: self.config.admin_database.clone(),
            shutdown: shutdown_rx.clone(),
        });

        let sweeper = tokio::spawn(run_sweeper(shared.pools.clone(), self.config.pool.clone()));
        let metrics_listener = match self.config.metrics_addr {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        let metrics_addr = metrics_listener.as_ref().and_then(|l| l.local_addr().ok());
        let metrics = metrics_listener.map(|listener| {
            tokio::spawn(crate::metrics::run_metrics_server(
                listener,
                shared.runtime.clone(),
                shared.pools.clone(),
            ))
        });
        let accept = tokio::spawn(accept_loop(
            listener,
            shared,
            self.config.max_client_conn,
            shutdown_rx,
            self.config.shutdown_drain,
        ));

        info!(
            %local_addr,
            route = %self.config.route.name,
            pool_mode = ?self.config.pool.mode,
            pool_max_size = self.config.pool.max_size,
            max_client_conn = self.config.max_client_conn,
            admin_database = ?self.config.admin_database,
            metrics_addr = ?metrics_addr,
            tls = self.config.tls_server.is_some(),
            "pgferry listening"
        );

        Ok(ProxyServer {
            local_addr,
            metrics_addr,
            shutdown_tx,
            handle: tokio::spawn(async move {
                let _ = accept.await;
                sweeper.abort();
                if let Some(metrics) = metrics {
                    metrics.abort();
                }
            }),
        })
    }
}

impl ProxyBuilder {
    /// Listen address, e.g. `"127.0.0.1:6432"` (port 0 for an ephemeral port).
    pub fn listen(mut self, addr: &str) -> Result<Self, ConfigError> {
        self.listen_addr = Some(
            SocketAddr::from_str(addr).map_err(|e| ConfigError::ListenAddr(addr.to_owned(), e))?,
        );
        Ok(self)
    }

    /// Register the interceptor (the extension point): an object
    /// implementing [`crate::Interceptor`]. The pump dispatches phase hooks
    /// to it for every session and query cycle.
    pub fn service<I: crate::Interceptor>(mut self, interceptor: I) -> Self {
        self.interceptor = Some(Arc::new(interceptor));
        self
    }

    /// Set the whole route at once (alternative to [`upstream`]/[`password`]).
    pub fn route(mut self, route: RouteConfig) -> Self {
        self.route = Some(route);
        self
    }

    /// Set the route's upstream libpq connection string, e.g.
    /// `"host=127.0.0.1 port=5432"` (a single default endpoint). `user`/
    /// `dbname` come from each client's startup message.
    pub fn upstream(mut self, connstring: impl Into<String>) -> Self {
        self.route = Some(RouteConfig {
            name: "default".to_owned(),
            endpoints: vec![crate::config::EndpointConfig {
                id: "default".to_owned(),
                upstream: connstring.into(),
                password: None,
                tls: None,
            }],
            password: None,
            tls: crate::config::UpstreamTlsConfig::default(),
        });
        self
    }

    /// Add a named upstream endpoint (multiple endpoints enable routing
    /// and failover; the first added is the default target).
    pub fn endpoint(mut self, id: impl Into<String>, connstring: impl Into<String>) -> Self {
        let endpoint = crate::config::EndpointConfig {
            id: id.into(),
            upstream: connstring.into(),
            password: None,
            tls: None,
        };
        match &mut self.route {
            // replace the implicit default endpoint from `upstream()`
            Some(route) if route.endpoints.len() == 1 && route.endpoints[0].id == "default" => {
                route.endpoints = vec![endpoint];
            }
            Some(route) => route.endpoints.push(endpoint),
            None => {
                self.route = Some(RouteConfig {
                    name: "default".to_owned(),
                    endpoints: vec![endpoint],
                    password: None,
                    tls: crate::config::UpstreamTlsConfig::default(),
                })
            }
        }
        self
    }

    /// Upstream password, used for upstream authentication.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        if let Some(route) = &mut self.route {
            route.password = Some(password.into());
        }
        self
    }

    /// Read the upstream password from this environment variable at
    /// build time.
    pub fn password_env(self, var: &str) -> Self {
        match std::env::var(var) {
            Ok(password) => self.password(password),
            Err(_) => {
                tracing::warn!(var, "password env variable not set");
                self
            }
        }
    }

    /// Pool tuning (sizes, timeouts). Defaults: see [`PoolConfig`].
    pub fn pool_config(mut self, config: PoolConfig) -> Self {
        self.pool_config = Some(config);
        self
    }

    /// Cap on concurrent downstream client sessions (default 128).
    pub fn max_client_conn(mut self, max: usize) -> Self {
        self.max_client_conn = max;
        self
    }

    /// Admin console: clients whose startup `database` matches this name
    /// get `SHOW POOLS` / `SHOW CLIENTS` / `SHOW STATS` answered locally.
    pub fn admin_database(mut self, name: impl Into<String>) -> Self {
        self.admin_database = Some(name.into());
        self
    }

    /// Prometheus `/metrics` endpoint.
    pub fn metrics_addr(mut self, addr: &str) -> Result<Self, ConfigError> {
        self.metrics_addr = Some(
            SocketAddr::from_str(addr).map_err(|e| ConfigError::ListenAddr(addr.to_owned(), e))?,
        );
        Ok(self)
    }

    /// Downstream TLS termination (PEM cert chain + key paths).
    pub fn tls_server(
        mut self,
        cert: impl Into<std::path::PathBuf>,
        key: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.tls_server = Some(crate::config::ServerTlsConfig {
            cert: cert.into(),
            key: key.into(),
        });
        self
    }

    /// Graceful-shutdown drain window (default 30s).
    pub fn shutdown_drain(mut self, drain: std::time::Duration) -> Self {
        self.shutdown_drain = Some(drain);
        self
    }

    pub fn build(self) -> Result<Proxy, ConfigError> {
        // multi-endpoint routes without a custom service get the built-in
        // failover router (first-alive + passive health)
        let interceptor = match (self.interceptor, self.route.as_ref()) {
            (Some(i), _) => Some(i),
            (None, Some(route)) if route.endpoints.len() > 1 => Some(Arc::new(
                crate::routing::FailoverRouter::from_endpoints(&route.endpoints),
            )
                as Arc<dyn crate::intercept::SessionInterceptor>),
            (None, _) => None,
        };
        Ok(Proxy {
            interceptor,
            config: ProxyConfig {
                listen_addr: self.listen_addr.ok_or(ConfigError::Missing("listen"))?,
                route: self.route.ok_or(ConfigError::Missing("upstream"))?,
                pool: self.pool_config.unwrap_or_default(),
                max_client_conn: if self.max_client_conn == 0 {
                    128
                } else {
                    self.max_client_conn
                },
                admin_database: self.admin_database,
                metrics_addr: self.metrics_addr,
                tls_server: self.tls_server,
                shutdown_drain: self
                    .shutdown_drain
                    .unwrap_or_else(|| std::time::Duration::from_secs(30)),
            },
        })
    }
}

/// A running proxy: the accept loop plus shutdown control.
#[derive(Debug)]
pub struct ProxyServer {
    local_addr: SocketAddr,
    metrics_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl ProxyServer {
    /// The bound address (resolves the port when configured with port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The bound metrics endpoint address, when configured.
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    /// Stop accepting new connections and abort active sessions
    /// immediately.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
    }

    /// Stop accepting new connections, signal sessions to finish their
    /// current work, wait for the drain window (configured via
    /// [`ProxyBuilder::shutdown_drain`], default 30s), then return.
    pub async fn shutdown_graceful(self) -> std::io::Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.handle
            .await
            .map_err(|e| std::io::Error::other(format!("server task failed: {e}")))?;
        Ok(())
    }

    /// Wait for the accept loop to finish (e.g. after `shutdown`).
    pub async fn wait(self) -> std::io::Result<()> {
        self.handle
            .await
            .map_err(|e| std::io::Error::other(format!("accept loop failed: {e}")))?;
        Ok(())
    }
}

async fn accept_loop(
    listener: TcpListener,
    shared: Arc<ProxyShared>,
    max_client_conn: usize,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_drain: std::time::Duration,
) {
    let client_slots = Arc::new(Semaphore::new(max_client_conn));
    let mut sessions = JoinSet::new();
    let mut shutdown_rx = shutdown_rx;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                break;
            }
            slot = client_slots.clone().acquire_owned() => {
                let Ok(slot) = slot else { break };
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((socket, addr)) => {
                                let shared = shared.clone();
                                sessions.spawn(async move {
                                    if let Err(error) =
                                        run_session(socket, addr, shared, slot).await
                                    {
                                        error!(%addr, %error, "session failed");
                                    }
                                });
                            }
                            Err(error) => {
                                error!(%error, "accept failed");
                            }
                        }
                    }
                }
            }
        }
    }

    // Graceful drain: stop accepting, signal sessions to finish their
    // current work (the pump's shutdown branch), wait up to the drain
    // window, then abort whatever remains.
    info!("draining sessions (graceful shutdown)");
    let drained = tokio::time::timeout(shutdown_drain, async {
        while sessions.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        let remaining = sessions.len();
        sessions.abort_all();
        warn!(remaining, "shutdown drain timed out; aborted sessions");
    } else {
        info!("all sessions drained");
    }
}
