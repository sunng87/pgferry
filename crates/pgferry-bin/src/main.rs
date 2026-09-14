//! pgferry binary: TOML config + CLI wrapper around the pgferry library.

use std::path::PathBuf;

use clap::Parser;
use pgferry::{Proxy, RouteConfig};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "pgferry", about = "A programmable PostgreSQL proxy")]
struct Args {
    /// Path to a TOML configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Listen address; overrides the config file.
    #[arg(short, long)]
    listen: Option<String>,
    /// Upstream libpq connection string, e.g. "host=127.0.0.1 port=5432".
    #[arg(short, long)]
    upstream: Option<String>,
    /// Upstream password; overrides the config file.
    #[arg(long)]
    password: Option<String>,
}

/// File format of `pgferry.toml` (see pgferry.toml at the repo root).
#[derive(Debug, Deserialize)]
struct FileConfig {
    listen: String,
    #[serde(default)]
    max_client_conn: Option<usize>,
    /// Admin console database name (e.g. "pgferry_admin").
    #[serde(default)]
    admin_database: Option<String>,
    /// Prometheus /metrics endpoint address.
    #[serde(default)]
    metrics_addr: Option<String>,
    /// Graceful-shutdown drain window in seconds (default 30).
    #[serde(default)]
    shutdown_drain_secs: Option<u64>,
    /// Downstream TLS termination.
    #[serde(default)]
    server_tls: Option<FileServerTls>,
    route: FileRoute,
    #[serde(default)]
    pool: FilePool,
}

#[derive(Debug, Clone, Deserialize)]
struct FileServerTls {
    cert: String,
    key: String,
}

#[derive(Debug, Deserialize)]
struct FileRoute {
    name: Option<String>,
    /// Single-endpoint shorthand.
    upstream: Option<String>,
    /// Named endpoints (routing/failover). Overrides `upstream` when set.
    #[serde(default)]
    endpoints: Vec<FileEndpoint>,
    password: Option<String>,
    password_env: Option<String>,
    /// PEM CA file to verify the upstream TLS certificate.
    #[serde(default)]
    tls_ca: Option<String>,
    /// Accept the upstream certificate without verification.
    #[serde(default)]
    tls_insecure: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FileEndpoint {
    id: String,
    upstream: String,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct FilePool {
    /// "session" (default) or "transaction"
    mode: Option<String>,
    max_size: Option<usize>,
    idle_timeout_secs: Option<u64>,
    max_lifetime_secs: Option<u64>,
    stale_after_secs: Option<u64>,
    sweep_interval_secs: Option<u64>,
}

impl FilePool {
    fn into_config(self) -> pgferry::pool::PoolConfig {
        let mut config = pgferry::pool::PoolConfig::default();
        if let Some(v) = self.mode {
            config.mode = match v.as_str() {
                "session" => pgferry::pool::PoolMode::Session,
                "transaction" => pgferry::pool::PoolMode::Transaction,
                other => {
                    eprintln!("unknown pool mode {other:?}; using session");
                    pgferry::pool::PoolMode::Session
                }
            };
        }
        if let Some(v) = self.max_size {
            config.max_size = v;
        }
        if let Some(v) = self.idle_timeout_secs {
            config.idle_timeout = std::time::Duration::from_secs(v);
        }
        if let Some(v) = self.max_lifetime_secs {
            config.max_lifetime = std::time::Duration::from_secs(v);
        }
        if let Some(v) = self.stale_after_secs {
            config.stale_after = std::time::Duration::from_secs(v);
        }
        if let Some(v) = self.sweep_interval_secs {
            config.sweep_interval = std::time::Duration::from_secs(v);
        }
        config
    }
}

fn resolve_password(route: &FileRoute, cli_password: Option<String>) -> Option<String> {
    if let Some(password) = cli_password {
        return Some(password);
    }
    if let Some(password) = &route.password {
        return Some(password.clone());
    }
    if let Some(var) = &route.password_env {
        return std::env::var(var).ok();
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let file = match &args.config {
        Some(path) => Some(toml::from_str::<FileConfig>(&std::fs::read_to_string(
            path,
        )?)?),
        None => None,
    };

    let max_client_conn = file.as_ref().and_then(|f| f.max_client_conn).unwrap_or(128);
    let pool = file.as_ref().map(|f| f.pool.clone()).unwrap_or_default();

    let listen = args
        .listen
        .or_else(|| file.as_ref().map(|f| f.listen.clone()))
        .ok_or("no listen address: use --listen or a config file")?;

    let file_route = file.as_ref().map(|f| &f.route);
    let upstream = args
        .upstream
        .or_else(|| file_route.and_then(|r| r.upstream.clone()))
        .unwrap_or_default();

    let route = RouteConfig {
        name: file_route
            .and_then(|r| r.name.clone())
            .unwrap_or_else(|| "default".to_owned()),
        endpoints: {
            let mut endpoints: Vec<pgferry::config::EndpointConfig> = file_route
                .iter()
                .flat_map(|r| r.endpoints.iter())
                .map(|e| pgferry::config::EndpointConfig {
                    id: e.id.clone(),
                    upstream: e.upstream.clone(),
                    password: e.password.clone(),
                    tls: None,
                })
                .collect();
            if endpoints.is_empty() {
                endpoints.push(pgferry::config::EndpointConfig {
                    id: "default".to_owned(),
                    upstream,
                    password: None,
                    tls: None,
                });
            }
            endpoints
        },
        password: file_route.and_then(|r| resolve_password(r, args.password.clone())),
        tls: pgferry::config::UpstreamTlsConfig {
            ca: file_route
                .and_then(|r| r.tls_ca.clone())
                .map(std::path::PathBuf::from),
            insecure: file_route.and_then(|r| r.tls_insecure).unwrap_or(false),
        },
    };

    let mut builder = Proxy::builder()
        .listen(&listen)?
        .route(route.clone())
        .pool_config(pool.into_config())
        .max_client_conn(max_client_conn);

    if let Some(admin_database) = file.as_ref().and_then(|f| f.admin_database.clone()) {
        builder = builder.admin_database(admin_database);
    }
    if let Some(metrics_addr) = file.as_ref().and_then(|f| f.metrics_addr.clone()) {
        builder = builder.metrics_addr(&metrics_addr)?;
    }
    if let Some(tls) = file.as_ref().and_then(|f| f.server_tls.clone()) {
        builder = builder.tls_server(tls.cert, tls.key);
    }
    if let Some(drain) = file.as_ref().and_then(|f| f.shutdown_drain_secs) {
        builder = builder.shutdown_drain(std::time::Duration::from_secs(drain));
    }
    let proxy = builder.build()?;

    tracing::info!(
        listen = %proxy.config().listen_addr,
        route = %route.name,
        endpoints = ?route.endpoints.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        password_set = route.password.is_some(),
        "starting pgferry"
    );

    let server = proxy.serve().await?;

    // SIGINT or SIGTERM → graceful shutdown (drain sessions, then stop)
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
    tracing::info!("shutting down (graceful)");
    server.shutdown_graceful().await?;
    Ok(())
}
