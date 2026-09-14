//! Upstream connection management.
//!
//! Connections are established with pgwire's client API
//! ([`PgWireClient::connect`]), which performs the full startup handshake
//! including cleartext/MD5/SCRAM authentication. After `connect` returns,
//! the [`PgWireClient`] is used purely as a message pipe
//! (`Sink<PgWireFrontendMessage>` + `Stream<Item = PgWireBackendMessage>`)
//! for the rest of the session.

use std::sync::Arc;

use pgwire::api::client::ClientInfo as _;
use pgwire::api::client::Config;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::tokio::client::PgWireClient;

/// Connect to an upstream endpoint. `user`/`database` come from the
/// downstream client's startup message; the connstring, password and TLS
/// settings come from the endpoint/route configuration.
///
/// Note: tracked startup parameters (application_name, client_encoding)
/// are NOT set here — the pool applies them per-checkout with `SET` so
/// that every pooled connection's post-`DISCARD ALL` baseline stays the
/// server default.
pub async fn connect_parts(
    connstring: &str,
    user: &str,
    database: Option<&str>,
    password: Option<&str>,
    tls_connector: Option<pgwire::tokio::TlsConnector>,
) -> Result<PgWireClient, pgwire::error::PgWireClientError> {
    let mut config: Config = connstring.parse()?;

    config.user(user);
    if let Some(db) = database {
        config.dbname(db);
    }
    if let Some(password) = password {
        config.password(password);
    }

    let client = PgWireClient::connect(
        Arc::new(config),
        DefaultStartupHandler::new(),
        tls_connector,
    )
    .await?;
    tracing::debug!(
        upstream_pid = client.process_id(),
        parameters = ?client.server_parameters().keys().collect::<Vec<_>>(),
        "upstream connected"
    );
    Ok(client)
}
