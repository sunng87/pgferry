//! M4 operational-surface integration tests: admin console, metrics
//! endpoint, graceful shutdown.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo as ServerClientInfo;
use pgwire::api::client::ClientInfo as _;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientPortalStore, PgWireServerHandlers};
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::process_socket;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use pgferry::{Proxy, ProxyServer};

/// Minimal echo backend ("SELECT n" → one row).
struct EchoBackend;

#[async_trait]
impl SimpleQueryHandler for EchoBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        Ok(vec![Response::Execution(Tag::new(query))])
    }
}

impl PgWireServerHandlers for EchoBackend {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(EchoBackend)
    }
}

async fn spawn_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _ = process_socket(socket, None, EchoBackend).await;
            });
        }
    });
    addr
}

async fn spawn_proxy(upstream: SocketAddr) -> ProxyServer {
    Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .upstream(format!("host=127.0.0.1 port={}", upstream.port()))
        .admin_database("pgferry_admin")
        .metrics_addr("127.0.0.1:0")
        .unwrap()
        .shutdown_drain(Duration::from_secs(2))
        .build()
        .unwrap()
        .serve()
        .await
        .unwrap()
}

async fn connect_client(proxy: SocketAddr, database: &str) -> PgWireClient {
    let mut config = pgwire::api::client::Config::new();
    config.host("127.0.0.1");
    config.port(proxy.port());
    config.user("testuser");
    config.dbname(database);
    PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .unwrap()
}

/// SHOW POOLS / SHOW CLIENTS / unknown commands on the admin console.
#[tokio::test]
async fn admin_console() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy(upstream).await;

    // one regular session (to appear in SHOW CLIENTS / pools)
    let mut client = connect_client(proxy.local_addr(), "testdb").await;
    client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 1")
        .await
        .unwrap();

    let mut admin = connect_client(proxy.local_addr(), "pgferry_admin").await;

    // admin startup parameters are synthetic
    assert!(admin.server_parameters().contains_key("server_version"));
    assert_eq!(
        admin
            .server_parameters()
            .get("server_version")
            .map(String::as_str),
        Some(format!("pgferry-{}", env!("CARGO_PKG_VERSION")).as_str())
    );

    // SHOW CLIENTS: the admin session itself + the regular client
    let mut responses = admin
        .simple_query(DefaultSimpleQueryHandler::new(), "show clients")
        .await
        .unwrap();
    let clients = text_rows(responses.remove(0));
    assert!(clients.len() >= 2, "expected >=2 clients, got {clients:?}");
    assert!(clients.iter().any(|r| r.contains("pgferry_admin")));
    assert!(clients.iter().any(|r| r.contains("testdb")));

    // SHOW POOLS: the route's pool appears with its knobs
    let mut responses = admin
        .simple_query(DefaultSimpleQueryHandler::new(), "show pools")
        .await
        .unwrap();
    let pools = text_rows(responses.remove(0));
    assert!(
        pools
            .iter()
            .any(|r| r.contains("testdb") && r.contains("testuser"))
    );

    // SHOW STATS works
    let mut responses = admin
        .simple_query(DefaultSimpleQueryHandler::new(), "show stats")
        .await
        .unwrap();
    assert!(!text_rows(responses.remove(0)).is_empty());

    // unknown command → SQLSTATE 42601, session stays usable
    let error = admin
        .simple_query(DefaultSimpleQueryHandler::new(), "drop table nope")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("42601"), "{error}");
    let mut responses = admin
        .simple_query(DefaultSimpleQueryHandler::new(), "show help")
        .await
        .unwrap();
    assert!(!text_rows(responses.remove(0)).is_empty());

    proxy.shutdown();
}

/// The Prometheus endpoint serves pgferry_* metrics.
#[tokio::test]
async fn metrics_endpoint() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy(upstream).await;

    // generate some traffic so pool metrics are nonzero
    let mut client = connect_client(proxy.local_addr(), "testdb").await;
    client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 1")
        .await
        .unwrap();
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = tokio::net::TcpStream::connect(metrics_addr_of(&proxy))
        .await
        .unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();

    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains("pgferry_sessions_active"), "{body}");
    assert!(body.contains("pgferry_pool_checkouts_total"), "{body}");
    assert!(body.contains("pgferry_pool_connections_idle"), "{body}");

    // 404 for other paths
    let mut stream = tokio::net::TcpStream::connect(metrics_addr_of(&proxy))
        .await
        .unwrap();
    stream
        .write_all(b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    stream.read_to_string(&mut body).await.unwrap();
    assert!(body.starts_with("HTTP/1.1 404"), "{body}");

    proxy.shutdown();
}

/// Graceful shutdown: in-flight sessions drain, the client connection
/// closes, and the server task exits without aborts.
#[tokio::test]
async fn graceful_shutdown() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy(upstream).await;
    let mut client = connect_client(proxy.local_addr(), "testdb").await;
    client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 1")
        .await
        .unwrap();

    proxy.shutdown_graceful().await.unwrap();

    // the client connection is closed by the drained session
    let error = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 2")
        .await;
    assert!(error.is_err(), "client should be disconnected after drain");
}

fn text_rows(response: pgwire::api::client::query::Response) -> Vec<String> {
    let mut reader = response.into_data_rows_reader();
    let mut rows = Vec::new();
    while let Some(mut row) = reader.next_row() {
        let mut cols = Vec::new();
        while let Ok(Some(value)) = row.next_value::<String>() {
            cols.push(value);
        }
        rows.push(cols.join("|"));
    }
    rows
}

/// The bound metrics endpoint address (port 0 in tests).
fn metrics_addr_of(proxy: &ProxyServer) -> SocketAddr {
    proxy.metrics_addr().expect("metrics endpoint configured")
}
