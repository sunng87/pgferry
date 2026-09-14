//! End-to-end passthrough tests with zero external dependencies:
//!
//! - fake upstream: a pgwire server-api server answering simple queries with
//!   a single row echoing the query text (and counting accepted connections)
//! - pgferry proxy in front of it
//! - a pgwire client-api client connecting through the proxy
//!
//! M1 adds pooling assertions: sequential sessions reuse one upstream
//! connection, and `max_size` caps concurrent upstreams.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo as ServerClientInfo;
use pgwire::api::client::ClientInfo as _;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::api::client::query::Response as ClientResponse;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use pgferry::{PoolConfig, Proxy};

/// Echo backend: answers every simple query with one text row containing the
/// query text. A query of the form `sleep:<millis>:<text>` sleeps before
/// answering, for concurrency tests.
struct EchoBackend;

#[async_trait]
impl SimpleQueryHandler for EchoBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(rest) = query.strip_prefix("sleep:")
            && let Some((millis, text)) = rest.split_once(':')
        {
            tokio::time::sleep(Duration::from_millis(millis.parse().unwrap_or_default())).await;
            return Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
                &format!("SLEPT {text}"),
            ))]);
        }

        let fields = vec![FieldInfo::new(
            "echo".into(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )];
        let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
        encoder.encode_field(&query.to_owned())?;
        let data_row = encoder.take_row();

        let mut response =
            QueryResponse::new(Arc::new(fields), futures::stream::iter(vec![Ok(data_row)]));
        response.set_command_tag("SELECT");

        Ok(vec![Response::Query(response)])
    }
}

impl PgWireServerHandlers for EchoBackend {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(EchoBackend)
    }
}

/// Running fake upstream: address plus a connection (accept) counter.
struct FakeUpstream {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
}

impl FakeUpstream {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

async fn spawn_upstream() -> FakeUpstream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = connections.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let _ = process_socket(socket, None, EchoBackend).await;
            });
        }
    });
    FakeUpstream { addr, connections }
}

async fn spawn_proxy(upstream: SocketAddr) -> pgferry::ProxyServer {
    spawn_proxy_with(upstream, PoolConfig::default()).await
}

async fn spawn_proxy_with(upstream: SocketAddr, pool: PoolConfig) -> pgferry::ProxyServer {
    Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .upstream(format!("host=127.0.0.1 port={}", upstream.port()))
        .pool_config(pool)
        .build()
        .unwrap()
        .serve()
        .await
        .unwrap()
}

async fn connect_client(proxy: SocketAddr) -> PgWireClient {
    let mut config = pgwire::api::client::Config::new();
    config.host("127.0.0.1");
    config.port(proxy.port());
    config.user("testuser");
    config.dbname("testdb");

    PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .unwrap()
}

fn echo_result(responses: Vec<ClientResponse>) -> String {
    assert_eq!(responses.len(), 1);
    let response = responses.into_iter().next().unwrap();
    let ClientResponse::Query((_, fields, rows)) = &response else {
        panic!("expected query response, got {response:?}");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "echo");
    assert_eq!(rows.len(), 1);

    // decode the single TEXT column from the raw DataRow
    let mut reader = response.into_data_rows_reader();
    let mut row = reader.next_row().expect("one row");
    row.next_value::<String>()
        .expect("decode ok")
        .expect("non-null")
}

/// Wait until the upstream accept counter stabilizes at `expected` (the
/// session teardown + checkin is asynchronous with respect to the client
/// dropping its connection).
async fn await_connections(upstream: &FakeUpstream, expected: usize) {
    for _ in 0..200 {
        if upstream.connections() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "expected at least {expected} upstream connections, saw {}",
        upstream.connections()
    );
}

#[tokio::test]
async fn simple_query_passthrough() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy(upstream.addr).await;
    let mut client = connect_client(proxy.local_addr()).await;

    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 'hello world'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "SELECT 'hello world'");

    // the proxy replayed upstream startup parameters
    assert!(client.server_parameters().contains_key("server_version"));

    // a second query on the same session
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "SELECT 1");

    proxy.shutdown();
}

#[tokio::test]
async fn sequential_sessions_reuse_one_upstream() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy(upstream.addr).await;

    // connect, query, drop; then again — each session must check the
    // connection back in, so one upstream connection serves all of them.
    // The small gap between sessions lets the previous checkin (sanitize +
    // park, a few loopback roundtrips) land: like pgbouncer, a checkout
    // racing an in-flight checkin connects fresh rather than waiting.
    for i in 0..3 {
        let mut client = connect_client(proxy.local_addr()).await;
        let responses = client
            .simple_query(DefaultSimpleQueryHandler::new(), &format!("SELECT {i}"))
            .await
            .unwrap();
        assert_eq!(echo_result(responses), format!("SELECT {i}"));
        drop(client);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    await_connections(&upstream, 1).await;
    // give checkin (sanitize + park) a moment to complete, then assert no
    // additional upstream connection was created
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        upstream.connections(),
        1,
        "sequential sessions must reuse the pooled upstream connection"
    );

    proxy.shutdown();
}

#[tokio::test]
async fn pool_max_size_caps_concurrent_upstreams() {
    let upstream = spawn_upstream().await;
    let pool = PoolConfig {
        max_size: 2,
        stale_after: Duration::from_secs(600), // don't probe in this test
        ..PoolConfig::default()
    };
    let proxy = spawn_proxy_with(upstream.addr, pool).await;

    // Four concurrent slow sessions against a pool of two: sessions 3 and 4
    // wait at session startup (before authentication completes) for a
    // connection to be checked in. Checkout blocking at startup is the
    // session-pooling behavior; all four must eventually be served.
    let addr = proxy.local_addr();
    let tasks: Vec<_> = (0..4)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = connect_client(addr).await;
                client
                    .simple_query(DefaultSimpleQueryHandler::new(), &format!("sleep:250:{i}"))
                    .await
                    .is_ok()
            })
        })
        .collect();
    for task in tasks {
        assert!(task.await.unwrap(), "every client must be served");
    }

    assert_eq!(
        upstream.connections(),
        2,
        "max_size=2 must cap concurrent upstream connections"
    );

    proxy.shutdown();
}
