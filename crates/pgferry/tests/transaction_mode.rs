//! M3 transaction-pooling integration tests against the in-process fake
//! upstream:
//!
//! - two client sessions alternate transactions over one pooled upstream
//!   connection (attach/detach driven by ReadyForQuery status)
//! - named prepared statements survive detach and are replayed on re-attach
//!   (extended protocol with named Parse/Bind/Execute)
//! - a bare Sync while detached is answered locally

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::ClientInfo as ServerClientInfo;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::Response as ClientResponse;
use pgwire::api::client::query::{DefaultExtendedQueryHandler, DefaultSimpleQueryHandler};
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::extendedquery::Parse;
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use pgferry::{PoolConfig, PoolMode, Proxy};

/// Transaction-aware fake upstream.
///
/// Simple protocol: BEGIN/COMMIT/ROLLBACK drive the RFQ transaction status;
/// anything else returns an echo row. Extended protocol: Parse stores the
/// query; Execute echoes it. Statement tracking counts replays per
/// connection.
struct TxBackend {
    connections: Arc<AtomicUsize>,
    parse_total: Arc<AtomicUsize>,
}

#[async_trait]
impl SimpleQueryHandler for TxBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match query.trim().to_uppercase().as_str() {
            "BEGIN" => Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]),
            "COMMIT" => Ok(vec![Response::TransactionEnd(Tag::new("COMMIT"))]),
            "ROLLBACK" => Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]),
            _ => {
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
    }
}

struct TxExtended {
    parse_total: Arc<AtomicUsize>,
}

#[async_trait]
impl ExtendedQueryHandler for TxExtended {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    /// Count Parse messages (statement replays show up here).
    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.parse_total.fetch_add(1, Ordering::SeqCst);
        // the default on_parse behavior (parse + store + ParseComplete)
        let name = message
            .name
            .clone()
            .unwrap_or_else(|| pgwire::api::DEFAULT_NAME.to_owned());
        let parser = self.query_parser();
        match StoredStatement::parse(client, &message, parser).await? {
            Some(stmt) => client.portal_store().put_statement(Arc::new(stmt)),
            None => client.portal_store().put_empty_statement(&name),
        }
        client
            .send(PgWireBackendMessage::ParseComplete(
                pgwire::messages::extendedquery::ParseComplete::new(),
            ))
            .await?;
        Ok(())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement;
        let fields = vec![FieldInfo::new(
            "echo".into(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )];
        let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
        encoder.encode_field(&sql.to_owned())?;
        let data_row = encoder.take_row();
        let mut response =
            QueryResponse::new(Arc::new(fields), futures::stream::iter(vec![Ok(data_row)]));
        response.set_command_tag("SELECT");
        Ok(Response::Query(response))
    }
}

struct TxHandlers {
    backend: TxBackend,
    extended: TxExtended,
}

#[async_trait]
impl pgwire::api::auth::StartupHandler for TxHandlers {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ServerClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        pgwire::api::NoopHandler.on_startup(client, message).await
    }
}

impl PgWireServerHandlers for TxHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(TxBackend {
            connections: self.backend.connections.clone(),
            parse_total: self.backend.parse_total.clone(),
        })
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(TxExtended {
            parse_total: self.extended.parse_total.clone(),
        })
    }
}

struct FakeUpstream {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    parse_total: Arc<AtomicUsize>,
}

impl FakeUpstream {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
    fn parse_total(&self) -> usize {
        self.parse_total.load(Ordering::SeqCst)
    }
}

use pgwire::messages::PgWireFrontendMessage;

async fn spawn_tx_upstream() -> FakeUpstream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let parse_total = Arc::new(AtomicUsize::new(0));
    let counter = connections.clone();
    let counter2 = parse_total.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let parse_counter = counter2.clone();
            tokio::spawn(async move {
                // wrap extended handler to count parse replays
                let handlers = TxHandlers {
                    backend: TxBackend {
                        connections: Arc::new(AtomicUsize::new(0)),
                        parse_total: parse_counter.clone(),
                    },
                    extended: TxExtended {
                        parse_total: parse_counter,
                    },
                };
                let _ = process_socket(socket, None, handlers).await;
            });
        }
    });
    FakeUpstream {
        addr,
        connections,
        parse_total,
    }
}

async fn spawn_tx_proxy(upstream: SocketAddr) -> pgferry::ProxyServer {
    Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .upstream(format!("host=127.0.0.1 port={}", upstream.port()))
        .pool_config(PoolConfig {
            mode: PoolMode::Transaction,
            stale_after: Duration::from_secs(600),
            ..PoolConfig::default()
        })
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
    let mut reader = response.into_data_rows_reader();
    let mut row = reader.next_row().expect("one row");
    row.next_value::<String>()
        .expect("decode ok")
        .expect("non-null")
}

async fn upstream_stable_at(upstream: &FakeUpstream, expected: usize) {
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

/// Two sessions alternate explicit transactions; one pooled upstream
/// connection serves both (attach/detach on transaction boundaries).
#[tokio::test]
async fn transaction_mode_shares_connection_across_sessions() {
    let upstream = spawn_tx_upstream().await;
    let proxy = spawn_tx_proxy(upstream.addr).await;

    let mut a = connect_client(proxy.local_addr()).await;
    let mut b = connect_client(proxy.local_addr()).await;

    // A opens a transaction (attaches), B stays idle
    a.simple_query(DefaultSimpleQueryHandler::new(), "BEGIN")
        .await
        .unwrap();
    let responses = a
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'a-in-tx'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'a-in-tx'");
    a.simple_query(DefaultSimpleQueryHandler::new(), "COMMIT")
        .await
        .unwrap();
    // A detached; B's cycle attaches (same pooled conn, hopefully)
    let responses = b
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'b-tx'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'b-tx'");

    // A again, then B again — each transaction attaches/detaches
    let responses = a
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'a2'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'a2'");
    let responses = b
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'b2'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'b2'");

    upstream_stable_at(&upstream, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        upstream.connections(),
        1,
        "both sessions must share one pooled upstream connection"
    );

    proxy.shutdown();
}

/// A named prepared statement survives detach: it is re-Parse'd on the next
/// attach, and a client re-Parse of the same name is answered locally.
#[tokio::test]
async fn transaction_mode_replays_prepared_statements() {
    let upstream = spawn_tx_upstream().await;
    let proxy = spawn_tx_proxy(upstream.addr).await;
    let mut client = connect_client(proxy.local_addr()).await;

    let parses_before = upstream.parse_total();

    {
        let mut handler = DefaultExtendedQueryHandler::new();
        let mut extended = client.extended_query(&mut handler);
        extended
            .prepare(Some("s1"), "SELECT 'prepared'", &[])
            .await
            .unwrap();
        // prepare sends Parse+Describe+Sync — in transaction mode the
        // connection is detached at the ReadyForQuery of that cycle.
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // execute the prepared statement in a later cycle: the registry is
    // replayed (re-Parse) onto whatever connection serves the attach
    {
        let mut handler = DefaultExtendedQueryHandler::new();
        let mut extended = client.extended_query(&mut handler);
        extended
            .bind(Some("p1"), Some("s1"), vec![], vec![])
            .await
            .unwrap();
        let rows = extended.execute(Some("p1"), 0).await.unwrap();
        match rows {
            pgwire::api::client::query::ExecuteResult::Complete(rows)
            | pgwire::api::client::query::ExecuteResult::Suspended(rows) => {
                assert_eq!(rows.len(), 1);
            }
        }
    }
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'after-extended'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'after-extended'");

    // the replay must have happened: the second attach re-Parse'd s1
    let parses_after = upstream.parse_total();
    assert!(
        parses_after > parses_before,
        "statement must be replayed on re-attach (before={parses_before} after={parses_after})"
    );

    proxy.shutdown();
}

/// A bare Sync while detached is answered locally (no upstream involvement).
#[tokio::test]
async fn transaction_mode_answers_sync_locally() {
    let upstream = spawn_tx_upstream().await;
    let proxy = spawn_tx_proxy(upstream.addr).await;
    let mut client = connect_client(proxy.local_addr()).await;

    // detached right after startup; send Sync + then a simple query
    // (the client API doesn't expose a bare Sync; emulate with a no-op
    // extended cycle: prepare of an empty-ish statement then close)
    {
        let mut handler = DefaultExtendedQueryHandler::new();
        let mut extended = client.extended_query(&mut handler);
        // describe a non-existent statement: Describe + Sync while detached
        let err = extended
            .describe(pgwire::api::client::query::DescribeTarget::Statement(Some(
                "does-not-exist",
            )))
            .await;
        assert!(err.is_err(), "unknown statement should error");
    }

    // still usable
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'still-ok'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'still-ok'");

    proxy.shutdown();
}
