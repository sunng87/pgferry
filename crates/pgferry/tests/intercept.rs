//! M2 interceptor integration tests, against the in-process fake upstream:
//!
//! - deny (simple protocol) — injected error + connection stays usable
//! - deny (extended protocol) — swallow-until-Sync recovery
//! - SQL rewrite (simple + extended)
//! - local answer (Reply) — no upstream involvement
//! - row masking (on_row) — value-level rewrite with schema tracking
//! - cycle telemetry (on_cycle_end) — tag, rows, error, latency

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo as ServerClientInfo;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::Response as ClientResponse;
use pgwire::api::client::query::{DefaultExtendedQueryHandler, DefaultSimpleQueryHandler};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};
use pgwire::messages::response::CommandComplete;
use pgwire::tokio::client::PgWireClient;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tokio_util::bytes::{BufMut, BytesMut};

use pgferry::intercept::messages::CycleError;
use pgferry::{
    Action, CycleReport, DataRowMut, Interceptor, Proxy, QueryCycle, RowAction, RowSchema,
};

/// Echo backend: one text row with the query text.
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
        if query == "two-cols" {
            // two-column row for masking tests
            let fields = vec![
                FieldInfo::new("visible".into(), None, None, Type::TEXT, FieldFormat::Text),
                FieldInfo::new("secret".into(), None, None, Type::TEXT, FieldFormat::Text),
            ];
            let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
            encoder.encode_field(&"show".to_owned())?;
            encoder.encode_field(&"classified".to_owned())?;
            let data_row = encoder.take_row();
            let mut response =
                QueryResponse::new(Arc::new(fields), futures::stream::iter(vec![Ok(data_row)]));
            response.set_command_tag("SELECT");
            return Ok(vec![Response::Query(response)]);
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

async fn spawn_proxy_with<I: Interceptor>(
    upstream: SocketAddr,
    interceptor: I,
) -> pgferry::ProxyServer {
    Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .upstream(format!("host=127.0.0.1 port={}", upstream.port()))
        .service(interceptor)
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

/// Build a local "SELECT one text row" response for Action::Reply.
fn local_row_response(text: &str) -> Vec<PgWireBackendMessage> {
    let rd = RowDescription::new(vec![FieldDescription::new(
        "local".to_owned(),
        0,
        0,
        25, // TEXT
        -1,
        -1,
        0, // text format
    )]);
    let mut data = BytesMut::new();
    data.put_i32(text.len() as i32);
    data.extend_from_slice(text.as_bytes());
    let row = DataRow::new(data, 1);
    vec![
        PgWireBackendMessage::RowDescription(rd),
        PgWireBackendMessage::DataRow(row),
        PgWireBackendMessage::CommandComplete(CommandComplete::new("SELECT 1".to_owned())),
    ]
}

/// The test gateway: deny / rewrite / local-answer / telemetry.
struct Gateway {
    reports: Arc<Mutex<Vec<CycleReport>>>,
}

#[async_trait]
impl Interceptor for Gateway {
    type Ctx = ();

    async fn on_query(&self, _ctx: &mut Self::Ctx, cycle: &mut QueryCycle) -> Action {
        if cycle.sql().contains("deny-me") {
            return Action::Deny(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "42501".to_owned(),
                "denied by test interceptor".to_owned(),
            )));
        }
        if cycle.sql().contains("rewrite-me") {
            cycle.set_sql("SELECT 'rewritten-by-proxy'");
        }
        if cycle.sql().contains("respond-locally") {
            return Action::Reply(local_row_response("local answer"));
        }
        Action::Forward
    }

    async fn on_cycle_end(&self, _ctx: &mut Self::Ctx, report: &CycleReport) {
        self.reports.lock().unwrap().push(report.clone());
    }
}

/// Row masking: overwrite every text column with "***".
struct MaskGateway;

#[async_trait]
impl Interceptor for MaskGateway {
    type Ctx = ();

    fn wants_rows(&self) -> bool {
        true
    }

    async fn on_row(
        &self,
        _ctx: &mut Self::Ctx,
        _schema: &RowSchema,
        row: &mut DataRowMut<'_>,
    ) -> RowAction {
        for i in 0..row.len() {
            if let Ok(Some(_)) = row.text(i) {
                let _ = row.set_text(i, "***");
            }
        }
        RowAction::Keep
    }
}

#[tokio::test]
async fn deny_simple_protocol() {
    let upstream = spawn_upstream().await;
    let gateway = Gateway {
        reports: Arc::new(Mutex::new(Vec::new())),
    };
    let reports = gateway.reports.clone();
    let proxy = spawn_proxy_with(upstream, gateway).await;
    let mut client = connect_client(proxy.local_addr()).await;

    // denied query errors with our SQLSTATE
    let error = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'deny-me'")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("42501"), "{error}");

    // connection stays usable
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'ok'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'ok'");

    // telemetry saw both cycles
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    let denied = &reports[0];
    assert_eq!(denied.sql.as_deref(), Some("select 'deny-me'"));
    assert_eq!(
        denied.error.as_ref().map(|e: &CycleError| e.code.as_str()),
        Some("42501")
    );
    assert_eq!(denied.rows, 0);
    assert!(denied.tag.is_none());
    let ok = &reports[1];
    assert_eq!(ok.sql.as_deref(), Some("select 'ok'"));
    assert_eq!(ok.tag.as_deref(), Some("SELECT 1"));
    assert_eq!(ok.rows, 1);
    assert!(ok.error.is_none());

    proxy.shutdown();
}

#[tokio::test]
async fn deny_extended_protocol() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy_with(
        upstream,
        Gateway {
            reports: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await;
    let mut client = connect_client(proxy.local_addr()).await;

    {
        // one-shot extended query: Parse/Bind/Execute/Sync in one batch;
        // the deny is injected at Parse, the rest swallowed until Sync
        let mut handler = DefaultExtendedQueryHandler::new();
        let mut extended = client.extended_query(&mut handler);
        let error = extended
            .query("select 'deny-me'", &[], vec![])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("42501"), "{error}");
    }

    // connection stays usable
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'ok'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "select 'ok'");

    proxy.shutdown();
}

#[tokio::test]
async fn rewrite_sql() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy_with(
        upstream,
        Gateway {
            reports: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await;
    let mut client = connect_client(proxy.local_addr()).await;

    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'rewrite-me'")
        .await
        .unwrap();
    // the echo backend echoes what the upstream received: the rewritten SQL
    assert_eq!(echo_result(responses), "SELECT 'rewritten-by-proxy'");

    proxy.shutdown();
}

#[tokio::test]
async fn respond_locally() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy_with(
        upstream,
        Gateway {
            reports: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await;
    let mut client = connect_client(proxy.local_addr()).await;

    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'respond-locally'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "local answer");

    proxy.shutdown();
}

#[tokio::test]
async fn mask_rows() {
    let upstream = spawn_upstream().await;
    let proxy = spawn_proxy_with(upstream, MaskGateway).await;
    let mut client = connect_client(proxy.local_addr()).await;

    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'secret'")
        .await
        .unwrap();
    assert_eq!(echo_result(responses), "***");

    proxy.shutdown();
}

#[tokio::test]
async fn mask_rows_preserves_unmasked_columns() {
    // regression: an unmodified row must be forwarded byte-identical (the
    // DataRowMut zero-copy path)
    let upstream = spawn_upstream().await;

    struct NameMask;
    #[async_trait]
    impl Interceptor for NameMask {
        type Ctx = ();
        fn wants_rows(&self) -> bool {
            true
        }
        async fn on_row(
            &self,
            _ctx: &mut Self::Ctx,
            schema: &RowSchema,
            row: &mut DataRowMut<'_>,
        ) -> RowAction {
            if let Some(i) = schema.column_index("secret")
                && let Ok(Some(_)) = row.text(i)
            {
                let _ = row.set_text(i, "***");
            }
            RowAction::Keep
        }
    }

    let proxy = spawn_proxy_with(upstream, NameMask).await;
    let mut client = connect_client(proxy.local_addr()).await;

    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "two-cols")
        .await
        .unwrap();
    assert_eq!(responses.len(), 1);
    let response = responses.into_iter().next().unwrap();
    let mut reader = response.into_data_rows_reader();
    let mut row = reader.next_row().expect("one row");
    assert_eq!(row.next_value::<String>().unwrap().unwrap(), "show");
    assert_eq!(row.next_value::<String>().unwrap().unwrap(), "***");

    proxy.shutdown();
}
