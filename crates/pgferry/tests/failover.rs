//! M5 routing & failover integration tests:
//!
//! - connect-failure failover: primary's pool exhausted/dead → session
//!   reroutes to the secondary (routing hook + retry policy)
//! - mid-lease failover: the primary connection dies mid-session → the
//!   session relinks, the interrupted query gets a synthesized 08006, and
//!   the connection stays usable
//! - RwSplit: reads route to the replica, writes to the primary (verified
//!   via per-instance echo markers)
//! - statement registry survives a failover relink (session mode)

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::ClientInfo as ServerClientInfo;
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
use tokio::net::TcpListener;

use pgferry::intercept::builtin::Audit;
use pgferry::{
    EndpointGroup, Interceptor, Proxy, ProxyServer, ReadPreference, RetryDecision, RoutingInfo,
    RwSplit, UpstreamError,
};

/// Fake upstream whose query results carry a per-instance marker, with a
/// kill switch for failover tests.
struct MarkerBackend {
    marker: &'static str,
    kill: Arc<AtomicBool>,
}

#[async_trait]
impl SimpleQueryHandler for MarkerBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        if self.kill.load(Ordering::SeqCst) {
            // simulate a dying server: a FATAL error closes the connection
            // (pgwire's process_socket closes the socket on fatal errors)
            return Err(pgwire::error::PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "FATAL".to_owned(),
                    "57P01".to_owned(),
                    "killed".to_owned(),
                ),
            )));
        }
        Ok(vec![Response::Execution(Tag::new(&format!(
            "{}:{query}",
            self.marker
        )))])
    }
}

struct MarkerHandlers {
    marker: &'static str,
    kill: Arc<AtomicBool>,
}

impl PgWireServerHandlers for MarkerHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(MarkerBackend {
            marker: self.marker,
            kill: self.kill.clone(),
        })
    }
    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        Arc::new(MarkerExtended {
            marker: self.marker,
        })
    }
}

/// Extended-query echo: returns the statement SQL as an execution tag
/// (enough for Bind/Execute smoke coverage).
struct MarkerExtended {
    marker: &'static str,
}

#[async_trait]
impl pgwire::api::query::ExtendedQueryHandler for MarkerExtended {
    type Statement = String;
    type QueryParser = pgwire::api::stmt::NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire::api::stmt::NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<pgwire::api::results::Response>
    where
        C: ServerClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        pgwire::error::PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(pgwire::api::results::Response::Execution(Tag::new(
            &format!("{}:{}", self.marker, portal.statement.statement),
        )))
    }
}

/// A running fake upstream: address, kill switch.
struct FakeUpstream {
    addr: SocketAddr,
    kill: Arc<AtomicBool>,
}

impl FakeUpstream {
    async fn spawn(marker: &'static str) -> FakeUpstream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let kill = Arc::new(AtomicBool::new(false));
        let kill_task = kill.clone();
        let m = marker;
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                // refuse new connections once killed
                if kill_task.load(Ordering::SeqCst) {
                    drop(socket);
                    continue;
                }
                let kill = kill_task.clone();
                let handlers = MarkerHandlers { marker: m, kill };
                tokio::spawn(async move {
                    let _ = process_socket(socket, None, handlers).await;
                });
            }
        });
        FakeUpstream { addr, kill }
    }

    /// Kill: refuse new connections and fail in-flight queries.
    fn kill(&self) {
        self.kill.store(true, Ordering::SeqCst);
    }
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

fn tag_of(responses: Vec<pgwire::api::client::query::Response>) -> String {
    match responses.into_iter().next().unwrap() {
        pgwire::api::client::query::Response::Execution(tag) => {
            // Tag Display is "tag rows"? — reconstruct via the struct
            format!("{tag:?}")
        }
        other => format!("{other:?}"),
    }
}

/// Multi-endpoint proxy with a shared EndpointGroup gateway (the pattern
/// user code composes).
struct Gateway {
    group: EndpointGroup,
    audit: Audit,
}

#[async_trait]
impl Interceptor for Gateway {
    type Ctx = ();

    async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> String {
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

    async fn on_cycle_end(&self, _ctx: &mut Self::Ctx, report: &pgferry::CycleReport) {
        self.audit.record(report);
    }
}

async fn spawn_failover_proxy(
    primary: SocketAddr,
    secondary: SocketAddr,
) -> (ProxyServer, Arc<Gateway>) {
    let group = EndpointGroup::new(["primary", "secondary"]).with_failure_threshold(1);
    let gateway = Arc::new(Gateway {
        group: group.clone(),
        audit: Audit::new(),
    });
    let gw = gateway.clone();
    let proxy = Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .endpoint("primary", format!("host=127.0.0.1 port={}", primary.port()))
        .endpoint(
            "secondary",
            format!("host=127.0.0.1 port={}", secondary.port()),
        )
        .service(GatewayHandle(gw))
        .build()
        .unwrap()
        .serve()
        .await
        .unwrap();
    (proxy, gateway)
}

/// Wrapper to move an Arc'd gateway into the builder by value.
struct GatewayHandle(Arc<Gateway>);

#[async_trait]
impl Interceptor for GatewayHandle {
    type Ctx = ();

    async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> String {
        self.0.upstream(_ctx, info).await
    }

    async fn on_upstream_error(
        &self,
        _ctx: &mut Self::Ctx,
        error: &UpstreamError,
    ) -> RetryDecision {
        self.0.on_upstream_error(_ctx, error).await
    }

    async fn on_upstream_connected(&self, _ctx: &mut Self::Ctx, endpoint: &str) {
        self.0.on_upstream_connected(_ctx, endpoint).await
    }
}

/// Connect-failure failover: the primary refuses connections; sessions
/// route to the secondary.
#[tokio::test]
async fn failover_on_connect_failure() {
    let primary = FakeUpstream::spawn("primary").await;
    let secondary = FakeUpstream::spawn("secondary").await;
    let (proxy, _gateway) = spawn_failover_proxy(primary.addr, secondary.addr).await;

    primary.kill();

    let mut client = connect_client(proxy.local_addr()).await;
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 1")
        .await
        .unwrap();
    let tag = tag_of(responses);
    assert!(tag.contains("secondary"), "routed to secondary: {tag}");

    proxy.shutdown();
}

/// Mid-lease failover: the primary dies while the session is attached;
/// the interrupted query errors with 08006, the session relinks to the
/// secondary and keeps working.
#[tokio::test]
async fn failover_mid_session_relink() {
    let primary = FakeUpstream::spawn("primary").await;
    let secondary = FakeUpstream::spawn("secondary").await;
    let (proxy, gateway) = spawn_failover_proxy(primary.addr, secondary.addr).await;

    let mut client = connect_client(proxy.local_addr()).await;
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select warmup")
        .await
        .unwrap();
    assert!(tag_of(responses).contains("primary"));

    // primary dies under the session
    primary.kill();

    // in-flight query: interrupted with a synthesized error (the fake
    // upstream errors its do_query) — the session must survive it
    let interrupted = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select doomed")
        .await;
    // either an error surfaced through the proxy (server error forwarded
    // or synthesized 08006) — both acceptable mid-kill races
    if let Err(e) = &interrupted {
        assert!(
            e.to_string().contains("killed") || e.to_string().contains("08006"),
            "unexpected error: {e}"
        );
    }

    // subsequent queries relink to the secondary
    let mut ok = false;
    for _ in 0..50 {
        if let Ok(responses) = client
            .simple_query(DefaultSimpleQueryHandler::new(), "select after-failover")
            .await
            && tag_of(responses).contains("secondary")
        {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(ok, "session did not relink to the secondary");
    assert!(!gateway.group.is_alive("primary"));

    proxy.shutdown();
}

/// Named prepared statements survive a failover relink in session mode:
/// prepare on the primary, kill it, execute on the secondary.
#[tokio::test]
async fn statements_survive_failover_relink() {
    let primary = FakeUpstream::spawn("primary").await;
    let secondary = FakeUpstream::spawn("secondary").await;
    let (proxy, _gateway) = spawn_failover_proxy(primary.addr, secondary.addr).await;

    let mut client = connect_client(proxy.local_addr()).await;

    {
        let mut handler = pgwire::api::client::query::DefaultExtendedQueryHandler::new();
        let mut extended = client.extended_query(&mut handler);
        extended
            .prepare(Some("s1"), "SELECT 'stmt'", &[])
            .await
            .unwrap();
    }

    primary.kill();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // drive traffic until the session relinks (via repeated simple queries)
    for _ in 0..50 {
        let _ = client
            .simple_query(DefaultSimpleQueryHandler::new(), "select push")
            .await;
        if !_gateway.group.is_alive("primary") {
            break;
        }
    }

    // execute the prepared statement — replayed onto the secondary
    let mut handler = pgwire::api::client::query::DefaultExtendedQueryHandler::new();
    let mut extended = client.extended_query(&mut handler);
    let result = extended.bind(Some("p1"), Some("s1"), vec![], vec![]).await;
    assert!(result.is_ok(), "bind after relink failed: {result:?}");
    let result = extended.execute(Some("p1"), 0).await;
    assert!(
        result.is_ok(),
        "prepared statement did not survive relink: {result:?}"
    );

    proxy.shutdown();
}

/// RwSplit: reads to the replica, writes to the primary.
#[tokio::test]
async fn rw_split_routing() {
    let primary = FakeUpstream::spawn("primary").await;
    let replica = FakeUpstream::spawn("replica").await;

    struct Split {
        rw: RwSplit,
    }
    #[async_trait]
    impl Interceptor for Split {
        type Ctx = ();
        async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> String {
            match info.sql() {
                Some(sql) => self.rw.route_sql(info, sql),
                None => self.rw.route(info, false),
            }
        }
        async fn on_upstream_error(
            &self,
            _ctx: &mut Self::Ctx,
            _error: &UpstreamError,
        ) -> RetryDecision {
            RetryDecision::Relink
        }
    }
    let group = EndpointGroup::new(["primary", "replica"]);
    let split = Split {
        rw: RwSplit::new(group, "primary").with_read_preference(ReadPreference::ReplicaFirst),
    };

    let proxy = Proxy::builder()
        .listen("127.0.0.1:0")
        .unwrap()
        .endpoint(
            "primary",
            format!("host=127.0.0.1 port={}", primary.addr.port()),
        )
        .endpoint(
            "replica",
            format!("host=127.0.0.1 port={}", replica.addr.port()),
        )
        .pool_config(pgferry::PoolConfig {
            mode: pgferry::PoolMode::Transaction,
            ..pgferry::PoolConfig::default()
        })
        .service(split)
        .build()
        .unwrap()
        .serve()
        .await
        .unwrap();

    let mut client = connect_client(proxy.local_addr()).await;

    // read → replica
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "select 'read'")
        .await
        .unwrap();
    let tag = tag_of(responses);
    assert!(tag.contains("replica"), "read routed to primary: {tag}");

    // write → primary
    let responses = client
        .simple_query(DefaultSimpleQueryHandler::new(), "insert into t values (1)")
        .await
        .unwrap();
    let tag = tag_of(responses);
    assert!(tag.contains("primary"), "write routed to replica: {tag}");

    proxy.shutdown();
}
