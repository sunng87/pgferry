//! Sharding (M6): scatter-gather as library code.
//!
//! The interceptor's `on_query` returns [`Action::Scatter`](crate::Action::Scatter)
//! with a [`ScatterRequest`] describing per-shard legs; the session
//! executes them concurrently on the endpoints' pools and composes ONE
//! client-visible query cycle from the results (first leg's
//! `RowDescription`, merged rows, a single `CommandComplete`/`ReadyForQuery`).
//! This is the `ExecutionPlan::Scatter` from the roadmap — `Reply`
//! generalized to N legs.
//!
//! [`ShardSet`] is the helper users compose: broadcast a query to all
//! shards (with [`MergePolicy::Sum`] for `count(*)`-style aggregations or
//! [`MergePolicy::Concat`] for row concatenation), or hash a key to its
//! owning shard for single-shard routing (which stays passthrough via the
//! `upstream()` hook).
//!
//! v1 restrictions (documented): scatter requires the simple protocol and
//! an idle transaction state; legs run one simple query each and are
//! checked in clean (no `DISCARD` unless a `ParameterStatus` was seen).

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use pgwire::api::client::ClientInfo as _;
use pgwire::error::ErrorInfo;
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::simplequery::Query;
use pgwire::tokio::client::PgWireClient;

use crate::ProxyShared;
use crate::pool::{Pool, UpstreamLease};
use crate::session::SessionParams;

/// How the legs' result sets compose into one.
#[derive(Debug, Clone)]
pub enum MergePolicy {
    /// One result set: the first leg's `RowDescription`, then every leg's
    /// rows (leg order). The command tag counts the total rows.
    Concat,
    /// Aggregation merge for `count(*)`/`sum()`-style queries: every leg
    /// must return rows with a single numeric first column; the result is
    /// one row with the summed value.
    Sum,
}

/// One scatter leg: run `sql` on `endpoint`'s pool.
#[derive(Debug, Clone)]
pub struct ScatterLeg {
    pub endpoint: String,
    pub sql: String,
}

/// A scatter-gather request (returned as `Action::Scatter` from
/// `on_query`).
#[derive(Debug, Clone)]
pub struct ScatterRequest {
    pub legs: Vec<ScatterLeg>,
    pub merge: MergePolicy,
    /// Command tag for the merged `CommandComplete` (e.g. `"SELECT"`).
    pub tag: String,
}

/// Shard-set helper: the shards are endpoint ids (configured on the
/// route). Broadcast scatters + key-hash single-shard routing.
#[derive(Debug, Clone)]
pub struct ShardSet {
    shards: Vec<String>,
}

impl ShardSet {
    pub fn new<I, S>(shards: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ShardSet {
            shards: shards.into_iter().map(Into::into).collect(),
        }
    }

    pub fn shards(&self) -> &[String] {
        &self.shards
    }

    /// The shard owning `key` (SipHash by key, stable within a process
    /// run). Single-shard traffic stays passthrough: return this id from
    /// the `upstream()` hook.
    pub fn shard_for(&self, key: &str) -> &str {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let h = hasher.finish() as usize;
        self.shards[h % self.shards.len()].as_str()
    }

    /// Broadcast `sql` to every shard.
    pub fn broadcast(&self, sql: &str, merge: MergePolicy, tag: &str) -> crate::Action {
        crate::Action::Scatter(ScatterRequest {
            legs: self
                .shards
                .iter()
                .map(|ep| ScatterLeg {
                    endpoint: ep.clone(),
                    sql: sql.to_owned(),
                })
                .collect(),
            merge,
            tag: tag.to_owned(),
        })
    }
}

/// A leg's outcome after execution.
struct LegOutcome {
    endpoint: String,
    /// The leg cycle's messages (`RowDescription`..`CommandComplete`;
    /// `ReadyForQuery` consumed, `ParameterStatus` applied).
    messages: Vec<PgWireBackendMessage>,
    rows: u64,
    /// Query-level error from the shard (ErrorResponse), when the leg
    /// completed its cycle with an error.
    error: Option<ErrorInfo>,
    /// A checked-out lease to return to the pool (None: session lease or
    /// destroyed).
    lease: Option<UpstreamLease>,
    /// Connection unusable (destroy instead of checkin).
    conn_broken: bool,
    /// A ParameterStatus was seen (the connection must be reset at
    /// checkin).
    dirty: bool,
}

/// A scatter failure surfaced to the client.
pub(crate) struct ScatterFailure {
    pub error: Box<ErrorInfo>,
    /// The session's lease broke during the scatter (must be destroyed).
    pub session_lease_broken: bool,
}

/// Execute a scatter request: run all legs concurrently, fail fast on the
/// first error (dropping the remaining leg futures closes their
/// connections — the backends cancel server-side on disconnect), then
/// merge per policy and write one client-visible cycle. Returns the merged
/// row count.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_scatter(
    downstream: &mut crate::session::Downstream,
    shared: &Arc<ProxyShared>,
    params: &SessionParams,
    factories: &mut HashMap<String, crate::pool::ConnectFactory>,
    request: &ScatterRequest,
    session_lease: &mut Option<UpstreamLease>,
    current_endpoint: &mut Option<String>,
    startup_parameters: &std::collections::BTreeMap<String, String>,
) -> Result<u64, ScatterFailure> {
    use futures::SinkExt as _;

    // validate endpoints up front
    for leg in &request.legs {
        if shared.route.endpoint(&leg.endpoint).is_none() {
            return Err(ScatterFailure {
                error: Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "0A000".to_owned(),
                    format!("pgferry scatter: unknown endpoint {:?}", leg.endpoint),
                )),
                session_lease_broken: false,
            });
        }
    }

    // resolve pools + factories per distinct endpoint
    let mut pools: HashMap<String, Pool> = HashMap::new();
    for leg in &request.legs {
        if pools.contains_key(&leg.endpoint) {
            continue;
        }
        let endpoint = shared.route.endpoint(&leg.endpoint).expect("validated");
        let factory = factories
            .entry(endpoint.id.clone())
            .or_insert_with(|| crate::session::make_factory_for(endpoint, &shared.route, params))
            .clone();
        let key = crate::pool::PoolKey {
            route: shared.route.name.clone(),
            endpoint: endpoint.id.clone(),
            user: params.user.clone(),
            database: params.database.clone(),
        };
        pools.insert(
            endpoint.id.clone(),
            shared
                .pools
                .get_or_create(key, shared.pool_config.clone(), factory),
        );
    }

    // partition: the leg on the session's current endpoint reuses the
    // session lease (avoids self-deadlock on small pools); others check
    // out from their pools
    let session_ep = current_endpoint.clone();
    let mut session_leg_idx: Option<usize> = None;
    let mut futures = FuturesUnordered::new();
    for (idx, leg) in request.legs.iter().enumerate() {
        let pool = pools.get(&leg.endpoint).unwrap().clone();
        if session_ep.as_deref() == Some(leg.endpoint.as_str())
            && session_lease.is_some()
            && session_leg_idx.is_none()
        {
            session_leg_idx = Some(idx);
        } else {
            let endpoint = leg.endpoint.clone();
            let sql = leg.sql.clone();
            let sp = startup_parameters.clone();
            futures.push(async move { run_leg_pooled(endpoint, sql, pool, sp).await });
        }
    }

    // collect outcomes (fail-fast on the first error)
    let mut pooled_outcomes: Vec<LegOutcome> = Vec::new();
    let mut first_error: Option<ErrorInfo> = None;
    while let Some(mut outcome) = futures.next().await {
        let failed = outcome.error.is_some() || outcome.conn_broken;
        if failed && first_error.is_none() {
            let endpoint = outcome.endpoint.clone();
            first_error = outcome.error.take().or_else(|| {
                Some(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "08006".to_owned(),
                    format!("shard {endpoint} failed"),
                ))
            });
            // dropping the stream aborts remaining legs; their leases drop
            // → sockets close → backends cancel server-side
            break;
        }
        pooled_outcomes.push(outcome);
    }
    drop(futures);

    // the session-lease leg runs sequentially (the lease is ours alone
    // during the scatter); skip it entirely when a pooled leg already
    // failed
    let mut session_outcome: Option<(Vec<PgWireBackendMessage>, u64, Option<ErrorInfo>, bool)> =
        None;
    if first_error.is_none()
        && let Some(idx) = session_leg_idx
        && let Some(lease) = session_lease.as_mut()
    {
        let leg = &request.legs[idx];
        let (messages, rows, error, dirty, broken) =
            run_query_collect(&mut lease.conn, &leg.sql).await;
        if broken {
            let endpoint = leg.endpoint.clone();
            return Err(ScatterFailure {
                error: Box::new(error.unwrap_or_else(|| {
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        "08006".to_owned(),
                        format!("shard {endpoint} failed"),
                    )
                })),
                session_lease_broken: true,
            });
        }
        session_outcome = Some((messages, rows, error, dirty));
    }

    // the session-lease leg's query error also fails the scatter
    if first_error.is_none()
        && let Some((_, _, error, _)) = &mut session_outcome
        && let Some(info) = error.take()
    {
        // the session lease is at ReadyForQuery (clean cycle) — keep it;
        // if the shard is dying the next cycle's relink handles it
        first_error = Some(info);
    }

    if let Some(error) = first_error {
        // return clean legs' connections; broken ones were destroyed
        // inside their futures
        for outcome in pooled_outcomes {
            if let Some(lease) = outcome.lease {
                lease.checkin_detached(outcome.dirty).await;
            }
        }
        return Err(ScatterFailure {
            error: Box::new(error),
            session_lease_broken: false,
        });
    }

    // merge and write the client-visible cycle
    let mut total_rows = 0u64;

    // gather the legs' messages by value (in a deterministic order:
    // session-lease leg first, then pooled outcomes in completion order)
    let mut sources: Vec<Vec<PgWireBackendMessage>> = Vec::new();
    if let Some((messages, rows, _, _)) = session_outcome.take() {
        total_rows += rows;
        sources.push(messages);
    }
    for outcome in &mut pooled_outcomes {
        total_rows += outcome.rows;
        sources.push(std::mem::take(&mut outcome.messages));
    }

    match &request.merge {
        MergePolicy::Concat => {
            let mut row_description_written = false;
            for message in sources.into_iter().flatten() {
                match message {
                    PgWireBackendMessage::RowDescription(rd) => {
                        if !row_description_written {
                            row_description_written = true;
                            downstream
                                .send(PgWireBackendMessage::RowDescription(rd))
                                .await
                                .map_err(|_| conn_gone())?;
                        }
                    }
                    PgWireBackendMessage::DataRow(row) => {
                        downstream
                            .send(PgWireBackendMessage::DataRow(row))
                            .await
                            .map_err(|_| conn_gone())?;
                    }
                    _ => {}
                }
            }
            let tag = pgwire::messages::response::CommandComplete::new(format!(
                "{} {total_rows}",
                request.tag
            ));
            downstream
                .send(PgWireBackendMessage::CommandComplete(tag))
                .await
                .map_err(|_| conn_gone())?;
        }
        MergePolicy::Sum => {
            let mut total: i128 = 0;
            let mut schema: Option<PgWireBackendMessage> = None;
            for message in sources.into_iter().flatten() {
                match message {
                    PgWireBackendMessage::RowDescription(rd) => {
                        if schema.is_none() {
                            schema = Some(PgWireBackendMessage::RowDescription(rd));
                        }
                    }
                    PgWireBackendMessage::DataRow(row) => {
                        total += first_column_i128(&row).ok_or_else(sum_failure)?;
                    }
                    _ => {}
                }
            }
            if let Some(schema) = schema {
                downstream.send(schema).await.map_err(|_| conn_gone())?;
            }
            let mut data = tokio_util::bytes::BytesMut::new();
            use tokio_util::bytes::BufMut as _;
            let text = total.to_string();
            data.put_i32(text.len() as i32);
            data.extend_from_slice(text.as_bytes());
            let row = pgwire::messages::data::DataRow::new(data, 1);
            downstream
                .send(PgWireBackendMessage::DataRow(row))
                .await
                .map_err(|_| conn_gone())?;
            let tag =
                pgwire::messages::response::CommandComplete::new(format!("{} 1", request.tag));
            downstream
                .send(PgWireBackendMessage::CommandComplete(tag))
                .await
                .map_err(|_| conn_gone())?;
        }
    }

    // checkin the pooled legs' connections (clean single-query cycles)
    for outcome in pooled_outcomes {
        if let Some(lease) = outcome.lease {
            lease.checkin_detached(outcome.dirty).await;
        }
    }

    Ok(if matches!(request.merge, MergePolicy::Sum) {
        1
    } else {
        total_rows
    })
}

fn sum_failure() -> ScatterFailure {
    ScatterFailure {
        error: Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "0A000".to_owned(),
            "pgferry scatter: Sum merge requires a numeric first column".to_owned(),
        )),
        session_lease_broken: false,
    }
}

fn conn_gone() -> ScatterFailure {
    ScatterFailure {
        error: Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "08006".to_owned(),
            "client connection lost".to_owned(),
        )),
        session_lease_broken: false,
    }
}

/// Run one leg on a freshly checked-out connection from `pool`.
async fn run_leg_pooled(
    endpoint: String,
    sql: String,
    pool: Pool,
    startup_parameters: std::collections::BTreeMap<String, String>,
) -> LegOutcome {
    let mut lease = match pool.checkout().await {
        Ok(lease) => lease,
        Err(error) => {
            return LegOutcome {
                endpoint,
                messages: Vec::new(),
                rows: 0,
                error: Some(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "08006".to_owned(),
                    format!("connect to shard failed: {error}"),
                )),
                lease: None,
                conn_broken: true,
                dirty: false,
            };
        }
    };
    if let Err(error) = lease.sync_startup_parameters(&startup_parameters).await {
        lease.destroy("scatter: parameter sync failed");
        return LegOutcome {
            endpoint,
            messages: Vec::new(),
            rows: 0,
            error: Some(ErrorInfo::new(
                "ERROR".to_owned(),
                "08006".to_owned(),
                format!("connect to shard failed: {error}"),
            )),
            lease: None,
            conn_broken: true,
            dirty: false,
        };
    }

    let (messages, rows, error, dirty, broken) = run_query_collect(&mut lease.conn, &sql).await;
    if broken {
        lease.destroy("scatter: connection lost");
        return LegOutcome {
            endpoint,
            messages,
            rows,
            error,
            lease: None,
            conn_broken: true,
            dirty,
        };
    }
    LegOutcome {
        endpoint,
        messages,
        rows,
        error,
        lease: Some(lease),
        conn_broken: false,
        dirty,
    }
}

/// Send one simple query and collect the cycle's messages up to (and
/// including the drain of) `ReadyForQuery`. Returns
/// `(messages, rows, error, dirty, conn_broken)`.
async fn run_query_collect(
    conn: &mut PgWireClient,
    sql: &str,
) -> (
    Vec<PgWireBackendMessage>,
    u64,
    Option<ErrorInfo>,
    bool,
    bool,
) {
    use futures::SinkExt as _;
    let mut messages = Vec::new();
    let mut rows = 0u64;
    let mut error = None;
    let mut dirty = false;

    if conn.send(simple_query_message(sql)).await.is_err() {
        return (messages, rows, None, dirty, true);
    }

    while let Some(item) = conn.next().await {
        match item {
            Ok(PgWireBackendMessage::ReadyForQuery(ready)) => {
                conn.set_transaction_status(ready.status);
                return (messages, rows, error, dirty, false);
            }
            Ok(PgWireBackendMessage::ParameterStatus(ps)) => {
                conn.set_server_parameter(ps.name, ps.value);
                dirty = true;
            }
            Ok(PgWireBackendMessage::ErrorResponse(er)) => {
                error = Some(pgwire::error::ErrorInfo::from(er));
            }
            Ok(msg) => {
                if matches!(msg, PgWireBackendMessage::DataRow(_)) {
                    rows += 1;
                }
                messages.push(msg);
            }
            Err(_) => return (messages, rows, error, dirty, true),
        }
    }
    (messages, rows, error, dirty, true)
}

fn simple_query_message(sql: &str) -> pgwire::messages::PgWireFrontendMessage {
    pgwire::messages::PgWireFrontendMessage::Query(Query::new(sql.to_owned()))
}

/// Parse the first column of a text-format DataRow as an integer.
fn first_column_i128(row: &pgwire::messages::data::DataRow) -> Option<i128> {
    use tokio_util::bytes::Buf as _;
    let mut cursor: &[u8] = &row.data;
    let len = cursor.get_i32();
    if len < 0 {
        return None;
    }
    let bytes = &cursor[..len as usize];
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}
