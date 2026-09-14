//! Admin console: `SHOW POOLS` / `SHOW CLIENTS` / `SHOW STATS` answered
//! locally, for clients whose startup `database` matches the configured
//! admin database. The session terminates here — no upstream lease, no
//! pool. This is the `ExecutionPlan::Local` pattern from the roadmap,
//! first real user.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use futures::{SinkExt, StreamExt};

use pgwire::api::auth::{ServerParameterProvider, finish_authentication};
use pgwire::error::ErrorInfo;
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::SecretKey;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::ProxyShared;
use crate::runtime::SessionInfo;
use crate::session::Downstream;

/// Startup parameters reported by the admin console.
fn admin_parameters() -> std::collections::BTreeMap<String, String> {
    [
        (
            "server_version".to_owned(),
            format!("pgferry-{}", env!("CARGO_PKG_VERSION")),
        ),
        ("server_encoding".to_owned(), "UTF8".to_owned()),
        ("client_encoding".to_owned(), "UTF8".to_owned()),
        ("DateStyle".to_owned(), "ISO, MDY".to_owned()),
        ("integer_datetimes".to_owned(), "on".to_owned()),
        ("standard_conforming_strings".to_owned(), "on".to_owned()),
        ("is_superuser".to_owned(), "on".to_owned()),
    ]
    .into_iter()
    .collect()
}

#[derive(Debug)]
struct AdminParameterProvider {
    parameters: std::collections::BTreeMap<String, String>,
}

impl ServerParameterProvider for AdminParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<std::collections::HashMap<String, String>>
    where
        C: pgwire::api::ClientInfo,
    {
        Some(
            self.parameters
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }
}

/// Run the admin console session to completion.
pub(crate) async fn run_admin_session(
    mut downstream: Downstream,
    shared: Arc<ProxyShared>,
    info: Arc<SessionInfo>,
    pid: i32,
    secret_key: SecretKey,
) -> std::io::Result<()> {
    let started = Instant::now();
    tracing::info!("admin session starting");

    // downstream startup: trust auth, static parameters, cancel keys
    let provider = AdminParameterProvider {
        parameters: admin_parameters(),
    };
    finish_authentication(&mut downstream, &provider).await?;

    let mut cancel_rx = shared.router.register(pid, secret_key.to_bytes());
    let mut shutdown_rx = shared.shutdown.clone();

    let result: Result<(), std::io::Error> = 'session: loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                tracing::info!("admin session: shutdown signal");
                break 'session Ok(());
            }
            msg = downstream.next() => {
                match msg {
                    None | Some(Ok(PgWireFrontendMessage::Terminate(_))) => {
                        break 'session Ok(());
                    }
                    Some(Err(error)) => {
                        tracing::warn!(%error, "admin session decode error");
                        break 'session Ok(());
                    }
                    Some(Ok(PgWireFrontendMessage::Sync(_))) => {
                        // stray Sync: answer ReadyForQuery to stay usable
                        let _ = send_ready_for_query(&mut downstream).await;
                    }
                    Some(Ok(PgWireFrontendMessage::Query(query))) => {
                        if dispatch(&mut downstream, &shared, &info, query).await.is_err() {
                            break 'session Ok(());
                        }
                    }
                    Some(Ok(_)) => {
                        // unsupported on the admin console (extended
                        // protocol, COPY...): answer with an error
                        let info_err = ErrorInfo::new(
                            "ERROR".to_owned(),
                            "0A000".to_owned(),
                            "pgferry admin console: only simple queries are supported".to_owned(),
                        );
                        if downstream
                            .send(PgWireBackendMessage::ErrorResponse(info_err.into()))
                            .await
                            .is_err()
                        {
                            break 'session Ok(());
                        }
                        let _ = send_ready_for_query(&mut downstream).await;
                    }
                }
            }
            _ = cancel_rx.recv() => {
                // nothing long-running on the admin console; no-op
            }
        }
    };

    shared.router.unregister(pid, secret_key.to_bytes());
    tracing::info!(elapsed = ?started.elapsed(), "admin session ended");
    result
}

/// Execute one admin `Query`: dispatch `SHOW ...` or answer with an error,
/// then `ReadyForQuery`.
async fn dispatch(
    downstream: &mut Downstream,
    shared: &Arc<ProxyShared>,
    info: &Arc<SessionInfo>,
    query: Query,
) -> Result<(), std::io::Error> {
    let command = query
        .query
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_lowercase();

    let outcome = match command.as_str() {
        "show pools" => Ok(show_pools(shared)),
        "show clients" => Ok(show_clients(shared)),
        "show stats" => Ok(show_stats(shared)),
        "show help" | "help" => Ok(show_help()),
        other => Err(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            format!(
                "unknown admin command {other:?}; supported: SHOW POOLS | SHOW CLIENTS | SHOW STATS | SHOW HELP"
            ),
        )),
    };
    let _ = info; // reserved for future per-session admin state

    match outcome {
        Ok(messages) => {
            for message in messages {
                downstream.send(message).await?;
            }
        }
        Err(error_info) => {
            downstream
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
        }
    }
    send_ready_for_query(downstream).await
}

/// Build a single-column-per-string result set as backend messages.
fn text_result(columns: &[&str], rows: Vec<Vec<String>>, tag: &str) -> Vec<PgWireBackendMessage> {
    use pgwire::api::Type;
    use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};

    let fields: Arc<Vec<_>> = Arc::new(
        columns
            .iter()
            .map(|name| {
                FieldInfo::new(
                    (*name).to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                )
            })
            .collect(),
    );

    let row_count = rows.len();
    let mut messages = Vec::with_capacity(row_count + 2);
    let row_description = pgwire::messages::data::RowDescription::new(
        fields
            .iter()
            .map(|f| {
                pgwire::messages::data::FieldDescription::new(
                    f.name().to_owned(),
                    0,
                    0,
                    pgwire::api::Type::TEXT.oid(),
                    -1,
                    -1,
                    0,
                )
            })
            .collect(),
    );
    messages.push(PgWireBackendMessage::RowDescription(row_description));

    for row in rows {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for value in row {
            // encode as Option<String> to keep NULLs expressible
            encoder
                .encode_field(&Some(value))
                .expect("text encoding cannot fail");
        }
        messages.push(PgWireBackendMessage::DataRow(encoder.take_row()));
    }
    messages.push(PgWireBackendMessage::CommandComplete(
        pgwire::messages::response::CommandComplete::new(format!("{tag} {row_count}")),
    ));
    messages
}

fn show_pools(shared: &Arc<ProxyShared>) -> Vec<PgWireBackendMessage> {
    let mut rows = Vec::new();
    for pool in shared.pools.pools() {
        let key = pool.key();
        let m = pool.metrics();
        let clients = shared
            .runtime
            .sessions()
            .into_iter()
            .filter(|s| {
                !s.is_admin
                    && s.user == key.user
                    && s.database == key.database.clone().unwrap_or_default()
            })
            .count();
        rows.push(vec![
            key.database.clone().unwrap_or_default(),
            key.user.clone(),
            key.route.clone(),
            clients.to_string(),
            m.waiting.load(Ordering::Relaxed).to_string(),
            m.active.load(Ordering::Relaxed).to_string(),
            pool.idle_len().to_string(),
            pool.max_size().to_string(),
        ]);
    }
    text_result(
        &[
            "database",
            "user",
            "route",
            "cl_active",
            "cl_waiting",
            "sv_active",
            "sv_idle",
            "max_size",
        ],
        rows,
        "SHOW",
    )
}

fn show_clients(shared: &Arc<ProxyShared>) -> Vec<PgWireBackendMessage> {
    let rows = shared
        .runtime
        .sessions()
        .iter()
        .map(|s| {
            vec![
                s.pid.to_string(),
                s.addr.to_string(),
                s.user.clone(),
                s.database.clone(),
                if s.is_admin {
                    "admin".into()
                } else {
                    "client".into()
                },
                if s.attached.load(Ordering::Relaxed) {
                    "attached".into()
                } else {
                    "idle".into()
                },
                format!("{:.1}", s.started.elapsed().as_secs_f64()),
            ]
        })
        .collect();
    text_result(
        &[
            "pid",
            "client",
            "user",
            "database",
            "kind",
            "state",
            "connected_for",
        ],
        rows,
        "SHOW",
    )
}

fn show_stats(shared: &Arc<ProxyShared>) -> Vec<PgWireBackendMessage> {
    let mut rows = Vec::new();
    for pool in shared.pools.pools() {
        let key = pool.key();
        let m = pool.metrics();
        rows.push(vec![
            key.database.clone().unwrap_or_default(),
            key.user.clone(),
            m.checkouts.load(Ordering::Relaxed).to_string(),
            m.reused.load(Ordering::Relaxed).to_string(),
            m.created.load(Ordering::Relaxed).to_string(),
            m.checked_in.load(Ordering::Relaxed).to_string(),
            m.dropped.load(Ordering::Relaxed).to_string(),
            m.evicted.load(Ordering::Relaxed).to_string(),
            m.probed.load(Ordering::Relaxed).to_string(),
        ]);
    }
    text_result(
        &[
            "database",
            "user",
            "checkouts",
            "reused",
            "created",
            "checked_in",
            "dropped",
            "evicted",
            "probed",
        ],
        rows,
        "SHOW",
    )
}

fn show_help() -> Vec<PgWireBackendMessage> {
    text_result(
        &["command"],
        vec![
            vec!["SHOW POOLS".to_owned()],
            vec!["SHOW CLIENTS".to_owned()],
            vec!["SHOW STATS".to_owned()],
            vec!["SHOW HELP".to_owned()],
        ],
        "SHOW",
    )
}

async fn send_ready_for_query(downstream: &mut Downstream) -> Result<(), std::io::Error> {
    downstream
        .send(PgWireBackendMessage::ReadyForQuery(
            pgwire::messages::response::ReadyForQuery::new(
                pgwire::messages::response::TransactionStatus::Idle,
            ),
        ))
        .await
}
