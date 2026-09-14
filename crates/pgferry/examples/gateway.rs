//! Example gateway: pgferry with built-in interceptor helpers composed into
//! a custom Interceptor — the programmable-proxy pattern this project is
//! for.
//!
//! Usage:
//!
//!   gateway --listen 127.0.0.1:7432 --upstream "host=127.0.0.1 port=5432" \
//!       [--deny <regex>]... [--rewrite <from> <to>]... [--mask-col <name>]...
//!
//! Behavior: queries matching any --deny pattern are rejected with SQLSTATE
//! 42501; --rewrite rewrites SQL text; rows are masked by column name; every
//! cycle is logged (the `Audit` helper's role).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pgferry::intercept::builtin::{Audit, DenyList, Rewrite};
use pgferry::{
    Action, CycleReport, DataRowMut, EndpointGroup, EndpointId, Interceptor, MergePolicy,
    PoolConfig, PoolMode, Proxy, QueryCycle, RetryDecision, RoutingInfo, RowAction, RowSchema,
    RwSplit, ShardSet, UpstreamError,
};

struct Gateway {
    deny: DenyList,
    rewrites: Vec<Rewrite>,
    mask_columns: Vec<String>,
    audit: Audit,
    rw_split: Option<RwSplit>,
    shards: Option<ShardSet>,
    shard_all: Option<regex::Regex>,
    group: EndpointGroup,
    // test hook: what the session actually denied/rewrote, for assertions
    reports: Arc<Mutex<Vec<CycleReport>>>,
}

#[async_trait]
impl Interceptor for Gateway {
    type Ctx = ();

    async fn on_query(&self, _ctx: &mut Self::Ctx, cycle: &mut QueryCycle) -> Action {
        if let Some(error) = self.deny.check(cycle.sql()) {
            return Action::Deny(Box::new(error));
        }
        for rewrite in &self.rewrites {
            if let Some(new_sql) = rewrite.rewrite(cycle.sql()) {
                cycle.set_sql(new_sql);
            }
        }
        // scatter-gather: matching queries broadcast to every shard,
        // merged (count/sum → Sum, otherwise Concat)
        if let (Some(shards), Some(pattern)) = (&self.shards, &self.shard_all)
            && pattern.is_match(cycle.sql())
        {
            let lowered = cycle.sql().trim_start().to_lowercase();
            let merge = if lowered.starts_with("select count") || lowered.starts_with("select sum")
            {
                MergePolicy::Sum
            } else {
                MergePolicy::Concat
            };
            return shards.broadcast(cycle.sql(), merge, "SELECT");
        }
        Action::Forward
    }

    async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> EndpointId {
        match &self.rw_split {
            Some(split) => match info.sql() {
                Some(sql) => split.route_sql(info, sql),
                None => split.route(info, false),
            },
            None => self
                .group
                .select_first_alive(info)
                .unwrap_or_else(|| info.default_endpoint().to_owned()),
        }
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

    fn wants_rows(&self) -> bool {
        !self.mask_columns.is_empty()
    }

    async fn on_row(
        &self,
        _ctx: &mut Self::Ctx,
        schema: &RowSchema,
        row: &mut DataRowMut<'_>,
    ) -> RowAction {
        for name in &self.mask_columns {
            if let Some(index) = schema.column_index(name)
                && let Ok(Some(_)) = row.text(index)
            {
                let _ = row.set_text(index, "***");
            }
        }
        RowAction::Keep
    }

    async fn on_cycle_end(&self, _ctx: &mut Self::Ctx, report: &CycleReport) {
        self.audit.record(report);
        self.reports.lock().unwrap().push(report.clone());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut listen = "127.0.0.1:7432".to_string();
    let mut upstream: Option<String> = None;
    let mut endpoints: Vec<(String, String)> = Vec::new();
    let mut rw_split_primary: Option<String> = None;
    let mut shard_all: Option<String> = None;
    let mut deny: Vec<String> = Vec::new();
    let mut rewrites: Vec<(String, String)> = Vec::new();
    let mut mask_columns: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().expect("--listen <addr>"),
            "--upstream" => upstream = Some(args.next().expect("--upstream <connstring>")),
            "--endpoint" => {
                let spec = args.next().expect("--endpoint <id>=<connstring>");
                let (id, conn) = spec.split_once('=').expect("--endpoint <id>=<connstring>");
                endpoints.push((id.to_owned(), conn.to_owned()));
            }
            "--rw-split" => rw_split_primary = Some(args.next().expect("--rw-split <primary-id>")),
            "--shard-all" => shard_all = Some(args.next().expect("--shard-all <regex>")),
            "--deny" => deny.push(args.next().expect("--deny <regex>")),
            "--rewrite" => {
                let from = args.next().expect("--rewrite <from> <to>");
                let to = args.next().expect("--rewrite <from> <to>");
                rewrites.push((from, to));
            }
            "--mask-col" => mask_columns.push(args.next().expect("--mask-col <name>")),
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    let upstream = upstream.unwrap_or_else(|| "host=127.0.0.1 port=5432".to_owned());

    let group = EndpointGroup::new(endpoints.iter().map(|(id, _)| id.clone()));
    let gw_uses_rw_split = rw_split_primary.is_some() || shard_all.is_some();
    let rw_split = rw_split_primary.map(|primary| RwSplit::new(group.clone(), primary));
    let shards = if shard_all.is_some() && !endpoints.is_empty() {
        Some(ShardSet::new(endpoints.iter().map(|(id, _)| id.clone())))
    } else {
        None
    };
    let shard_all = shard_all.map(|p| regex::Regex::new(&p).expect("invalid --shard-all regex"));

    let gateway = Gateway {
        deny: DenyList::new(deny),
        rewrites: rewrites
            .into_iter()
            .map(|(f, t)| Rewrite::new(&f, &t))
            .collect(),
        mask_columns,
        audit: Audit::new(),
        rw_split,
        shards,
        shard_all,
        group,
        reports: Arc::new(Mutex::new(Vec::new())),
    };

    // rw-split routes per query cycle → transaction pooling
    let mut builder = Proxy::builder().listen(&listen)?;
    if endpoints.is_empty() {
        builder = builder.upstream(upstream);
    } else {
        for (id, conn) in endpoints {
            builder = builder.endpoint(id, conn);
        }
    }
    if gw_uses_rw_split {
        builder = builder.pool_config(PoolConfig {
            mode: PoolMode::Transaction,
            ..PoolConfig::default()
        });
    }
    builder.service(gateway).build()?.run().await?;
    Ok(())
}
