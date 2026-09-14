# pgferry

A **programmable PostgreSQL proxy framework** built on
[pgwire](https://github.com/sunng87/pgwire) — for building poolers, routers,
failover gateways and sharding proxies in Rust, the way
[pingora](https://github.com/cloudflare/pingora) is for HTTP.

pgwire's **server API** faces downstream clients, its **client API**
connects upstream, and a message pump forwards typed wire-protocol messages
between them. Behavior is **code-driven**: you implement the `Interceptor`
trait and compose built-in helpers (`EndpointGroup`, `RwSplit`, `ShardSet`)
as ordinary values. No plugin registries, no routing-in-config.

## Write a proxy (the point of the project)

Routing and failover are the same extension point — built-ins as fields:

```rust
struct HaGateway {
    group: EndpointGroup,   // passive health tracking
    split: RwSplit,         // reads → replica, writes → primary
}

#[async_trait::async_trait]
impl Interceptor for HaGateway {
    type Ctx = ();
    async fn upstream(&self, _ctx: &mut (), info: &RoutingInfo<'_>) -> EndpointId {
        match info.sql() {
            Some(sql) if is_read(sql) => self.split.route_sql(info, sql),
            _ => self.group.select_first_alive(info)
                .unwrap_or_else(|| info.default_endpoint().into()),
        }
    }
    async fn on_upstream_error(&self, _ctx: &mut (), e: &UpstreamError) -> RetryDecision {
        self.group.report_failure(e.endpoint());  // failover steering
        RetryDecision::Relink
    }
    async fn on_upstream_connected(&self, _ctx: &mut (), ep: &str) {
        self.group.report_success(ep);
    }
}

let proxy = Proxy::builder()
    .listen("127.0.0.1:6432")?
    .endpoint("primary", PRIMARY_CONN)
    .endpoint("replica", REPLICA_CONN)
    .service(HaGateway { group, split })
    .build()?;
```

Multi-endpoint routes without a custom service get the built-in
`FailoverRouter` automatically (first-alive + passive health).

Scatter-gather is the same extension point — broadcast a query to every
shard and merge:

```rust
async fn on_query(&self, _ctx: &mut (), cycle: &mut QueryCycle) -> Action {
    if is_aggregate(cycle.sql()) {
        self.shards.broadcast(cycle.sql(), MergePolicy::Sum, "SELECT")
    } else if is_shard_scan(cycle.sql()) {
        self.shards.broadcast(cycle.sql(), MergePolicy::Concat, "SELECT")
    } else {
        Action::Forward
    }
}
```

Legs run concurrently on the endpoints' pools; the client sees one result
set (first leg's schema, merged rows, single `CommandComplete`). A failed
shard aborts the rest and surfaces a clean error; single-shard traffic
stays passthrough via `ShardSet::shard_for` + the `upstream()` hook.

```rust
use pgferry::{Proxy, Interceptor};

struct MyGateway { /* built-in helpers as fields */ }

#[async_trait::async_trait]
impl Interceptor for MyGateway {
    type Ctx = ();
    // on_query / on_row / on_cycle_end / upstream / on_upstream_error hooks
}

let proxy = Proxy::builder()
    .listen("127.0.0.1:6432")?          // infra
    .service(MyGateway { /* ... */ })?
    .build()?;
proxy.run().await?;
```

## Try the reference binary or the example gateway

`pgferry-bin` is a minimal reference composition (like pingora's example
load balancer) — a transparent pooled passthrough proxy, useful for smoke
testing the core. `examples/gateway` shows the extension pattern (deny / SQL
rewrite / row masking / audit via the built-in helpers):

```sh
cargo run -p pgferry-bin -- --listen 127.0.0.1:6432 --upstream "host=127.0.0.1 port=5432"
psql "host=127.0.0.1 port=6432 user=postgres"

cargo run -p pgferry --example gateway -- \
    --listen 127.0.0.1:6433 --upstream "host=127.0.0.1 port=5432" \
    --deny '^drop table' --rewrite 'e2e-rewrite-me' 'rewritten-by-gateway' \
    --mask-col secret
```

`user`/`dbname` from the client's startup packet are used verbatim against
the upstream; the upstream password comes from `--password` or
`password_env`. Pool knobs via `--config pgferry.toml` (infra-level; see
[pgferry.toml](pgferry.toml)) — including `mode = "transaction"` for
transaction pooling. Downstream auth is trust; the upstream
connection authenticates for real (cleartext/MD5/SCRAM via pgwire's client
API).

Transaction-mode limitations (v1): `CREATE TEMP TABLE`, `LISTEN`/`NOTIFY`
persistence, and `SET`s of non-reported GUCs (e.g. `search_path`) are not
preserved across transactions — use session mode if you need them.

## Test

Two suites, both wired for CI:

1. **Integration** (no external dependencies — uses a pgwire in-process
   fake upstream):

   ```sh
   cargo test --workspace
   ```

2. **End-to-end** (real PostgreSQL + psql + pgbench): builds pgferry,
   starts a throwaway `initdb` cluster on an ephemeral port, and asserts
   passthrough, pooling (backend reuse,
   abandoned-transaction rollback, parameter replay, pool caps), COPY both
   ways, LISTEN/NOTIFY, error recovery, protocol cancel, and pgbench in
   all three protocol modes:

   ```sh
   scripts/e2e.sh              # needs postgres/psql/pgbench/cargo in PATH
   nix develop -c scripts/e2e.sh   # reproducible environment via nix
   ```

## Layout

```
crates/pgferry       framework library: listener, session pump, pools, cancel router
crates/pgferry-bin   reference binary
scripts/e2e.sh       end-to-end acceptance test
```
