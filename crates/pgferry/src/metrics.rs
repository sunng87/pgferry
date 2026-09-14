//! Prometheus `/metrics` endpoint (hand-rolled HTTP/1.1, zero extra deps).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::pool::PoolManager;
use crate::runtime::RuntimeState;

/// Serve the metrics endpoint on an already-bound listener (abort on
/// shutdown).
pub async fn run_metrics_server(
    listener: TcpListener,
    state: Arc<RuntimeState>,
    pools: PoolManager,
) -> std::io::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "metrics endpoint listening");
    loop {
        let (socket, _) = listener.accept().await?;
        let state = state.clone();
        let pools = pools.clone();
        tokio::spawn(async move {
            let _ = handle(socket, state, pools).await;
        });
    }
}

async fn handle(
    mut socket: TcpStream,
    state: Arc<RuntimeState>,
    pools: PoolManager,
) -> std::io::Result<()> {
    // minimal HTTP: read the request head (bounded), serve GET /metrics
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16 * 1024 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    let (status, body) = if method == "GET" && path == "/metrics" {
        ("200 OK", render(&state, &pools))
    } else {
        ("404 Not Found", "not found\n".to_owned())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await
}

/// Render the prometheus text format.
fn render(state: &RuntimeState, pools: &PoolManager) -> String {
    let mut out = String::with_capacity(2048);

    // global session metrics
    out.push_str("# TYPE pgferry_sessions_active gauge\n");
    out.push_str(&format!(
        "pgferry_sessions_active {}\n",
        state.sessions_active()
    ));
    out.push_str("# TYPE pgferry_sessions_total counter\n");
    out.push_str(&format!(
        "pgferry_sessions_total {}\n",
        state.sessions_total()
    ));

    for pool in pools.pools() {
        let key = pool.key();
        let labels = format!(
            "route=\"{}\",user=\"{}\",database=\"{}\"",
            escape(&key.route),
            escape(&key.user),
            escape(key.database.as_deref().unwrap_or("")),
        );
        let m = pool.metrics();

        macro_rules! counter {
            ($name:ident, $field:ident) => {{
                let name = stringify!($name);
                out.push_str(&format!(
                    "# TYPE pgferry_pool_{name}_total counter\npgferry_pool_{name}_total{{{labels}}} {}\n",
                    m.$field.load(Ordering::Relaxed)
                ));
            }};
        }
        counter!(checkouts, checkouts);
        counter!(checkouts_reused, reused);
        counter!(connections_created, created);
        counter!(checkins, checked_in);
        counter!(connections_dropped, dropped);
        counter!(connections_evicted, evicted);
        counter!(stale_probes, probed);

        out.push_str(&format!(
            "# TYPE pgferry_pool_connections_active gauge\npgferry_pool_connections_active{{{labels}}} {}\n",
            m.active.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# TYPE pgferry_pool_connections_idle gauge\npgferry_pool_connections_idle{{{labels}}} {}\n",
            pool.idle_len()
        ));
        out.push_str(&format!(
            "# TYPE pgferry_pool_clients_waiting gauge\npgferry_pool_clients_waiting{{{labels}}} {}\n",
            m.waiting.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "# TYPE pgferry_pool_max_size gauge\npgferry_pool_max_size{{{labels}}} {}\n",
            pool.max_size()
        ));
    }

    out
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
