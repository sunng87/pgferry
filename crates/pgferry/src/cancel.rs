//! Cancel request routing.
//!
//! PostgreSQL cancels a running query by opening a **new** connection to the
//! server and sending a `CancelRequest` carrying the `(pid, secret_key)` pair
//! from the original connection's `BackendKeyData`.
//!
//! pgferry presents *virtual* `(pid, secret_key)` pairs to downstream clients
//! and maps them back to the session that owns them. When a cancel request
//! arrives, the router signals the session task, which issues the cancel on
//! its upstream connection (via [`pgwire::tokio::client::PgWireClient::cancel`],
//! which uses the *real* upstream keys).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;
use tokio_util::bytes::Bytes;

/// Virtual `(pid, secret_key)` → live-session cancel channel.
type SessionMap = HashMap<(i32, Bytes), mpsc::Sender<()>>;

/// Registry mapping virtual `(pid, secret_key)` pairs to live sessions.
#[derive(Debug, Clone, Default)]
pub struct CancelRouter {
    sessions: Arc<RwLock<SessionMap>>,
}

impl CancelRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the current session and return the receiver the session task
    /// selects on. The receiver resolves when a cancel request targets this
    /// session.
    pub fn register(&self, pid: i32, secret_key: Bytes) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(1);
        self.sessions.write().unwrap().insert((pid, secret_key), tx);
        rx
    }

    /// Fire a cancel request at the session owning `(pid, secret_key)`.
    ///
    /// Returns `true` if the session was found. Per PostgreSQL semantics the
    /// cancel connection gets no reply either way.
    pub fn cancel(&self, pid: i32, secret_key: Bytes) -> bool {
        let sessions = self.sessions.read().unwrap();
        match sessions.get(&(pid, secret_key)) {
            Some(tx) => {
                let _ = tx.try_send(());
                true
            }
            None => false,
        }
    }

    /// Remove a session from the registry (called when the session ends).
    pub fn unregister(&self, pid: i32, secret_key: Bytes) {
        self.sessions.write().unwrap().remove(&(pid, secret_key));
    }
}
