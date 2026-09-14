//! Shared runtime state: the session registry feeding `SHOW CLIENTS`,
//! metrics, and admin introspection.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Per-session info, shared between the session task and the admin
/// console / metrics renderer.
pub struct SessionInfo {
    /// Virtual session id (also the cancel pid).
    pub pid: i32,
    pub addr: SocketAddr,
    pub user: String,
    pub database: String,
    /// Admin-console session (never leases an upstream).
    pub is_admin: bool,
    /// Whether the session currently holds an upstream lease (transaction
    /// pooling: true between attach and detach).
    pub attached: AtomicBool,
    pub started: Instant,
}

impl std::fmt::Debug for SessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionInfo")
            .field("pid", &self.pid)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("attached", &self.attached.load(Ordering::Relaxed))
            .finish()
    }
}

/// Registry of live sessions + global counters.
#[derive(Default)]
pub struct RuntimeState {
    sessions: RwLock<Vec<Arc<SessionInfo>>>,
    sessions_total: AtomicU64,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session; returns a guard that unregisters on drop.
    pub fn register(self: &Arc<Self>, info: Arc<SessionInfo>) -> SessionGuard {
        self.sessions_total.fetch_add(1, Ordering::Relaxed);
        self.sessions.write().unwrap().push(Arc::clone(&info));
        SessionGuard {
            state: Arc::clone(self),
            pid: info.pid,
        }
    }

    /// Snapshot of live sessions (for `SHOW CLIENTS` / metrics).
    pub fn sessions(&self) -> Vec<Arc<SessionInfo>> {
        self.sessions.read().unwrap().clone()
    }

    pub fn sessions_active(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    pub fn sessions_total(&self) -> u64 {
        self.sessions_total.load(Ordering::Relaxed)
    }

    fn unregister(&self, pid: i32) {
        let mut sessions = self.sessions.write().unwrap();
        sessions.retain(|s| s.pid != pid);
    }
}

/// RAII guard removing a session from the [`RuntimeState`] on drop.
pub struct SessionGuard {
    state: Arc<RuntimeState>,
    pid: i32,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.state.unregister(self.pid);
    }
}
