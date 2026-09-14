//! Built-in interceptor helpers — ordinary values your hooks call.
//!
//! These are not registered anywhere: put them in fields on your gateway
//! struct and call them from your [`Interceptor`](super::Interceptor)
//! hooks. Composition is code.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use regex::Regex;

use super::CycleReport;

/// Deny queries matching any pattern (checked against SQL text).
///
/// ```ignore
/// struct Gateway { deny: DenyList }
/// async fn on_query(...) -> Action {
///     if let Some(err) = self.deny.check(cycle.sql()) { return Action::Deny(Box::new(err)); }
///     Action::Forward
/// }
/// ```
#[derive(Debug, Clone)]
pub struct DenyList {
    patterns: Vec<Regex>,
    code: String,
    message: String,
}

impl DenyList {
    pub fn new<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        DenyList {
            patterns: patterns
                .into_iter()
                .map(|p| Regex::new(format!("(?is)^{}", p.as_ref()).as_str()))
                .collect::<Result<_, _>>()
                .expect("invalid deny pattern"),
            code: "42501".to_owned(),
            message: "insufficient privilege (pgferry deny-list)".to_owned(),
        }
    }

    /// Set the SQLSTATE code and message of denials.
    pub fn with_message(mut self, code: &str, message: &str) -> Self {
        self.code = code.to_owned();
        self.message = message.to_owned();
        self
    }

    /// Returns the denial error if the SQL matches, `None` to allow.
    pub fn check(&self, sql: &str) -> Option<pgwire::error::ErrorInfo> {
        if self.patterns.iter().any(|p| p.is_match(sql)) {
            Some(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                self.code.clone(),
                self.message.clone(),
            ))
        } else {
            None
        }
    }
}

/// Rewrite SQL text with regex replacement, applied at `on_query`.
#[derive(Debug, Clone)]
pub struct Rewrite {
    pattern: Regex,
    replacement: String,
}

impl Rewrite {
    pub fn new(pattern: &str, replacement: &str) -> Self {
        Rewrite {
            pattern: Regex::new(pattern).expect("invalid rewrite pattern"),
            replacement: replacement.to_owned(),
        }
    }

    /// Rewrite `sql` if the pattern matches.
    pub fn rewrite(&self, sql: &str) -> Option<String> {
        if self.pattern.is_match(sql) {
            Some(
                self.pattern
                    .replace_all(sql, self.replacement.as_str())
                    .into_owned(),
            )
        } else {
            None
        }
    }
}

/// Cycle telemetry collector for `on_cycle_end`: aggregates counts and
/// keeps recent per-cycle reports. Also the shape of an audit logger.
#[derive(Debug, Clone)]
pub struct Audit {
    inner: Arc<AuditInner>,
}

#[derive(Debug, Default)]
struct AuditInner {
    cycles: AtomicU64,
    errors: AtomicU64,
    rows: AtomicU64,
    latency_total_us: AtomicU64,
    recent: Mutex<Vec<CycleReport>>,
}

impl Audit {
    pub fn new() -> Self {
        Audit {
            inner: Arc::new(AuditInner::default()),
        }
    }

    /// Record a finished cycle (call from `on_cycle_end`).
    pub fn record(&self, report: &CycleReport) {
        self.inner.cycles.fetch_add(1, Ordering::Relaxed);
        if report.error.is_some() {
            self.inner.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.rows.fetch_add(report.rows, Ordering::Relaxed);
        self.inner
            .latency_total_us
            .fetch_add(report.latency.as_micros() as u64, Ordering::Relaxed);
        if let Ok(mut recent) = self.inner.recent.lock() {
            recent.push(report.clone());
            let len = recent.len();
            if len > 128 {
                recent.drain(..len - 128);
            }
        }
        tracing::info!(
            sql = report.sql.as_deref().unwrap_or(""),
            tag = report.tag.as_deref().unwrap_or(""),
            rows = report.rows,
            dropped = report.rows_dropped,
            latency = ?report.latency,
            error = ?report.error.as_ref().map(|e| e.message.as_str()),
            "query cycle"
        );
    }

    pub fn cycles(&self) -> u64 {
        self.inner.cycles.load(Ordering::Relaxed)
    }

    pub fn errors(&self) -> u64 {
        self.inner.errors.load(Ordering::Relaxed)
    }

    pub fn rows(&self) -> u64 {
        self.inner.rows.load(Ordering::Relaxed)
    }

    pub fn snapshot_recent(&self) -> Vec<CycleReport> {
        self.inner
            .recent
            .lock()
            .map(|r| r.clone())
            .unwrap_or_default()
    }
}

impl Default for Audit {
    fn default() -> Self {
        Self::new()
    }
}
