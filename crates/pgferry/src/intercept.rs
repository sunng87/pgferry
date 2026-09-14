//! The interceptor API — pgferry's extension point (M2).
//!
//! One trait with phase-based hooks over the session / query-cycle
//! lifecycle (pingora's `ProxyHttp` model), plus raw message hooks as the
//! escape hatch. All composition is code: users implement this trait on
//! their gateway struct and hand it to [`crate::Proxy::builder().service()`];
//! built-in helpers ([`builtin`]) are ordinary values the hooks call.
//!
//! Hook phases, in the order a query cycle flows through them:
//!
//! ```text
//! frontend Query/Parse ──► on_query      (validate / deny / rewrite / respond)
//!                       ──► on_frontend   (raw &mut, in-place mutation)
//! upstream messages     ──► on_backend   (raw &mut, in-place mutation)
//!   DataRow             ──► on_row       (value-level rewrite, schema-aware)
//! ReadyForQuery         ──► on_cycle_end (telemetry: tag, rows, latency, error)
//! ```
//!
//! # Deny and error recovery
//!
//! `Action::Deny` is honored from [`Interceptor::on_query`] only: the
//! session sends `ErrorResponse` downstream and — in the extended protocol
//! — swallows frontend messages until `Sync`, then sends `ReadyForQuery`
//! itself (mirroring PostgreSQL's error recovery). The connection stays
//! usable. From the raw hooks, `Deny` is not supported (it could not
//! preserve extended-cycle consistency) and is treated as `Close` after
//! logging.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::bytes::{Buf, BufMut, BytesMut};

use pgwire::error::ErrorInfo;
use pgwire::messages::data::{DataRow, RowDescription};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub use messages::CycleError;

use crate::config::EndpointId;

/// Re-export message-level types shared with the session pump.
pub mod messages {
    use pgwire::error::ErrorInfo;
    use pgwire::messages::PgWireBackendMessage;

    /// An error snapshot attached to a [`super::CycleReport`].
    #[derive(Debug, Clone)]
    pub struct CycleError {
        /// SQLSTATE error code, e.g. `42501`.
        pub code: String,
        pub message: String,
    }

    impl CycleError {
        pub(crate) fn from_info(info: &ErrorInfo) -> Self {
            CycleError {
                code: info.code.clone(),
                message: info.message.clone(),
            }
        }

        /// Build the `ErrorResponse` message this error would be sent as.
        pub fn to_error_response(&self) -> PgWireBackendMessage {
            let info = ErrorInfo::new("ERROR".to_owned(), self.code.clone(), self.message.clone());
            PgWireBackendMessage::ErrorResponse(info.into())
        }
    }
}

/// What to do with a message / query cycle. Returned by every hook.
#[derive(Debug)]
pub enum Action {
    /// Forward (possibly mutated in place). The hot path: no allocation.
    Forward,
    /// Replace the message(s) with these backend messages (sent downstream;
    /// the original is not forwarded upstream). For query-level hooks this
    /// answers the cycle locally: the messages are sent followed by
    /// `ReadyForQuery`, and the upstream is not involved at all.
    Reply(Vec<PgWireBackendMessage>),
    /// Deny the query: send `ErrorResponse` downstream and run injected
    /// error recovery (see the module docs). Only honored from
    /// [`Interceptor::on_query`].
    Deny(Box<ErrorInfo>),
    /// Scatter-gather (M6): run the request's legs concurrently on their
    /// endpoints' pools and answer the cycle with the merged result —
    /// see [`crate::scatter`]. Only honored from [`Interceptor::on_query`]
    /// on simple-protocol cycles outside explicit transactions; elsewhere
    /// it is treated as `Close` after logging.
    Scatter(crate::scatter::ScatterRequest),
    /// Close the session.
    Close,
}

/// What to do with a row in [`Interceptor::on_row`].
#[derive(Debug, PartialEq, Eq)]
pub enum RowAction {
    /// Keep the row (possibly modified via [`DataRowMut`]).
    Keep,
    /// Drop the row from the result set.
    Drop,
}

/// Context for a routing decision (the `upstream()` hook): the route's
/// endpoints, the session identity, and the SQL of the cycle being routed
/// when known (extended-protocol attaches mid-cycle resolve it from the
/// tracked statement registry).
pub struct RoutingInfo<'a> {
    endpoints: &'a [crate::config::EndpointConfig],
    pub user: &'a str,
    pub database: Option<&'a str>,
    pub sql: Option<&'a str>,
}

impl<'a> RoutingInfo<'a> {
    pub(crate) fn new(
        endpoints: &'a [crate::config::EndpointConfig],
        user: &'a str,
        database: Option<&'a str>,
        sql: Option<&'a str>,
    ) -> Self {
        RoutingInfo {
            endpoints,
            user,
            database,
            sql,
        }
    }

    /// The route's endpoints, in configured order (first = default).
    pub fn endpoints(&self) -> &'a [crate::config::EndpointConfig] {
        self.endpoints
    }

    /// Look up an endpoint by id.
    pub fn endpoint(&self, id: &str) -> Option<&'a crate::config::EndpointConfig> {
        self.endpoints.iter().find(|e| e.id == id)
    }

    /// The default (first configured) endpoint id.
    pub fn default_endpoint(&self) -> &str {
        self.endpoints.first().map(|e| e.id.as_str()).unwrap_or("")
    }

    /// SQL of the cycle being routed, when known.
    pub fn sql(&self) -> Option<&str> {
        self.sql
    }
}

/// An upstream failure reported to [`Interceptor::on_upstream_error`].
#[derive(Debug, Clone)]
pub enum UpstreamError {
    /// Connecting (or probing a pooled connection to) the endpoint failed.
    Connect { endpoint: String, message: String },
    /// An established connection to the endpoint broke mid-lease.
    ConnectionLost { endpoint: String, message: String },
}

impl UpstreamError {
    /// The endpoint the failure relates to.
    pub fn endpoint(&self) -> &str {
        match self {
            UpstreamError::Connect { endpoint, .. }
            | UpstreamError::ConnectionLost { endpoint, .. } => endpoint,
        }
    }
}

/// What to do after an upstream error (pingora's retry protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Re-enter routing (`upstream()` is called again) and re-attach.
    /// The interrupted query, if any, is answered with a synthesized
    /// `08006` error — queries are never replayed automatically.
    Relink,
    /// End the session.
    Close,
}

/// Which protocol flavor started a query cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CycleKind {
    /// Simple protocol `Query` message.
    Simple,
    /// Extended protocol `Parse` message.
    Extended {
        /// Statement name; `None` for the unnamed statement.
        statement: Option<String>,
    },
}

/// A query cycle starting: what [`Interceptor::on_query`] sees and can
/// rewrite.
#[derive(Debug)]
pub struct QueryCycle {
    sql: String,
    kind: CycleKind,
    rewritten: bool,
}

impl QueryCycle {
    pub(crate) fn new(sql: String, kind: CycleKind) -> Self {
        QueryCycle {
            sql,
            kind,
            rewritten: false,
        }
    }

    /// The SQL text of the cycle (`Query.query` or `Parse.query`).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Rewrite the SQL; the forwarded message carries the new text.
    pub fn set_sql(&mut self, sql: impl Into<String>) {
        self.sql = sql.into();
        self.rewritten = true;
    }

    pub fn kind(&self) -> CycleKind {
        self.kind.clone()
    }

    pub(crate) fn take_rewritten(&mut self) -> Option<String> {
        if self.rewritten {
            self.rewritten = false;
            Some(std::mem::take(&mut self.sql))
        } else {
            None
        }
    }
}

/// Telemetry for a finished query cycle, passed to
/// [`Interceptor::on_cycle_end`] (the logging phase).
#[derive(Debug, Clone)]
pub struct CycleReport {
    /// SQL text of the cycle (post-rewrite).
    pub sql: Option<String>,
    pub kind: CycleKind,
    /// The last `CommandComplete` tag of the cycle, e.g. `"SELECT 5"`.
    pub tag: Option<String>,
    /// Rows sent downstream.
    pub rows: u64,
    /// Rows dropped by `on_row` (when row interception is active).
    pub rows_dropped: u64,
    /// Error, if the cycle ended in `ErrorResponse` (including injected
    /// denials).
    pub error: Option<CycleError>,
    /// Cycle duration (cycle start → `ReadyForQuery` sent downstream).
    pub latency: Duration,
}

/// Column metadata from the session's current `RowDescription`.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub type_oid: u32,
    /// Wire format: `0` text, `1` binary.
    pub format: i16,
}

impl ColumnMeta {
    pub fn is_text(&self) -> bool {
        self.format == 0
    }
}

/// The result-set schema in effect for a `DataRow` (tracked by the session
/// from the preceding `RowDescription`).
#[derive(Debug, Clone, Default)]
pub struct RowSchema {
    pub columns: Vec<ColumnMeta>,
}

impl RowSchema {
    pub(crate) fn from_row_description(desc: &RowDescription) -> Self {
        RowSchema {
            columns: desc
                .fields
                .iter()
                .map(|f| ColumnMeta {
                    name: f.name.clone(),
                    type_oid: f.type_id,
                    format: f.format_code,
                })
                .collect(),
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Errors from value-level row mutation.
#[derive(Debug, thiserror::Error)]
pub enum RowError {
    #[error("column index {0} out of range (schema has {1} columns)")]
    OutOfRange(usize, usize),
    #[error("column {0} is in binary format; text mutation is not supported")]
    BinaryColumn(usize),
    #[error("column {0} is null")]
    Null(usize),
}

/// A value slot in a parsed row: either a range into the original message
/// bytes, or a rewritten value.
#[derive(Debug)]
enum ValueSlot {
    /// Range into the original `DataRow.data`.
    Range { offset: usize, len: usize },
    /// Rewritten value.
    Owned(BytesMut),
}

/// A decoded, mutable `DataRow` handed to [`Interceptor::on_row`].
///
/// Values are parsed as ranges into the raw message bytes (zero-copy); the
/// message is rebuilt on [`DataRowMut::apply`] only when a value was
/// rewritten — message lengths are recomputed, so framing cannot be
/// corrupted by editing values.
pub struct DataRowMut<'a> {
    row: &'a mut DataRow,
    values: Vec<Option<ValueSlot>>,
    modified: bool,
}

impl<'a> DataRowMut<'a> {
    /// Parse the row's raw bytes. Fails if the data does not match the
    /// field count.
    pub(crate) fn parse(row: &'a mut DataRow) -> Result<Self, RowError> {
        let field_count = row.field_count;
        let mut values = Vec::with_capacity(field_count as usize);
        let mut cursor: &[u8] = &row.data;
        let mut offset = 0usize;
        for _ in 0..field_count {
            if cursor.remaining() < 4 {
                return Err(RowError::OutOfRange(usize::MAX, field_count as usize));
            }
            let len = cursor.get_i32();
            offset += 4;
            if len < 0 {
                values.push(None);
            } else {
                let len = len as usize;
                if cursor.remaining() < len {
                    return Err(RowError::OutOfRange(usize::MAX, field_count as usize));
                }
                values.push(Some(ValueSlot::Range { offset, len }));
                offset += len;
                cursor.advance(len);
            }
        }
        Ok(DataRowMut {
            row,
            values,
            modified: false,
        })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn is_null(&self, index: usize) -> Result<bool, RowError> {
        self.values
            .get(index)
            .map(|v| v.is_none())
            .ok_or(RowError::OutOfRange(index, self.values.len()))
    }

    /// Get a column's value as UTF-8 text (text-format columns only).
    pub fn text(&self, index: usize) -> Result<Option<&str>, RowError> {
        match self.values.get(index) {
            None => Err(RowError::OutOfRange(index, self.values.len())),
            Some(None) => Ok(None),
            Some(Some(ValueSlot::Owned(bytes))) => std::str::from_utf8(bytes)
                .map(Some)
                .map_err(|_| RowError::BinaryColumn(index)),
            Some(Some(ValueSlot::Range { offset, len })) => {
                std::str::from_utf8(&self.row.data[*offset..*offset + *len])
                    .map(Some)
                    .map_err(|_| RowError::BinaryColumn(index))
            }
        }
    }

    /// Overwrite a text column's value.
    pub fn set_text(&mut self, index: usize, value: impl AsRef<str>) -> Result<(), RowError> {
        if index >= self.values.len() {
            return Err(RowError::OutOfRange(index, self.values.len()));
        }
        self.values[index] = Some(ValueSlot::Owned(BytesMut::from(value.as_ref().as_bytes())));
        self.modified = true;
        Ok(())
    }

    /// Set a column to NULL.
    pub fn set_null(&mut self, index: usize) -> Result<(), RowError> {
        if index >= self.values.len() {
            return Err(RowError::OutOfRange(index, self.values.len()));
        }
        self.values[index] = None;
        self.modified = true;
        Ok(())
    }

    /// Whether any mutation happened (the session skips re-encoding
    /// otherwise).
    pub fn is_modified(&self) -> bool {
        self.modified
    }

    /// Re-encode modified values back into the raw message. No-op unless
    /// modified.
    pub(crate) fn apply(self) -> &'a mut DataRow {
        if self.modified {
            let mut data = BytesMut::with_capacity(
                self.values
                    .iter()
                    .map(|v| {
                        4 + match v {
                            None => 0,
                            Some(ValueSlot::Range { len, .. }) => *len,
                            Some(ValueSlot::Owned(bytes)) => bytes.len(),
                        }
                    })
                    .sum(),
            );
            for value in &self.values {
                match value {
                    Some(ValueSlot::Range { offset, len }) => {
                        data.put_i32(*len as i32);
                        data.extend_from_slice(&self.row.data[*offset..*offset + *len]);
                    }
                    Some(ValueSlot::Owned(bytes)) => {
                        data.put_i32(bytes.len() as i32);
                        data.extend_from_slice(bytes);
                    }
                    None => data.put_i32(-1),
                }
            }
            self.row.data = data;
        }
        self.row
    }
}

/// The pgferry extension point.
///
/// Implement this on your gateway struct and register it with
/// [`crate::ProxyBuilder::service`]. Every hook has a default no-op, so
/// start from the phases you need (the async_trait attribute is required
/// on impls, as with pgwire's handler traits).
///
/// The associated [`Interceptor::Ctx`] is per-session state created at
/// session start and threaded through every hook (pingora's `CTX`).
#[async_trait]
pub trait Interceptor: Send + Sync + 'static {
    /// Per-session state, threaded through every hook.
    type Ctx: Send + Default + 'static;

    /// Query-cycle start (`Query` in the simple protocol, `Parse` in the
    /// extended protocol — the point where SQL becomes visible). Validate,
    /// deny, rewrite, or answer locally.
    async fn on_query(&self, _ctx: &mut Self::Ctx, _cycle: &mut QueryCycle) -> Action {
        Action::Forward
    }

    /// Raw frontend (client → server) message hook: full power, in-place
    /// mutation, allocation-free hot path. Fires after `on_query` for
    /// cycle-starting messages (on the rewritten form).
    ///
    /// `Deny` is not honored here (module docs); use `on_query`.
    async fn on_frontend(&self, _ctx: &mut Self::Ctx, _msg: &mut PgWireFrontendMessage) -> Action {
        Action::Forward
    }

    /// Raw backend (server → client) message hook. Note: for `DataRow`
    /// this fires after `on_row` (on the rewritten row).
    async fn on_backend(&self, _ctx: &mut Self::Ctx, _msg: &mut PgWireBackendMessage) -> Action {
        Action::Forward
    }

    /// Value-level row rewriting. The session decodes the `DataRow` against
    /// its tracked schema only when [`Interceptor::wants_rows`] is `true`.
    async fn on_row(
        &self,
        _ctx: &mut Self::Ctx,
        _schema: &RowSchema,
        _row: &mut DataRowMut<'_>,
    ) -> RowAction {
        RowAction::Keep
    }

    /// Whether [`Interceptor::on_row`] should fire. Opt in to pay for row
    /// decoding; the session calls this once per session.
    fn wants_rows(&self) -> bool {
        false
    }

    /// When true, the session rewrites the trailing row count of
    /// `CommandComplete` tags when `on_row` dropped rows in the cycle
    /// (never silent, always opt-in). Defaults to false: dropped rows make
    /// the tag lie unless you enable this.
    fn adjust_command_counts(&self) -> bool {
        false
    }

    /// Cycle telemetry (logging phase): tag, rows, latency, error. Called
    /// when the cycle's `ReadyForQuery` is sent downstream — including
    /// cycles denied or answered locally.
    async fn on_cycle_end(&self, _ctx: &mut Self::Ctx, _report: &CycleReport) {}

    /// Routing hook (pingora's `upstream_peer`): pick the endpoint for the
    /// next attach — session start in session mode, cycle start in
    /// transaction mode, and any re-attach after a failure. Default: the
    /// first configured endpoint. Compose [`crate::routing::EndpointGroup`]
    /// / [`crate::routing::RwSplit`] here.
    async fn upstream(&self, _ctx: &mut Self::Ctx, info: &RoutingInfo<'_>) -> EndpointId {
        info.default_endpoint().to_owned()
    }

    /// Error policy (pingora's `fail_to_connect` / `error_while_proxy`):
    /// called on connect/probe failure or mid-lease connection loss.
    /// `Relink` re-enters [`Interceptor::upstream`]; the default marks
    /// nothing and always relinks — use [`crate::routing::EndpointGroup`]
    /// to track health and steer subsequent decisions.
    async fn on_upstream_error(
        &self,
        _ctx: &mut Self::Ctx,
        _error: &UpstreamError,
    ) -> RetryDecision {
        RetryDecision::Relink
    }

    /// Called when a connection to `endpoint` is successfully attached
    /// (the place to report health successes to an
    /// [`crate::routing::EndpointGroup`]).
    async fn on_upstream_connected(&self, _ctx: &mut Self::Ctx, _endpoint: &str) {}
}

/// No-op interceptor used when none is registered (the reference binary).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInterceptor;

#[async_trait]
impl Interceptor for NoopInterceptor {
    type Ctx = ();
}

/// Type-erased interceptor the session drives (blanket-implemented for
/// every [`Interceptor`]). Not public API; it exists so `Proxy` can stay
/// non-generic.
#[async_trait]
pub(crate) trait SessionInterceptor: Send + Sync {
    fn new_session_ctx(&self) -> Box<dyn Any + Send>;
    fn wants_rows(&self) -> bool;
    fn adjust_command_counts(&self) -> bool;
    async fn on_query(&self, ctx: &mut (dyn Any + Send), cycle: &mut QueryCycle) -> Action;
    async fn on_frontend(
        &self,
        ctx: &mut (dyn Any + Send),
        msg: &mut PgWireFrontendMessage,
    ) -> Action;
    async fn on_backend(
        &self,
        ctx: &mut (dyn Any + Send),
        msg: &mut PgWireBackendMessage,
    ) -> Action;
    async fn on_row(
        &self,
        ctx: &mut (dyn Any + Send),
        schema: &RowSchema,
        row: &mut DataRowMut<'_>,
    ) -> RowAction;
    async fn on_cycle_end(&self, ctx: &mut (dyn Any + Send), report: &CycleReport);
    async fn upstream(&self, ctx: &mut (dyn Any + Send), info: &RoutingInfo<'_>) -> EndpointId;
    async fn on_upstream_error(
        &self,
        ctx: &mut (dyn Any + Send),
        error: &UpstreamError,
    ) -> RetryDecision;
    async fn on_upstream_connected(&self, ctx: &mut (dyn Any + Send), endpoint: &str);
}

#[async_trait]
impl<I: Interceptor> SessionInterceptor for I {
    fn new_session_ctx(&self) -> Box<dyn Any + Send> {
        Box::new(I::Ctx::default())
    }

    fn wants_rows(&self) -> bool {
        Interceptor::wants_rows(self)
    }

    fn adjust_command_counts(&self) -> bool {
        Interceptor::adjust_command_counts(self)
    }

    async fn on_query(&self, ctx: &mut (dyn Any + Send), cycle: &mut QueryCycle) -> Action {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_query(self, c, cycle).await
    }

    async fn on_frontend(
        &self,
        ctx: &mut (dyn Any + Send),
        msg: &mut PgWireFrontendMessage,
    ) -> Action {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_frontend(self, c, msg).await
    }

    async fn on_backend(
        &self,
        ctx: &mut (dyn Any + Send),
        msg: &mut PgWireBackendMessage,
    ) -> Action {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_backend(self, c, msg).await
    }

    async fn on_row(
        &self,
        ctx: &mut (dyn Any + Send),
        schema: &RowSchema,
        row: &mut DataRowMut<'_>,
    ) -> RowAction {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_row(self, c, schema, row).await
    }

    async fn on_cycle_end(&self, ctx: &mut (dyn Any + Send), report: &CycleReport) {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_cycle_end(self, c, report).await
    }

    async fn upstream(&self, ctx: &mut (dyn Any + Send), info: &RoutingInfo<'_>) -> EndpointId {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::upstream(self, c, info).await
    }

    async fn on_upstream_error(
        &self,
        ctx: &mut (dyn Any + Send),
        error: &UpstreamError,
    ) -> RetryDecision {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_upstream_error(self, c, error).await
    }

    async fn on_upstream_connected(&self, ctx: &mut (dyn Any + Send), endpoint: &str) {
        let c = ctx
            .downcast_mut::<I::Ctx>()
            .expect("session ctx type matches the interceptor");
        Interceptor::on_upstream_connected(self, c, endpoint).await
    }
}

/// Built-in interceptor helpers: ordinary values your hooks call.
pub mod builtin;
