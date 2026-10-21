// Prost request literals end with `..Default::default()` so a new optional
// Slingshot field does not break this adapter. Clippy sees no effect until
// that field exists.
#![allow(clippy::needless_update)]

use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EsbQuery, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
    ReadResponse, StoreDataDir, StoreManager, StoreManagerFactory, VecReadResponse,
};
use bench_core::wait_for_ready;
use bench_testcontainers::boomerang::{Boomerang, BOOMERANG_PORT};
use prost_types::Timestamp;
use proto::event_store_service_client::EventStoreServiceClient;
use proto::{
    subscribe_request, AppendCondition, AppendDcbRequest, AppendStreamRequest, EventMetadata,
    GetCurrentSequenceRequest, Query, QueryItem, ReadByQueryRequest, ReadByQueryStreamRequest,
    ReadByQueryStreamResponse, ReadStreamRequest, SequencedEventProto, SerializedEventProto,
    SubscribeRequest, SubscribeResponse,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic_types::StatusExt;

pub mod proto {
    // Generated from protos/sellistix/slingshot/v1/event_store.proto.
    // The old boomerang.eventstore package is not wire-compatible with this server.
    tonic::include_proto!("sellistix.slingshot.v1");
}

const UNCONDITIONAL_TAG: &str = "__esb_unconditional__";

/// Smoke and official readers use `limit: 10`. Unary `ReadByQuery` already has `limit`;
/// stream RPC `limit` is 0 = unbounded (R2). Stay on the stream for unbounded / `read_all`
/// and for limits above this cap so a 1e6 `full_scan` is not buffered in one response.
const UNARY_READ_LIMIT_MAX: u64 = 256;

fn use_unary_read(limit: Option<u64>) -> bool {
    matches!(limit, Some(n) if (1..=UNARY_READ_LIMIT_MAX).contains(&n))
}

/// Stream reads omit `limit` when the harness has no limit. On this contract
/// `0` is a real limit, and the old `0` meant unbounded, so both are absent.
fn stream_rpc_limit(limit: Option<u64>) -> Option<i32> {
    match limit {
        Some(n) if n > 0 => Some(i32::try_from(n).unwrap_or(i32::MAX)),
        _ => None,
    }
}

/// Tag and type filters become one query item. No tag and no type is an empty
/// item list, which matches every event.
fn query_proto(tag: &str, event_type: Option<&str>) -> Query {
    let types = event_type
        .map(|event_type| vec![event_type.to_string()])
        .unwrap_or_default();
    let tags = if tag.is_empty() {
        Vec::new()
    } else {
        vec![tag.to_string()]
    };
    if types.is_empty() && tags.is_empty() {
        return Query {
            ..Default::default()
        };
    }
    Query {
        items: vec![QueryItem {
            types,
            tags,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// .NET UTC ticks at 1970-01-01T00:00:00Z. The old read calls sent this clock
/// as ticks; the typed contract uses `google.protobuf.Timestamp`.
const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
const TICKS_PER_SECOND: i64 = 10_000_000;

fn timestamp_from_utc_ticks(ticks: i64) -> Timestamp {
    let since_unix = i128::from(ticks) - i128::from(UNIX_EPOCH_TICKS);
    let seconds = since_unix.div_euclid(i128::from(TICKS_PER_SECOND));
    let nanos = since_unix.rem_euclid(i128::from(TICKS_PER_SECOND)) * 100;
    Timestamp {
        seconds: seconds as i64,
        nanos: nanos as i32,
        ..Default::default()
    }
}

/// `None` and `Some(0)` are the old "no time limit" sentinel.
fn optional_to_time(utc_ticks: Option<i64>) -> Option<Timestamp> {
    match utc_ticks {
        Some(ticks) if ticks != 0 => Some(timestamp_from_utc_ticks(ticks)),
        _ => None,
    }
}

pub struct BoomerangStoreManager {
    uri: String,
    container: Option<ContainerAsync<Boomerang>>,
    use_docker: bool,
    data_dir: StoreDataDir,
    memory_limit_mb: Option<u64>,
    docker_platform: Option<String>,
    store_name: &'static str,
    durability: &'static str,
}

impl BoomerangStoreManager {
    /// The single published Boomerang posture: durability `Fsync`.
    ///
    /// The suite used to run `boomerang` (Synchronous) and `boomerang-fsync` side by side.
    /// Once group commit landed, Fsync matched or beat Synchronous at every worker count, so
    /// the weaker mode no longer earns a column — we publish the durable one.
    pub fn fsync(data_dir: Option<String>, use_docker: bool) -> Self {
        Self::new("boomerang", "Fsync", data_dir, use_docker)
    }

    fn new(
        store_name: &'static str,
        durability: &'static str,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Self {
        Self {
            uri: Self::get_uri(),
            container: None,
            use_docker,
            data_dir: StoreDataDir::new(data_dir, store_name),
            memory_limit_mb: None,
            docker_platform: None,
            store_name,
            durability,
        }
    }

    fn format_uri(host_port: u16) -> String {
        format!("http://127.0.0.1:{host_port}")
    }

    fn get_uri() -> String {
        let uri = std::env::var("BOOMERANG_URI")
            .ok()
            .unwrap_or_else(|| Self::format_uri(BOOMERANG_PORT.as_u16()));
        println!("Boomerang Server URI: {uri}");
        uri
    }
}

#[async_trait]
impl StoreManager for BoomerangStoreManager {
    fn use_docker(&self) -> bool {
        self.use_docker
    }

    async fn start(&mut self) -> Result<()> {
        // wait_for_ready discards the underlying error, so an illegal
        // SLINGSHOT_CONTEXT must fail here — before Docker and before dialing.
        let _ = slingshot_context_from_env()?;

        if self.use_docker {
            let mount_path = self.data_dir.setup()?;
            #[cfg_attr(not(unix), allow(unused_variables))]
            let is_bind_mount = mount_path.is_some();
            let mut image: ContainerRequest<_> = Boomerang::new(mount_path, self.durability).into();

            #[cfg(unix)]
            if is_bind_mount {
                let uid = unsafe { libc::getuid() };
                let gid = unsafe { libc::getgid() };
                image = image.with_user(format!("{uid}:{gid}"));
            }

            image = image.with_ulimit("nofile", 1_048_576, Some(1_048_576));

            if let Some(ref platform) = self.docker_platform {
                image = image.with_platform(platform);
            }

            if let Some(limit_mb) = self.memory_limit_mb {
                let bytes = limit_mb * 1024 * 1024;
                image = image.with_host_config_modifier(move |host_config| {
                    host_config.memory = Some(bytes as i64);
                });
            }

            let container = image.start().await?;
            let host_port = container.get_host_port_ipv4(BOOMERANG_PORT).await?;
            self.uri = Self::format_uri(host_port);
            self.container = Some(container);

            let uri = self.uri.clone();
            wait_for_ready(
                self.store_name,
                || {
                    let uri = uri.clone();
                    async move {
                        let mut client = connect_client(&uri).await?;
                        client
                            .get_current_sequence(GetCurrentSequenceRequest {
                                ..Default::default()
                            })
                            .await
                            .map_err(|status| grpc_status_error(status, "GetCurrentSequence"))?;
                        Ok::<(), anyhow::Error>(())
                    }
                },
                Duration::from_secs(60),
            )
            .await?;
        }
        Ok(())
    }

    async fn pull(&mut self) -> Result<()> {
        // The image is built locally (`scripts/smoke-*.sh`) and tagged boomerang:local,
        // or whatever BOOMERANG_IMAGE names. There is no registry pull.
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(container) = self.container.take() {
            println!("Stopping container");
            container.stop().await?;
            println!("Stopped container");
        }
        self.data_dir.cleanup()?;
        Ok(())
    }

    fn container_id(&self) -> Option<String> {
        self.container.as_ref().map(|c| c.id().to_string())
    }

    fn set_memory_limit(&mut self, limit_mb: Option<u64>) {
        self.memory_limit_mb = limit_mb;
    }

    fn set_docker_platform(&mut self, platform: Option<String>) {
        self.docker_platform = platform;
    }

    fn name(&self) -> &'static str {
        self.store_name
    }

    fn describe(&self) -> serde_json::Value {
        let mut desc = Boomerang::describe(self.durability);
        if let Some(limit_mb) = self.memory_limit_mb {
            desc["memory_limit_mb"] = serde_json::json!(limit_mb);
        }
        desc
    }

    async fn create_adapter(&mut self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(BoomerangAdapter::connect(&self.uri).await?))
    }

    async fn logs(&self) -> Result<String> {
        if let Some(container) = &self.container {
            let stdout = container.stdout_to_vec().await?;
            let stderr = container.stderr_to_vec().await?;
            let mut logs = String::from_utf8_lossy(&stdout).to_string();
            if !stderr.is_empty() {
                logs.push_str("\n--- STDERR ---\n");
                logs.push_str(&String::from_utf8_lossy(&stderr));
            }
            Ok(logs)
        } else {
            Ok("No logs".to_string())
        }
    }
}

const SLINGSHOT_CONTEXT_HEADER: &str = "x-slingshot-context";
const DEFAULT_SLINGSHOT_CONTEXT: &str = "default";

/// Every client is this type. The interceptor sits on the channel, so unary
/// calls, streaming reads, subscribe, and calls after the channel reconnects
/// all send `x-slingshot-context`.
type BoomerangClient =
    EventStoreServiceClient<InterceptedService<Channel, SlingshotContextInterceptor>>;

/// Adds `x-slingshot-context` from `SLINGSHOT_CONTEXT` (default `default`).
/// Built once per client after the value is checked at startup.
#[derive(Clone, Debug)]
struct SlingshotContextInterceptor {
    value: MetadataValue<Ascii>,
}

impl SlingshotContextInterceptor {
    fn parse(raw: &str) -> Result<Self> {
        if let Some(reason) = illegal_slingshot_context(raw) {
            anyhow::bail!(
                "SLINGSHOT_CONTEXT is not a legal ASCII header value (got {raw:?}): {reason}"
            );
        }
        let value = raw.parse::<MetadataValue<Ascii>>().map_err(|err| {
            anyhow::anyhow!(
                "SLINGSHOT_CONTEXT is not a legal ASCII header value (got {raw:?}): {err}"
            )
        })?;
        Ok(Self { value })
    }
}

/// `None` means `SLINGSHOT_CONTEXT` is unset, which selects `default`.
fn resolve_slingshot_context(value: Option<&str>) -> Result<SlingshotContextInterceptor> {
    SlingshotContextInterceptor::parse(value.unwrap_or(DEFAULT_SLINGSHOT_CONTEXT))
}

/// Env read for `resolve_slingshot_context`. Callers pass the `std::env::var` result.
fn slingshot_context_from_var(
    var: std::result::Result<String, std::env::VarError>,
) -> Result<SlingshotContextInterceptor> {
    match var {
        Ok(value) => resolve_slingshot_context(Some(&value)),
        Err(std::env::VarError::NotPresent) => resolve_slingshot_context(None),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!(
            "SLINGSHOT_CONTEXT is not a legal ASCII header value: the environment value is not valid Unicode; expected one or more printable ASCII characters (bytes 0x20-0x7E) with no leading or trailing whitespace"
        ),
    }
}

fn slingshot_context_from_env() -> Result<SlingshotContextInterceptor> {
    slingshot_context_from_var(std::env::var("SLINGSHOT_CONTEXT"))
}

/// Printable ASCII (`0x20`–`0x7E`), non-empty, with no leading or trailing whitespace.
fn illegal_slingshot_context(value: &str) -> Option<&'static str> {
    if value != value.trim() {
        return Some("leading or trailing whitespace is not allowed");
    }
    if value.is_empty() || value.bytes().any(|byte| !(0x20..=0x7e).contains(&byte)) {
        return Some(
            "expected one or more printable ASCII characters (bytes 0x20-0x7E) with no leading or trailing whitespace",
        );
    }
    None
}

impl Interceptor for SlingshotContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert(SLINGSHOT_CONTEXT_HEADER, self.value.clone());
        Ok(request)
    }
}

async fn connect_client(uri: &str) -> Result<BoomerangClient> {
    connect_with_interceptor(uri, slingshot_context_from_env()?).await
}

#[cfg(test)]
async fn connect_client_with(uri: &str, context: Option<&str>) -> Result<BoomerangClient> {
    connect_with_interceptor(uri, resolve_slingshot_context(context)?).await
}

async fn connect_with_interceptor(
    uri: &str,
    interceptor: SlingshotContextInterceptor,
) -> Result<BoomerangClient> {
    let endpoint = Endpoint::from_shared(uri.to_string())?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_timeout(Duration::from_secs(10))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024));
    let channel = endpoint.connect().await?;
    Ok(EventStoreServiceClient::with_interceptor(
        channel,
        interceptor,
    ))
}

pub struct BoomerangAdapter {
    client: BoomerangClient,
}

impl BoomerangAdapter {
    pub async fn connect(uri: &str) -> Result<Self> {
        Ok(Self {
            client: connect_client(uri).await?,
        })
    }

    fn convert_events(events: &[EventData]) -> Vec<SerializedEventProto> {
        events
            .iter()
            .map(|evt| SerializedEventProto {
                type_name: evt.event_type.to_string(),
                payload: evt.payload.to_vec(),
                // Opaque / unspecified bytes. Raw benchmark payloads are not MemoryPack.
                codec_id: 255,
                // Leave schema_version absent. A present 0 is a different value on
                // this contract; absent means frame version 1, the same as a
                // missing or zero version on an older row.
                ..Default::default()
            })
            .collect()
    }

    fn batch_tags(events: &[EventData]) -> Vec<String> {
        events
            .iter()
            .flat_map(|evt| evt.tags.iter().map(|t| t.to_string()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// One metadata message for the batch. No pairs means the field is absent,
    /// the same as the old empty metadata bytes.
    fn event_metadata(events: &[EventData]) -> Option<EventMetadata> {
        let mut custom = std::collections::HashMap::new();
        for event in events {
            for (key, value) in event.metadata.iter() {
                custom.insert(key.clone(), value.clone());
            }
        }
        if custom.is_empty() {
            None
        } else {
            Some(EventMetadata {
                custom,
                ..Default::default()
            })
        }
    }

    fn query_from_esb(query: EsbQuery) -> Query {
        Query {
            items: query
                .items
                .into_iter()
                .map(|item| QueryItem {
                    types: item.types,
                    tags: item.tags,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Unconditional appends still name a tag that matches nothing. An empty
    /// query matches every event, which would fail the append.
    fn append_condition(condition: Option<EsbAppendCondition>) -> AppendCondition {
        match condition {
            None => AppendCondition {
                fail_if_events_match: Some(Query {
                    items: vec![QueryItem {
                        tags: vec![UNCONDITIONAL_TAG.to_string()],
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Some(condition) => AppendCondition {
                fail_if_events_match: Some(Self::query_from_esb(condition.fail_if_events_match)),
                after: condition.after.map(|after| after as i64),
                ..Default::default()
            },
        }
    }

    fn append_dcb_request(
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> AppendDcbRequest {
        AppendDcbRequest {
            events: Self::convert_events(events),
            tags: Self::batch_tags(events),
            condition: Some(Self::append_condition(condition)),
            metadata: Self::event_metadata(events),
            ..Default::default()
        }
    }

    fn append_stream_request(
        events: &[EventData],
        stream_position: Option<usize>,
    ) -> anyhow::Result<AppendStreamRequest> {
        Ok(AppendStreamRequest {
            stream_id: Self::stream_id_from_events(events)?,
            events: Self::convert_events(events),
            expected_version: Self::expected_version(stream_position),
            tags: Self::batch_tags(events),
            metadata: Self::event_metadata(events),
            ..Default::default()
        })
    }

    fn read_stream_request(stream_id: &str, to_time_ticks: Option<i64>) -> ReadStreamRequest {
        ReadStreamRequest {
            stream_id: stream_id.to_string(),
            from_version: 0,
            to_version: None,
            to_time: optional_to_time(to_time_ticks),
            ..Default::default()
        }
    }

    fn read_by_query_request(
        tag: &str,
        event_type: Option<&str>,
        from_offset: Option<u64>,
        limit: u64,
        to_time_ticks: Option<i64>,
    ) -> ReadByQueryRequest {
        ReadByQueryRequest {
            from_sequence_position: Self::inclusive_from(from_offset),
            limit: Some(i32::try_from(limit).unwrap_or(i32::MAX)),
            to_sequence_position: None,
            query: Some(query_proto(tag, event_type)),
            to_time: optional_to_time(to_time_ticks),
            ..Default::default()
        }
    }

    fn read_by_query_stream_request(
        tag: &str,
        event_type: Option<&str>,
        from_offset: Option<u64>,
        limit: Option<u64>,
        to_time_ticks: Option<i64>,
    ) -> ReadByQueryStreamRequest {
        ReadByQueryStreamRequest {
            from_sequence_position: Self::inclusive_from(from_offset),
            to_sequence_position: None,
            limit: stream_rpc_limit(limit),
            query: Some(query_proto(tag, event_type)),
            to_time: optional_to_time(to_time_ticks),
            ..Default::default()
        }
    }

    fn subscribe_request(after: Option<u64>) -> SubscribeRequest {
        // Inclusive start. No cursor starts at 0. A harness cursor is exclusive.
        let from_sequence_position = after
            .map(|position| position.saturating_add(1) as i64)
            .unwrap_or(0);
        SubscribeRequest {
            subscriber_id: format!("esb-{}", uuid::Uuid::new_v4()),
            from_sequence_position,
            scope: Some(subscribe_request::Scope::GlobalAll(true)),
            ..Default::default()
        }
    }

    fn last_position(positions: &[i64]) -> Option<u64> {
        positions
            .iter()
            .copied()
            .filter(|&p| p >= 0)
            .max()
            .map(|p| p as u64)
    }

    /// Graded prepopulate puts distinct selectivity tags on each event in one
    /// `append_to_stream` batch. The portable proto only has batch-level tags, so a
    /// union would make every event match every `s10`/`s100`/`s1000` key. Split
    /// unconditional batches when tag sets differ.
    fn tags_heterogeneous(events: &[EventData]) -> bool {
        if events.len() <= 1 {
            return false;
        }
        let first = events[0].tags.as_ref();
        events[1..].iter().any(|e| e.tags.as_ref() != first)
    }

    async fn append_dcb_batch(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> anyhow::Result<Option<u64>> {
        let request = Self::append_dcb_request(events, condition);
        let mut client = self.client.clone();
        let response = check_rpc(client.append_dcb(request).await, "AppendDcb")?.into_inner();
        Ok(Self::last_position(&response.sequence_positions))
    }

    /// Harness contract: first event tag is the logical stream id (`append_to_stream`).
    fn stream_id_from_events(events: &[EventData]) -> anyhow::Result<String> {
        events
            .first()
            .and_then(|e| e.tags.first())
            .map(|t| t.as_ref())
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "append_to_stream requires EventData.tags[0] as the harness stream identifier"
                )
            })
    }

    /// Absent when the harness does not ask for a version check. `Some(0)` is
    /// version 0; the old `-1` sentinel is not sent.
    fn expected_version(stream_position: Option<usize>) -> Option<i64> {
        stream_position.map(|position| position as i64)
    }

    async fn append_stream_batch(
        &self,
        events: &[EventData],
        stream_position: Option<usize>,
    ) -> anyhow::Result<Option<u64>> {
        let request = Self::append_stream_request(events, stream_position)?;
        let mut client = self.client.clone();
        let response = check_rpc(client.append_stream(request).await, "AppendStream")?.into_inner();
        Ok(Self::last_position(&response.sequence_positions))
    }

    #[allow(dead_code)] // kept for tests; the hot path reads by query
    async fn read_stream_by_id(&self, stream_id: &str) -> anyhow::Result<Vec<SequencedEventProto>> {
        let request = Self::read_stream_request(stream_id, None);
        let mut client = self.client.clone();
        Ok(check_rpc(client.read_stream(request).await, "ReadStream")?
            .into_inner()
            .events)
    }

    /// Harness `from_offset` is exclusive. Absent starts at the beginning of the log.
    fn inclusive_from(from_offset: Option<u64>) -> Option<i64> {
        from_offset.map(|after| after.saturating_add(1) as i64)
    }
}

const SLINGSHOT_ERROR_DOMAIN: &str = "slingshot.sellistix";
const REASON_NOT_LEADER: &str = "NOT_LEADER";
const REASON_APPEND_OUTCOME_UNKNOWN: &str = "APPEND_OUTCOME_UNKNOWN";

/// Slingshot failure decoded from a `google.rpc.ErrorInfo` detail
/// (domain `slingshot.sellistix`) on the `grpc-status-details-bin` trailer.
///
/// Every variant is terminal. `NOT_LEADER` means the call reached a follower.
/// There is no in-band flag on the append response. `APPEND_OUTCOME_UNKNOWN`
/// may already be durable. Version and condition conflicts (`ABORTED`,
/// including `WRONG_EXPECTED_VERSION` and `APPEND_CONDITION_FAILED`) must not
/// be repeated.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SlingshotFailure {
    NotLeader { leader_endpoint: String },
    AppendOutcomeUnknown,
    Conflict { reason: String },
}

impl SlingshotFailure {
    fn retryable(&self) -> bool {
        match self {
            Self::NotLeader { .. } | Self::AppendOutcomeUnknown | Self::Conflict { .. } => false,
        }
    }
}

/// Read a Slingshot reason when the status carries one. `ABORTED` with no
/// reason is still a version or condition conflict.
fn slingshot_failure(status: &tonic::Status) -> Option<SlingshotFailure> {
    if let Some(info) = status
        .get_details_error_info()
        .filter(|info| info.domain == SLINGSHOT_ERROR_DOMAIN)
    {
        match (status.code(), info.reason.as_str()) {
            (tonic::Code::Unavailable, REASON_NOT_LEADER) => {
                return Some(SlingshotFailure::NotLeader {
                    leader_endpoint: info
                        .metadata
                        .get("leader_endpoint")
                        .cloned()
                        .unwrap_or_default(),
                });
            }
            (tonic::Code::FailedPrecondition, REASON_APPEND_OUTCOME_UNKNOWN) => {
                return Some(SlingshotFailure::AppendOutcomeUnknown);
            }
            (tonic::Code::Aborted, reason) => {
                return Some(SlingshotFailure::Conflict {
                    reason: reason.to_string(),
                });
            }
            _ => {}
        }
    } else if status.code() == tonic::Code::Aborted {
        return Some(SlingshotFailure::Conflict {
            reason: String::new(),
        });
    }
    None
}

/// `NOT_LEADER`, `APPEND_OUTCOME_UNKNOWN`, and version/condition conflicts are
/// never retried. Bare `FAILED_PRECONDITION` and `ABORTED` are not retried
/// either, including when the reason trailer has not been attached yet.
fn grpc_status_retryable(status: &tonic::Status) -> bool {
    if slingshot_failure(status).is_some() {
        return false;
    }
    !matches!(
        status.code(),
        tonic::Code::Aborted | tonic::Code::FailedPrecondition
    )
}

fn follower_error(leader_endpoint: &str, rpc: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{rpc} reached a follower (leader_endpoint={leader_endpoint}). Point BOOMERANG_URI at the Raft leader."
    )
}

fn grpc_status_error(status: tonic::Status, rpc: &str) -> anyhow::Error {
    let Some(failure) = slingshot_failure(&status) else {
        // No decoded reason. Still do not retry ABORTED or FAILED_PRECONDITION;
        // those codes are the conflict and unknown-outcome statuses.
        return anyhow::Error::from(status);
    };
    debug_assert!(!failure.retryable());
    debug_assert!(!grpc_status_retryable(&status));
    match failure {
        SlingshotFailure::NotLeader { leader_endpoint } => follower_error(&leader_endpoint, rpc),
        SlingshotFailure::AppendOutcomeUnknown => anyhow::anyhow!(
            "{rpc} failed with APPEND_OUTCOME_UNKNOWN (FAILED_PRECONDITION); not retrying: {status}"
        ),
        SlingshotFailure::Conflict { reason } => {
            let detail = if reason.is_empty() {
                "ABORTED".to_string()
            } else {
                reason
            };
            anyhow::anyhow!(
                "{rpc} version or condition conflict ({detail}); not retrying: {status}"
            )
        }
    }
}

fn check_rpc<T>(result: Result<T, tonic::Status>, rpc: &str) -> Result<T> {
    result.map_err(|status| grpc_status_error(status, rpc))
}

#[async_trait]
impl EventStoreAdapter for BoomerangAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn append_dcb(
        &self,
        events: &[EventData],
        condition: Option<EsbAppendCondition>,
    ) -> anyhow::Result<Option<u64>> {
        if condition.is_none() && Self::tags_heterogeneous(events) {
            let mut last = None;
            for evt in events {
                last = self
                    .append_dcb_batch(std::slice::from_ref(evt), None)
                    .await?;
            }
            return Ok(last);
        }
        self.append_dcb_batch(events, condition).await
    }

    async fn append_to_stream(
        &self,
        events: &[EventData],
        stream_position: Option<usize>,
        _global_position: Option<u64>,
    ) -> anyhow::Result<Option<u64>> {
        // A0 Branch A: portable AppendStream. `global_position` is the DCB `after`
        // cursor (UmaDB); KurrentDB and Boomerang stream appends ignore it.
        if events.is_empty() {
            return Ok(None);
        }
        if Self::tags_heterogeneous(events) {
            let mut last = None;
            for evt in events {
                last = self
                    .append_stream_batch(std::slice::from_ref(evt), stream_position)
                    .await?;
            }
            return Ok(last);
        }
        self.append_stream_batch(events, stream_position).await
    }

    async fn read_stream(&self, req: ReadRequest) -> anyhow::Result<Box<dyn ReadResponse>> {
        // Small limits use one ReadByQuery response. Larger and unbounded reads
        // use the stream. An empty query item list matches every event.
        let event_type = req.event_type.as_deref();
        if use_unary_read(req.limit) {
            let request = Self::read_by_query_request(
                &req.tag,
                event_type,
                req.from_offset,
                req.limit.expect("use_unary_read"),
                None,
            );
            let mut client = self.client.clone();
            let response =
                check_rpc(client.read_by_query(request).await, "ReadByQuery")?.into_inner();
            let events = response.events.into_iter().map(map_read_event).collect();
            return Ok(Box::new(VecReadResponse::new(events)));
        }

        let request = Self::read_by_query_stream_request(
            &req.tag,
            event_type,
            req.from_offset,
            req.limit,
            None,
        );
        let mut client = self.client.clone();
        let stream = check_rpc(
            client.read_by_query_stream(request).await,
            "ReadByQueryStream",
        )?
        .into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream: EventStream::ByQuery(stream),
            remaining: req.limit,
        }))
    }

    async fn subscribe(&self, after: Option<u64>) -> anyhow::Result<Box<dyn ReadResponse>> {
        let request = Self::subscribe_request(after);
        let mut client = self.client.clone();
        let stream = check_rpc(client.subscribe(request).await, "Subscribe")?.into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream: EventStream::Subscribe(stream),
            remaining: None,
        }))
    }

    async fn read_all(&self) -> Result<Box<dyn ReadResponse>> {
        let request = Self::read_by_query_stream_request("", None, None, None, None);
        let mut client = self.client.clone();
        let stream = check_rpc(
            client.read_by_query_stream(request).await,
            "ReadByQueryStream",
        )?
        .into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream: EventStream::ByQuery(stream),
            remaining: None,
        }))
    }
}

enum EventStream {
    ByQuery(tonic::Streaming<ReadByQueryStreamResponse>),
    Subscribe(tonic::Streaming<SubscribeResponse>),
}

impl EventStream {
    /// The stream item is a wrapper. The sequenced event is field 1. A wrapper
    /// with no event is skipped so it is not the end of the stream.
    async fn message(&mut self) -> Result<Option<SequencedEventProto>, tonic::Status> {
        loop {
            let event = match self {
                Self::ByQuery(stream) => match stream.message().await? {
                    Some(response) => response.event,
                    None => return Ok(None),
                },
                Self::Subscribe(stream) => match stream.message().await? {
                    Some(response) => response.event,
                    None => return Ok(None),
                },
            };
            if event.is_some() {
                return Ok(event);
            }
        }
    }
}

struct BoomerangReadResponse {
    stream: EventStream,
    remaining: Option<u64>,
}

#[async_trait]
impl ReadResponse for BoomerangReadResponse {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        if matches!(self.remaining, Some(0)) {
            while self
                .stream
                .message()
                .await
                .map_err(|status| grpc_status_error(status, "stream"))?
                .is_some()
            {}
            return Ok(None);
        }
        match self
            .stream
            .message()
            .await
            .map_err(|status| grpc_status_error(status, "stream"))?
        {
            Some(proto) => {
                if let Some(ref mut n) = self.remaining {
                    *n = n.saturating_sub(1);
                }
                Ok(Some(map_read_event(proto)))
            }
            None => Ok(None),
        }
    }
}

fn map_read_event(proto: SequencedEventProto) -> ReadEvent {
    let (event_type, payload) = match proto.event {
        Some(evt) => (evt.type_name, evt.payload),
        None => (String::new(), Vec::new()),
    };
    ReadEvent {
        offset: proto.sequence_position.max(0) as u64,
        event_type,
        payload,
        metadata: metadata_pairs(proto.metadata),
    }
}

fn metadata_pairs(metadata: Option<EventMetadata>) -> Vec<(String, String)> {
    let Some(metadata) = metadata else {
        return Vec::new();
    };
    metadata.custom.into_iter().collect()
}

pub struct BoomerangFactory;

impl StoreManagerFactory for BoomerangFactory {
    fn name(&self) -> &'static str {
        "boomerang"
    }

    fn create_store_manager(
        &self,
        data_dir: Option<String>,
        use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(BoomerangStoreManager::fsync(data_dir, use_docker)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_core::adapter::EsbQueryItem;

    fn sample_events() -> Vec<EventData> {
        vec![
            EventData {
                payload: Arc::from(vec![1, 2, 3]),
                event_type: Arc::from("type1"),
                tags: Arc::from([Arc::from("tag1")]),
                metadata: Arc::from([]),
            },
            EventData {
                payload: Arc::from(vec![4, 5, 6]),
                event_type: Arc::from("type2"),
                tags: Arc::from([Arc::from("tag2")]),
                metadata: Arc::from([("timestamp".to_string(), "123456789".to_string())]),
            },
        ]
    }

    #[tokio::test]
    async fn test_append_two_events() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;
        let pos = adapter.append_dcb(&sample_events(), None).await?;
        assert!(pos.is_some(), "append should return a sequence position");
        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_all() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;
        adapter.append_dcb(&sample_events(), None).await?;

        let mut read_response = adapter.read_all().await?;
        let mut received = Vec::new();
        while let Some(event) = read_response.next_event().await? {
            received.push(event);
        }

        assert!(received.len() >= 2);
        let found1 = received
            .iter()
            .any(|e| e.event_type == "type1" && e.payload == vec![1, 2, 3]);
        let found2 = received
            .iter()
            .any(|e| e.event_type == "type2" && e.payload == vec![4, 5, 6]);
        assert!(found1, "Event type1 not found in read_all results");
        assert!(found2, "Event type2 not found in read_all results");

        let with_ts = received
            .iter()
            .find(|e| e.event_type == "type2")
            .expect("type2");
        assert!(
            with_ts
                .metadata
                .iter()
                .any(|(k, v)| k == "timestamp" && v == "123456789"),
            "custom timestamp should round-trip; got {:?}",
            with_ts.metadata
        );

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_stream_by_tag() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;
        adapter.append_dcb(&sample_events(), None).await?;

        let mut read_response = adapter
            .read_stream(ReadRequest {
                tag: "tag1".to_string(),
                event_type: None,
                from_offset: None,
                limit: Some(10),
            })
            .await?;
        let mut received = Vec::new();
        while let Some(event) = read_response.next_event().await? {
            received.push(event);
        }
        assert!(
            received
                .iter()
                .any(|e| e.event_type == "type1" && e.payload == vec![1, 2, 3]),
            "tag1 read should find type1; got {:?}",
            received
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>()
        );
        assert!(
            received.iter().all(|e| e.event_type != "type2"),
            "tag1 must not match type2 after per-event tag split; got {:?}",
            received
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>()
        );

        manager.stop().await?;
        Ok(())
    }

    #[test]
    fn use_unary_read_small_limit_only() {
        assert!(use_unary_read(Some(1)));
        assert!(use_unary_read(Some(10)));
        assert!(use_unary_read(Some(256)));
        assert!(!use_unary_read(None));
        assert!(!use_unary_read(Some(0)));
        assert!(!use_unary_read(Some(257)));
        assert!(!use_unary_read(Some(1_000_000)));
    }

    #[test]
    fn query_proto_empty_items_match_every_event() {
        let all = query_proto("", None);
        assert!(all.items.is_empty());

        let filtered = query_proto("t", Some("e"));
        assert_eq!(filtered.items[0].tags, vec!["t"]);
        assert_eq!(filtered.items[0].types, vec!["e"]);
        assert!(filtered.items[0].partition_filter.is_none());

        let tag_only = query_proto("t", None);
        assert!(tag_only.items[0].types.is_empty());
        assert_eq!(tag_only.items[0].tags, vec!["t"]);

        let type_only = query_proto("", Some("e"));
        assert_eq!(type_only.items[0].types, vec!["e"]);
        assert!(type_only.items[0].tags.is_empty());
    }

    #[test]
    fn stream_rpc_limit_absent_means_unbounded() {
        assert_eq!(stream_rpc_limit(None), None);
        assert_eq!(stream_rpc_limit(Some(0)), None);
        assert_eq!(stream_rpc_limit(Some(257)), Some(257));
    }

    #[test]
    fn append_to_stream_uses_first_tag_as_stream_id() {
        let events = sample_events();
        assert_eq!(
            BoomerangAdapter::stream_id_from_events(&events).unwrap(),
            "tag1"
        );
        assert_eq!(BoomerangAdapter::expected_version(None), None);
        assert_eq!(BoomerangAdapter::expected_version(Some(0)), Some(0));
        assert_eq!(BoomerangAdapter::expected_version(Some(9)), Some(9));
        let empty = EventData {
            payload: Arc::from(vec![1u8]),
            event_type: Arc::from("t"),
            tags: Arc::from([]),
            metadata: Arc::from([]),
        };
        assert!(BoomerangAdapter::stream_id_from_events(&[empty]).is_err());
    }

    #[tokio::test]
    async fn test_read_stream_respects_unary_limit() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;

        let tagged: Vec<EventData> = (0..5)
            .map(|i| EventData {
                payload: Arc::from(vec![i]),
                event_type: Arc::from("limited"),
                tags: Arc::from([Arc::from("limit-tag")]),
                metadata: Arc::from([]),
            })
            .collect();
        adapter.append_dcb(&tagged, None).await?;

        let mut read_response = adapter
            .read_stream(ReadRequest {
                tag: "limit-tag".to_string(),
                event_type: None,
                from_offset: None,
                limit: Some(2),
            })
            .await?;
        let mut received = Vec::new();
        while let Some(event) = read_response.next_event().await? {
            received.push(event);
        }
        assert_eq!(
            received.len(),
            2,
            "unary ReadByQuery must stop at limit; got {:?}",
            received
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>()
        );
        assert!(received.iter().all(|e| e.event_type == "limited"));

        let mut all = adapter.read_all().await?;
        let mut all_received = Vec::new();
        while let Some(event) = all.next_event().await? {
            all_received.push(event);
        }
        assert!(
            all_received
                .iter()
                .filter(|e| e.event_type == "limited")
                .count()
                >= 5,
            "read_all must still stream every matching event; got {}",
            all_received.len()
        );

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;

        let mut subscription = adapter.subscribe(None).await?;
        adapter.append_dcb(&sample_events(), None).await?;

        let event1 = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("subscribe timed out")?;
        let event2 = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("subscribe timed out")?;
        assert!(event1.is_some());
        assert!(event2.is_some());

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_append_to_stream_preserves_ten_event_logical_stream() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;
        let concrete = adapter
            .as_any()
            .downcast_ref::<BoomerangAdapter>()
            .expect("BoomerangAdapter");

        let stream_id = "stream-a0-logical-";
        let mut last_pos = None;
        for i in 0..10 {
            let evt = EventData {
                payload: Arc::from(vec![i as u8; 8]),
                event_type: Arc::from(format!("test-{i}")),
                tags: Arc::from([Arc::from(stream_id)]),
                metadata: Arc::from([]),
            };
            last_pos = adapter.append_to_stream(&[evt], None, None).await?;
        }
        assert!(
            last_pos.is_some(),
            "append_to_stream must return a global position"
        );

        let events = concrete.read_stream_by_id(stream_id).await?;
        assert_eq!(
            events.len(),
            10,
            "ten events must share one Boomerang stream"
        );
        for (i, evt) in events.iter().enumerate() {
            assert_eq!(evt.stream_id, stream_id);
            assert_eq!(evt.version, (i + 1) as i64);
            assert_eq!(evt.tags, vec![stream_id.to_string()]);
            assert_eq!(
                evt.event.as_ref().map(|e| e.type_name.as_str()),
                Some(format!("test-{i}")).as_deref()
            );
        }
        let positions: Vec<i64> = events.iter().map(|e| e.sequence_position).collect();
        for w in positions.windows(2) {
            assert!(w[1] > w[0], "global positions must increase");
        }

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_append_dcb_stays_on_dcb_stream_ids() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;
        let concrete = adapter
            .as_any()
            .downcast_ref::<BoomerangAdapter>()
            .expect("BoomerangAdapter");

        let tag = "dcb-a0-tag";
        let evt = EventData {
            payload: Arc::from(vec![9u8; 8]),
            event_type: Arc::from("dcb-type"),
            tags: Arc::from([Arc::from(tag)]),
            metadata: Arc::from([]),
        };
        let pos = adapter.append_dcb(&[evt.clone()], None).await?;
        assert!(pos.is_some());

        let stream_named_like_tag = concrete.read_stream_by_id(tag).await?;
        assert!(
            stream_named_like_tag.is_empty(),
            "append_dcb must not treat the tag as a traditional stream id"
        );

        let condition = EsbAppendCondition {
            fail_if_events_match: bench_core::adapter::EsbQuery {
                items: vec![EsbQueryItem::new().tags(vec![tag.to_string()])],
            },
            after: None,
        };
        let conflict = adapter.append_dcb(&[evt], Some(condition)).await;
        assert!(conflict.is_err(), "same-tag FailIfMatches must conflict");

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_append_to_stream_expected_version_conflict() -> Result<()> {
        let mut manager = BoomerangStoreManager::fsync(None, true);
        manager.start().await?;
        let adapter = manager.create_adapter().await?;

        let stream_id = "stream-a0-occ";
        let evt = EventData {
            payload: Arc::from(vec![1u8]),
            event_type: Arc::from("t"),
            tags: Arc::from([Arc::from(stream_id)]),
            metadata: Arc::from([]),
        };
        adapter.append_to_stream(&[evt.clone()], None, None).await?;
        let conflict = adapter.append_to_stream(&[evt], Some(0), None).await;
        assert!(
            conflict.is_err(),
            "expected version 0 must conflict after the stream is at version 1"
        );

        manager.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_factory_exposes_single_fsync_store() {
        let factory = BoomerangFactory;
        assert_eq!(factory.name(), "boomerang");

        // The published store must be the durable one; a silent fall back to Synchronous
        // would make every future number incomparable with the W1 baseline.
        let manager = BoomerangStoreManager::fsync(None, false);
        assert_eq!(manager.durability, "Fsync");
        assert_eq!(manager.store_name, "boomerang");
    }

    fn one_tag_one_type(tag: &str, event_type: &str) -> EsbAppendCondition {
        EsbAppendCondition::new(
            EsbQuery::new().item(
                EsbQueryItem::new()
                    .tags(vec![tag.to_string()])
                    .types(vec![event_type.to_string()]),
            ),
        )
        .after(Some(0))
    }

    #[test]
    fn typed_request_builders_keep_today_behaviour() {
        let events = sample_events();
        let dcb = BoomerangAdapter::append_dcb_request(&events, None);
        let condition = dcb.condition.expect("condition is required");
        let fail_if = condition
            .fail_if_events_match
            .expect("fail if events match");
        assert_eq!(fail_if.items[0].tags, vec![UNCONDITIONAL_TAG]);
        assert!(fail_if.items[0].types.is_empty());
        assert!(condition.after.is_none());
        assert!(condition.consistency_marker.is_none());
        assert!(dcb.tenant_id.is_none());
        assert!(dcb.request_id.is_empty());
        assert_eq!(dcb.tags, vec!["tag1".to_string(), "tag2".to_string()]);
        assert_eq!(dcb.events[0].type_name, "type1");
        assert_eq!(dcb.events[0].payload, vec![1, 2, 3]);
        assert_eq!(dcb.events[0].codec_id, 255);
        assert!(dcb.events[0].schema_version.is_none());
        assert_eq!(dcb.events[1].payload, vec![4, 5, 6]);
        let custom = &dcb.metadata.expect("custom metadata").custom;
        assert_eq!(
            custom.get("timestamp").map(String::as_str),
            Some("123456789")
        );

        let guarded = BoomerangAdapter::append_dcb_request(
            &events[..1],
            Some(one_tag_one_type("stream-0-", "test-0")),
        );
        let guarded_condition = guarded.condition.expect("condition");
        assert_eq!(guarded_condition.after, Some(0));
        let guarded_query = guarded_condition
            .fail_if_events_match
            .expect("fail if events match");
        assert_eq!(guarded_query.items[0].tags, vec!["stream-0-"]);
        assert_eq!(guarded_query.items[0].types, vec!["test-0"]);
        assert!(guarded.metadata.is_none());

        let bare = EventData {
            payload: Arc::from(vec![9u8]),
            event_type: Arc::from("bare"),
            tags: Arc::from([Arc::from("stream-a")]),
            metadata: Arc::from([]),
        };
        let append =
            BoomerangAdapter::append_stream_request(std::slice::from_ref(&bare), None).unwrap();
        assert_eq!(append.stream_id, "stream-a");
        assert!(append.expected_version.is_none());
        assert!(append.metadata.is_none());
        assert!(append.request_id.is_empty());
        assert_eq!(append.events[0].codec_id, 255);
        assert!(append.events[0].schema_version.is_none());

        let checked = BoomerangAdapter::append_stream_request(&[bare], Some(0)).unwrap();
        assert_eq!(checked.expected_version, Some(0));

        let by_id = BoomerangAdapter::read_stream_request("stream-a", None);
        assert_eq!(by_id.from_version, 0);
        assert!(by_id.to_version.is_none());
        assert!(by_id.to_time.is_none());

        let unary =
            BoomerangAdapter::read_by_query_request("tag1", Some("type1"), Some(4), 10, None);
        assert_eq!(unary.from_sequence_position, Some(5));
        assert_eq!(unary.limit, Some(10));
        assert!(unary.to_sequence_position.is_none());
        assert!(unary.to_time.is_none());
        assert!(unary.tenant_id.is_none());
        let unary_query = unary.query.expect("query");
        assert_eq!(unary_query.items[0].tags, vec!["tag1"]);
        assert_eq!(unary_query.items[0].types, vec!["type1"]);

        let stream = BoomerangAdapter::read_by_query_stream_request("", None, None, None, None);
        assert!(stream.from_sequence_position.is_none());
        assert!(stream.to_sequence_position.is_none());
        assert!(stream.limit.is_none());
        assert!(stream.to_time.is_none());
        assert!(stream.tenant_id.is_none());
        assert!(stream.query.expect("query all").items.is_empty());

        let limited =
            BoomerangAdapter::read_by_query_stream_request("t", None, None, Some(257), None);
        assert_eq!(limited.limit, Some(257));

        let from_start = BoomerangAdapter::subscribe_request(None);
        assert_eq!(from_start.from_sequence_position, 0);
        assert!(from_start.tenant_id.is_none());
        assert!(matches!(
            from_start.scope,
            Some(subscribe_request::Scope::GlobalAll(true))
        ));
        assert!(from_start.subscriber_id.starts_with("esb-"));

        let caught_up = BoomerangAdapter::subscribe_request(Some(8));
        assert_eq!(caught_up.from_sequence_position, 9);
    }

    #[test]
    fn utc_ticks_convert_to_protobuf_timestamp() {
        let epoch = timestamp_from_utc_ticks(UNIX_EPOCH_TICKS);
        assert_eq!(epoch.seconds, 0);
        assert_eq!(epoch.nanos, 0);

        // 2020-01-01T00:00:00.000000100Z is one tick past that second.
        let ticks = UNIX_EPOCH_TICKS + 1_577_836_800 * TICKS_PER_SECOND + 1;
        let stamp = timestamp_from_utc_ticks(ticks);
        assert_eq!(stamp.seconds, 1_577_836_800);
        assert_eq!(stamp.nanos, 100);

        // One tick before the Unix epoch keeps nanos non-negative.
        let before = timestamp_from_utc_ticks(UNIX_EPOCH_TICKS - 1);
        assert_eq!(before.seconds, -1);
        assert_eq!(before.nanos, 999_999_900);

        assert!(optional_to_time(None).is_none());
        assert!(optional_to_time(Some(0)).is_none());
        let request = BoomerangAdapter::read_stream_request("stream", Some(UNIX_EPOCH_TICKS));
        let to_time = request.to_time.expect("time limit");
        assert_eq!(to_time.seconds, 0);
        assert_eq!(to_time.nanos, 0);
        let open = BoomerangAdapter::read_by_query_request("t", None, None, 10, Some(0));
        assert!(open.to_time.is_none());
        let bounded = BoomerangAdapter::read_by_query_stream_request(
            "",
            None,
            None,
            None,
            Some(UNIX_EPOCH_TICKS + TICKS_PER_SECOND),
        );
        assert_eq!(bounded.to_time.expect("time limit").seconds, 1);
    }

    #[test]
    fn optional_fields_are_absent_instead_of_sentinels() {
        let read = ReadStreamRequest {
            stream_id: "stream".into(),
            from_version: 0,
            ..Default::default()
        };
        assert!(read.to_version.is_none());
        assert!(read.to_time.is_none());

        let real_bounds = ReadStreamRequest {
            stream_id: "stream".into(),
            from_version: 0,
            to_version: Some(-1),
            to_time: Some(Timestamp {
                seconds: 0,
                nanos: 0,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(real_bounds.to_version, Some(-1));
        assert!(real_bounds.to_time.is_some());

        let query = ReadByQueryRequest {
            limit: Some(10),
            ..Default::default()
        };
        assert!(query.from_sequence_position.is_none());
        assert!(query.to_sequence_position.is_none());
        assert!(query.tenant_id.is_none());
        assert_eq!(query.limit, Some(10));

        let zero_limit = ReadByQueryStreamRequest {
            limit: Some(0),
            from_sequence_position: Some(-1),
            ..Default::default()
        };
        assert_eq!(zero_limit.limit, Some(0));
        assert_eq!(zero_limit.from_sequence_position, Some(-1));
        assert!(zero_limit.to_sequence_position.is_none());

        let append = AppendStreamRequest {
            stream_id: "stream".into(),
            expected_version: Some(0),
            ..Default::default()
        };
        assert_eq!(append.expected_version, Some(0));
        assert!(append.metadata.is_none());

        let no_check = AppendStreamRequest {
            stream_id: "stream".into(),
            ..Default::default()
        };
        assert!(no_check.expected_version.is_none());

        let updated = proto::GetStreamsUpdatedAfterRequest {
            after_sequence_position: 4,
            ..Default::default()
        };
        assert_eq!(updated.after_sequence_position, 4);
        assert!(updated.limit.is_none());
        assert!(updated.tenant_id.is_none());
        let zero_is_real = proto::GetStreamsUpdatedAfterRequest {
            after_sequence_position: 4,
            limit: Some(0),
            ..Default::default()
        };
        assert_eq!(zero_is_real.limit, Some(0));
    }

    #[test]
    fn metadata_custom_round_trips_through_the_typed_message() {
        let written = BoomerangAdapter::event_metadata(&sample_events()).expect("custom");
        assert_eq!(
            written.custom.get("timestamp").map(String::as_str),
            Some("123456789")
        );
        assert!(written.user_id.is_none());
        assert!(written.timestamp.is_none());
        assert!(written.event_id.is_none());

        let pairs = metadata_pairs(Some(written));
        assert!(pairs
            .iter()
            .any(|(key, value)| key == "timestamp" && value == "123456789"));

        let bare = EventData {
            payload: Arc::from(vec![1u8]),
            event_type: Arc::from("t"),
            tags: Arc::from([Arc::from("s")]),
            metadata: Arc::from([]),
        };
        assert!(BoomerangAdapter::event_metadata(&[bare]).is_none());
        assert!(metadata_pairs(None).is_empty());
        assert!(metadata_pairs(Some(EventMetadata {
            ..Default::default()
        }))
        .is_empty());

        let mut custom = std::collections::HashMap::new();
        custom.insert("timestamp".to_string(), "123456789".to_string());
        custom.insert("source".to_string(), "bench".to_string());
        let read = map_read_event(SequencedEventProto {
            event: Some(SerializedEventProto {
                type_name: "type2".into(),
                payload: vec![4, 5, 6],
                codec_id: 255,
                ..Default::default()
            }),
            sequence_position: 7,
            metadata: Some(EventMetadata {
                custom,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(read.offset, 7);
        assert_eq!(read.event_type, "type2");
        assert_eq!(read.payload, vec![4, 5, 6]);
        assert_eq!(read.metadata.len(), 2);
        assert!(read
            .metadata
            .iter()
            .any(|(key, value)| key == "timestamp" && value == "123456789"));
        assert!(read
            .metadata
            .iter()
            .any(|(key, value)| key == "source" && value == "bench"));

        let missing = map_read_event(SequencedEventProto {
            ..Default::default()
        });
        assert!(missing.metadata.is_empty());
        assert!(missing.event_type.is_empty());
    }

    /// Compile check: every prost request literal fills fields the adapter does
    /// not set from `Default`. A new optional proto field must not break this.
    #[test]
    fn prost_request_literals_fill_new_fields_from_default() {
        let append_stream = AppendStreamRequest {
            stream_id: "stream".into(),
            events: vec![],
            expected_version: None,
            tags: vec!["t".into()],
            metadata: None,
            ..Default::default()
        };
        assert_eq!(append_stream.stream_id, "stream");
        assert!(append_stream.expected_version.is_none());
        assert!(append_stream.metadata.is_none());
        assert!(append_stream.request_id.is_empty());

        let append_dcb = AppendDcbRequest {
            events: vec![],
            tags: vec!["t".into()],
            condition: Some(AppendCondition {
                fail_if_events_match: Some(Query {
                    items: vec![QueryItem {
                        tags: vec![UNCONDITIONAL_TAG.into()],
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            metadata: None,
            ..Default::default()
        };
        assert!(append_dcb.tenant_id.is_none());
        assert!(append_dcb.request_id.is_empty());
        assert!(append_dcb
            .condition
            .as_ref()
            .and_then(|condition| condition.after)
            .is_none());

        let read_stream = ReadStreamRequest {
            stream_id: "stream".into(),
            from_version: 0,
            to_version: None,
            to_time: None,
            ..Default::default()
        };
        assert!(read_stream.to_version.is_none());
        assert!(read_stream.to_time.is_none());

        let read_by_query = ReadByQueryRequest {
            from_sequence_position: None,
            limit: Some(10),
            to_sequence_position: None,
            query: Some(Query {
                ..Default::default()
            }),
            to_time: None,
            ..Default::default()
        };
        assert_eq!(read_by_query.limit, Some(10));
        assert!(read_by_query.tenant_id.is_none());

        let read_by_query_stream = ReadByQueryStreamRequest {
            from_sequence_position: None,
            to_sequence_position: None,
            limit: None,
            query: Some(Query {
                items: vec![QueryItem {
                    types: vec!["e".into()],
                    tags: vec!["t".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            to_time: None,
            ..Default::default()
        };
        assert!(read_by_query_stream.limit.is_none());
        assert_eq!(
            read_by_query_stream.query.expect("query").items[0].tags,
            vec!["t"]
        );

        let subscribe = SubscribeRequest {
            subscriber_id: "esb".into(),
            from_sequence_position: 1,
            scope: Some(subscribe_request::Scope::GlobalAll(true)),
            ..Default::default()
        };
        assert_eq!(subscribe.from_sequence_position, 1);
        assert!(subscribe.tenant_id.is_none());

        let _current = GetCurrentSequenceRequest {
            ..Default::default()
        };

        let event = SerializedEventProto {
            type_name: "type".into(),
            payload: vec![1],
            codec_id: 255,
            ..Default::default()
        };
        assert_eq!(event.codec_id, 255);
        assert!(event.schema_version.is_none());

        let metadata = EventMetadata {
            custom: std::collections::HashMap::from([("timestamp".into(), "1".into())]),
            ..Default::default()
        };
        assert_eq!(
            metadata.custom.get("timestamp").map(String::as_str),
            Some("1")
        );
        assert!(metadata.correlation_id.is_none());

        // Registry calls. Same literal shape, so a new optional field stays source-compatible.
        let stream = proto::GetStreamRequest {
            stream_id: "stream".into(),
            ..Default::default()
        };
        assert!(stream.tenant_id.is_none());
        let by_type = proto::GetStreamsByAggregateTypeRequest {
            aggregate_type: "order".into(),
            ..Default::default()
        };
        assert!(by_type.tenant_id.is_none());
        let by_tag = proto::GetStreamsByTagRequest {
            tag: "t".into(),
            ..Default::default()
        };
        assert!(by_tag.tenant_id.is_none());
        let enumerated = proto::EnumerateStreamIdsRequest {
            prefix: String::new(),
            ..Default::default()
        };
        assert!(enumerated.tenant_id.is_none());
        let count = proto::GetStreamCountRequest {
            prefix: String::new(),
            ..Default::default()
        };
        assert!(count.tenant_id.is_none());
        let updated = proto::GetStreamsUpdatedAfterRequest {
            after_sequence_position: 4,
            limit: None,
            ..Default::default()
        };
        assert_eq!(updated.after_sequence_position, 4);
        assert!(updated.limit.is_none());
    }

    fn status_with_reason(
        code: tonic::Code,
        reason: &str,
        metadata: &[(&str, &str)],
    ) -> tonic::Status {
        let metadata: std::collections::HashMap<String, String> = metadata
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        tonic::Status::with_error_details(
            code,
            reason,
            tonic_types::ErrorDetails::with_error_info(reason, SLINGSHOT_ERROR_DOMAIN, metadata),
        )
    }

    #[test]
    fn not_leader_reason_fails_without_an_in_band_field() {
        let status = status_with_reason(
            tonic::Code::Unavailable,
            REASON_NOT_LEADER,
            &[("leader_endpoint", "http://10.0.0.8:50051")],
        );
        let failure = slingshot_failure(&status).expect("NOT_LEADER");
        assert_eq!(
            failure,
            SlingshotFailure::NotLeader {
                leader_endpoint: "http://10.0.0.8:50051".into(),
            }
        );
        assert!(!failure.retryable());
        assert!(!grpc_status_retryable(&status));

        let err = grpc_status_error(status, "AppendStream");
        let message = err.to_string();
        assert!(
            message.contains("reached a follower")
                && message.contains("http://10.0.0.8:50051")
                && message.contains("Point BOOMERANG_URI at the Raft leader"),
            "{message}"
        );

        // The append response no longer carries a follower flag. The status
        // reason above is the whole signal.
        let response = proto::AppendStreamResponse {
            sequence_positions: vec![4],
            ..Default::default()
        };
        assert_eq!(response.sequence_positions, vec![4]);
        assert!(response.consistency_marker.is_none());
        assert!(response.last_stream_version.is_none());
        let dcb = proto::AppendDcbResponse {
            ..Default::default()
        };
        assert!(dcb.sequence_positions.is_empty());
        assert!(dcb.consistency_marker.is_none());
        assert!(dcb.last_stream_version.is_none());
    }

    #[test]
    fn unknown_outcome_and_conflicts_are_not_retried() {
        let unknown = status_with_reason(
            tonic::Code::FailedPrecondition,
            REASON_APPEND_OUTCOME_UNKNOWN,
            &[],
        );
        assert_eq!(
            slingshot_failure(&unknown),
            Some(SlingshotFailure::AppendOutcomeUnknown)
        );
        assert!(!grpc_status_retryable(&unknown));
        let unknown_err = grpc_status_error(unknown, "AppendDcb");
        assert!(
            unknown_err.to_string().contains("APPEND_OUTCOME_UNKNOWN")
                && unknown_err.to_string().contains("not retrying"),
            "{unknown_err}"
        );

        for reason in [
            "WRONG_EXPECTED_VERSION",
            "APPEND_CONDITION_FAILED",
            "SOME_OTHER_ABORT",
        ] {
            let status = status_with_reason(tonic::Code::Aborted, reason, &[]);
            assert_eq!(
                slingshot_failure(&status),
                Some(SlingshotFailure::Conflict {
                    reason: reason.into(),
                })
            );
            assert!(!slingshot_failure(&status).unwrap().retryable());
            assert!(!grpc_status_retryable(&status));
            let err = grpc_status_error(status, "AppendStream");
            assert!(
                err.to_string().contains(reason) && err.to_string().contains("not retrying"),
                "{err}"
            );
        }

        let aborted = tonic::Status::new(tonic::Code::Aborted, "conflict");
        assert_eq!(
            slingshot_failure(&aborted),
            Some(SlingshotFailure::Conflict {
                reason: String::new(),
            })
        );
        assert!(!grpc_status_retryable(&aborted));

        let precondition = tonic::Status::new(tonic::Code::FailedPrecondition, "unknown");
        assert!(slingshot_failure(&precondition).is_none());
        assert!(!grpc_status_retryable(&precondition));
    }

    #[test]
    fn unrelated_status_is_not_a_slingshot_failure() {
        let unavailable = tonic::Status::unavailable("upstream reset");
        assert!(slingshot_failure(&unavailable).is_none());
        assert!(grpc_status_retryable(&unavailable));
        let err = grpc_status_error(unavailable, "ReadByQuery");
        assert!(err.to_string().contains("upstream reset"));

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("leader_endpoint".into(), "http://other:1".into());
        let foreign = tonic::Status::with_error_details(
            tonic::Code::Unavailable,
            "NOT_LEADER",
            tonic_types::ErrorDetails::with_error_info("NOT_LEADER", "other.domain", metadata),
        );
        assert!(slingshot_failure(&foreign).is_none());
        assert!(grpc_status_retryable(&foreign));
    }

    fn context_header(request: &tonic::Request<()>) -> &str {
        request
            .metadata()
            .get(SLINGSHOT_CONTEXT_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("x-slingshot-context")
    }

    #[test]
    fn slingshot_context_header_present_on_each_call() {
        let interceptor = resolve_slingshot_context(Some("tenant-a")).unwrap();
        // Clone stands in for `client.clone()` on unary, stream, and subscribe.
        let mut calls = [interceptor.clone(), interceptor];
        for interceptor in &mut calls {
            let mut request = tonic::Request::new(());
            request
                .metadata_mut()
                .insert("x-request-id", "req".parse().unwrap());
            let request = interceptor.call(request).unwrap();
            assert_eq!(context_header(&request), "tenant-a");
            assert_eq!(
                request
                    .metadata()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok()),
                Some("req")
            );
        }
    }

    #[test]
    fn slingshot_context_defaults_when_unset() {
        for interceptor in [
            resolve_slingshot_context(None).unwrap(),
            slingshot_context_from_var(Err(std::env::VarError::NotPresent)).unwrap(),
        ] {
            let request = interceptor.clone().call(tonic::Request::new(())).unwrap();
            assert_eq!(context_header(&request), DEFAULT_SLINGSHOT_CONTEXT);
        }
    }

    #[test]
    fn slingshot_context_value_override() {
        for interceptor in [
            resolve_slingshot_context(Some("bench-tenant")).unwrap(),
            slingshot_context_from_var(Ok("bench-tenant".to_string())).unwrap(),
        ] {
            let mut interceptor = interceptor;
            let request = interceptor.call(tonic::Request::new(())).unwrap();
            assert_eq!(context_header(&request), "bench-tenant");
        }
    }

    #[test]
    fn slingshot_context_invalid_value_rejected() {
        for value in [
            "",
            "bad\nvalue",
            "bad\rvalue",
            "bad\tvalue",
            "has\u{7f}del",
            "café",
        ] {
            let err = resolve_slingshot_context(Some(value)).expect_err(value);
            let message = err.to_string();
            assert!(
                message.contains("SLINGSHOT_CONTEXT")
                    && message.contains("not a legal ASCII header value"),
                "{value:?} => {message}"
            );
        }

        assert!(resolve_slingshot_context(Some("default")).is_ok());
        assert!(resolve_slingshot_context(Some("A Z_1.")).is_ok());
    }

    #[test]
    fn slingshot_context_rejects_leading_or_trailing_whitespace() {
        for value in [
            " default",
            "default ",
            " default ",
            " ",
            "\tdefault",
            "default\n",
        ] {
            let err = resolve_slingshot_context(Some(value)).expect_err(value);
            let message = err.to_string();
            assert!(
                message.contains("SLINGSHOT_CONTEXT")
                    && message.contains("leading or trailing whitespace"),
                "{value:?} => {message}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn slingshot_context_non_unicode_rejected() {
        use std::os::unix::ffi::OsStringExt;

        let err = slingshot_context_from_var(Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::from_vec(vec![0xff]),
        )))
        .expect_err("non-unicode");
        let message = err.to_string();
        assert!(
            message.contains("SLINGSHOT_CONTEXT")
                && message.contains("not a legal ASCII header value")
                && message.contains("not valid Unicode"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn startup_rejects_invalid_context_before_dial() {
        // The error text is the header check, and the call finishes within 2s.
        // That does not prove the address would have blocked.
        let dial = tokio::time::timeout(
            Duration::from_secs(2),
            connect_client_with("http://192.0.2.1:81", Some("nope\n")),
        )
        .await;
        let err = dial
            .expect("invalid context returned within 2s")
            .expect_err("invalid context");
        assert!(
            err.to_string().contains("not a legal ASCII header value"),
            "{err}"
        );
    }

    #[derive(Clone)]
    struct RecordingStore {
        calls: std::sync::Arc<std::sync::Mutex<Vec<RecordedCall>>>,
    }

    struct RecordedCall {
        rpc: &'static str,
        context: Option<String>,
    }

    impl RecordingStore {
        fn record(&self, rpc: &'static str, metadata: &tonic::metadata::MetadataMap) {
            let context = metadata
                .get(SLINGSHOT_CONTEXT_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            self.calls
                .lock()
                .expect("recording")
                .push(RecordedCall { rpc, context });
        }
    }

    struct EmptyStream<T> {
        _item: std::marker::PhantomData<T>,
    }

    impl<T> EmptyStream<T> {
        fn new() -> Self {
            Self {
                _item: std::marker::PhantomData,
            }
        }
    }

    impl<T> tonic::codegen::tokio_stream::Stream for EmptyStream<T> {
        type Item = std::result::Result<T, tonic::Status>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Ready(None)
        }
    }

    #[async_trait::async_trait]
    impl proto::event_store_service_server::EventStoreService for RecordingStore {
        async fn append_stream(
            &self,
            _request: tonic::Request<AppendStreamRequest>,
        ) -> std::result::Result<tonic::Response<proto::AppendStreamResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn append_dcb(
            &self,
            _request: tonic::Request<AppendDcbRequest>,
        ) -> std::result::Result<tonic::Response<proto::AppendDcbResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn read_stream(
            &self,
            _request: tonic::Request<ReadStreamRequest>,
        ) -> std::result::Result<tonic::Response<proto::ReadStreamResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        type ReadStreamEnumerableStream = EmptyStream<proto::ReadStreamEnumerableResponse>;

        async fn read_stream_enumerable(
            &self,
            _request: tonic::Request<proto::ReadStreamEnumerableRequest>,
        ) -> std::result::Result<tonic::Response<Self::ReadStreamEnumerableStream>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn read_by_query(
            &self,
            _request: tonic::Request<ReadByQueryRequest>,
        ) -> std::result::Result<tonic::Response<proto::ReadByQueryResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        type ReadByQueryStreamStream = EmptyStream<proto::ReadByQueryStreamResponse>;

        async fn read_by_query_stream(
            &self,
            request: tonic::Request<ReadByQueryStreamRequest>,
        ) -> std::result::Result<tonic::Response<Self::ReadByQueryStreamStream>, tonic::Status>
        {
            self.record("ReadByQueryStream", request.metadata());
            Ok(tonic::Response::new(EmptyStream::new()))
        }

        type SubscribeStream = EmptyStream<proto::SubscribeResponse>;

        async fn subscribe(
            &self,
            _request: tonic::Request<SubscribeRequest>,
        ) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn get_current_sequence(
            &self,
            request: tonic::Request<GetCurrentSequenceRequest>,
        ) -> std::result::Result<tonic::Response<proto::GetCurrentSequenceResponse>, tonic::Status>
        {
            self.record("GetCurrentSequence", request.metadata());
            Ok(tonic::Response::new(proto::GetCurrentSequenceResponse {
                current_sequence: 0,
                ..Default::default()
            }))
        }

        async fn get_stream(
            &self,
            _request: tonic::Request<proto::GetStreamRequest>,
        ) -> std::result::Result<tonic::Response<proto::GetStreamResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn get_streams_by_aggregate_type(
            &self,
            _request: tonic::Request<proto::GetStreamsByAggregateTypeRequest>,
        ) -> std::result::Result<
            tonic::Response<proto::GetStreamsByAggregateTypeResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn get_streams_by_tag(
            &self,
            _request: tonic::Request<proto::GetStreamsByTagRequest>,
        ) -> std::result::Result<tonic::Response<proto::GetStreamsByTagResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn enumerate_stream_ids(
            &self,
            _request: tonic::Request<proto::EnumerateStreamIdsRequest>,
        ) -> std::result::Result<tonic::Response<proto::EnumerateStreamIdsResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn get_stream_count(
            &self,
            _request: tonic::Request<proto::GetStreamCountRequest>,
        ) -> std::result::Result<tonic::Response<proto::GetStreamCountResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("stub"))
        }

        async fn get_streams_updated_after(
            &self,
            _request: tonic::Request<proto::GetStreamsUpdatedAfterRequest>,
        ) -> std::result::Result<
            tonic::Response<proto::GetStreamsUpdatedAfterResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("stub"))
        }
    }

    #[tokio::test]
    async fn slingshot_context_header_reaches_unary_and_streaming_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = RecordingStore {
            calls: std::sync::Arc::clone(&calls),
        };
        let incoming = tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(proto::event_store_service_server::EventStoreServiceServer::new(store))
                .serve_with_incoming(incoming)
                .await;
        });

        let mut interceptor = slingshot_context_from_env().expect("SLINGSHOT_CONTEXT");
        let expected_request = interceptor.call(tonic::Request::new(())).unwrap();
        let expected = context_header(&expected_request).to_string();

        let mut client: BoomerangClient = connect_client(&format!("http://{addr}")).await.unwrap();
        client
            .get_current_sequence(GetCurrentSequenceRequest {
                ..Default::default()
            })
            .await
            .unwrap();
        let mut stream = client
            .read_by_query_stream(ReadByQueryStreamRequest {
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().is_none());

        let calls = calls.lock().expect("recording");
        for rpc in ["GetCurrentSequence", "ReadByQueryStream"] {
            let call = calls
                .iter()
                .find(|call| call.rpc == rpc)
                .unwrap_or_else(|| {
                    panic!(
                        "{rpc} was not recorded; saw {:?}",
                        calls.iter().map(|call| call.rpc).collect::<Vec<_>>()
                    )
                });
            assert_eq!(call.context.as_deref(), Some(expected.as_str()), "{rpc}");
        }
    }
}
