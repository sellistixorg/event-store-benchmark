
use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EsbAppendCondition, EsbQueryItem, EventData, EventStoreAdapter, ReadEvent, ReadRequest,
    ReadResponse, StoreDataDir, StoreManager, StoreManagerFactory, VecReadResponse,
};
use bench_core::wait_for_ready;
use bench_testcontainers::boomerang::{Boomerang, BOOMERANG_PORT};
use proto::event_store_service_client::EventStoreServiceClient;
use proto::{
    subscribe_request, AppendDcbRequest, AppendStreamRequest, GetCurrentSequenceRequest,
    QueryItemProto, QueryProto, ReadStreamRequest,
    ReadByQueryRequest, ReadByQueryStreamRequest, SequencedEventProto, SerializedEventProto,
    SubscribeRequest,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tonic::transport::{Channel, Endpoint};

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

fn stream_rpc_limit(limit: Option<u64>) -> i32 {
    match limit {
        Some(n) if n > 0 => i32::try_from(n).unwrap_or(i32::MAX),
        _ => 0,
    }
}

fn query_proto(tag: &str, event_type: Option<&str>) -> Option<QueryProto> {
    let has_type = event_type.is_some();
    let has_tag = !tag.is_empty();
    if !has_type && !has_tag {
        return None;
    }
    Some(QueryProto {
        items: vec![QueryItemProto {
            types: event_type
                .map(|t| vec![t.to_string()])
                .unwrap_or_default(),
            tags: if has_tag {
                vec![tag.to_string()]
            } else {
                Vec::new()
            },
        }],
    })
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
        if self.use_docker {
            let mount_path = self.data_dir.setup()?;
            #[cfg_attr(not(unix), allow(unused_variables))]
            let is_bind_mount = mount_path.is_some();
            let mut image: ContainerRequest<_> =
                Boomerang::new(mount_path, self.durability).into();

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
                            .get_current_sequence(GetCurrentSequenceRequest {})
                            .await?;
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

async fn connect_client(uri: &str) -> Result<EventStoreServiceClient<Channel>> {
    let endpoint = Endpoint::from_shared(uri.to_string())?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_timeout(Duration::from_secs(10))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024));
    let channel = endpoint.connect().await?;
    Ok(EventStoreServiceClient::new(channel))
}

pub struct BoomerangAdapter {
    client: EventStoreServiceClient<Channel>,
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
                codec_id: 255, // opaque / unspecified bytes — raw sittings are not MemoryPack
                schema_version: 0,
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

    fn metadata_json(events: &[EventData]) -> Vec<u8> {
        let mut custom = serde_json::Map::new();
        for evt in events {
            for (k, v) in evt.metadata.iter() {
                custom.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
        }
        if custom.is_empty() {
            return Vec::new();
        }
        serde_json::to_vec(&serde_json::json!({ "custom": custom })).unwrap_or_default()
    }

    fn condition_json(condition: Option<EsbAppendCondition>) -> Vec<u8> {
        match condition {
            None => serde_json::to_vec(&serde_json::json!({
                "FailIfEventsMatch": {
                    "Items": [{ "Types": [], "Tags": [UNCONDITIONAL_TAG] }]
                }
            }))
            .expect("unconditional condition json"),
            Some(cond) => {
                let items: Vec<serde_json::Value> = cond
                    .fail_if_events_match
                    .items
                    .into_iter()
                    .map(|item: EsbQueryItem| query_item_json(&item.types, &item.tags))
                    .collect();
                let mut body = serde_json::json!({
                    "FailIfEventsMatch": { "Items": items }
                });
                if let Some(after) = cond.after {
                    body["After"] = serde_json::json!(after);
                }
                serde_json::to_vec(&body).expect("condition json")
            }
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
        let request = AppendDcbRequest {
            events: Self::convert_events(events),
            condition_json: Self::condition_json(condition),
            metadata_json: Self::metadata_json(events),
            tags: Self::batch_tags(events),
        };
        let mut client = self.client.clone();
        let response = client.append_dcb(request).await?.into_inner();
        refuse_follower(response.not_leader, &response.leader_endpoint, "AppendDcb")?;
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

    fn expected_version(stream_position: Option<usize>) -> i64 {
        stream_position.map(|p| p as i64).unwrap_or(-1)
    }

    async fn append_stream_batch(
        &self,
        events: &[EventData],
        stream_position: Option<usize>,
    ) -> anyhow::Result<Option<u64>> {
        let request = AppendStreamRequest {
            stream_id: Self::stream_id_from_events(events)?,
            events: Self::convert_events(events),
            expected_version: Self::expected_version(stream_position),
            metadata_json: Self::metadata_json(events),
            tags: Self::batch_tags(events),
        };
        let mut client = self.client.clone();
        let response = client.append_stream(request).await?.into_inner();
        refuse_follower(
            response.not_leader,
            &response.leader_endpoint,
            "AppendStream",
        )?;
        Ok(Self::last_position(&response.sequence_positions))
    }

    #[allow(dead_code)] // kept for tests; the hot path reads by query
    async fn read_stream_by_id(
        &self,
        stream_id: &str,
    ) -> anyhow::Result<Vec<SequencedEventProto>> {
        let request = ReadStreamRequest {
            stream_id: stream_id.to_string(),
            from_version: 0,
            to_version: -1,
            to_timestamp: 0,
        };
        let mut client = self.client.clone();
        Ok(client.read_stream(request).await?.into_inner().events)
    }

    #[allow(dead_code)] // kept for tests; hot path sends empty query_json once proto is set
    fn query_json(tag: &str, event_type: Option<&str>) -> Vec<u8> {
        let has_type = event_type.is_some();
        let has_tag = !tag.is_empty();
        if !has_type && !has_tag {
            return Vec::new();
        }
        let types: Vec<String> = event_type
            .map(|t| vec![t.to_string()])
            .unwrap_or_default();
        let tags = if has_tag {
            vec![tag.to_string()]
        } else {
            Vec::new()
        };
        serde_json::to_vec(&serde_json::json!({
            "Items": [query_item_json(&types, &tags)]
        }))
        .unwrap_or_default()
    }

    /// Harness `from_offset` is exclusive; Boomerang `from_sequence_position` is inclusive.
    fn inclusive_from(from_offset: Option<u64>) -> i64 {
        match from_offset {
            None => -1,
            Some(after) => after.saturating_add(1) as i64,
        }
    }
}

/// Cluster followers return `not_leader` instead of committing. A smoke run pointed at
/// the wrong node must fail here; an empty sequence list would otherwise look like success.
fn refuse_follower(not_leader: bool, leader_endpoint: &str, rpc: &str) -> Result<()> {
    if not_leader {
        anyhow::bail!(
            "{rpc} reached a follower (leader_endpoint={leader_endpoint}). Point BOOMERANG_URI at the Raft leader."
        );
    }
    Ok(())
}

/// PascalCase QueryItem JSON. Omit empty Types/Tags so Boomerang treats them as
/// unrestricted (`null`) rather than an empty allow-list that matches nothing.
fn query_item_json(types: &[String], tags: &[String]) -> serde_json::Value {
    let mut item = serde_json::Map::new();
    if !types.is_empty() {
        item.insert(
            "Types".to_string(),
            serde_json::Value::Array(
                types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            ),
        );
    }
    if !tags.is_empty() {
        item.insert(
            "Tags".to_string(),
            serde_json::Value::Array(
                tags.iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            ),
        );
    }
    serde_json::Value::Object(item)
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
        let query = query_proto(&req.tag, req.event_type.as_deref());
        // Proto is the hot path (R3). JSON dual-write was a pre-Gate-A shim for images
        // that ignored field 6; those images are gone. Empty query_json skips the
        // PascalCase JSON parse on every unary/stream read.
        let from_sequence_position = Self::inclusive_from(req.from_offset);

        if use_unary_read(req.limit) {
            let request = ReadByQueryRequest {
                query_json: Vec::new(),
                from_sequence_position,
                limit: req.limit.expect("use_unary_read") as i32,
                to_sequence_position: -1,
                to_timestamp: 0,
                query,
            };
            let mut client = self.client.clone();
            let response = client.read_by_query(request).await?.into_inner();
            let events = response.events.into_iter().map(map_read_event).collect();
            return Ok(Box::new(VecReadResponse::new(events)));
        }

        let request = ReadByQueryStreamRequest {
            query_json: Vec::new(),
            from_sequence_position,
            to_sequence_position: -1,
            to_timestamp: 0,
            limit: stream_rpc_limit(req.limit),
            query,
        };
        let mut client = self.client.clone();
        let stream = client.read_by_query_stream(request).await?.into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream,
            remaining: req.limit,
        }))
    }

    async fn subscribe(&self, after: Option<u64>) -> anyhow::Result<Box<dyn ReadResponse>> {
        let from = after.map(|a| a.saturating_add(1) as i64).unwrap_or(0);
        let request = SubscribeRequest {
            subscriber_id: format!("esb-{}", uuid::Uuid::new_v4()),
            from_sequence_position: from,
            scope: Some(subscribe_request::Scope::GlobalAll(true)),
        };
        let mut client = self.client.clone();
        let stream = client.subscribe(request).await?.into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream,
            remaining: None,
        }))
    }

    async fn read_all(&self) -> Result<Box<dyn ReadResponse>> {
        let request = ReadByQueryStreamRequest {
            query_json: Vec::new(),
            from_sequence_position: -1,
            to_sequence_position: -1,
            to_timestamp: 0,
            limit: 0,
            query: None,
        };
        let mut client = self.client.clone();
        let stream = client.read_by_query_stream(request).await?.into_inner();
        Ok(Box::new(BoomerangReadResponse {
            stream,
            remaining: None,
        }))
    }
}

struct BoomerangReadResponse {
    stream: tonic::Streaming<SequencedEventProto>,
    remaining: Option<u64>,
}

#[async_trait]
impl ReadResponse for BoomerangReadResponse {
    async fn next_event(&mut self) -> anyhow::Result<Option<ReadEvent>> {
        if matches!(self.remaining, Some(0)) {
            while self.stream.message().await?.is_some() {}
            return Ok(None);
        }
        match self.stream.message().await? {
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
        metadata: metadata_from_json(&proto.metadata_json),
    }
}

fn metadata_from_json(bytes: &[u8]) -> Vec<(String, String)> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Vec::new();
    };
    let Some(obj) = value.as_object() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(custom) = obj.get("custom").and_then(|c| c.as_object()) {
        for (k, v) in custom {
            if let Some(s) = v.as_str() {
                out.push((k.clone(), s.to_string()));
            }
        }
    }
    for (k, v) in obj {
        if k == "custom" {
            continue;
        }
        if let Some(s) = v.as_str() {
            out.push((k.clone(), s.to_string()));
        }
    }
    out
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
                metadata: Arc::from([(
                    "timestamp".to_string(),
                    "123456789".to_string(),
                )]),
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
    fn query_proto_hot_path_omits_all() {
        assert!(query_proto("", None).is_none());
        let q = query_proto("t", Some("e")).expect("tagged typed query");
        assert_eq!(q.items[0].tags, vec!["t"]);
        assert_eq!(q.items[0].types, vec!["e"]);
    }

    #[test]
    fn query_json_emits_pascal_case_items() {
        let json = BoomerangAdapter::query_json("t", Some("e"));
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["Items"][0]["Tags"][0], "t");
        assert_eq!(v["Items"][0]["Types"][0], "e");
    }

    /// D2 magnitude: harness-side `one_tag_one_type` + `after = 0` build + serialize.
    /// Timed loop (not a keep signal). Print mean ns; the plan records it.
    #[test]
    fn d2_condition_json_h2h_cost() {
        let tag = "stream-0-";
        let event_type = "test-0";
        let iterations = 200_000u32;

        let sample = h2h_condition_json(tag, event_type);
        let v: serde_json::Value = serde_json::from_slice(&sample).unwrap();
        assert_eq!(v["FailIfEventsMatch"]["Items"][0]["Tags"][0], tag);
        assert_eq!(v["FailIfEventsMatch"]["Items"][0]["Types"][0], event_type);
        assert_eq!(v["After"], 0);

        for _ in 0..20_000 {
            let _ = h2h_condition_json(tag, event_type);
        }

        let started = std::time::Instant::now();
        let mut bytes = 0usize;
        for _ in 0..iterations {
            bytes = h2h_condition_json(tag, event_type).len();
        }
        let elapsed = started.elapsed();
        let mean_ns = elapsed.as_nanos() as f64 / f64::from(iterations);
        println!(
            "D2 harness condition_json H2H ({iterations} iters): {mean_ns:.1} ns ({:.3} µs) bytes={bytes}",
            mean_ns / 1000.0
        );
        assert!(mean_ns > 0.0);
        assert!(bytes > 0);
    }

    fn h2h_condition_json(tag: &str, event_type: &str) -> Vec<u8> {
        let condition = Some(EsbAppendCondition::new(
            bench_core::adapter::EsbQuery::new().item(
                EsbQueryItem::new()
                    .tags(vec![tag.to_string()])
                    .types(vec![event_type.to_string()]),
            ),
        ).after(Some(0)));
        BoomerangAdapter::condition_json(condition)
    }

    #[test]
    fn stream_rpc_limit_zero_means_unbounded() {
        assert_eq!(stream_rpc_limit(None), 0);
        assert_eq!(stream_rpc_limit(Some(0)), 0);
        assert_eq!(stream_rpc_limit(Some(257)), 257);
    }

    #[test]
    fn append_to_stream_uses_first_tag_as_stream_id() {
        let events = sample_events();
        assert_eq!(
            BoomerangAdapter::stream_id_from_events(&events).unwrap(),
            "tag1"
        );
        assert_eq!(BoomerangAdapter::expected_version(None), -1);
        assert_eq!(BoomerangAdapter::expected_version(Some(0)), 0);
        assert_eq!(BoomerangAdapter::expected_version(Some(9)), 9);
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
            all_received.iter().filter(|e| e.event_type == "limited").count() >= 5,
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
        assert!(last_pos.is_some(), "append_to_stream must return a global position");

        let events = concrete.read_stream_by_id(stream_id).await?;
        assert_eq!(events.len(), 10, "ten events must share one Boomerang stream");
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
        adapter
            .append_to_stream(&[evt.clone()], None, None)
            .await?;
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
}


