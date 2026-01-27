mod common;

use bytes::Bytes;
use common::{create_tls_node, wait_for_active_peers, wait_for_condition};
use kameo_remote::handshake::{Feature, PeerCapabilities, CURRENT_PROTOCOL_VERSION};
use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use kameo_remote::{
    framing, GossipConfig, GossipError, GossipRegistryHandle, MessageType, SecretKey,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::Duration;

const STREAMING_ACTOR_ID: u64 = 7_777_777;
const STREAMING_TYPE_HASH: u32 = 0xBA5E_CAFE;
const LEGACY_TELL_TYPE_HASH: u32 = 0xDEC0_D1A6;
const SAMPLE_PAYLOAD: &str = r#"{"event_type":"options_indicators","symbol":"BTC","timestamp":1769195164937,"data":{"atm_iv":"18.83","calc_duration_ms":0,"call_wing_iv":"18.963475202851292940016683097","current_iv":"37.52","fear_index":29,"fear_level":"GREED","funding_rate":"0.00007663","gex":"-7981130.3462000","gex_regime":"SHORT_GAMMA","gex_resistance_levels":["100000","91000","92000","96000","95000"],"gex_support_levels":["91000","85000","88000","80000","75000"],"implied_volatility":"37.52","iv_high_52w":"64.87999999999999545252649111","iv_low_52w":"32.86999999999999744204615125","iv_percentile":"14.520547945205479452054794520","iv_rank":"14.526710402999071686865380160","iv_rank_level":"LOW","max_pain":{"2026-01-24":"90000","2026-01-25":"90000","2026-01-26":"89000","2026-01-27":"90000","2026-01-30":"90000","2026-02-13":"90000","2026-02-27":"90000","2026-03-27":"95000","2026-06-26":"95000","2026-09-25":"95000","2026-12-25":"90000"},"max_pain_distance_percent":"-0.2721351813423791677798327900","max_pain_primary":"90000","max_pain_primary_expiry":"2026-01-30","max_pain_primary_oi":"96536.6","options_count":539,"pcr_signal":"BULLISH","put_call_oi_ratio":"0.7253703386251792635456343316","put_call_volume_ratio":"0.5128577475719022646567739811","put_wing_iv":"21.521549194585192467669968993","realized_volatility":"37.26724061251267272609766222","skew_10d":"8.79","skew_25d":"2.558073991733899527653285896","skew_regime":"NORMAL","spot_price":"90265.9","timestamp":"2026-01-23T19:06:04.936499074Z","total_call_oi":"191327.2","total_charm":"-91078.88852259515","total_put_oi":"138791.1","total_vanna":"170761.40611891158","underlying":"BTC","vrp":"0.25275938748732727390233778","vrp_signal":"NEUTRAL","vrp_z_score":null}}"#;

#[derive(Clone, Default)]
struct OptionsIndicatorHandler {
    tell_payload: Arc<Mutex<Option<Bytes>>>,
    ask_payload: Arc<Mutex<Option<Bytes>>>,
    delivery_counts: Arc<Mutex<HashMap<u32, usize>>>,
}

impl OptionsIndicatorHandler {
    fn new() -> Self {
        Self::default()
    }

    async fn last_tell(&self) -> Option<Bytes> {
        self.tell_payload.lock().await.clone()
    }

    async fn last_ask(&self) -> Option<Bytes> {
        self.ask_payload.lock().await.clone()
    }

    async fn count_for(&self, type_hash: u32) -> usize {
        let counts = self.delivery_counts.lock().await;
        counts.get(&type_hash).copied().unwrap_or(0)
    }
}

impl ActorMessageHandler for OptionsIndicatorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: &str,
        type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let payload_bytes = Bytes::copy_from_slice(payload);
        let tells = self.tell_payload.clone();
        let asks = self.ask_payload.clone();
        let counts = self.delivery_counts.clone();
        Box::pin(async move {
            {
                let mut map = counts.lock().await;
                *map.entry(type_hash).or_insert(0) += 1;
            }
            if correlation_id.is_some() {
                asks.lock().await.replace(payload_bytes.clone());
                let ack = Bytes::from(format!("ack:{}", payload_bytes.len()).into_bytes());
                Ok(Some(ack))
            } else {
                tells.lock().await.replace(payload_bytes);
                Ok(None)
            }
        })
    }
}

#[tokio::test]
async fn streaming_tell_and_ask_with_sample_payload() {
    let config = GossipConfig::default();
    let node_a = create_tls_node(config.clone()).await.unwrap();
    let node_b = create_tls_node(config).await.unwrap();

    let handler = Arc::new(OptionsIndicatorHandler::new());
    node_b
        .registry
        .set_actor_message_handler(handler.clone())
        .await;

    // Wire the peers together over TLS.
    let addr_b = node_b.registry.bind_addr;
    let peer_b_id = node_b.registry.peer_id.clone();
    let peer_b = node_a.add_peer(&peer_b_id).await;
    peer_b.connect(&addr_b).await.unwrap();

    assert!(
        wait_for_active_peers(&node_a, 1, Duration::from_secs(5)).await,
        "peers should establish TLS connection"
    );
    let conn = node_a
        .get_connection_to_peer(&peer_b_id)
        .await
        .expect("connection to peer");
    let mut features = HashSet::new();
    features.insert(Feature::Streaming);
    node_a.registry.peer_capabilities.insert(
        conn.addr,
        PeerCapabilities {
            version: CURRENT_PROTOCOL_VERSION,
            features,
        },
    );
    let sample_bytes = Bytes::from(SAMPLE_PAYLOAD.as_bytes().to_vec());

    // 1) Streaming tell (fire-and-forget) with the sample payload.
    stream_tell_with_retry(
        &conn,
        sample_bytes.clone(),
        STREAMING_TYPE_HASH,
        STREAMING_ACTOR_ID,
    )
    .await
    .expect("stream tell");

    assert!(
        wait_for_condition(Duration::from_secs(5), || {
            let handler = handler.clone();
            async move { handler.last_tell().await.is_some() }
        })
        .await,
        "receiver should observe streaming tell payload"
    );
    let recorded_tell = handler.last_tell().await.unwrap();
    assert_eq!(
        recorded_tell, sample_bytes,
        "tell payload must match sample"
    );

    // 2) Streaming ask (request/response) with the same payload.
    let reply = stream_ask_with_retry(
        &conn,
        sample_bytes.clone(),
        STREAMING_TYPE_HASH,
        STREAMING_ACTOR_ID,
        Duration::from_secs(5),
    )
    .await
    .expect("streaming ask reply");
    assert_eq!(
        reply.as_ref(),
        format!("ack:{}", sample_bytes.len()).as_bytes()
    );
    let recorded_ask = handler.last_ask().await.unwrap();
    assert_eq!(recorded_ask, sample_bytes, "ask payload must match sample");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn streaming_vs_tell_throughput_sample_payload_1000() {
    const ITERATIONS: usize = 1_000;

    let config = GossipConfig::default();
    let node_a = create_tls_node(config.clone()).await.unwrap();
    let node_b = create_tls_node(config).await.unwrap();

    let handler = Arc::new(OptionsIndicatorHandler::new());
    node_b
        .registry
        .set_actor_message_handler(handler.clone())
        .await;

    let addr_b = node_b.registry.bind_addr;
    let peer_b_id = node_b.registry.peer_id.clone();
    let peer_b = node_a.add_peer(&peer_b_id).await;
    peer_b.connect(&addr_b).await.unwrap();

    assert!(
        wait_for_active_peers(&node_a, 1, Duration::from_secs(5)).await,
        "TLS peers should connect"
    );
    let conn = node_a
        .get_connection_to_peer(&peer_b_id)
        .await
        .expect("connection to peer");

    // Force-enable streaming capability for the connection under test.
    let mut features = HashSet::new();
    features.insert(Feature::Streaming);
    node_a.registry.peer_capabilities.insert(
        conn.addr,
        PeerCapabilities {
            version: CURRENT_PROTOCOL_VERSION,
            features,
        },
    );

    let sample_bytes = Bytes::from(SAMPLE_PAYLOAD.as_bytes().to_vec());

    // Measure streaming send duration.
    let initial_stream_count = handler.count_for(STREAMING_TYPE_HASH).await;
    let streaming_start = Instant::now();
    for _ in 0..ITERATIONS {
        conn.stream_large_message(
            sample_bytes.clone(),
            STREAMING_TYPE_HASH,
            STREAMING_ACTOR_ID,
        )
        .await
        .expect("stream payload");
    }
    let streaming_elapsed = streaming_start.elapsed();
    assert!(
        wait_for_condition(Duration::from_secs(20), || {
            let handler = handler.clone();
            async move {
                handler.count_for(STREAMING_TYPE_HASH).await >= initial_stream_count + ITERATIONS
            }
        })
        .await,
        "all streaming payloads should arrive"
    );

    // Measure legacy tell duration using ActorTell envelopes.
    let tell_frame = build_actor_tell_frame(
        STREAMING_ACTOR_ID,
        LEGACY_TELL_TYPE_HASH,
        sample_bytes.as_ref(),
    );
    let initial_tell_count = handler.count_for(LEGACY_TELL_TYPE_HASH).await;
    let tell_start = Instant::now();
    for _ in 0..ITERATIONS {
        conn.send_binary_message(tell_frame.clone())
            .await
            .expect("send non-streaming tell");
    }
    let tell_elapsed = tell_start.elapsed();
    assert!(
        wait_for_condition(Duration::from_secs(20), || {
            let handler = handler.clone();
            async move {
                handler.count_for(LEGACY_TELL_TYPE_HASH).await >= initial_tell_count + ITERATIONS
            }
        })
        .await,
        "all non-streaming tells should arrive"
    );

    println!(
        "STREAMING: {} msgs -> {:?} total ({:.2} µs/msg)",
        ITERATIONS,
        streaming_elapsed,
        streaming_elapsed.as_secs_f64() * 1_000_000.0 / ITERATIONS as f64
    );
    println!(
        "TELL: {} msgs -> {:?} total ({:.2} µs/msg)",
        ITERATIONS,
        tell_elapsed,
        tell_elapsed.as_secs_f64() * 1_000_000.0 / ITERATIONS as f64
    );
    println!(
        "Streaming vs Tell speedup: {:.2}x",
        tell_elapsed.as_secs_f64() / streaming_elapsed.as_secs_f64()
    );

    // Ensure the last payload observed in each path matches the JSON sample.
    assert_eq!(handler.last_tell().await.unwrap(), sample_bytes);

    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn streaming_recovers_after_remote_reconnect() {
    let secret_key_a = SecretKey::generate();
    let secret_key_b = SecretKey::generate();
    let config = GossipConfig::default();

    let node_a = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_key_a.clone(),
        Some(config.clone()),
    )
    .await
    .expect("create node_a");
    let addr_a = node_a.registry.bind_addr;
    let peer_id_a = node_a.registry.peer_id.clone();

    let node_b = GossipRegistryHandle::new_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        secret_key_b,
        Some(config.clone()),
    )
    .await
    .expect("create node_b");

    let handler_a = Arc::new(OptionsIndicatorHandler::new());
    node_a
        .registry
        .set_actor_message_handler(handler_a.clone())
        .await;

    let peer_entry = node_b.add_peer(&peer_id_a).await;
    peer_entry.connect(&addr_a).await.unwrap();

    assert!(
        wait_for_active_peers(&node_b, 1, Duration::from_secs(5)).await,
        "remote node should connect to seed via TLS"
    );

    let conn = node_b
        .get_connection_to_peer(&peer_id_a)
        .await
        .expect("connection to node_a");
    let mut features = HashSet::new();
    features.insert(Feature::Streaming);
    node_b.registry.peer_capabilities.insert(
        conn.addr,
        PeerCapabilities {
            version: CURRENT_PROTOCOL_VERSION,
            features,
        },
    );
    let sample_bytes = Bytes::from(SAMPLE_PAYLOAD.as_bytes().to_vec());

    stream_tell_with_retry(
        &conn,
        sample_bytes.clone(),
        STREAMING_TYPE_HASH,
        STREAMING_ACTOR_ID,
    )
    .await
    .expect("initial streaming tell");

    assert!(
        wait_for_condition(Duration::from_secs(5), || {
            let handler = handler_a.clone();
            async move { handler.last_tell().await.is_some() }
        })
        .await,
        "node_a should receive streaming payload before restart"
    );

    node_a.shutdown().await;
    drop(node_a);

    assert!(
        wait_for_condition(Duration::from_secs(5), || {
            let registry = node_b.registry.clone();
            async move { !registry.has_active_connection(&addr_a).await }
        })
        .await,
        "node_b should observe socket termination"
    );

    assert!(
        wait_for_condition(Duration::from_secs(5), || async {
            match TcpListener::bind(addr_a).await {
                Ok(listener) => {
                    drop(listener);
                    true
                }
                Err(_) => false,
            }
        })
        .await,
        "listening port must be free before restart"
    );

    let node_a_restarted = GossipRegistryHandle::new_with_tls(addr_a, secret_key_a, Some(config))
        .await
        .expect("restart node_a");
    let handler_after = Arc::new(OptionsIndicatorHandler::new());
    node_a_restarted
        .registry
        .set_actor_message_handler(handler_after.clone())
        .await;

    assert!(
        wait_for_active_peers(&node_b, 1, Duration::from_secs(15)).await,
        "node_b should automatically reconnect via background worker"
    );

    let conn = node_b
        .get_connection_to_peer(&peer_id_a)
        .await
        .expect("connection after reconnect");
    let mut features = HashSet::new();
    features.insert(Feature::Streaming);
    node_b.registry.peer_capabilities.insert(
        conn.addr,
        PeerCapabilities {
            version: CURRENT_PROTOCOL_VERSION,
            features,
        },
    );

    stream_tell_with_retry(
        &conn,
        sample_bytes.clone(),
        STREAMING_TYPE_HASH,
        STREAMING_ACTOR_ID,
    )
    .await
    .expect("streaming tell after reconnect");

    assert!(
        wait_for_condition(Duration::from_secs(5), || {
            let handler = handler_after.clone();
            async move { handler.last_tell().await.is_some() }
        })
        .await,
        "node_a should receive streaming payload after reconnect"
    );

    node_b.shutdown().await;
    node_a_restarted.shutdown().await;
}

fn build_actor_tell_frame(actor_id: u64, type_hash: u32, payload: &[u8]) -> Bytes {
    let total_len = framing::ACTOR_HEADER_LEN + payload.len();
    let mut frame = vec![0u8; framing::LENGTH_PREFIX_LEN + total_len];
    frame[..4].copy_from_slice(&(total_len as u32).to_be_bytes());
    frame[4] = MessageType::ActorTell as u8;
    frame[5..7].copy_from_slice(&0u16.to_be_bytes());
    // bytes 7..16 are reserved/padding and already zeroed
    frame[16..24].copy_from_slice(&actor_id.to_be_bytes());
    frame[24..28].copy_from_slice(&type_hash.to_be_bytes());
    frame[28..32].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    frame[32..].copy_from_slice(payload);
    Bytes::from(frame)
}

async fn stream_tell_with_retry(
    conn: &kameo_remote::connection_pool::ConnectionHandle,
    payload: Bytes,
    type_hash: u32,
    actor_id: u64,
) -> Result<(), GossipError> {
    let mut attempts = 0;
    loop {
        match conn
            .stream_large_message(payload.clone(), type_hash, actor_id)
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) if is_capability_delay(&err) && attempts < 20 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                attempts += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn stream_ask_with_retry(
    conn: &kameo_remote::connection_pool::ConnectionHandle,
    payload: Bytes,
    type_hash: u32,
    actor_id: u64,
    timeout: Duration,
) -> Result<Bytes, GossipError> {
    let mut attempts = 0;
    loop {
        match conn
            .ask_actor(actor_id, type_hash, payload.clone(), timeout)
            .await
        {
            Ok(bytes) => return Ok(bytes),
            Err(err) if is_capability_delay(&err) && attempts < 20 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                attempts += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn is_capability_delay(err: &GossipError) -> bool {
    matches!(
        err,
        GossipError::InvalidStreamFrame(msg) if msg.contains("streaming capability")
    )
}
