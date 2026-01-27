mod common;

use bytes::Bytes;
use common::{create_tls_node, wait_for_condition};
use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use kameo_remote::{typed, GossipConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

const TEST_ACTOR_NAME: &str = "tls_test_actor";
const TEST_ACTOR_ID: u64 = typed::fnv1a_hash(TEST_ACTOR_NAME);
const TEST_TYPE_HASH: u32 = (TEST_ACTOR_ID & 0xFFFF_FFFF) as u32;

struct TestActorHandler;

impl ActorMessageHandler for TestActorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: &str,
        _type_hash: u32,
        payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let payload_len = payload.len();
        let should_reply = correlation_id.is_some();
        Box::pin(async move {
            if should_reply {
                let response = Bytes::from(format!("RESPONSE:{}", payload_len).into_bytes());
                Ok(Some(response))
            } else {
                Ok(None)
            }
        })
    }
}

fn encode_actor_payload(payload: impl Into<Vec<u8>>) -> Bytes {
    Bytes::from(payload.into())
}

/// Test true end-to-end ask/reply over TCP sockets
#[tokio::test]
async fn test_end_to_end_ask_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    let config = GossipConfig::default();
    let handle_a = create_tls_node(config.clone()).await.unwrap();
    let handle_b = create_tls_node(config).await.unwrap();
    handle_b
        .registry
        .set_actor_message_handler(Arc::new(TestActorHandler))
        .await;

    let addr_b = handle_b.registry.bind_addr;
    let peer_b_id = handle_b.registry.peer_id.clone();
    let peer_b = handle_a.add_peer(&peer_b_id).await;
    peer_b.connect(&addr_b).await.unwrap();

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            handle_a.stats().await.active_peers > 0
        })
        .await,
        "TLS peer should connect"
    );

    info!("=== Test 1: Basic ask/reply with actor handler ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let request = encode_actor_payload(b"Hello from test");
        let start = std::time::Instant::now();
        let response = conn
            .ask_actor(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                request,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        info!(
            "Got response in {:?}: {:?}",
            elapsed,
            String::from_utf8_lossy(response.as_ref())
        );
        assert_eq!(response.as_ref(), b"RESPONSE:15"); // Mock response with request length
    }

    info!("=== Test 2: Multiple concurrent asks with correlation tracking ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let mut handles = Vec::new();
        let start = std::time::Instant::now();

        for i in 0..10 {
            let conn_clone = conn.clone();
            let handle = tokio::spawn(async move {
                let request = format!("Concurrent request {}", i).into_bytes();
                let payload_len = request.len();
                let response = conn_clone
                    .ask_actor(
                        TEST_ACTOR_ID,
                        TEST_TYPE_HASH,
                        Bytes::from(request),
                        Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                (i, response, payload_len)
            });
            handles.push(handle);
        }

        // Verify all responses are correctly correlated
        for handle in handles {
            let (i, response, payload_len) = handle.await.unwrap();
            let expected = format!("RESPONSE:{}", payload_len).into_bytes();
            assert_eq!(response.as_ref(), expected);
            info!("Request {} correctly correlated", i);
        }

        let elapsed = start.elapsed();
        info!("Processed 10 concurrent asks in {:?}", elapsed);
    }

    info!("=== Test 3: Delegated reply pattern ===");
    {
        // This simulates the pattern where:
        // 1. Node A sends an ask to Node B
        // 2. Node B's connection handler creates a ReplyTo
        // 3. Node B passes the ReplyTo to an actor for processing
        // 4. The actor sends the response back through the ReplyTo

        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();
        // Simulate what would happen on Node B:
        // When an Ask arrives, the connection handler would create a ReplyTo
        // and pass it to an actor. We'll simulate this with ask_with_reply_to

        let request_payload = b"Process this request".to_vec();

        // On Node A: Send the ask and wait for response
        let response_future = {
            let conn_clone = conn.clone();
            let payload = Bytes::from(request_payload.clone());
            tokio::spawn(async move {
                conn_clone
                    .ask_actor(
                        TEST_ACTOR_ID,
                        TEST_TYPE_HASH,
                        payload,
                        Duration::from_secs(5),
                    )
                    .await
            })
        };

        // Give the message time to arrive at Node B
        sleep(Duration::from_millis(10)).await;

        let response = response_future.await.unwrap().unwrap();
        info!(
            "Got response: {:?}",
            String::from_utf8_lossy(response.as_ref())
        );
        assert_eq!(response.as_ref(), b"RESPONSE:20");
    }

    info!("=== Test 4: Performance test ===");
    {
        let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

        let num_requests = 100;
        let start = std::time::Instant::now();

        for i in 0..num_requests {
            let payload = Bytes::from(format!("Perf test {}", i).into_bytes());
            let response = conn
                .ask_actor(
                    TEST_ACTOR_ID,
                    TEST_TYPE_HASH,
                    payload,
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
            assert!(response.as_ref().starts_with(b"RESPONSE:"));
        }

        let elapsed = start.elapsed();
        let throughput = num_requests as f64 / elapsed.as_secs_f64();

        info!("Sequential: {} requests in {:?}", num_requests, elapsed);
        info!("Throughput: {:.0} req/sec", throughput);
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test timeout handling
#[tokio::test]
async fn test_ask_timeout() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=warn".parse().unwrap()),
        ))
        .try_init();

    // Create a single TLS-enabled node but don't start the peer it's trying to reach
    let handle_a = create_tls_node(GossipConfig::default()).await.unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8004".parse().unwrap(); // This won't exist

    // Try to connect to non-existent node
    let peer_b_id = kameo_remote::KeyPair::new_for_testing("node_b").peer_id();
    let peer_b = handle_a.add_peer(&peer_b_id).await;

    // Connection should fail
    match peer_b.connect(&addr_b).await {
        Err(e) => info!("Expected connection failure: {}", e),
        Ok(_) => panic!("Should not connect to non-existent node"),
    }

    handle_a.shutdown().await;
}

/// Benchmark ring-buffer ask vs streaming ask for the same payload size.
#[tokio::test]
#[ignore]
async fn benchmark_streaming_ask_vs_ring_buffer() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=warn".parse().unwrap()),
        ))
        .try_init();

    let config = GossipConfig::default();
    let handle_a = create_tls_node(config.clone()).await.unwrap();
    let handle_b = create_tls_node(config).await.unwrap();
    handle_b
        .registry
        .set_actor_message_handler(Arc::new(TestActorHandler))
        .await;

    let addr_b = handle_b.registry.bind_addr;
    let peer_b_id = handle_b.registry.peer_id.clone();
    let peer_b = handle_a.add_peer(&peer_b_id).await;
    peer_b.connect(&addr_b).await.unwrap();

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            handle_a.stats().await.active_peers > 0
        })
        .await,
        "TLS peer should connect"
    );

    let conn = handle_a.get_connection_to_peer(&peer_b_id).await.unwrap();

    // Test 1: Small payload - uses ring buffer
    let small_payload_size = 256 * 1024; // 256KB (below default streaming threshold)
    let small_payload_bytes = Bytes::from(vec![0u8; small_payload_size]);

    let iterations = 50;

    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let response = conn
            .ask_actor(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                small_payload_bytes.clone(),
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert!(response.as_ref().starts_with(b"RESPONSE:"));
    }
    let ring_elapsed = start.elapsed();

    // Test 2: Large payload - uses streaming automatically
    let large_payload_size = 2 * 1024 * 1024; // 2MB (above default streaming threshold)
    let large_payload_bytes = Bytes::from(vec![0u8; large_payload_size]);

    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let response = conn
            .ask_actor(
                TEST_ACTOR_ID,
                TEST_TYPE_HASH,
                large_payload_bytes.clone(),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(response.as_ref().starts_with(b"RESPONSE:"));
    }
    let streaming_elapsed = start.elapsed();

    println!(
        "Ring buffer ask ({} bytes): {:?} for {} iters",
        small_payload_size, ring_elapsed, iterations
    );
    println!(
        "Streaming ask ({} bytes): {:?} for {} iters",
        large_payload_size, streaming_elapsed, iterations
    );

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}
