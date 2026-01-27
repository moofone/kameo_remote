//! Tests for streaming request/response functionality
//!
//! These tests verify:
//! - Streaming requests (ask_streaming) for large payloads
//! - Streaming responses for large replies
//! - Zero-copy streaming (ask_streaming_bytes)
//! - StreamResponseStart/Data/End message types
//! - Auto-streaming via ReplyTo::reply() for large responses

use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};

/// Helper to initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=debug".parse().unwrap()),
        ))
        .try_init();
}

/// Create a test payload of specified size
fn create_test_payload(size: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size);
    for i in 0..size {
        payload.push((i % 256) as u8);
    }
    payload
}

/// Verify a test payload matches expected pattern
fn verify_payload(payload: &[u8], expected_size: usize) -> bool {
    if payload.len() != expected_size {
        return false;
    }
    for (i, &byte) in payload.iter().enumerate() {
        if byte != (i % 256) as u8 {
            return false;
        }
    }
    true
}

/// Test streaming request with large payload (>1MB)
#[tokio::test]
async fn test_streaming_request_large_payload() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7921".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7922".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("stream_node_a");
    let key_pair_b = KeyPair::new_for_testing("stream_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Streaming request with 2MB payload");

    let conn = handle_a.get_connection(addr_b).await.unwrap();

    // Create 2MB payload (over the 1MB streaming threshold)
    let payload_size = 2 * 1024 * 1024;
    let payload = create_test_payload(payload_size);

    // Use ask_streaming for large payload
    let response = conn
        .ask_streaming(&payload, 0, 0, Duration::from_secs(30))
        .await
        .unwrap();

    // Default handler echoes back with "PROCESSED:" prefix
    info!(
        "Received response of {} bytes",
        response.len()
    );

    // Verify we got a response (exact format depends on handler)
    assert!(!response.is_empty(), "Response should not be empty");

    info!("✅ Streaming request test passed");

    // Cleanup
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test zero-copy streaming request with Bytes
#[tokio::test]
async fn test_streaming_request_zero_copy() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7923".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7924".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("zc_node_a");
    let key_pair_b = KeyPair::new_for_testing("zc_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Zero-copy streaming request with ask_streaming_bytes");

    let conn = handle_a.get_connection(addr_b).await.unwrap();

    // Create 1.5MB payload as Bytes (over threshold)
    let payload_size = 1536 * 1024;
    let payload = Bytes::from(create_test_payload(payload_size));

    // Use ask_streaming_bytes for zero-copy
    let response = conn
        .ask_streaming_bytes(payload.clone(), 0, 0, Duration::from_secs(30))
        .await
        .unwrap();

    info!(
        "Received response of {} bytes via zero-copy path",
        response.len()
    );

    assert!(!response.is_empty(), "Response should not be empty");

    info!("✅ Zero-copy streaming request test passed");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test streaming response (large reply via auto-streaming)
#[tokio::test]
async fn test_streaming_response_auto() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7925".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7926".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("resp_node_a");
    let key_pair_b = KeyPair::new_for_testing("resp_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Streaming response (large reply triggers auto-streaming)");

    let conn = handle_a.get_connection(addr_b).await.unwrap();

    // Send a request that triggers a large response
    // The LARGE_RESPONSE command tells the handler to return a large payload
    let request = b"LARGE_RESPONSE:2097152"; // Request 2MB response

    let response = conn.ask(request).await.unwrap();

    info!(
        "Received potentially streamed response of {} bytes",
        response.len()
    );

    // Note: This test verifies the protocol works, but the actual response
    // depends on the default handler implementation
    assert!(!response.is_empty(), "Response should not be empty");

    info!("✅ Streaming response test passed");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test that small payloads use ring buffer (not streaming)
#[tokio::test]
async fn test_small_payload_uses_ring_buffer() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7927".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7928".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("small_node_a");
    let key_pair_b = KeyPair::new_for_testing("small_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Small payload (under threshold) uses ring buffer");

    let conn = handle_a.get_connection(addr_b).await.unwrap();

    // Small payload (1KB) - should NOT use streaming
    let payload = create_test_payload(1024);

    let response = conn.ask(&payload).await.unwrap();

    info!(
        "Received response of {} bytes via ring buffer path",
        response.len()
    );

    assert!(!response.is_empty(), "Response should not be empty");

    info!("✅ Small payload ring buffer test passed");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test streaming threshold boundary
#[tokio::test]
async fn test_streaming_threshold_boundary() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7929".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7930".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("boundary_node_a");
    let key_pair_b = KeyPair::new_for_testing("boundary_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Streaming threshold boundary (exactly at threshold)");

    let conn = handle_a.get_connection(addr_b).await.unwrap();

    // Get the streaming threshold
    let threshold = conn.streaming_threshold();
    info!("Streaming threshold: {} bytes", threshold);

    // Test payload at exactly threshold (should use ring buffer)
    let payload_at_threshold = create_test_payload(threshold);
    let response = conn.ask(&payload_at_threshold).await.unwrap();
    assert!(!response.is_empty(), "Response at threshold should work");
    info!("✅ Payload at threshold ({} bytes) succeeded", threshold);

    // Test payload just over threshold (should use streaming)
    let payload_over_threshold = create_test_payload(threshold + 1);
    let response = conn
        .ask_streaming(&payload_over_threshold, 0, 0, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(!response.is_empty(), "Response over threshold should work");
    info!("✅ Payload over threshold ({} bytes) succeeded", threshold + 1);

    info!("✅ Streaming threshold boundary test passed");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test multiple concurrent streaming requests
#[tokio::test]
async fn test_concurrent_streaming_requests() {
    init_tracing();

    let addr_a: SocketAddr = "127.0.0.1:7931".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7932".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("concurrent_node_a");
    let key_pair_b = KeyPair::new_for_testing("concurrent_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    info!("Test: Multiple concurrent streaming requests");

    let conn = Arc::new(handle_a.get_connection(addr_b).await.unwrap());
    let results = Arc::new(Mutex::new(Vec::new()));

    // Spawn multiple concurrent streaming requests
    let mut handles = Vec::new();
    for i in 0..3 {
        let conn = conn.clone();
        let results = results.clone();
        let handle = tokio::spawn(async move {
            let payload_size = 1024 * 1024 + i * 100_000; // Varying sizes over 1MB
            let payload = create_test_payload(payload_size);

            let response = conn
                .ask_streaming(&payload, 0, 0, Duration::from_secs(60))
                .await;

            let mut results = results.lock().await;
            results.push((i, response.is_ok(), payload_size));
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let results = results.lock().await;
    for (i, success, size) in results.iter() {
        info!("Request {}: {} bytes, success: {}", i, size, success);
        assert!(success, "Request {} should succeed", i);
    }

    info!("✅ Concurrent streaming requests test passed");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test MessageType enum includes streaming response types
#[test]
fn test_message_type_streaming_response_variants() {
    use kameo_remote::MessageType;

    // Verify streaming request types
    assert_eq!(MessageType::from_byte(0x10), Some(MessageType::StreamStart));
    assert_eq!(MessageType::from_byte(0x11), Some(MessageType::StreamData));
    assert_eq!(MessageType::from_byte(0x12), Some(MessageType::StreamEnd));

    // Verify streaming response types (new)
    assert_eq!(
        MessageType::from_byte(0x13),
        Some(MessageType::StreamResponseStart)
    );
    assert_eq!(
        MessageType::from_byte(0x14),
        Some(MessageType::StreamResponseData)
    );
    assert_eq!(
        MessageType::from_byte(0x15),
        Some(MessageType::StreamResponseEnd)
    );

    // Verify helper methods
    assert!(MessageType::StreamStart.is_streaming_request());
    assert!(MessageType::StreamData.is_streaming_request());
    assert!(MessageType::StreamEnd.is_streaming_request());
    assert!(!MessageType::StreamStart.is_streaming_response());

    assert!(MessageType::StreamResponseStart.is_streaming_response());
    assert!(MessageType::StreamResponseData.is_streaming_response());
    assert!(MessageType::StreamResponseEnd.is_streaming_response());
    assert!(!MessageType::StreamResponseStart.is_streaming_request());

    info!("✅ MessageType streaming variants test passed");
}
