//! Test transparent RESPONSE streaming for ask()
//!
//! This test verifies that ask() automatically receives streamed responses
//! for large response payloads (> threshold), with zero-copy reassembly.

use bytes::Bytes;
use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler};
use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test handler that returns large responses to test streaming
    struct LargeResponseHandler {
        response_size: usize,
    }

    impl ActorMessageHandler for LargeResponseHandler {
        fn handle_actor_message(
            &self,
            _actor_id: &str,
            _type_hash: u32,
            _payload: &[u8],
            _correlation_id: Option<u16>,
        ) -> ActorMessageFuture<'_> {
            let response_size = self.response_size;
            Box::pin(async move {
                // Return large response - the registry will send it using
                // send_response_bytes() which automatically streams responses
                // larger than the threshold
                let large_response = vec![b'R'; response_size];
                Ok::<Option<Bytes>, kameo_remote::GossipError>(Some(Bytes::from(large_response)))
            })
        }
    }

    #[tokio::test]
    async fn test_response_streaming_2mb() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        // Server with large response handler
        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let response_size = 2 * 1024 * 1024; // 2MB
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler { response_size }))
            .await;

        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        println!("Streaming threshold: {} bytes", threshold);
        println!("Response size: {} bytes", response_size);
        assert!(
            response_size > threshold,
            "Response should exceed streaming threshold"
        );

        // Send ask request - will receive streamed response
        let start = std::time::Instant::now();
        let response = conn
            .ask_actor(
                1,          // actor_id
                0x12345678, // type_hash
                Bytes::from(&b"small request"[..]),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // Verify response content
        assert_eq!(response.len(), response_size, "Response size should match");
        assert!(
            response.iter().all(|&b| b == b'R'),
            "All bytes should be 'R'"
        );

        println!("✅ Received 2MB streamed response in {:?}", elapsed);

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_small_response_no_streaming() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        // Server with small response handler
        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let small_response_size = 16 * 1024; // 16KB - below threshold
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler {
                response_size: small_response_size,
            }))
            .await;

        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        println!("Streaming threshold: {} bytes", threshold);
        println!("Response size: {} bytes", small_response_size);
        assert!(
            small_response_size <= threshold,
            "Response should NOT exceed streaming threshold"
        );

        // Send ask request - will receive non-streamed response
        let start = std::time::Instant::now();
        let response = conn
            .ask_actor(
                1,          // actor_id
                0x12345678, // type_hash
                Bytes::from(&b"small request"[..]),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // Verify response content
        assert_eq!(
            response.len(),
            small_response_size,
            "Response size should match"
        );
        assert!(
            response.iter().all(|&b| b == b'R'),
            "All bytes should be 'R'"
        );

        println!("✅ Received small (non-streamed) response in {:?}", elapsed);

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_response_exactly_at_threshold() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        // Response exactly at threshold - should use ring buffer (fast path)
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler {
                response_size: threshold,
            }))
            .await;

        let response = conn
            .ask_actor(
                1,
                0x12345678,
                Bytes::from("threshold test"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        assert_eq!(
            response.len(),
            threshold,
            "Response size should match threshold"
        );
        assert!(
            response.iter().all(|&b| b == b'R'),
            "All bytes should be 'R'"
        );

        println!("✅ Response at threshold ({}) bytes succeeded", threshold);

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_response_just_above_threshold() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        // Response just above threshold - should trigger streaming
        let response_size = threshold + 1;
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler { response_size }))
            .await;

        let response = conn
            .ask_actor(
                1,
                0x12345678,
                Bytes::from("above threshold test"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        assert_eq!(
            response.len(),
            response_size,
            "Response size should be just above threshold"
        );
        assert!(
            response.iter().all(|&b| b == b'R'),
            "All bytes should be 'R'"
        );

        println!(
            "✅ Response just above threshold ({} bytes) succeeded",
            response_size
        );

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_response_just_below_threshold() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        // Response just below threshold - should use ring buffer (fast path)
        let response_size = threshold.saturating_sub(1);
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler { response_size }))
            .await;

        let response = conn
            .ask_actor(
                1,
                0x12345678,
                Bytes::from("below threshold test"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        assert_eq!(
            response.len(),
            response_size,
            "Response size should be just below threshold"
        );
        assert!(
            response.iter().all(|&b| b == b'R'),
            "All bytes should be 'R'"
        );

        println!(
            "✅ Response just below threshold ({} bytes) succeeded",
            response_size
        );

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_multiple_concurrent_streaming_responses() {
        let addr_server: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr_client: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let key_server = KeyPair::new_for_testing("server");
        let key_client = KeyPair::new_for_testing("client");

        let peer_id_server = key_server.peer_id();

        let config = GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let server =
            GossipRegistryHandle::new_with_keypair(addr_server, key_server, Some(config.clone()))
                .await
                .unwrap();
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        // Connect client to server
        let server_bind_addr = server.registry.bind_addr;
        let peer = client.add_peer(&peer_id_server).await;
        peer.connect(&server_bind_addr).await.unwrap();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();

        // Set up handler with 1MB response
        let response_size = 1024 * 1024; // 1MB
        server
            .registry
            .set_actor_message_handler(Arc::new(LargeResponseHandler { response_size }))
            .await;

        // Send multiple concurrent requests
        let mut handles = Vec::new();
        for i in 0..5 {
            let conn_clone = conn.clone();
            let handle = tokio::spawn(async move {
                let response = conn_clone
                    .ask_actor(
                        1,
                        0x12345678,
                        Bytes::from(format!("concurrent request {}", i)),
                        Duration::from_secs(10),
                    )
                    .await
                    .unwrap();

                assert_eq!(response.len(), response_size, "Response size should match");
                assert!(
                    response.iter().all(|&b| b == b'R'),
                    "All bytes should be 'R'"
                );
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        for handle in handles {
            handle.await.unwrap();
        }

        println!("✅ 5 concurrent streaming responses succeeded");

        client.shutdown().await;
        server.shutdown().await;
    }
}
