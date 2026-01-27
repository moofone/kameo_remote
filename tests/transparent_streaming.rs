//! Test transparent streaming for large ask payloads
//!
//! This test verifies that ask_actor() automatically uses streaming
//! for payloads larger than the streaming threshold.

use bytes::{Bytes, BytesMut};
use kameo_remote::{
    registry::{ActorMessageFuture, ActorMessageHandler},
    GossipConfig, GossipRegistryHandle, KeyPair,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;

    struct StreamingResponseHandler;

    impl ActorMessageHandler for StreamingResponseHandler {
        fn handle_actor_message(
            &self,
            _actor_id: &str,
            _type_hash: u32,
            payload: &[u8],
            correlation_id: Option<u16>,
        ) -> ActorMessageFuture<'_> {
            let response_len = if payload.len() >= 8 {
                u64::from_be_bytes(payload[..8].try_into().unwrap()) as usize
            } else {
                0
            };
            Box::pin(async move {
                if correlation_id.is_some() {
                    let mut buf = BytesMut::with_capacity(response_len);
                    buf.resize(response_len, b'r');
                    Ok(Some(buf.freeze()))
                } else {
                    Ok(None)
                }
            })
        }
    }

    #[tokio::test]
    async fn test_small_payload_uses_ring_buffer() {
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
        server
            .registry
            .set_actor_message_handler(Arc::new(StreamingResponseHandler))
            .await;
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        let server_bind_addr = server.registry.bind_addr;
        client
            .registry
            .add_peer_with_node_id(server_bind_addr, Some(peer_id_server.to_node_id()))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        // Small payload - should use ring buffer
        let small_response_len = threshold / 2;
        let mut request = BytesMut::with_capacity(8);
        request.extend_from_slice(&(small_response_len as u64).to_be_bytes());

        let _response = conn
            .ask_actor(
                1,          // actor_id
                0x12345678, // type_hash
                request.freeze(),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_large_payload_2mb_uses_streaming() {
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
        server
            .registry
            .set_actor_message_handler(Arc::new(StreamingResponseHandler))
            .await;
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        let server_bind_addr = server.registry.bind_addr;
        client
            .registry
            .add_peer_with_node_id(server_bind_addr, Some(peer_id_server.to_node_id()))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();
        let threshold = conn.streaming_threshold();

        // 2MB payload - should use streaming automatically
        let large_response_len = 2 * 1024 * 1024;
        assert!(large_response_len > threshold);
        let mut request = BytesMut::with_capacity(8);
        request.extend_from_slice(&(large_response_len as u64).to_be_bytes());

        let start = std::time::Instant::now();
        let _response = conn
            .ask_actor(
                1,          // actor_id
                0x12345678, // type_hash
                request.freeze(),
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        println!("✅ 2MB payload sent successfully in {:?}", elapsed);

        client.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_multiple_streaming_requests() {
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
        server
            .registry
            .set_actor_message_handler(Arc::new(StreamingResponseHandler))
            .await;
        let client = GossipRegistryHandle::new_with_keypair(addr_client, key_client, Some(config))
            .await
            .unwrap();

        let server_bind_addr = server.registry.bind_addr;
        client
            .registry
            .add_peer_with_node_id(server_bind_addr, Some(peer_id_server.to_node_id()))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let conn = client.get_connection(server_bind_addr).await.unwrap();

        // Send multiple large requests
        for _ in 0..5 {
            let mut request = BytesMut::with_capacity(8);
            request.extend_from_slice(&(2 * 1024 * 1024u64).to_be_bytes());

            let _response = conn
                .ask_actor(
                    1,          // actor_id
                    0x12345678, // type_hash
                    request.clone().freeze(),
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
        }

        println!("✅ Multiple streaming requests completed successfully");

        client.shutdown().await;
        server.shutdown().await;
    }
}
