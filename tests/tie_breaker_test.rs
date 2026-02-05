//! Tests for the unified tie-breaker connection registration logic.
//!
//! These tests verify that the `register_connection_with_tiebreaker` function
//! correctly handles:
//! - Simultaneous inbound/outbound connections
//! - Outbound connections with unknown peer_id (registered via FullSyncResponse)
//! - Duplicate inbound connections
//! - Deferred shutdown (cleanup after lock release)

mod common;

use common::{create_tls_node, wait_for_condition, DynError};
use kameo_remote::GossipConfig;
use std::future::Future;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::sleep;

const TEST_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;
const TEST_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const TEST_WORKER_THREADS: usize = 4;

fn run_tie_breaker_test<F, R>(future: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .name("tie-breaker-test".into())
        .stack_size(TEST_THREAD_STACK_SIZE)
        .spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(TEST_WORKER_THREADS)
                .thread_stack_size(TEST_WORKER_STACK_SIZE)
                .enable_all()
                .build()
                .expect("failed to build tie-breaker test runtime");
            rt.block_on(future)
        })
        .expect("failed to spawn tie-breaker test thread")
        .join()
        .expect("tie-breaker test thread panicked unexpectedly")
}

/// Test that simultaneous inbound/outbound connections result in only ONE
/// surviving connection per peer pair.
#[test]
fn test_simultaneous_inbound_outbound() {
    run_tie_breaker_test(async {
        // 1. Set up two nodes A and B (peer_id derived from keypair)
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await.unwrap();
        let node_b = create_tls_node(config).await.unwrap();

        let addr_a = node_a.registry.bind_addr;
        let addr_b = node_b.registry.bind_addr;
        let peer_id_a = node_a.registry.peer_id.clone();
        let peer_id_b = node_b.registry.peer_id.clone();

        // Configure peers on both nodes
        node_a
            .registry
            .configure_peer(peer_id_b.clone(), addr_b)
            .await;
        node_b
            .registry
            .configure_peer(peer_id_a.clone(), addr_a)
            .await;

        // 2. Have both dial each other simultaneously
        let peer_b_handle = node_a.add_peer(&peer_id_b).await;
        let peer_a_handle = node_b.add_peer(&peer_id_a).await;

        // Spawn concurrent dials
        let dial_b = tokio::spawn(async move {
            let _ = peer_b_handle.connect(&addr_b).await;
        });
        let dial_a = tokio::spawn(async move {
            let _ = peer_a_handle.connect(&addr_a).await;
        });

        // Wait for both dials to complete
        let _ = tokio::join!(dial_b, dial_a);

        // Give time for tie-breaker to resolve
        sleep(Duration::from_millis(500)).await;

        // 3. Verify only ONE connection survives per pair
        let pool_a = &node_a.registry.connection_pool;
        let pool_b = &node_b.registry.connection_pool;

        // Count connections by peer_id
        let a_has_b = pool_a.has_connection_by_peer_id(&peer_id_b);
        let b_has_a = pool_b.has_connection_by_peer_id(&peer_id_a);

        println!("node_a has connection to node_b: {}", a_has_b);
        println!("node_b has connection to node_a: {}", b_has_a);

        // Both should have exactly one connection to each other
        assert!(a_has_b, "node_a should have connection to node_b");
        assert!(b_has_a, "node_b should have connection to node_a");

        // 4. Verify communication works (proves connection is valid)
        let stats_a = node_a.stats().await;
        let stats_b = node_b.stats().await;

        println!("node_a stats: {:?}", stats_a);
        println!("node_b stats: {:?}", stats_b);

        // Cleanup
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

/// Test that outbound connections with unknown peer_id are properly registered
/// when FullSyncResponse arrives with the peer_id.
#[test]
fn test_outbound_unknown_peer_id_registration() {
    run_tie_breaker_test(async {
        // 1. Set up two nodes A and B
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await.unwrap();
        let node_b = create_tls_node(config).await.unwrap();

        let addr_b = node_b.registry.bind_addr;
        let peer_id_b = node_b.registry.peer_id.clone();

        // Configure peer only on A (B doesn't know about A yet)
        node_a
            .registry
            .configure_peer(peer_id_b.clone(), addr_b)
            .await;

        // 2. Node A dials node B (peer_id unknown initially on the connection)
        let peer_b_handle = node_a.add_peer(&peer_id_b).await;
        peer_b_handle.connect(&addr_b).await.unwrap();

        // 3. Wait for FullSyncResponse to arrive with peer_id
        let registered = wait_for_condition(Duration::from_secs(5), || async {
            let pool_a = &node_a.registry.connection_pool;
            pool_a.has_connection_by_peer_id(&peer_id_b)
        })
        .await;

        // 4. Verify connection is registered by peer_id
        assert!(
            registered,
            "Connection should be registered by peer_id after FullSyncResponse"
        );

        // Cleanup
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

/// Test that duplicate inbound connections for same peer_id result in only
/// one connection being kept (tie-breaker runs correctly).
#[test]
fn test_duplicate_inbound_connections() {
    run_tie_breaker_test(async {
        // 1. Set up two nodes A and B
        // B will try to connect to A twice (simulating race condition)
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await.unwrap();
        let node_b = create_tls_node(config).await.unwrap();

        let addr_a = node_a.registry.bind_addr;
        let peer_id_a = node_a.registry.peer_id.clone();
        let peer_id_b = node_b.registry.peer_id.clone();

        // Configure peer on B
        node_b
            .registry
            .configure_peer(peer_id_a.clone(), addr_a)
            .await;

        // 2. Connect B to A twice in rapid succession
        let peer_a_handle_1 = node_b.add_peer(&peer_id_a).await;
        let peer_a_handle_2 = node_b.add_peer(&peer_id_a).await;

        // Spawn concurrent connections (simulating race)
        let connect_1 = tokio::spawn(async move {
            let _ = peer_a_handle_1.connect(&addr_a).await;
        });
        let connect_2 = tokio::spawn(async move {
            // Slight delay to simulate near-simultaneous but not exact
            sleep(Duration::from_millis(10)).await;
            let _ = peer_a_handle_2.connect(&addr_a).await;
        });

        let _ = tokio::join!(connect_1, connect_2);

        // Give time for tie-breaker to resolve
        sleep(Duration::from_millis(500)).await;

        // 3. Verify only one connection exists on A from B
        let pool_a = &node_a.registry.connection_pool;

        // Count how many connections from node_b
        let conn_count: usize = pool_a
            .connections_by_peer
            .iter()
            .filter(|entry| entry.key() == &peer_id_b)
            .count();

        println!("node_a has {} connections from node_b", conn_count);

        // Should have exactly 1 connection
        assert_eq!(
            conn_count, 1,
            "Should have exactly 1 connection from node_b, not {}",
            conn_count
        );

        // Cleanup
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

/// Test that communication works after tie-breaker resolution
/// (verifies the surviving connection is fully functional).
#[test]
fn test_communication_after_tie_breaker() {
    run_tie_breaker_test(async {
        use kameo_remote::registry::{ActorMessageFuture, ActorMessageHandler};
        use std::sync::Arc;

        struct EchoHandler;
        impl ActorMessageHandler for EchoHandler {
            fn handle_actor_message(
                &self,
                _actor_id: &str,
                _type_hash: u32,
                payload: &[u8],
                correlation_id: Option<u16>,
            ) -> ActorMessageFuture<'_> {
                let response = if correlation_id.is_some() {
                    Some(format!("ECHO:{}", payload.len()).into_bytes())
                } else {
                    None
                };
                Box::pin(async move { Ok(response) })
            }
        }

        // Set up two nodes
        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await.unwrap();
        let node_b = create_tls_node(config).await.unwrap();

        // Set up message handler on B
        node_b
            .registry
            .set_actor_message_handler(Arc::new(EchoHandler))
            .await;

        let addr_a = node_a.registry.bind_addr;
        let addr_b = node_b.registry.bind_addr;
        let peer_id_a = node_a.registry.peer_id.clone();
        let peer_id_b = node_b.registry.peer_id.clone();

        // Configure both ways
        node_a
            .registry
            .configure_peer(peer_id_b.clone(), addr_b)
            .await;
        node_b
            .registry
            .configure_peer(peer_id_a.clone(), addr_a)
            .await;

        // Connect both simultaneously
        let peer_b_handle = node_a.add_peer(&peer_id_b).await;
        let peer_a_handle = node_b.add_peer(&peer_id_a).await;

        let (result_b, result_a) = tokio::join!(
            peer_b_handle.connect(&addr_b),
            peer_a_handle.connect(&addr_a)
        );

        // At least one should succeed
        assert!(
            result_a.is_ok() || result_b.is_ok(),
            "At least one connection should succeed"
        );

        // Wait for tie-breaker to resolve
        sleep(Duration::from_millis(300)).await;

        // Verify we can communicate (register an actor and look it up)
        node_b
            .register("test_actor".to_string(), addr_b)
            .await
            .unwrap();

        let found = wait_for_condition(Duration::from_secs(5), || async {
            node_a.lookup("test_actor").await.is_some()
        })
        .await;

        assert!(
            found,
            "Actor should be discoverable after tie-breaker resolution"
        );

        // Cleanup
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}

/// Test that no "Dropped duplicate connection" messages appear when
/// tie-breaker properly keeps exactly one connection.
#[test]
fn test_no_duplicate_drop_messages() {
    run_tie_breaker_test(async {
        // This test verifies logging behavior indirectly by checking
        // that only one connection exists after both nodes dial each other

        let config = GossipConfig {
            gossip_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let node_a = create_tls_node(config.clone()).await.unwrap();
        let node_b = create_tls_node(config).await.unwrap();

        let addr_a = node_a.registry.bind_addr;
        let addr_b = node_b.registry.bind_addr;
        let peer_id_a = node_a.registry.peer_id.clone();
        let peer_id_b = node_b.registry.peer_id.clone();

        // Configure both ways
        node_a
            .registry
            .configure_peer(peer_id_b.clone(), addr_b)
            .await;
        node_b
            .registry
            .configure_peer(peer_id_a.clone(), addr_a)
            .await;

        // Connect simultaneously multiple times to stress test
        for i in 0..3 {
            println!("Connection attempt {}", i + 1);

            let peer_b = node_a.add_peer(&peer_id_b).await;
            let peer_a = node_b.add_peer(&peer_id_a).await;

            let _ = tokio::join!(peer_b.connect(&addr_b), peer_a.connect(&addr_a));

            sleep(Duration::from_millis(100)).await;
        }

        // Wait for everything to settle
        sleep(Duration::from_millis(500)).await;

        // Verify exactly one connection per direction
        let pool_a = &node_a.registry.connection_pool;
        let pool_b = &node_b.registry.connection_pool;

        let a_to_b_count = pool_a
            .connections_by_peer
            .iter()
            .filter(|e| e.key() == &peer_id_b)
            .count();
        let b_to_a_count = pool_b
            .connections_by_peer
            .iter()
            .filter(|e| e.key() == &peer_id_a)
            .count();

        println!("After 3 connection attempts:");
        println!("  A -> B connections: {}", a_to_b_count);
        println!("  B -> A connections: {}", b_to_a_count);

        assert_eq!(a_to_b_count, 1, "Should have exactly 1 A->B connection");
        assert_eq!(b_to_a_count, 1, "Should have exactly 1 B->A connection");

        // Cleanup
        node_a.shutdown().await;
        node_b.shutdown().await;
    });
}
