use bytes::Bytes;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair};

/// Test that connection mappings remain valid after multiple gossip rounds.
///
/// This test verifies that the fix for the FullSync/FullSyncResponse addr_to_peer_id
/// removal bug works correctly. Without the fix, address mappings would get corrupted
/// over time as gossip rounds remove the ephemeral address entries.
///
/// The key aspect tested is that get_connection() continues to work after many gossip
/// rounds, which requires the address mappings to remain consistent.
#[tokio::test]
async fn test_connection_survives_multiple_gossip_rounds() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes with SHORT gossip interval to trigger many rounds quickly
    let addr_a: SocketAddr = "127.0.0.1:7921".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7922".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("lifecycle_node_a");
    let key_pair_b = KeyPair::new_for_testing("lifecycle_node_b");

    let peer_id_b = key_pair_b.peer_id();

    // Use short gossip interval to trigger multiple rounds
    let config_a = GossipConfig {
        gossip_interval: Duration::from_millis(500), // Fast gossip to trigger bug
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_millis(500), // Fast gossip to trigger bug
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config_a))
        .await
        .expect("Failed to create node A");

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config_b))
        .await
        .expect("Failed to create node B");

    // Connect A -> B (single direction is sufficient for this test)
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b
        .connect(&addr_b)
        .await
        .expect("Failed to connect A -> B");

    // Wait for connection to stabilize
    sleep(Duration::from_millis(200)).await;

    // Initial verification - connection should be available
    handle_a
        .get_connection(addr_b)
        .await
        .expect("Initial connection failed");
    info!("Initial connection established");

    // Wait for multiple gossip rounds (which trigger FullSync/FullSyncResponse)
    // With 500ms interval, 5 seconds = ~10 gossip rounds
    info!("Waiting for multiple gossip rounds...");
    sleep(Duration::from_secs(5)).await;
    info!("Multiple gossip rounds completed");

    // After many gossip rounds, connection should STILL be available
    // This is the critical test - without the fix, get_connection would fail
    // because the address mappings would be corrupted
    handle_a
        .get_connection(addr_b)
        .await
        .expect("Connection should still be available after gossip rounds - fix verified!");

    info!("Connection still available after gossip rounds - PASS");

    // Cleanup
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test that addr_to_peer_id mappings are preserved after reindexing.
///
/// This specifically tests the fix for the bug where FullSync removes
/// the ephemeral address mapping before reindex, causing orphaned entries.
#[tokio::test]
async fn test_addr_mappings_preserved_after_fullsync() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=debug".parse().unwrap()),
        ))
        .try_init();

    let addr_a: SocketAddr = "127.0.0.1:7923".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7924".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("mapping_node_a");
    let key_pair_b = KeyPair::new_for_testing("mapping_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config_a = GossipConfig {
        gossip_interval: Duration::from_millis(200), // Very fast gossip
        ..Default::default()
    };

    let config_b = GossipConfig {
        gossip_interval: Duration::from_millis(200),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config_a))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config_b))
        .await
        .unwrap();

    // Connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    // Wait for connection to stabilize
    sleep(Duration::from_millis(500)).await;

    // Verify initial state
    let conn = handle_a
        .get_connection(addr_b)
        .await
        .expect("Initial connection");
    let response = conn
        .ask(Bytes::from_static(b"ECHO:test"))
        .await
        .expect("Initial ask");
    assert_eq!(response.as_ref(), b"ECHOED:test");

    // Now let many gossip rounds happen
    for round in 0..20 {
        sleep(Duration::from_millis(250)).await;

        // Try to send a message each round
        match handle_a.get_connection(addr_b).await {
            Ok(conn) => {
                let request = format!("ECHO:round{}", round);
                match conn.ask(Bytes::copy_from_slice(request.as_bytes())).await {
                    Ok(response) => {
                        let expected = format!("ECHOED:round{}", round);
                        assert_eq!(
                            response.as_ref(),
                            expected.as_bytes(),
                            "Round {} mismatch",
                            round
                        );
                        info!("Round {} message delivered successfully", round);
                    }
                    Err(e) => {
                        panic!(
                            "Round {} ask failed: {} - address mappings likely corrupted!",
                            round, e
                        );
                    }
                }
            }
            Err(e) => {
                panic!(
                    "Round {} connection lost: {} - address mappings corrupted!",
                    round, e
                );
            }
        }
    }

    info!("All 20 rounds passed - address mappings are correctly preserved");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test rapid reconnection doesn't leave orphaned address entries.
///
/// This test verifies that after a node disconnects and a new node binds to the
/// same address, the connection mappings are correctly updated without orphaned
/// entries that would prevent proper routing.
#[tokio::test]
async fn test_reconnect_cleanup() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    let addr_a: SocketAddr = "127.0.0.1:7925".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7926".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("reconnect_node_a");
    let key_pair_b = KeyPair::new_for_testing("reconnect_node_b");

    let peer_id_b = key_pair_b.peer_id();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300), // Long interval - we control timing
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new_with_keypair(addr_a, key_pair_a, Some(config.clone()))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new_with_keypair(addr_b, key_pair_b, Some(config))
        .await
        .unwrap();

    // Initial connect
    let peer_b = handle_a.add_peer(&peer_id_b).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    // Verify initial connection available
    handle_a
        .get_connection(addr_b)
        .await
        .expect("Initial connection should work");
    info!("Initial connection established");

    // Disconnect by shutting down B
    info!("Shutting down node B to force disconnect");
    handle_b.shutdown().await;
    sleep(Duration::from_millis(500)).await;

    // Restart B with same address but NEW identity
    info!("Restarting node B with new identity");
    let key_pair_b2 = KeyPair::new_for_testing("reconnect_node_b2");
    let peer_id_b2 = key_pair_b2.peer_id();
    let handle_b2 = GossipRegistryHandle::new_with_keypair(
        addr_b,
        key_pair_b2,
        Some(GossipConfig {
            gossip_interval: Duration::from_secs(300),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Reconnect to the new peer
    let peer_b2 = handle_a.add_peer(&peer_id_b2).await;
    peer_b2.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // The critical test: verify get_connection works for the NEW peer
    // This would fail if old address mappings weren't cleaned up properly
    handle_a
        .get_connection(addr_b)
        .await
        .expect("Reconnection should work - address mappings correctly updated");

    info!("Reconnection successful - no orphaned address entries");

    handle_a.shutdown().await;
    handle_b2.shutdown().await;
}
