use kameo_remote::*;
use std::time::Duration;
use tokio::time::sleep;

/// Test that demonstrates the deprecated peer initialization and the correct method
#[tokio::test]
async fn test_peer_initialization_deprecation_warning() {
    println!("🧪 Testing deprecated peer initialization");

    let node1_addr = "127.0.0.1:35001".parse().unwrap();
    let node2_addr = "127.0.0.1:35002".parse().unwrap();

    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("node1")),
        ..Default::default()
    };

    println!("\n⚠️ Using deprecated method (should show warnings):");
    let handle1 = GossipRegistryHandle::new(
        node1_addr,
        vec![node2_addr], // This will show deprecation warning
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Check what peers are configured
    let stats1 = handle1.stats().await;
    println!("Node1 stats after initialization: {:?}", stats1);

    // Cleanup
    handle1.shutdown().await;
}

/// Test the proposed fix: Allow specifying peer names when initializing
#[tokio::test]
async fn test_peer_initialization_with_names() {
    println!("🧪 Testing proposed peer initialization with names");

    let node1_addr = "127.0.0.1:35003".parse().unwrap();
    let node2_addr = "127.0.0.1:35004".parse().unwrap();
    let node3_addr = "127.0.0.1:35005".parse().unwrap();

    // Proposed API: Accept tuples of (peer_name, peer_addr)
    // This would require changing the new() signature or adding a new constructor

    // For now, demonstrate the workaround
    println!("\n✅ Workaround: Always use empty peer list + manual add");

    // Create all nodes with empty peer lists
    let handle1 = GossipRegistryHandle::new(
        node1_addr,
        vec![],
        Some(GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("node1")),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let handle2 = GossipRegistryHandle::new(
        node2_addr,
        vec![],
        Some(GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("node2")),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let handle3 = GossipRegistryHandle::new(
        node3_addr,
        vec![],
        Some(GossipConfig {
            key_pair: Some(KeyPair::new_for_testing("node3")),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Now add peers with correct names
    // Node1 knows Node2 and Node3
    let peer2_from_1 = handle1.add_peer(&PeerId::new("node2")).await;
    let peer3_from_1 = handle1.add_peer(&PeerId::new("node3")).await;
    peer2_from_1.connect(&node2_addr).await.unwrap();
    peer3_from_1.connect(&node3_addr).await.unwrap();

    // Node2 knows Node1 and Node3
    let peer1_from_2 = handle2.add_peer(&PeerId::new("node1")).await;
    let peer3_from_2 = handle2.add_peer(&PeerId::new("node3")).await;
    peer1_from_2.connect(&node1_addr).await.unwrap();
    peer3_from_2.connect(&node3_addr).await.unwrap();

    // Node3 knows Node1 and Node2
    let peer1_from_3 = handle3.add_peer(&PeerId::new("node1")).await;
    let peer2_from_3 = handle3.add_peer(&PeerId::new("node2")).await;
    peer1_from_3.connect(&node1_addr).await.unwrap();
    peer2_from_3.connect(&node2_addr).await.unwrap();

    // Wait for connections
    sleep(Duration::from_millis(200)).await;

    // Register actors
    handle1
        .register("service1".to_string(), "127.0.0.1:46001".parse().unwrap())
        .await
        .unwrap();
    handle2
        .register("service2".to_string(), "127.0.0.1:46002".parse().unwrap())
        .await
        .unwrap();
    handle3
        .register("service3".to_string(), "127.0.0.1:46003".parse().unwrap())
        .await
        .unwrap();

    // Wait for gossip
    sleep(Duration::from_millis(500)).await;

    // Test discovery from all nodes
    println!("\n🔍 Testing full mesh discovery:");

    // Each node should discover all other services
    for (handle, node_name) in [
        (&handle1, "node1"),
        (&handle2, "node2"),
        (&handle3, "node3"),
    ] {
        println!("\nFrom {}:", node_name);
        for service in ["service1", "service2", "service3"] {
            let result = handle.lookup(service).await;
            println!("  lookup('{}') = {:?}", service, result.is_some());
            assert!(
                result.is_some(),
                "{} should discover {}",
                node_name,
                service
            );
        }
    }

    // Cleanup
    handle1.shutdown().await;
    handle2.shutdown().await;
    handle3.shutdown().await;
}
