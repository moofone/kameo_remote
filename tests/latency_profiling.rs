use kameo_remote::registry::{RegistryChange, RegistryDelta};
use kameo_remote::*;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;

#[tokio::test]
async fn profile_gossip_latency_components() {
    println!("🔬 Profiling Gossip Latency Components");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:25001".parse().unwrap();
    let node2_addr = "127.0.0.1:25002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    // Wait for connections
    sleep(Duration::from_millis(100)).await;

    // Test 1: Measure serialization time
    let actor_location = ActorLocation::new("127.0.0.1:26001".parse().unwrap());
    let change = RegistryChange::ActorAdded {
        name: "test_actor".to_string(),
        location: actor_location,
        priority: RegistrationPriority::Immediate,
    };

    let delta = RegistryDelta {
        since_sequence: 0,
        current_sequence: 1,
        changes: vec![change],
        sender_addr: node1_addr,
        wall_clock_time: crate::current_timestamp(),
        precise_timing_nanos: crate::current_timestamp_nanos(),
    };

    let serialization_start = Instant::now();
    let serialized = bincode::serialize(&delta).unwrap();
    let serialization_time = serialization_start.elapsed();

    println!(
        "📊 Serialization: {:?} ({} bytes)",
        serialization_time,
        serialized.len()
    );

    // Test 2: Measure deserialization time
    let deserialization_start = Instant::now();
    let _deserialized: RegistryDelta = bincode::deserialize(&serialized).unwrap();
    let deserialization_time = deserialization_start.elapsed();

    println!("📊 Deserialization: {:?}", deserialization_time);

    // Test 3: Measure connection pool message creation
    let conn = node1.get_connection(node2_addr).await.unwrap();

    let message_creation_start = Instant::now();
    let mut message = Vec::with_capacity(4 + serialized.len());
    message.extend_from_slice(&(serialized.len() as u32).to_be_bytes());
    message.extend_from_slice(&serialized);
    let message_creation_time = message_creation_start.elapsed();

    println!(
        "📊 Message creation: {:?} ({} bytes)",
        message_creation_time,
        message.len()
    );

    // Test 4: Measure channel send time
    let channel_send_start = Instant::now();
    conn.tell(&serialized).await.unwrap();
    let channel_send_time = channel_send_start.elapsed();

    println!("📊 Channel send: {:?}", channel_send_time);

    // Test 5: Measure end-to-end gossip time WITHOUT polling delays
    let e2e_start = Instant::now();
    node1
        .register_urgent(
            "latency_test_actor".to_string(),
            "127.0.0.1:26002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Wait for propagation with minimal polling delay
    let mut propagated = false;
    let mut checks = 0;
    while !propagated && e2e_start.elapsed() < Duration::from_secs(5) && checks < 1000 {
        if node2.lookup("latency_test_actor").await.is_some() {
            propagated = true;
        } else {
            // Use much smaller delay to avoid measurement artifacts
            sleep(Duration::from_micros(10)).await; // 10µs instead of 1ms
            checks += 1;
        }
    }

    let e2e_time = e2e_start.elapsed();
    println!("📊 Polling checks: {}", checks);
    println!("📊 End-to-end gossip: {:?}", e2e_time);

    // Summary
    println!("\n🎯 Latency Breakdown:");
    println!("   - Serialization: {:?}", serialization_time);
    println!("   - Deserialization: {:?}", deserialization_time);
    println!("   - Message creation: {:?}", message_creation_time);
    println!("   - Channel send: {:?}", channel_send_time);
    println!("   - End-to-end: {:?}", e2e_time);

    let total_overhead =
        serialization_time + deserialization_time + message_creation_time + channel_send_time;
    println!("   - Total measured overhead: {:?}", total_overhead);
    println!(
        "   - Unaccounted time: {:?}",
        e2e_time.saturating_sub(total_overhead)
    );

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    // For localhost, the total should be well under 100µs
    assert!(
        e2e_time < Duration::from_millis(10),
        "End-to-end latency should be <10ms"
    );
}
