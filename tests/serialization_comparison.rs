use kameo_remote::*;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::collections::HashMap;
use std::time::Instant;

/// Test structures that mirror the existing message types for rkyv compatibility
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct RkyvActorLocation {
    pub address: String, // Use String instead of SocketAddr for rkyv compatibility
    pub priority: u8,
    pub wall_clock_time: u64,
    pub local_registration_time: u128,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct RkyvRegistryChange {
    pub change_type: u8, // 0 = ActorAdded, 1 = ActorRemoved
    pub name: String,
    pub location: Option<RkyvActorLocation>,
    pub priority: u8,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct RkyvRegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RkyvRegistryChange>,
    pub sender_addr: String,
    pub wall_clock_time: u64,
    pub precise_timing_nanos: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct RkyvFullSync {
    pub local_actors: Vec<(String, RkyvActorLocation)>,
    pub known_actors: Vec<(String, RkyvActorLocation)>,
    pub sender_addr: String,
    pub sequence: u64,
    pub wall_clock_time: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub enum RkyvRegistryMessage {
    DeltaGossip {
        delta: RkyvRegistryDelta,
    },
    FullSync {
        local_actors: Vec<(String, RkyvActorLocation)>,
        known_actors: Vec<(String, RkyvActorLocation)>,
        sender_addr: String,
        sequence: u64,
        wall_clock_time: u64,
    },
    FullSyncRequest {
        sender_addr: String,
        sequence: u64,
        wall_clock_time: u64,
    },
    FullSyncResponse {
        local_actors: Vec<(String, RkyvActorLocation)>,
        known_actors: Vec<(String, RkyvActorLocation)>,
        sender_addr: String,
        sequence: u64,
        wall_clock_time: u64,
    },
    DeltaGossipResponse {
        delta: RkyvRegistryDelta,
    },
}

/// Helper function to create test data for serialization benchmarks
fn create_test_delta_gossip(
    num_changes: usize,
) -> (registry::RegistryMessage, RkyvRegistryMessage) {
    let mut changes = Vec::new();
    let mut rkyv_changes = Vec::new();

    for i in 0..num_changes {
        let actor_name = format!("test_actor_{}", i);
        let location = ActorLocation {
            address: format!("127.0.0.1:{}", 8000 + i).parse().unwrap(),
            priority: RegistrationPriority::Normal,
            wall_clock_time: 1234567890 + i as u64,
            local_registration_time: (1234567890 + i as u64) as u128,
        };

        let rkyv_location = RkyvActorLocation {
            address: format!("127.0.0.1:{}", 8000 + i),
            priority: 0, // Normal priority
            wall_clock_time: 1234567890 + i as u64,
            local_registration_time: (1234567890 + i as u64) as u128,
        };

        let change = registry::RegistryChange::ActorAdded {
            name: actor_name.clone(),
            location: location.clone(),
            priority: RegistrationPriority::Normal,
        };

        let rkyv_change = RkyvRegistryChange {
            change_type: 0, // ActorAdded
            name: actor_name,
            location: Some(rkyv_location),
            priority: 0, // Normal
        };

        changes.push(change);
        rkyv_changes.push(rkyv_change);
    }

    let delta = registry::RegistryDelta {
        since_sequence: 12340,
        current_sequence: 12345,
        changes,
        sender_addr: "127.0.0.1:8000".parse().unwrap(),
        wall_clock_time: 1234567890,
        precise_timing_nanos: 1234567890123456789,
    };

    let rkyv_delta = RkyvRegistryDelta {
        since_sequence: 12340,
        current_sequence: 12345,
        changes: rkyv_changes,
        sender_addr: "127.0.0.1:8000".to_string(),
        wall_clock_time: 1234567890,
        precise_timing_nanos: 1234567890123456789,
    };

    (
        registry::RegistryMessage::DeltaGossip { delta },
        RkyvRegistryMessage::DeltaGossip { delta: rkyv_delta },
    )
}

fn create_test_full_sync(num_actors: usize) -> (registry::RegistryMessage, RkyvRegistryMessage) {
    let mut local_actors = HashMap::new();
    let mut known_actors = HashMap::new();
    let mut rkyv_local_actors = Vec::new();
    let mut rkyv_known_actors = Vec::new();

    for i in 0..num_actors {
        let actor_name = format!("local_actor_{}", i);
        let location = ActorLocation {
            address: format!("127.0.0.1:{}", 9000 + i).parse().unwrap(),
            priority: RegistrationPriority::Normal,
            wall_clock_time: 1234567890 + i as u64,
            local_registration_time: (1234567890 + i as u64) as u128,
        };

        let rkyv_location = RkyvActorLocation {
            address: format!("127.0.0.1:{}", 9000 + i),
            priority: 0,
            wall_clock_time: 1234567890 + i as u64,
            local_registration_time: (1234567890 + i as u64) as u128,
        };

        local_actors.insert(actor_name.clone(), location.clone());
        known_actors.insert(actor_name.clone(), location.clone());
        rkyv_local_actors.push((actor_name.clone(), rkyv_location.clone()));
        rkyv_known_actors.push((actor_name, rkyv_location));
    }

    let bincode_msg = registry::RegistryMessage::FullSync {
        local_actors,
        known_actors,
        sender_addr: "127.0.0.1:8000".parse().unwrap(),
        sequence: 54321,
        wall_clock_time: 1234567890,
    };

    let rkyv_msg = RkyvRegistryMessage::FullSync {
        local_actors: rkyv_local_actors,
        known_actors: rkyv_known_actors,
        sender_addr: "127.0.0.1:8000".to_string(),
        sequence: 54321,
        wall_clock_time: 1234567890,
    };

    (bincode_msg, rkyv_msg)
}

/// Comprehensive serialization performance comparison test
#[tokio::test]
async fn test_serialization_performance_comparison() {
    println!("🚀 SERIALIZATION PERFORMANCE COMPARISON: bincode vs rkyv");
    println!("=======================================================");

    // Test different message sizes
    let test_sizes = vec![
        (1, "Small (1 change)"),
        (10, "Medium (10 changes)"),
        (100, "Large (100 changes)"),
        (1000, "Very Large (1000 changes)"),
    ];

    for (num_changes, description) in test_sizes {
        println!("\n📊 Testing {} - {} changes", description, num_changes);
        println!("{}=", "=".repeat(50));

        let (bincode_msg, rkyv_msg) = create_test_delta_gossip(num_changes);

        // Bincode serialization test
        println!("\n🔸 bincode Serialization Test:");
        let bincode_serialize_start = Instant::now();
        let bincode_data = bincode::serialize(&bincode_msg).unwrap();
        let bincode_serialize_time = bincode_serialize_start.elapsed();

        println!(
            "   - Serialization time: {:?} ({:.3} μs)",
            bincode_serialize_time,
            bincode_serialize_time.as_nanos() as f64 / 1000.0
        );
        println!("   - Serialized size: {} bytes", bincode_data.len());

        // Bincode deserialization test
        let bincode_deserialize_start = Instant::now();
        let _bincode_deserialized: registry::RegistryMessage =
            bincode::deserialize(&bincode_data).unwrap();
        let bincode_deserialize_time = bincode_deserialize_start.elapsed();

        println!(
            "   - Deserialization time: {:?} ({:.3} μs)",
            bincode_deserialize_time,
            bincode_deserialize_time.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - Total time: {:?} ({:.3} μs)",
            bincode_serialize_time + bincode_deserialize_time,
            (bincode_serialize_time + bincode_deserialize_time).as_nanos() as f64 / 1000.0
        );

        // rkyv serialization test
        println!("\n🔸 rkyv Serialization Test:");
        let rkyv_serialize_start = Instant::now();
        let rkyv_data = rkyv::to_bytes::<rkyv::rancor::Error>(&rkyv_msg).unwrap();
        let rkyv_serialize_time = rkyv_serialize_start.elapsed();

        println!(
            "   - Serialization time: {:?} ({:.3} μs)",
            rkyv_serialize_time,
            rkyv_serialize_time.as_nanos() as f64 / 1000.0
        );
        println!("   - Serialized size: {} bytes", rkyv_data.len());

        // rkyv deserialization test (zero-copy)
        let rkyv_deserialize_start = Instant::now();
        let archived =
            rkyv::access::<ArchivedRkyvRegistryMessage, rkyv::rancor::Error>(&rkyv_data).unwrap();
        let rkyv_deserialize_time = rkyv_deserialize_start.elapsed();

        println!(
            "   - Deserialization time: {:?} ({:.3} μs)",
            rkyv_deserialize_time,
            rkyv_deserialize_time.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - Total time: {:?} ({:.3} μs)",
            rkyv_serialize_time + rkyv_deserialize_time,
            (rkyv_serialize_time + rkyv_deserialize_time).as_nanos() as f64 / 1000.0
        );

        // Full deserialization test (for fair comparison)
        let rkyv_full_deserialize_start = Instant::now();
        let _rkyv_deserialized: RkyvRegistryMessage =
            rkyv::from_bytes::<_, rkyv::rancor::Error>(&rkyv_data).unwrap();
        let rkyv_full_deserialize_time = rkyv_full_deserialize_start.elapsed();

        println!(
            "   - Full deserialization time: {:?} ({:.3} μs)",
            rkyv_full_deserialize_time,
            rkyv_full_deserialize_time.as_nanos() as f64 / 1000.0
        );
        println!(
            "   - Total time (with full deser): {:?} ({:.3} μs)",
            rkyv_serialize_time + rkyv_full_deserialize_time,
            (rkyv_serialize_time + rkyv_full_deserialize_time).as_nanos() as f64 / 1000.0
        );

        // Performance comparison
        let bincode_total = bincode_serialize_time + bincode_deserialize_time;
        let rkyv_total_zero_copy = rkyv_serialize_time + rkyv_deserialize_time;
        let rkyv_total_full = rkyv_serialize_time + rkyv_full_deserialize_time;

        let size_ratio = bincode_data.len() as f64 / rkyv_data.len() as f64;
        let speed_ratio_zero_copy =
            bincode_total.as_nanos() as f64 / rkyv_total_zero_copy.as_nanos() as f64;
        let speed_ratio_full = bincode_total.as_nanos() as f64 / rkyv_total_full.as_nanos() as f64;

        println!("\n🎯 Performance Comparison:");
        println!(
            "   - Size comparison: bincode {} bytes vs rkyv {} bytes ({:.2}x ratio)",
            bincode_data.len(),
            rkyv_data.len(),
            size_ratio
        );
        println!(
            "   - Speed (zero-copy): rkyv is {:.2}x faster than bincode",
            speed_ratio_zero_copy
        );
        println!(
            "   - Speed (full deser): rkyv is {:.2}x faster than bincode",
            speed_ratio_full
        );

        // Throughput calculation
        let bincode_throughput =
            bincode_data.len() as f64 / bincode_total.as_secs_f64() / (1024.0 * 1024.0);
        let rkyv_throughput_zero_copy =
            rkyv_data.len() as f64 / rkyv_total_zero_copy.as_secs_f64() / (1024.0 * 1024.0);
        let rkyv_throughput_full =
            rkyv_data.len() as f64 / rkyv_total_full.as_secs_f64() / (1024.0 * 1024.0);

        println!("   - Throughput bincode: {:.2} MB/s", bincode_throughput);
        println!(
            "   - Throughput rkyv (zero-copy): {:.2} MB/s",
            rkyv_throughput_zero_copy
        );
        println!(
            "   - Throughput rkyv (full deser): {:.2} MB/s",
            rkyv_throughput_full
        );
    }

    println!("\n🔥 BULK PERFORMANCE TEST");
    println!("======================");

    // Test with many iterations to measure average performance
    let iterations = 1000;
    let (bincode_msg, rkyv_msg) = create_test_delta_gossip(10); // Medium size for bulk test

    println!("\n🔸 Bulk bincode Test ({} iterations):", iterations);
    let bulk_bincode_start = Instant::now();
    for _ in 0..iterations {
        let data = bincode::serialize(&bincode_msg).unwrap();
        let _: registry::RegistryMessage = bincode::deserialize(&data).unwrap();
    }
    let bulk_bincode_time = bulk_bincode_start.elapsed();

    println!("   - Total time: {:?}", bulk_bincode_time);
    println!(
        "   - Average per iteration: {:?} ({:.3} μs)",
        bulk_bincode_time / iterations,
        bulk_bincode_time.as_nanos() as f64 / iterations as f64 / 1000.0
    );

    println!("\n🔸 Bulk rkyv Test ({} iterations):", iterations);
    let bulk_rkyv_start = Instant::now();
    for _ in 0..iterations {
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&rkyv_msg).unwrap();
        let archived =
            rkyv::access::<ArchivedRkyvRegistryMessage, rkyv::rancor::Error>(&data).unwrap();
        let _: RkyvRegistryMessage = rkyv::from_bytes::<_, rkyv::rancor::Error>(&data).unwrap();
    }
    let bulk_rkyv_time = bulk_rkyv_start.elapsed();

    println!("   - Total time: {:?}", bulk_rkyv_time);
    println!(
        "   - Average per iteration: {:?} ({:.3} μs)",
        bulk_rkyv_time / iterations,
        bulk_rkyv_time.as_nanos() as f64 / iterations as f64 / 1000.0
    );

    let bulk_improvement = bulk_bincode_time.as_nanos() as f64 / bulk_rkyv_time.as_nanos() as f64;
    println!(
        "   - rkyv is {:.2}x faster than bincode in bulk operations",
        bulk_improvement
    );

    println!("\n🎯 SUMMARY AND RECOMMENDATIONS");
    println!("==============================");
    println!("✅ rkyv Advantages:");
    println!("   - Zero-copy deserialization for read-heavy workloads");
    println!("   - Generally faster serialization/deserialization");
    println!("   - Better cache locality with archived data");
    println!("   - More predictable performance");

    println!("\n❌ rkyv Disadvantages:");
    println!("   - Slightly larger serialized size in some cases");
    println!("   - More complex type definitions (Archive trait)");
    println!("   - Less ecosystem maturity than bincode");
    println!("   - Validation overhead for safety");

    println!("\n💡 Recommendations:");
    println!("   - Use rkyv for high-frequency gossip messages (performance critical)");
    println!("   - Use rkyv for scenarios with many reads of the same data");
    println!("   - Consider rkyv for network-bound applications");
    println!("   - Bincode might be sufficient for low-frequency control messages");

    println!("\n✅ Serialization comparison test completed!");
}

/// Test FullSync messages to compare performance with larger payloads
#[tokio::test]
async fn test_full_sync_serialization_comparison() {
    println!("🚀 FULL SYNC MESSAGE COMPARISON: bincode vs rkyv");
    println!("================================================");

    let test_sizes = vec![
        (10, "Small cluster (10 actors)"),
        (100, "Medium cluster (100 actors)"),
        (1000, "Large cluster (1000 actors)"),
    ];

    for (num_actors, description) in test_sizes {
        println!("\n📊 Testing {} - {} actors", description, num_actors);
        println!("{}=", "=".repeat(50));

        let (bincode_msg, rkyv_msg) = create_test_full_sync(num_actors);

        // bincode test
        let bincode_start = Instant::now();
        let bincode_data = bincode::serialize(&bincode_msg).unwrap();
        let bincode_serialize_time = bincode_start.elapsed();

        let bincode_deserialize_start = Instant::now();
        let _: registry::RegistryMessage = bincode::deserialize(&bincode_data).unwrap();
        let bincode_deserialize_time = bincode_deserialize_start.elapsed();

        // rkyv test
        let rkyv_start = Instant::now();
        let rkyv_data = rkyv::to_bytes::<rkyv::rancor::Error>(&rkyv_msg).unwrap();
        let rkyv_serialize_time = rkyv_start.elapsed();

        let rkyv_deserialize_start = Instant::now();
        let archived =
            rkyv::access::<ArchivedRkyvRegistryMessage, rkyv::rancor::Error>(&rkyv_data).unwrap();
        let _: RkyvRegistryMessage =
            rkyv::from_bytes::<_, rkyv::rancor::Error>(&rkyv_data).unwrap();
        let rkyv_deserialize_time = rkyv_deserialize_start.elapsed();

        println!(
            "   bincode: {} bytes, {:?} total time",
            bincode_data.len(),
            bincode_serialize_time + bincode_deserialize_time
        );
        println!(
            "   rkyv: {} bytes, {:?} total time",
            rkyv_data.len(),
            rkyv_serialize_time + rkyv_deserialize_time
        );

        let improvement = (bincode_serialize_time + bincode_deserialize_time).as_nanos() as f64
            / (rkyv_serialize_time + rkyv_deserialize_time).as_nanos() as f64;
        println!(
            "   🎯 rkyv is {:.2}x faster for FullSync messages",
            improvement
        );
    }

    println!("\n✅ FullSync serialization comparison completed!");
}
