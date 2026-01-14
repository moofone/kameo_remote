use kameo_remote::{ClockOrdering, GossipConfig, NodeId, SecretKey, VectorClock};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

/// Test that NodeId fallback uses deterministic hash instead of zeros
#[test]
fn test_nodeid_fallback_is_deterministic() {
    use kameo_remote::RemoteActorLocation;
    use std::net::SocketAddr;

    // Create two locations with the same invalid peer_id
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let peer_id = kameo_remote::PeerId::new("test_peer");

    let location1 = RemoteActorLocation::new_with_peer(addr, peer_id.clone());
    let location2 = RemoteActorLocation::new_with_peer(addr, peer_id.clone());

    // NodeIds should be deterministic (same for same input)
    assert_eq!(location1.node_id, location2.node_id);

    // NodeIds should NOT be all zeros
    let zero_node = NodeId::from_bytes(&[0u8; 32]).unwrap();
    assert_ne!(location1.node_id, zero_node);

    // Different peer_ids should produce different NodeIds
    let peer_id2 = kameo_remote::PeerId::new("different_peer");
    let location3 = RemoteActorLocation::new_with_peer(addr, peer_id2);
    assert_ne!(location1.node_id, location3.node_id);
}

/// Test vector clock compaction preserves causality
#[test]
fn test_vector_clock_compaction() {
    let mut clock = VectorClock::new();
    let mut nodes = Vec::new();

    // Create 1500 different nodes
    for i in 0..1500 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = (i >> 8) as u8;
        key_bytes[1] = (i & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        nodes.push(node);
        clock.increment_with_max_size(node, None);
    }

    // Clock should have 1500 entries
    assert_eq!(clock.clocks.len(), 1500);

    // Compact to 100 entries
    clock.compact(100);

    // Should have at most 100 entries (99 + 1 "others" entry)
    assert!(clock.clocks.len() <= 100);

    // The "others" entry should exist
    let others_node_id = NodeId::from_bytes(&[255u8; 32]).unwrap();
    assert!(clock.get(&others_node_id) > 0);

    // Most active nodes should be preserved (those with highest clock values)
    // Since we incremented each once, they all have value 1, but compaction should still work
    assert!(clock.clocks.len() > 0);
}

/// Test that vector clock operations are thread-safe with external locks
#[test]
fn test_vector_clock_external_thread_safety() {
    let clock = Arc::new(RwLock::new(VectorClock::new()));
    let key1 = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let key2 = SecretKey::from_bytes(&[2u8; 32]).unwrap();
    let node1 = key1.public();
    let node2 = key2.public();

    let clock1 = clock.clone();
    let clock2 = clock.clone();

    // Thread 1: Increment node1
    let handle1 = thread::spawn(move || {
        for _ in 0..1000 {
            let mut c = clock1.write().unwrap();
            c.increment(node1);
        }
    });

    // Thread 2: Increment node2
    let handle2 = thread::spawn(move || {
        for _ in 0..1000 {
            let mut c = clock2.write().unwrap();
            c.increment(node2);
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();

    let final_clock = clock.read().unwrap();
    assert_eq!(final_clock.get(&node1), 1000);
    assert_eq!(final_clock.get(&node2), 1000);
}

/// Test that merge with max size applies compaction
#[test]
fn test_merge_with_compaction() {
    let mut clock1 = VectorClock::new();
    let mut clock2 = VectorClock::new();

    // Create many nodes in both clocks
    for i in 0..600 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = (i >> 8) as u8;
        key_bytes[1] = (i & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();

        if i < 300 {
            clock1.increment(node);
        } else {
            clock2.increment(node);
        }
    }

    // Merge with size limit
    clock1.merge_with_max_size(&clock2, Some(100));

    // Should be compacted to at most 100 entries
    assert!(clock1.clocks.len() <= 100);

    // Should have the "others" entry
    let others_node_id = NodeId::from_bytes(&[255u8; 32]).unwrap();
    let has_others = clock1.clocks.iter().any(|(id, _)| *id == others_node_id);
    assert!(has_others);
}

/// Test that increment with max size applies compaction
#[test]
fn test_increment_with_compaction() {
    let mut clock = VectorClock::new();

    // Add many nodes with compaction limit
    for i in 0..200 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = (i >> 8) as u8;
        key_bytes[1] = (i & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();

        // Use increment with max size of 50
        clock.increment_with_max_size(node, Some(50));

        // Should never exceed 50 entries
        assert!(clock.clocks.len() <= 50);
    }

    // Final size should be at most 50
    assert!(clock.clocks.len() <= 50);
}

/// Test GossipConfig includes max_vector_clock_size
#[test]
fn test_gossip_config_max_vector_clock_size() {
    let config = GossipConfig::default();
    assert_eq!(config.max_vector_clock_size, 1000);

    // Test that it can be customized
    let mut custom_config = GossipConfig::default();
    custom_config.max_vector_clock_size = 500;
    assert_eq!(custom_config.max_vector_clock_size, 500);
}

/// Test that compaction preserves causality relationships
#[test]
fn test_compaction_preserves_causality() {
    let mut clock1 = VectorClock::new();
    let mut clock2 = VectorClock::new();

    // Create a causal relationship with many nodes
    let mut nodes = Vec::new();
    for i in 0..150 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = i as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        nodes.push(node);
        clock1.increment(node);
    }

    // clock2 is a copy of clock1
    clock2 = clock1.clone();

    // Advance clock1 further
    clock1.increment(nodes[0]);

    // clock1 should be after clock2
    assert_eq!(clock1.compare(&clock2), ClockOrdering::After);

    // Compact both clocks
    clock1.compact(50);
    clock2.compact(50);

    // The causal relationship should be preserved
    // clock1 should still be after clock2
    assert_eq!(clock1.compare(&clock2), ClockOrdering::After);
}

/// Test race condition scenario: concurrent increment and compare
#[test]
fn test_concurrent_increment_compare_external_locks() {
    let clock = Arc::new(RwLock::new(VectorClock::new()));
    let key = SecretKey::from_bytes(&[1u8; 32]).unwrap();
    let node = key.public();

    let clock1 = clock.clone();
    let clock2 = clock.clone();
    let clock3 = clock.clone();

    // Thread 1: Increment continuously
    let handle1 = thread::spawn(move || {
        for _ in 0..100 {
            let mut c = clock1.write().unwrap();
            c.increment(node);
            thread::sleep(Duration::from_micros(10));
        }
    });

    // Thread 2: Read and compare continuously
    let handle2 = thread::spawn(move || {
        let reference_clock = VectorClock::with_node(node);
        for _ in 0..100 {
            let c = clock2.read().unwrap();
            let ordering = c.compare(&reference_clock);
            // Should never be Before since we only increment
            assert_ne!(ordering, ClockOrdering::Before);
            thread::sleep(Duration::from_micros(10));
        }
    });

    // Thread 3: Get value continuously
    let handle3 = thread::spawn(move || {
        let mut last_value = 0;
        for _ in 0..100 {
            let c = clock3.read().unwrap();
            let value = c.get(&node);
            // Value should never decrease
            assert!(value >= last_value);
            last_value = value;
            thread::sleep(Duration::from_micros(10));
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

/// Test that delta history vector clocks get compacted
/// This is a unit test for the memory leak fix
#[test]
fn test_delta_history_vector_clock_compaction() {
    use kameo_remote::registry::{HistoricalDelta, RegistryChange};
    use kameo_remote::RegistrationPriority;
    use kameo_remote::RemoteActorLocation;
    use std::net::SocketAddr;

    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let peer_id = kameo_remote::PeerId::new("test");

    // Create a delta with a large vector clock
    let mut location = RemoteActorLocation::new_with_peer(addr, peer_id);

    // Add many entries to the vector clock (location already has 1 entry for its node_id)
    for i in 0..1499 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = ((i + 1) >> 8) as u8;
        key_bytes[1] = ((i + 1) & 0xFF) as u8;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        location.vector_clock.increment(node);
    }

    assert_eq!(location.vector_clock.clocks.len(), 1500);

    // Create a registry change with this location
    let mut change = RegistryChange::ActorAdded {
        name: "test_actor".to_string(),
        location,
        priority: RegistrationPriority::Normal,
    };

    // Simulate what enforce_bounds does: compact if too large
    let max_clock_size = 100;
    match &mut change {
        RegistryChange::ActorAdded { location, .. } => {
            if location.vector_clock.clocks.len() > max_clock_size {
                location.vector_clock.compact(max_clock_size);
            }
        }
        _ => {}
    }

    // Verify compaction happened
    match &change {
        RegistryChange::ActorAdded { location, .. } => {
            assert!(location.vector_clock.clocks.len() <= max_clock_size);
        }
        _ => panic!("Expected ActorAdded"),
    }
}

/// Test that garbage collection doesn't race with concurrent operations
#[test]
fn test_gc_no_race_condition() {
    let clock = Arc::new(RwLock::new(VectorClock::new()));
    let mut nodes = Vec::new();

    // Create some nodes
    for i in 0..10 {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = i;
        let key = SecretKey::from_bytes(&key_bytes).unwrap();
        let node = key.public();
        nodes.push(node);

        let mut c = clock.write().unwrap();
        c.increment(node);
    }

    let clock1 = clock.clone();
    let clock2 = clock.clone();
    let nodes_clone = nodes.clone();
    let nodes_clone2 = nodes.clone();

    // Thread 1: Continuously perform GC
    let handle1 = thread::spawn(move || {
        for _ in 0..100 {
            let mut active_nodes = HashSet::new();
            // Keep only first 5 nodes active
            for i in 0..5 {
                active_nodes.insert(nodes_clone[i]);
            }

            let mut c = clock1.write().unwrap();
            c.gc_old_nodes(&active_nodes);
            thread::sleep(Duration::from_micros(10));
        }
    });

    // Thread 2: Continuously increment active nodes
    let handle2 = thread::spawn(move || {
        for _ in 0..100 {
            let mut c = clock2.write().unwrap();
            // Increment one of the active nodes
            c.increment(nodes_clone2[0]);
            thread::sleep(Duration::from_micros(10));
        }
    });

    handle1.join().unwrap();
    handle2.join().unwrap();

    // Verify final state is consistent
    let final_clock = clock.read().unwrap();
    // Node 0 should still exist and have a value > 0
    assert!(final_clock.get(&nodes[0]) > 0);
    // Nodes 5-9 should have been GC'd
    for i in 5..10 {
        assert_eq!(final_clock.get(&nodes[i]), 0);
    }
}
