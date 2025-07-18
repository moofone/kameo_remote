use kameo_remote::{GossipConfig, GossipRegistryHandle, RegistrationPriority};
use std::time::Duration;
use tokio::time::sleep;

// Test configuration constant
const TEST_GOSSIP_INTERVAL_SECS: u64 = 10;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .init();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(TEST_GOSSIP_INTERVAL_SECS),
        ..Default::default()
    };

    println!("Starting Node B on 127.0.0.1:8002...");
    let handle = GossipRegistryHandle::new(
        "127.0.0.1:8002".parse().unwrap(),
        vec!["127.0.0.1:8001".parse().unwrap()], // Connect to Node A
        Some(config),
    )
    .await
    .expect("Failed to create node B");

    println!("Node B started at {}", handle.registry.bind_addr);
    println!("Node B connected to Node A at 127.0.0.1:8001");

    // Register a local actor with immediate priority for fast propagation
    handle
        .register_with_priority(
            "actor_b".to_string(),
            "127.0.0.1:9002".parse().unwrap(),
            RegistrationPriority::Immediate,
        )
        .await
        .expect("Failed to register actor");
    println!("Registered actor_b on Node B with immediate priority");

    // Monitor peer status
    loop {
        sleep(Duration::from_secs(2)).await;
        let stats = handle.stats().await;
        println!("Node B - Active peers: {}, Failed peers: {}, Known actors: {}", 
            stats.active_peers, stats.failed_peers, stats.known_actors);
    }
}
