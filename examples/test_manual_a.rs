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

    println!("Starting Node A on 127.0.0.1:8001...");
    let handle = GossipRegistryHandle::new(
        "127.0.0.1:8001".parse().unwrap(), 
        vec!["127.0.0.1:8002".parse().unwrap()], // Connect to Node B
        Some(config)
    )
        .await
        .expect("Failed to create node A");

    println!("Node A started at {}", handle.registry.bind_addr);
    println!("Node A is ready to accept connections from Node B");

    // Register a local actor with immediate priority
    handle
        .register_with_priority(
            "actor_a".to_string(), 
            "127.0.0.1:9001".parse().unwrap(),
            RegistrationPriority::Immediate
        )
        .await
        .expect("Failed to register actor");
    println!("Registered actor_a on Node A with immediate priority");

    // Monitor peer status
    loop {
        sleep(Duration::from_secs(2)).await;
        let stats = handle.stats().await;
        println!(
            "Node A - Active peers: {}, Failed peers: {}, Known actors: {}",
            stats.active_peers, stats.failed_peers, stats.known_actors
        );

        // Check if we can see actor_b from Node B
        // if let Some(actor_b) = handle.lookup("actor_b").await {
        //     println!("  ✓ Can see actor_b at {}", actor_b.address);
        // }
    }
}
