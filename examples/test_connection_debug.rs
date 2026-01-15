use kameo_remote::{GossipConfig, GossipRegistryHandle};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // Initialize logging with debug level
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=debug")
        .init();

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(10),
        ..Default::default()
    };

    // Start Node A
    println!("Starting Node A on 127.0.0.1:8001...");
    let handle_a = GossipRegistryHandle::new(
        "127.0.0.1:8001".parse().unwrap(),
        vec![], // No peers initially
        Some(config.clone()),
    )
    .await
    .expect("Failed to create node A");
    println!("Node A started");

    // Wait a bit
    sleep(Duration::from_secs(1)).await;

    // Start Node B
    println!("Starting Node B on 127.0.0.1:8002...");
    let handle_b = GossipRegistryHandle::new(
        "127.0.0.1:8002".parse().unwrap(),
        vec!["127.0.0.1:8001".parse().unwrap()], // Connect to Node A
        Some(config),
    )
    .await
    .expect("Failed to create node B");
    println!("Node B started");

    // Monitor stats
    for i in 0..10 {
        sleep(Duration::from_secs(2)).await;

        let stats_a = handle_a.stats().await;
        let stats_b = handle_b.stats().await;

        println!("\n=== Round {} ===", i + 1);
        println!(
            "Node A - Active: {}, Failed: {}",
            stats_a.active_peers, stats_a.failed_peers
        );
        println!(
            "Node B - Active: {}, Failed: {}",
            stats_b.active_peers, stats_b.failed_peers
        );

        // Check connections
        let pool_a = handle_a.registry.connection_pool.lock().await;
        let pool_b = handle_b.registry.connection_pool.lock().await;

        println!("Node A connections: {}", pool_a.connection_count());
        println!("Node B connections: {}", pool_b.connection_count());
    }
}
