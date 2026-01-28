use kameo_remote::*;
use std::time::Duration;

#[tokio::test]
async fn test_simple_local_lookup() {
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(300),
        ..Default::default()
    };
    
    let key_pair = KeyPair::new_for_testing("test");
    let handle = GossipRegistryHandle::new_with_keypair(
        "127.0.0.1:0".parse().unwrap(),
        key_pair,
        Some(config),
    ).await.unwrap();
    
    let addr = handle.registry.bind_addr;
    
    // Register a local actor
    handle.register_urgent(
        "test_actor".to_string(),
        addr,
        RegistrationPriority::Immediate,
    ).await.unwrap();
    
    // Try to look it up
    let result = handle.lookup("test_actor").await;
    
    match result {
        Some(actor) => println!("✅ Found actor at {}", actor.location.address),
        None => println!("❌ Actor not found!"),
    }
    
    handle.shutdown().await;
}
