use kameo_remote::{ActorLocation, GossipConfig, GossipRegistryHandle, RegistrationPriority};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_multi_node_gossip_propagation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();

    let node1_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node2_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node3_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Start three nodes
    let node1 = GossipRegistryHandle::new(node1_addr, vec![], None).await?;
    let node1_actual = node1.registry.bind_addr;

    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_actual], None).await?;
    let node2_actual = node2.registry.bind_addr;

    let node3 = GossipRegistryHandle::new(node3_addr, vec![node1_actual, node2_actual], None).await?;

    // Wait for bootstrap
    sleep(Duration::from_millis(500)).await;

    // Register actors on different nodes
    node1.register("actor1".to_string(), "127.0.0.1:9001".parse()?).await?;
    node2.register("actor2".to_string(), "127.0.0.1:9002".parse()?).await?;
    node3.register("actor3".to_string(), "127.0.0.1:9003".parse()?).await?;

    // Wait for gossip propagation
    sleep(Duration::from_secs(2)).await;

    // Verify all nodes see all actors
    for node in [&node1, &node2, &node3] {
        assert!(node.lookup("actor1").await.is_some());
        assert!(node.lookup("actor2").await.is_some());
        assert!(node.lookup("actor3").await.is_some());
    }

    // Verify stats
    let stats1 = node1.stats().await;
    let stats2 = node2.stats().await;
    let stats3 = node3.stats().await;

    assert_eq!(stats1.known_actors, 3);
    assert_eq!(stats2.known_actors, 3);
    assert_eq!(stats3.known_actors, 3);

    Ok(())
}

#[tokio::test]
async fn test_actor_update_propagation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();

    let node1_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node2_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Start two nodes
    let node1 = GossipRegistryHandle::new(node1_addr, vec![], None).await?;
    let node1_actual = node1.registry.bind_addr;

    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_actual], None).await?;

    // Wait for bootstrap
    sleep(Duration::from_millis(500)).await;

    // Register actor on node1
    node1.register("actor1".to_string(), "127.0.0.1:9001".parse()?).await?;

    // Wait for propagation
    sleep(Duration::from_millis(500)).await;

    // Verify node2 sees the actor
    let location = node2.lookup("actor1").await;
    assert!(location.is_some());
    assert_eq!(location.unwrap().address, "127.0.0.1:9001".parse::<SocketAddr>()?);

    // Update the actor location
    node1.register("actor1".to_string(), "127.0.0.1:9999".parse()?).await?;

    // Wait for update propagation
    sleep(Duration::from_millis(500)).await;

    // Verify node2 sees the updated location
    let updated_location = node2.lookup("actor1").await;
    assert!(updated_location.is_some());
    assert_eq!(updated_location.unwrap().address, "127.0.0.1:9999".parse::<SocketAddr>()?);

    Ok(())
}

#[tokio::test]
async fn test_actor_removal_propagation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();

    let node1_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node2_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Start two nodes
    let node1 = GossipRegistryHandle::new(node1_addr, vec![], None).await?;
    let node1_actual = node1.registry.bind_addr;

    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_actual], None).await?;

    // Wait for bootstrap
    sleep(Duration::from_millis(500)).await;

    // Register and then unregister actor on node1
    node1.register("actor1".to_string(), "127.0.0.1:9001".parse()?).await?;
    
    // Wait for propagation
    sleep(Duration::from_millis(500)).await;
    
    // Verify node2 sees the actor
    assert!(node2.lookup("actor1").await.is_some());

    // Unregister the actor
    node1.unregister("actor1").await?;

    // Wait for removal propagation
    sleep(Duration::from_millis(500)).await;

    // Verify node2 no longer sees the actor
    assert!(node2.lookup("actor1").await.is_none());

    Ok(())
}