use kameo_remote::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn debug_timing_variations() {
    println!("🔍 Debugging Timing Variations");

    // Setup two nodes with bootstrap completion
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:25101".parse().unwrap();
    let node2_addr = "127.0.0.1:25102".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    // Give a moment for connections to stabilize
    sleep(Duration::from_millis(50)).await;

    println!("📊 Testing 10 sequential registrations to capture timing variations:");

    for i in 0..10 {
        let actor_name = format!("test_actor_{}", i);
        let start_time = std::time::Instant::now();

        node1
            .register_urgent(
                actor_name.clone(),
                format!("127.0.0.1:2610{}", i).parse().unwrap(),
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();

        // Wait for propagation
        let mut propagated = false;
        while !propagated && start_time.elapsed() < Duration::from_secs(2) {
            if node2.lookup(&actor_name).await.is_some() {
                propagated = true;
                let total_time = start_time.elapsed();
                println!("  Registration {}: {:?}", i, total_time);
            } else {
                sleep(Duration::from_micros(10)).await;
            }
        }

        if !propagated {
            println!("  Registration {}: TIMEOUT", i);
        }

        // Small delay between registrations
        sleep(Duration::from_millis(10)).await;
    }

    node1.shutdown().await;
    node2.shutdown().await;
}
