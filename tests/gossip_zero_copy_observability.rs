mod common;

use common::{connect_bidirectional, create_tls_node, wait_for_condition};
use kameo_remote::{telemetry::gossip_zero_copy, GossipConfig};
use std::time::Duration;

#[tokio::test]
async fn telemetry_tracks_zero_copy_frames() -> Result<(), Box<dyn std::error::Error>> {
    gossip_zero_copy::reset();

    let config = GossipConfig {
        gossip_interval: Duration::from_millis(150),
        ..Default::default()
    };

    let node_a = create_tls_node(config.clone()).await?;
    let node_b = create_tls_node(config.clone()).await?;
    connect_bidirectional(&node_a, &node_b).await?;

    node_a
        .register("actor.telemetry".to_string(), "127.0.0.1:9301".parse()?)
        .await?;

    assert!(
        wait_for_condition(Duration::from_secs(3), || async {
            node_b.lookup("actor.telemetry").await.is_some()
        })
        .await,
        "actor should propagate to trigger gossip counters"
    );

    let snapshot = gossip_zero_copy::snapshot();
    assert!(
        snapshot.outbound_frames > 0,
        "expected outbound zero-copy frames to be recorded"
    );
    assert!(
        snapshot.inbound_frames > 0,
        "expected inbound zero-copy frames to be recorded"
    );

    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
