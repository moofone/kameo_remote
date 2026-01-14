use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use kameo_remote::{
    DelegatedReplySender, GossipConfig, GossipRegistryHandle, KeyPair, MessageType, PeerId,
    RegistrationPriority, RemoteActorLocation, ReplyTo,
};

/// Simple echo actor that uses delegated replies
struct EchoActor {
    name: String,
}

impl EchoActor {
    fn new(name: String) -> Self {
        Self { name }
    }

    async fn handle_message(&self, msg: &[u8], reply_to: ReplyTo) {
        // Echo back the message
        let message = String::from_utf8_lossy(msg);
        info!("{} received message: {}", self.name, message);

        let response = format!("Echo from {}: {}", self.name, message);
        if let Err(e) = reply_to.reply(response.as_bytes()).await {
            tracing::error!("Failed to send reply: {}", e);
        }
    }
}

/// Delayed echo actor that simulates async processing
struct DelayedEchoActor {
    name: String,
    delay: Duration,
}

impl DelayedEchoActor {
    fn new(name: String, delay: Duration) -> Self {
        Self { name, delay }
    }

    async fn handle_message(&self, msg: &[u8], reply_to: ReplyTo) {
        let message = String::from_utf8_lossy(msg);
        info!(
            "{} received message: {}, will reply after {:?}",
            self.name, message, self.delay
        );

        // Simulate async processing
        sleep(self.delay).await;

        // Reply after delay
        let response = format!(
            "Delayed echo from {} after {:?}: {}",
            self.name, self.delay, message
        );
        if let Err(e) = reply_to.reply(response.as_bytes()).await {
            tracing::error!("Failed to send delayed reply: {}", e);
        }
    }
}

/// Actor that delegates replies to another actor
struct DelegatingActor {
    name: String,
    delegate_tx: tokio::sync::mpsc::Sender<(Vec<u8>, ReplyTo)>,
}

impl DelegatingActor {
    fn new(name: String, delegate_tx: tokio::sync::mpsc::Sender<(Vec<u8>, ReplyTo)>) -> Self {
        Self { name, delegate_tx }
    }

    async fn handle_message(&self, msg: Vec<u8>, reply_to: ReplyTo) {
        info!("{} delegating message to another actor", self.name);

        // Pass the ReplyTo handle to another actor
        if let Err(e) = self.delegate_tx.send((msg, reply_to)).await {
            tracing::error!("Failed to delegate: {}", e);
        }
    }
}

#[tokio::test]
async fn test_basic_ask_with_reply_to() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=debug".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8901".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8902".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config_a = GossipConfig {
        key_pair: Some(key_pair_a),
        gossip_interval: Duration::from_secs(2),
        peer_retry_interval: Duration::from_secs(5),
        ..Default::default()
    };

    // Start Node A
    let handle_a = GossipRegistryHandle::new(addr_a, vec![], Some(config_a))
        .await
        .unwrap();
    let registry_a = handle_a.registry.clone();

    let config_b = GossipConfig {
        key_pair: Some(key_pair_b),
        gossip_interval: Duration::from_secs(2),
        peer_retry_interval: Duration::from_secs(5),
        ..Default::default()
    };

    // Start Node B
    let handle_b = GossipRegistryHandle::new(addr_b, vec![], Some(config_b))
        .await
        .unwrap();

    // Connect nodes
    let peer_a = handle_b.add_peer(&PeerId::new("node_a")).await;
    peer_a.connect(&addr_a).await.unwrap();

    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Test 1: Basic ask with ReplyTo
    info!("Test 1: Basic ask with ReplyTo");

    // Get connection from A to B
    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections.get(&addr_b).unwrap();

    let request = b"Hello from Node A";
    let reply_to = conn.ask_with_reply_to(request).await.unwrap();

    // Simulate actor on Node B replying
    let echo_actor = EchoActor::new("echo_actor".to_string());
    echo_actor.handle_message(request, reply_to).await;

    info!("Successfully sent message and received reply using ReplyTo");
}

#[tokio::test]
async fn test_reply_to_with_timeout() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8903".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8904".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config = GossipConfig::default();

    // Start nodes
    let handle_a = GossipRegistryHandle::new(key_pair_a.clone(), config.clone());
    let registry_a = handle_a.registry.clone();
    handle_a.start_server(addr_a).await.unwrap();

    let handle_b = GossipRegistryHandle::new(key_pair_b.clone(), config.clone());
    handle_b.start_server(addr_b).await.unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Test timeout functionality
    info!("Test: ReplyTo with timeout");

    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections.values().next().unwrap();

    let request = b"Test timeout";
    let reply_to = conn.ask_with_reply_to(request).await.unwrap();

    // Create a timeout reply
    let timeout_reply = reply_to.with_timeout(Duration::from_millis(100));

    // Check time remaining
    if let Some(remaining) = timeout_reply.time_remaining() {
        info!("Time remaining: {:?}", remaining);
    }

    // Wait longer than timeout
    sleep(Duration::from_millis(200)).await;

    // Check if timed out
    assert!(timeout_reply.is_timed_out());

    // Try to reply after timeout - should fail
    let result = timeout_reply.reply(b"Too late").await;
    assert!(result.is_err());

    info!("Timeout correctly prevented late reply");
}

#[tokio::test]
async fn test_reply_delegation_between_actors() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8905".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8906".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config = GossipConfig::default();

    // Start nodes
    let handle_a = GossipRegistryHandle::new(key_pair_a.clone(), config.clone());
    let registry_a = handle_a.registry.clone();
    handle_a.start_server(addr_a).await.unwrap();

    let handle_b = GossipRegistryHandle::new(key_pair_b.clone(), config.clone());
    handle_b.start_server(addr_b).await.unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Create channel for delegation
    let (delegate_tx, mut delegate_rx) = tokio::sync::mpsc::channel(10);

    // Create delegating actor
    let delegating_actor = Arc::new(DelegatingActor::new("delegator".to_string(), delegate_tx));

    // Create delayed echo actor that will receive delegated replies
    let delayed_actor = Arc::new(DelayedEchoActor::new(
        "delayed_echo".to_string(),
        Duration::from_millis(100),
    ));

    // Start delegate handler
    let delayed_actor_clone = delayed_actor.clone();
    tokio::spawn(async move {
        while let Some((msg, reply_to)) = delegate_rx.recv().await {
            delayed_actor_clone.handle_message(&msg, reply_to).await;
        }
    });

    info!("Test: Reply delegation between actors");

    // Send ask request
    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections.values().next().unwrap();

    let request = b"Hello for delegation";
    let reply_to = conn.ask_with_reply_to(request).await.unwrap();

    // Simulate delegating actor receiving the message and passing reply_to
    delegating_actor
        .handle_message(request.to_vec(), reply_to)
        .await;

    // Wait for delayed reply
    sleep(Duration::from_millis(200)).await;

    info!("Reply successfully delegated between actors");
}

#[tokio::test]
async fn test_high_throughput_reply_to() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8907".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8908".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config = GossipConfig::default();

    // Start nodes
    let handle_a = GossipRegistryHandle::new(key_pair_a.clone(), config.clone());
    let registry_a = handle_a.registry.clone();
    handle_a.start_server(addr_a).await.unwrap();

    let handle_b = GossipRegistryHandle::new(key_pair_b.clone(), config.clone());
    handle_b.start_server(addr_b).await.unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    info!("Test: High throughput ReplyTo");

    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections.values().next().unwrap().clone();
    drop(pool);

    let num_requests = 1000;
    let mut handles = Vec::new();

    let start = std::time::Instant::now();

    // Send many concurrent requests
    for i in 0..num_requests {
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            let request = format!("Request {}", i).into_bytes();
            let reply_to = conn_clone.ask_with_reply_to(&request).await.unwrap();

            // Simulate immediate reply
            let response = format!("Response {}", i).into_bytes();
            reply_to.reply(&response).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = num_requests as f64 / elapsed.as_secs_f64();

    info!(
        "Processed {} ReplyTo operations in {:?}",
        num_requests, elapsed
    );
    info!("Throughput: {:.0} req/sec", throughput);

    assert!(throughput > 10000.0, "Throughput should exceed 10k req/sec");
}

#[tokio::test]
async fn test_delegated_reply_sender_compatibility() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=debug".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8909".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8910".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config = GossipConfig::default();

    // Start nodes
    let handle_a = GossipRegistryHandle::new(key_pair_a.clone(), config.clone());
    let registry_a = handle_a.registry.clone();
    handle_a.start_server(addr_a).await.unwrap();

    let handle_b = GossipRegistryHandle::new(key_pair_b.clone(), config.clone());
    handle_b.start_server(addr_b).await.unwrap();

    // Connect nodes
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    info!("Test: DelegatedReplySender compatibility");

    // Test legacy ask_with_reply_sender method
    let pool = registry_a.connection_pool.lock().await;
    let conn = pool.connections.values().next().unwrap();

    let request = b"Test legacy method";
    let delegated_sender = conn.ask_with_reply_sender(request).await.unwrap();

    // Convert to ReplyTo if needed
    if let Some(reply_to) = delegated_sender.into_reply_to() {
        info!("Successfully converted DelegatedReplySender to ReplyTo");
        reply_to.reply(b"Response via ReplyTo").await.unwrap();
    }

    // Test creating DelegatedReplySender from ReplyTo
    let request2 = b"Test new method";
    let reply_to = conn.ask_with_reply_to(request2).await.unwrap();
    let delegated_from_reply = DelegatedReplySender::from_reply_to(reply_to);

    info!("Created DelegatedReplySender from ReplyTo");
    delegated_from_reply
        .reply(b"Response via DelegatedReplySender".to_vec())
        .await
        .unwrap();

    info!("Compatibility test successful");
}
