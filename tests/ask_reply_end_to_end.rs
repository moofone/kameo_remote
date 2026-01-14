use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use kameo_remote::{GossipConfig, GossipRegistryHandle, KeyPair, PeerId, ReplyTo};

/// Custom message handler that processes Ask messages
struct MessageHandler {
    name: String,
    // Store ReplyTo handles for processing
    pending_requests: Arc<Mutex<Vec<(Vec<u8>, ReplyTo)>>>,
}

impl MessageHandler {
    fn new(name: String) -> Self {
        Self {
            name,
            pending_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn add_request(&self, request: Vec<u8>, reply_to: ReplyTo) {
        let mut pending = self.pending_requests.lock().await;
        pending.push((request, reply_to));
    }

    async fn process_all(&self) {
        let mut pending = self.pending_requests.lock().await;
        while let Some((request, reply_to)) = pending.pop() {
            info!(
                "{} processing request: {:?}",
                self.name,
                String::from_utf8_lossy(&request)
            );

            // Create a custom response based on the request
            let response = format!(
                "Processed by {}: {}",
                self.name,
                String::from_utf8_lossy(&request)
            );

            // Send the response back over the TCP socket
            if let Err(e) = reply_to.reply(response.as_bytes()).await {
                tracing::error!("Failed to send reply: {}", e);
            } else {
                info!("{} sent response back over TCP", self.name);
            }
        }
    }
}

/// Test true end-to-end ask/reply over TCP sockets
#[tokio::test]
async fn test_end_to_end_ask_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=info".parse().unwrap()),
        ))
        .try_init();

    // Create two nodes
    let addr_a: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    let key_pair_a = KeyPair::new_for_testing("node_a");
    let key_pair_b = KeyPair::new_for_testing("node_b");

    let config_a = GossipConfig {
        key_pair: Some(key_pair_a),
        ..Default::default()
    };

    let config_b = GossipConfig {
        key_pair: Some(key_pair_b),
        ..Default::default()
    };

    // Start nodes
    let handle_a = GossipRegistryHandle::new(addr_a, vec![], Some(config_a))
        .await
        .unwrap();

    let handle_b = GossipRegistryHandle::new(addr_b, vec![], Some(config_b))
        .await
        .unwrap();

    // Connect nodes - both directions for bidirectional communication
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;
    peer_b.connect(&addr_b).await.unwrap();

    let peer_a = handle_b.add_peer(&PeerId::new("node_a")).await;
    peer_a.connect(&addr_a).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    info!("=== Test 1: Basic ask/reply with mock response ===");
    {
        let conn = handle_a.get_connection(addr_b).await.unwrap();

        let request = b"Hello from test";
        let start = std::time::Instant::now();
        let response = conn.ask(request).await.unwrap();
        let elapsed = start.elapsed();

        info!(
            "Got response in {:?}: {:?}",
            elapsed,
            String::from_utf8_lossy(&response)
        );
        assert_eq!(response, b"RESPONSE:15"); // Mock response with request length
    }

    info!("=== Test 2: Multiple concurrent asks with correlation tracking ===");
    {
        let conn = handle_a.get_connection(addr_b).await.unwrap();

        let mut handles = Vec::new();
        let start = std::time::Instant::now();

        for i in 0..10 {
            let conn_clone = conn.clone();
            let handle = tokio::spawn(async move {
                let request = format!("Concurrent request {}", i).into_bytes();
                let response = conn_clone.ask(&request).await.unwrap();
                (i, response, request)
            });
            handles.push(handle);
        }

        // Verify all responses are correctly correlated
        for handle in handles {
            let (i, response, request) = handle.await.unwrap();
            let expected = format!("RESPONSE:{}", request.len()).into_bytes();
            assert_eq!(response, expected);
            info!("Request {} correctly correlated", i);
        }

        let elapsed = start.elapsed();
        info!("Processed 10 concurrent asks in {:?}", elapsed);
    }

    info!("=== Test 3: Delegated reply pattern ===");
    {
        // This simulates the pattern where:
        // 1. Node A sends an ask to Node B
        // 2. Node B's connection handler creates a ReplyTo
        // 3. Node B passes the ReplyTo to an actor for processing
        // 4. The actor sends the response back through the ReplyTo

        let conn = handle_a.get_connection(addr_b).await.unwrap();
        let handler_b = MessageHandler::new("NodeB_Handler".to_string());

        // Simulate what would happen on Node B:
        // When an Ask arrives, the connection handler would create a ReplyTo
        // and pass it to an actor. We'll simulate this with ask_with_reply_to

        let request = b"Process this request";

        // On Node A: Send the ask and wait for response
        let response_future = {
            let conn_clone = conn.clone();
            let request = request.to_vec();
            tokio::spawn(async move { conn_clone.ask(&request).await })
        };

        // Give the message time to arrive at Node B
        sleep(Duration::from_millis(10)).await;

        // On Node B: The automatic mock response will be sent
        // In a real system, you would intercept the Ask message and
        // route it to an actor with the ReplyTo handle

        let response = response_future.await.unwrap().unwrap();
        info!("Got response: {:?}", String::from_utf8_lossy(&response));
        assert_eq!(response, b"RESPONSE:20"); // Mock response
    }

    info!("=== Test 4: Performance test ===");
    {
        let conn = handle_a.get_connection(addr_b).await.unwrap();

        let num_requests = 100;
        let start = std::time::Instant::now();

        for i in 0..num_requests {
            let request = format!("Perf test {}", i).into_bytes();
            let response = conn.ask(&request).await.unwrap();
            assert!(response.starts_with(b"RESPONSE:"));
        }

        let elapsed = start.elapsed();
        let throughput = num_requests as f64 / elapsed.as_secs_f64();

        info!("Sequential: {} requests in {:?}", num_requests, elapsed);
        info!("Throughput: {:.0} req/sec", throughput);
    }

    // Shutdown
    handle_a.shutdown().await;
    handle_b.shutdown().await;
}

/// Test timeout handling
#[tokio::test]
async fn test_ask_timeout() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::from_default_env().add_directive("kameo_remote=warn".parse().unwrap()),
        ))
        .try_init();

    // Create a node but don't start the second node
    let addr_a: SocketAddr = "127.0.0.1:8003".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8004".parse().unwrap(); // This won't exist

    let config = GossipConfig {
        key_pair: Some(KeyPair::new_for_testing("node_timeout")),
        ..Default::default()
    };

    let handle_a = GossipRegistryHandle::new(addr_a, vec![], Some(config))
        .await
        .unwrap();

    // Try to connect to non-existent node
    let peer_b = handle_a.add_peer(&PeerId::new("node_b")).await;

    // Connection should fail
    match peer_b.connect(&addr_b).await {
        Err(e) => info!("Expected connection failure: {}", e),
        Ok(_) => panic!("Should not connect to non-existent node"),
    }

    handle_a.shutdown().await;
}
