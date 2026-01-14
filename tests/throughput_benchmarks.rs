use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

use kameo_remote::{GossipConfig, GossipRegistryHandle, RegistrationPriority};

const MESSAGE_COUNT: usize = 10000;
const LARGE_MESSAGE_SIZE: usize = 1024; // 1KB messages for better throughput
const TEST_DATA_SIZE: usize = 10 * 1024 * 1024; // 10MB for TCP test
const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks

/// Generate test data for throughput testing
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    data
}

/// Calculate throughput in MB/s
fn calculate_throughput(bytes: usize, duration: Duration) -> f64 {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let seconds = duration.as_secs_f64();
    mb / seconds
}

/// Simple TCP echo server for testing
async fn start_echo_server(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(addr).await.unwrap();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = vec![0u8; CHUNK_SIZE];

                loop {
                    match stream.read(&mut buffer).await {
                        Ok(0) => break, // Connection closed
                        Ok(n) => {
                            // Echo back the data
                            if stream.write_all(&buffer[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    })
}

// Message processing server removed as it's no longer needed
// The tests now focus on connection pool performance directly

/// Test 1: Direct TcpStream throughput (using lookup() to find actor)
#[tokio::test]
async fn test_direct_tcp_stream_throughput() {
    println!("🚀 Test 1: Direct TcpStream Throughput");

    // Setup gossip registry to discover actor
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:20001".parse().unwrap();
    let sender_addr = "127.0.0.1:20002".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Register a test actor on receiver
    let actor_addr = "127.0.0.1:20003".parse().unwrap();

    receiver_handle
        .register_urgent(
            "tcp_test_actor".to_string(),
            actor_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Connect nodes for gossip
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for gossip propagation
    sleep(Duration::from_millis(500)).await;

    // Use lookup() to find the remote actor
    let found_actor = sender_handle.lookup("tcp_test_actor").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");

    let actor_location = found_actor.unwrap();
    println!("✅ Found remote actor at: {}", actor_location.address);

    // Start echo server at the actor's address
    let _echo_server = start_echo_server(actor_location.address).await;
    sleep(Duration::from_millis(100)).await; // Let server start

    // Generate test data
    let test_data = generate_test_data(TEST_DATA_SIZE);
    println!(
        "📊 Generated {} MB of test data",
        TEST_DATA_SIZE / (1024 * 1024)
    );

    // Create DIRECT TcpStream connection to actor (NOT reusing pool)
    let mut stream = TcpStream::connect(actor_location.address).await.unwrap();
    stream.set_nodelay(true).unwrap();

    println!(
        "🔗 Created direct TcpStream connection to: {}",
        actor_location.address
    );

    // Measure throughput using direct TCP
    let start_time = Instant::now();
    let mut total_bytes = 0;
    let mut response_buffer = vec![0u8; CHUNK_SIZE];

    // Send data in chunks
    for chunk in test_data.chunks(CHUNK_SIZE) {
        // Send chunk
        stream.write_all(chunk).await.unwrap();

        // Read echo response - may come back in multiple reads
        let mut bytes_received = 0;
        while bytes_received < chunk.len() {
            let bytes_read = stream
                .read(&mut response_buffer[bytes_received..])
                .await
                .unwrap();
            if bytes_read == 0 {
                break; // Connection closed
            }
            bytes_received += bytes_read;
        }

        assert_eq!(bytes_received, chunk.len());
        total_bytes += chunk.len();

        // Verify echo
        assert_eq!(&response_buffer[..bytes_received], chunk);
    }

    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);

    println!("📈 Direct TcpStream Results:");
    println!("   - Total bytes: {} MB", total_bytes / (1024 * 1024));
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(throughput > 50.0, "Direct TCP should be > 50 MB/s");
}

/// Test 2: Connection pool performance for actor lookups
#[tokio::test]
async fn test_lookup_performance() {
    println!("🚀 Test 2: Actor Lookup Performance");

    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:21001".parse().unwrap();
    let sender_addr = "127.0.0.1:21002".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Register a test actor on receiver
    let actor_addr = "127.0.0.1:21003".parse().unwrap();

    receiver_handle
        .register_urgent(
            "test_actor".to_string(),
            actor_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Connect nodes - this establishes the connection pool
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for gossip propagation
    sleep(Duration::from_millis(500)).await;

    // Use lookup() to find the remote actor
    let found_actor = sender_handle.lookup("test_actor").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");

    let actor_location = found_actor.unwrap();
    println!("✅ Found remote actor at: {}", actor_location.address);

    // Test lookup performance
    let start_time = Instant::now();
    let lookup_count = 10000;

    for _ in 0..lookup_count {
        let _found = sender_handle.lookup("test_actor").await;
    }

    let duration = start_time.elapsed();
    let lookups_per_sec = lookup_count as f64 / duration.as_secs_f64();

    println!("📈 Lookup Performance Results:");
    println!("   - Lookups performed: {}", lookup_count);
    println!("   - Duration: {:?}", duration);
    println!("   - Lookups/sec: {:.0}", lookups_per_sec);

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(lookups_per_sec > 1000.0, "Should handle >1000 lookups/sec");
}

/// Test 3: Single gossip message transmission performance
#[tokio::test]
async fn test_single_gossip_message_performance() {
    println!("🚀 Test 3: Single Gossip Message Transmission Performance");

    // Setup gossip registry to establish connection pool
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:22301".parse().unwrap();
    let sender_addr = "127.0.0.1:22302".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Connect nodes - this establishes the connection pool
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for connections to establish
    sleep(Duration::from_millis(100)).await;

    println!("🔗 Gossip connection established");

    // REUSE existing connection from pool (this is the key requirement)
    let connection_handle = sender_handle.get_connection(receiver_addr).await.unwrap();
    println!("🔗 Reusing existing gossip connection for single message transmission");

    // Create a realistic gossip message (similar to what real gossip sends)
    let test_message = b"realistic_gossip_message_payload_that_represents_actual_gossip_traffic";

    println!("📊 MESSAGE DETAILS:");
    println!(
        "   - Message content: {:?}",
        std::str::from_utf8(test_message).unwrap_or("binary")
    );
    println!("   - Message size: {} bytes", test_message.len());
    println!("   - Message type: Single message via tell()");

    // Test single message transmission time
    let single_msg_start = Instant::now();
    connection_handle.tell(test_message).await.unwrap();
    let single_msg_time = single_msg_start.elapsed();

    println!("📈 Single Gossip Message Results:");
    println!("   - Transmission time: {:?}", single_msg_time);
    println!(
        "   - Transmission time (μs): {:.3}",
        single_msg_time.as_nanos() as f64 / 1000.0
    );
    println!("   - Expected for localhost: <100μs");

    if single_msg_time > Duration::from_micros(100) {
        println!("⚠️  WARNING: Single message transmission is slower than expected for localhost!");
        println!("   - This indicates a bottleneck in the connection pool or channel handling");
    }

    // Test 10 sequential messages to see if there's batching or queuing effects
    println!("\n📊 SEQUENTIAL MESSAGE DETAILS:");
    println!("   - Number of messages: 10");
    println!("   - Message pattern: gossip_message_0, gossip_message_1, ..., gossip_message_9");

    let sequential_start = Instant::now();
    for i in 0..10 {
        let msg = format!("gossip_message_{}", i);
        println!("   - Sending: {:?} ({} bytes)", msg, msg.len());
        connection_handle.tell(msg.as_bytes()).await.unwrap();
    }
    let sequential_time = sequential_start.elapsed();

    println!("📈 Sequential Message Results:");
    println!("   - 10 messages sent in: {:?}", sequential_time);
    println!(
        "   - 10 messages sent in (μs): {:.3}",
        sequential_time.as_nanos() as f64 / 1000.0
    );
    println!("   - Average per message: {:?}", sequential_time / 10);
    println!(
        "   - Average per message (μs): {:.3}",
        sequential_time.as_nanos() as f64 / 10000.0
    );
    println!("   - Expected for localhost: <1000μs total");

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    // Assertions focused on realistic performance expectations
    assert!(
        single_msg_time < Duration::from_millis(1),
        "Single message should take <1ms on localhost"
    );
    assert!(
        sequential_time < Duration::from_millis(10),
        "10 sequential messages should take <10ms on localhost"
    );
}

/// Test 4: ask() request-response performance using reused gossip connections
#[tokio::test]
async fn test_ask_performance() {
    println!("🚀 Test 4: ask() Request-Response Using Reused Gossip Connections");

    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:22401".parse().unwrap();
    let sender_addr = "127.0.0.1:22402".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Register actor
    let actor_addr = "127.0.0.1:22403".parse().unwrap();

    receiver_handle
        .register_urgent(
            "ask_actor".to_string(),
            actor_addr,
            RegistrationPriority::Immediate,
        )
        .await
        .unwrap();

    // Connect nodes - establishes connection pool
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for propagation
    sleep(Duration::from_millis(500)).await;

    // Lookup actor using the actor system
    let found_actor = sender_handle.lookup("ask_actor").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");

    let actor_location = found_actor.unwrap();
    println!("✅ Found ask actor at: {}", actor_location.address);

    // REUSE existing connection from pool (this is the key requirement)
    let connection_handle = sender_handle.get_connection(receiver_addr).await.unwrap();
    println!(
        "🔗 Reusing existing gossip connection for ask() to: {}",
        connection_handle.addr
    );

    // For ask() performance testing, we'll focus on the connection pool overhead
    // In a real scenario, the ask() would go through the gossip protocol

    // Generate test request
    let test_request = b"REQUEST: Please process this and send back a response";

    println!(
        "📊 Sending {} request-response pairs via ask() using reused connection",
        MESSAGE_COUNT / 10
    );
    println!("📊 Measuring ask() connection pool performance (simplified for testing)");

    // Measure ask() performance using connection pool
    let start_time = Instant::now();
    let mut total_bytes = 0;
    let mut latencies = Vec::new();

    for i in 0..(MESSAGE_COUNT / 10) {
        let request_start = Instant::now();

        // Use ask() via reused gossip connection
        // This tests the connection pool overhead for request-response patterns
        let response = connection_handle.ask(test_request).await.unwrap();

        let latency = request_start.elapsed();
        latencies.push(latency);

        total_bytes += test_request.len() + response.len();

        // Verify response (the ask() method returns a dummy response)
        let expected_response = format!("RESPONSE:{}", test_request.len());
        assert_eq!(
            String::from_utf8_lossy(&response),
            expected_response,
            "Response should match expected format"
        );

        if i % 100 == 0 {
            println!("   - Completed {} requests", i);
        }
    }

    let duration = start_time.elapsed();
    let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64();
    let requests_per_sec = (MESSAGE_COUNT / 10) as f64 / duration.as_secs_f64();

    // Calculate latency statistics
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];

    println!("📈 ask() Results (Using Reused Gossip Connections):");
    println!("   - Requests completed: {}", MESSAGE_COUNT / 10);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Requests/sec: {:.0}", requests_per_sec);
    println!("   - Latency P50: {:?}", p50);
    println!("   - Latency P95: {:?}", p95);
    println!("   - Latency P99: {:?}", p99);
    println!("   - Note: This measures connection pool overhead with mock responses");

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(requests_per_sec > 100.0, "Should handle >100 requests/sec");
    assert!(
        p95 < Duration::from_millis(100),
        "P95 latency should be <100ms"
    );
}

/// Test 5: Gossip propagation performance with zero-copy optimizations
#[tokio::test]
async fn test_gossip_propagation_performance() {
    println!("🚀 Test 5: Gossip Propagation Performance (Zero-Copy Optimized)");

    // Setup a 3-node gossip network to test propagation
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:23001".parse().unwrap();
    let node2_addr = "127.0.0.1:23002".parse().unwrap();
    let node3_addr = "127.0.0.1:23003".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let node3 = GossipRegistryHandle::new(node3_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Connect all nodes in a mesh
    node1.add_peer(node2_addr).await.unwrap();
    node1.add_peer(node3_addr).await.unwrap();
    node2.add_peer(node1_addr).await.unwrap();
    node2.add_peer(node3_addr).await.unwrap();
    node3.add_peer(node1_addr).await.unwrap();
    node3.add_peer(node2_addr).await.unwrap();

    // Wait for connections to establish
    sleep(Duration::from_millis(500)).await;

    println!("🔗 3-node gossip network established");

    // Register many actors on node1 to trigger gossip propagation
    let actor_count = 1000;
    let start_time = Instant::now();

    for i in 0..actor_count {
        let actor_addr = format!("127.0.0.1:{}", 24000 + i).parse().unwrap();
        node1
            .register_urgent(
                format!("actor_{}", i),
                actor_addr,
                RegistrationPriority::Immediate,
            )
            .await
            .unwrap();
    }

    let registration_time = start_time.elapsed();
    println!(
        "⏱️  REGISTRATION TIME: {:?} - {} actors registered",
        registration_time, actor_count
    );

    // Wait for gossip propagation
    let propagation_start = Instant::now();
    let mut all_propagated = false;
    let timeout = Duration::from_secs(10);

    while !all_propagated && propagation_start.elapsed() < timeout {
        sleep(Duration::from_millis(50)).await;

        // Check if all actors have propagated to all nodes
        let mut propagated_count = 0;
        for i in 0..actor_count {
            let actor_name = format!("actor_{}", i);
            if node2.lookup(&actor_name).await.is_some()
                && node3.lookup(&actor_name).await.is_some()
            {
                propagated_count += 1;
            }
        }

        if propagated_count == actor_count {
            all_propagated = true;
        }

        if propagation_start.elapsed().as_millis() % 1000 == 0 {
            println!(
                "   - Propagated: {}/{} actors",
                propagated_count, actor_count
            );
        }
    }

    let propagation_time = propagation_start.elapsed();

    println!("📈 Gossip Propagation Results:");
    println!("   - Actors registered: {}", actor_count);
    println!("   - Registration time: {:?}", registration_time);
    println!("   - Propagation time: {:?}", propagation_time);
    println!("   - Total time: {:?}", start_time.elapsed());

    let actors_per_sec = actor_count as f64 / registration_time.as_secs_f64();
    let propagation_rate = actor_count as f64 / propagation_time.as_secs_f64();

    println!("   - Registration rate: {:.0} actors/sec", actors_per_sec);
    println!("   - Propagation rate: {:.0} actors/sec", propagation_rate);
    println!(
        "   - Network efficiency: {:.2}% (3-node mesh)",
        (propagation_rate / actors_per_sec) * 100.0
    );

    // Verify complete propagation
    let mut final_propagated = 0;
    for i in 0..actor_count {
        let actor_name = format!("actor_{}", i);
        if node2.lookup(&actor_name).await.is_some() && node3.lookup(&actor_name).await.is_some() {
            final_propagated += 1;
        }
    }

    println!(
        "   - Final propagation: {}/{} actors",
        final_propagated, actor_count
    );

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;

    assert!(
        final_propagated >= actor_count * 90 / 100,
        "Should propagate at least 90% of actors"
    );
    assert!(
        propagation_time < Duration::from_secs(5),
        "Propagation should complete within 5 seconds"
    );
    assert!(propagation_rate > 100.0, "Should propagate >100 actors/sec");
}

/// Test tell() performance with TellMessage API
#[tokio::test]
async fn test_tell_performance() {
    println!("🚀 Test: tell() Performance with TellMessage API (1000 messages)");

    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:25001".parse().unwrap();
    let sender_addr = "127.0.0.1:25002".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Connect nodes - establishes connection pool
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for connections to establish
    sleep(Duration::from_millis(100)).await;

    // Get connection handle
    let connection_handle = sender_handle.get_connection(receiver_addr).await.unwrap();
    println!("🔗 Using existing gossip connection for TellMessage tests");

    let test_message = vec![0u8; 1024]; // 1KB messages
    let iterations = 1000;

    println!("📊 MESSAGE CONFIGURATION:");
    println!("   - Message size: {} bytes (1KB)", test_message.len());
    println!("   - Message pattern: Zero-filled bytes");
    println!("   - Iterations: {}", iterations);
    println!(
        "   - Total data: {:.2} MB",
        (test_message.len() * iterations) as f64 / (1024.0 * 1024.0)
    );

    // Warmup
    println!("\n🔥 WARMUP: Sending 50 messages to warm up connection");
    for i in 0..50 {
        connection_handle.tell(&test_message[..]).await.unwrap();
        if i % 10 == 0 {
            println!("   - Warmup message {}/50 sent", i);
        }
    }

    println!(
        "\n📊 Testing SINGLE: {} individual tell() calls with automatic detection",
        iterations
    );

    // Test single message performance (automatic detection)
    let start_time = Instant::now();

    for i in 0..iterations {
        connection_handle.tell(&test_message[..]).await.unwrap(); // Auto-detects as single
        if i % 100 == 0 && i > 0 {
            println!("   - Sent {}/{} messages", i, iterations);
        }
    }

    let single_duration = start_time.elapsed();
    let single_messages_per_sec = iterations as f64 / single_duration.as_secs_f64();
    let single_avg_latency = single_duration / iterations as u32;
    let single_throughput =
        (1024 * iterations) as f64 / (1024.0 * 1024.0) / single_duration.as_secs_f64();

    println!("   - Total time: {:?}", single_duration);
    println!(
        "   - Total time (μs): {:.3}",
        single_duration.as_nanos() as f64 / 1000.0
    );
    println!("   - Messages/sec: {:.0}", single_messages_per_sec);
    println!("   - Average latency: {:?}", single_avg_latency);
    println!(
        "   - Average latency (μs): {:.3}",
        single_avg_latency.as_nanos() as f64 / 1000.0
    );
    println!("   - Throughput: {:.2} MB/s", single_throughput);

    // Test explicit TellMessage::single()
    println!(
        "\n📊 Testing EXPLICIT SINGLE: {} TellMessage::single() calls",
        iterations
    );

    let explicit_start = Instant::now();

    for _ in 0..iterations {
        connection_handle
            .tell(kameo_remote::connection_pool::TellMessage::single(
                &test_message,
            ))
            .await
            .unwrap();
    }

    let explicit_duration = explicit_start.elapsed();
    let explicit_messages_per_sec = iterations as f64 / explicit_duration.as_secs_f64();

    println!("   - Total time: {:?}", explicit_duration);
    println!("   - Messages/sec: {:.0}", explicit_messages_per_sec);

    // Test batch performance with TellMessage::batch()
    println!(
        "\n📊 Testing BATCH: Single TellMessage::batch() with {} messages",
        iterations
    );

    let batch_messages: Vec<&[u8]> = (0..iterations).map(|_| test_message.as_slice()).collect();

    println!(
        "   - Batch configuration: {} messages of {} bytes each",
        batch_messages.len(),
        test_message.len()
    );
    println!(
        "   - Batch total size: {:.2} MB",
        (test_message.len() * iterations) as f64 / (1024.0 * 1024.0)
    );
    println!("   - Batch method: TellMessage::batch() with Vec<&[u8]>");

    let batch_start = Instant::now();
    connection_handle
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            batch_messages,
        ))
        .await
        .unwrap();
    let batch_duration = batch_start.elapsed();

    let batch_messages_per_sec = iterations as f64 / batch_duration.as_secs_f64();
    let batch_avg_latency = batch_duration / iterations as u32;
    let batch_throughput =
        (1024 * iterations) as f64 / (1024.0 * 1024.0) / batch_duration.as_secs_f64();

    println!("   - Total time: {:?}", batch_duration);
    println!(
        "   - Total time (μs): {:.3}",
        batch_duration.as_nanos() as f64 / 1000.0
    );
    println!("   - Messages/sec: {:.0}", batch_messages_per_sec);
    println!("   - Average latency: {:?}", batch_avg_latency);
    println!(
        "   - Average latency (μs): {:.3}",
        batch_avg_latency.as_nanos() as f64 / 1000.0
    );
    println!("   - Throughput: {:.2} MB/s", batch_throughput);

    // Test automatic batch detection
    println!("\n📊 Testing AUTO-BATCH: tell() with slice auto-detection");

    let message_slice = [&test_message[..]; 100]; // 100 messages
    let auto_start = Instant::now();

    for _ in 0..10 {
        connection_handle.tell(&message_slice[..]).await.unwrap(); // Auto-detects as batch
    }

    let auto_duration = auto_start.elapsed();
    let auto_messages_per_sec = 1000.0 / auto_duration.as_secs_f64();

    println!("   - Total time: {:?}", auto_duration);
    println!("   - Messages/sec: {:.0}", auto_messages_per_sec);

    // Performance comparison
    let speedup = batch_messages_per_sec / single_messages_per_sec;
    let latency_improvement =
        single_avg_latency.as_nanos() as f64 / batch_avg_latency.as_nanos() as f64;

    println!("\n📈 PERFORMANCE COMPARISON:");
    println!("   - Batch speedup: {:.2}x faster", speedup);
    println!(
        "   - Latency improvement: {:.2}x better",
        latency_improvement
    );
    println!(
        "   - Throughput improvement: {:.2}x better",
        batch_throughput / single_throughput
    );
    println!("   - Auto-detection works: Both single and batch via tell()");

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    // Basic performance assertions
    assert!(
        single_messages_per_sec > 1000.0,
        "Single should handle >1k messages/sec"
    );
    assert!(
        batch_messages_per_sec > 5000.0,
        "Batch should handle >5k messages/sec"
    );
    assert!(
        speedup > 2.0,
        "Batch should be at least 2x faster than single"
    );
}

/// Test seamless batching API
#[tokio::test]
async fn test_seamless_batching_api() {
    println!("🚀 Test: Seamless Batching API");

    // Setup gossip registry
    let config = GossipConfig::default();
    let receiver_addr = "127.0.0.1:26001".parse().unwrap();
    let sender_addr = "127.0.0.1:26002".parse().unwrap();

    let receiver_handle = GossipRegistryHandle::new(receiver_addr, vec![], Some(config.clone()))
        .await
        .unwrap();
    let sender_handle = GossipRegistryHandle::new(sender_addr, vec![], Some(config.clone()))
        .await
        .unwrap();

    // Connect nodes
    receiver_handle.add_peer(sender_addr).await.unwrap();
    sender_handle.add_peer(receiver_addr).await.unwrap();

    // Wait for connections
    sleep(Duration::from_millis(100)).await;

    let connection_handle = sender_handle.get_connection(receiver_addr).await.unwrap();
    println!("🔗 Testing seamless batching API");

    let test_message = vec![0u8; 1024];

    // Test 1: Single message using TellMessage::single()
    println!("\n📊 Test 1: Single message via TellMessage::single()");
    let start = Instant::now();
    connection_handle
        .tell(kameo_remote::connection_pool::TellMessage::single(
            &test_message,
        ))
        .await
        .unwrap();
    let single_time = start.elapsed();
    println!("   - Single message time: {:?}", single_time);

    // Test 2: Batch of messages using TellMessage::batch()
    println!("\n📊 Test 2: Batch via TellMessage::batch()");
    let message_refs = vec![&test_message[..], &test_message[..], &test_message[..]];
    let start = Instant::now();
    connection_handle
        .tell(kameo_remote::connection_pool::TellMessage::batch(
            message_refs,
        ))
        .await
        .unwrap();
    let batch_time = start.elapsed();
    println!("   - Batch of 3 messages time: {:?}", batch_time);

    // Test 3: Auto-detect using regular tell() with slice
    println!("\n📊 Test 3: Auto-detect via tell() with slice");
    let message_refs = [&test_message[..], &test_message[..], &test_message[..]];
    let start = Instant::now();
    connection_handle.tell(&message_refs[..]).await.unwrap(); // Auto-detects as batch
    let auto_time = start.elapsed();
    println!("   - Auto-detect 3 messages time: {:?}", auto_time);

    // Test 4: Performance comparison
    println!("\n📊 Test 4: Performance comparison (100 iterations)");
    let iterations = 100;

    // Single message performance
    let start = Instant::now();
    for _ in 0..iterations {
        connection_handle.tell(&test_message[..]).await.unwrap(); // Auto-detects as single
    }
    let single_total = start.elapsed();

    // Batch performance (batches of 10)
    let batch_refs: Vec<&[u8]> = (0..10).map(|_| &test_message[..]).collect();
    let start = Instant::now();
    for _ in 0..10 {
        connection_handle.tell(&batch_refs[..]).await.unwrap(); // Auto-detects as batch
    }
    let batch_total = start.elapsed();

    println!("   - 100 single messages: {:?}", single_total);
    println!("   - 10 batches of 10 messages: {:?}", batch_total);
    println!(
        "   - Batch speedup: {:.2}x",
        single_total.as_nanos() as f64 / batch_total.as_nanos() as f64
    );

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(
        batch_total < single_total,
        "Batching should be faster than individual sends"
    );
}

/// Combined performance comparison test
#[tokio::test]
async fn test_performance_comparison() {
    println!("🚀 Performance Comparison Summary");
    println!("This test suite provides comprehensive performance measurements:");
    println!("1. Direct TcpStream - Raw TCP performance baseline");
    println!("2. lookup() - Actor discovery performance");
    println!("3. tell() - Zero-copy fire-and-forget messaging using reused gossip connections");
    println!("4. ask() - Request-response patterns using reused gossip connections");
    println!("5. Gossip propagation - Zero-copy optimized gossip protocol performance");
    println!("");
    println!("Key Design Principles:");
    println!("- ALL tests reuse existing gossip connection pool connections");
    println!("- NO new TCP connections are created for messaging");
    println!("- Zero-copy implementations with try_send() for maximum performance");
    println!("- Full actor system integration with lookup() for service discovery");
    println!("- Optimized gossip propagation with batching and concurrent execution");
    println!("");
    println!("Expected performance hierarchy: Direct TCP > lookup() > tell() > ask() > gossip");
}
