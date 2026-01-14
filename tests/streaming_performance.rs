use kameo_remote::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Custom streaming test struct - optimized for serialization performance
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamData {
    pub id: u64,
    pub timestamp: u64,
    pub sequence: u32,
    pub payload: Vec<u8>,
}

impl StreamData {
    /// Create a small test data packet (64 bytes total)
    pub fn small(id: u64, sequence: u32) -> Self {
        Self {
            id,
            timestamp: kameo_remote::current_timestamp_nanos(),
            sequence,
            payload: vec![0u8; 32], // 32 bytes of payload
        }
    }

    /// Create a large test data packet (4KB total)
    pub fn large(id: u64, sequence: u32) -> Self {
        Self {
            id,
            timestamp: kameo_remote::current_timestamp_nanos(),
            sequence,
            payload: vec![0u8; 4000], // ~4KB of payload
        }
    }

    /// Create a medium test data packet (1KB total)
    pub fn medium(id: u64, sequence: u32) -> Self {
        Self {
            id,
            timestamp: kameo_remote::current_timestamp_nanos(),
            sequence,
            payload: vec![0u8; 1000], // ~1KB of payload
        }
    }

    /// Get the approximate size in bytes
    pub fn size(&self) -> usize {
        8 + 8 + 4 + self.payload.len() // id + timestamp + sequence + payload
    }
}

/// Performance metrics for streaming tests
#[derive(Debug, Default)]
pub struct StreamingMetrics {
    pub total_bytes_sent: u64,
    pub total_messages_sent: u64,
    pub total_duration: Duration,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub avg_latency: Duration,
    pub throughput_mbps: f64,
    pub messages_per_second: f64,
}

impl StreamingMetrics {
    pub fn new() -> Self {
        Self {
            min_latency: Duration::from_secs(u64::MAX),
            max_latency: Duration::from_nanos(0),
            ..Default::default()
        }
    }

    pub fn add_latency(&mut self, latency: Duration) {
        if latency < self.min_latency {
            self.min_latency = latency;
        }
        if latency > self.max_latency {
            self.max_latency = latency;
        }
    }

    pub fn finalize(&mut self) {
        if self.total_duration.as_nanos() > 0 {
            self.throughput_mbps = (self.total_bytes_sent as f64 * 8.0)
                / (self.total_duration.as_nanos() as f64 / 1_000_000_000.0)
                / 1_000_000.0;
            self.messages_per_second =
                self.total_messages_sent as f64 / self.total_duration.as_secs_f64();
        }

        if self.min_latency == Duration::from_secs(u64::MAX) {
            self.min_latency = Duration::from_nanos(0);
        }
    }

    pub fn print_results(&self, test_name: &str) {
        println!("\n📊 {} Performance Results:", test_name);
        println!(
            "   📤 Total Bytes Sent: {} bytes ({:.2} MB)",
            self.total_bytes_sent,
            self.total_bytes_sent as f64 / 1_000_000.0
        );
        println!("   📨 Total Messages: {}", self.total_messages_sent);
        println!("   ⏱️  Total Duration: {:?}", self.total_duration);
        println!(
            "   📈 Throughput: {:.2} Mbps ({:.2} MB/s)",
            self.throughput_mbps,
            self.throughput_mbps / 8.0
        );
        println!("   🔄 Messages/sec: {:.2}", self.messages_per_second);
        println!("   ⚡ Min Latency: {:?}", self.min_latency);
        println!("   🐌 Max Latency: {:?}", self.max_latency);
        println!("   📊 Avg Latency: {:?}", self.avg_latency);
    }
}

/// Comprehensive streaming performance test suite
#[tokio::test]
async fn test_streaming_performance_suite() {
    println!("🚀 High-Performance Streaming Test Suite");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:35001".parse().unwrap();
    let node2_addr = "127.0.0.1:35002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    println!(
        "✅ Connection established: {} -> {}",
        node1_addr, node2_addr
    );

    // Test 1: Small message latency test
    println!("\n🔸 Test 1: Small Message Latency (64-byte packets)");
    let small_metrics = test_small_message_latency(&conn).await;
    small_metrics.print_results("Small Message Latency");

    // Test 2: Large message throughput test
    println!("\n🔸 Test 2: Large Message Throughput (4KB packets)");
    let large_metrics = test_large_message_throughput(&conn).await;
    large_metrics.print_results("Large Message Throughput");

    // Test 3: Batch streaming performance
    println!("\n🔸 Test 3: Batch Streaming Performance");
    let batch_metrics = test_batch_streaming(&conn).await;
    batch_metrics.print_results("Batch Streaming");

    // Test 4: Raw stream handle performance
    println!("\n🔸 Test 4: Raw Stream Handle Performance");
    let raw_metrics = test_raw_stream_performance(&conn).await;
    raw_metrics.print_results("Raw Stream Handle");

    // Test 5: Mixed workload performance
    println!("\n🔸 Test 5: Mixed Workload Performance");
    let mixed_metrics = test_mixed_workload(&conn).await;
    mixed_metrics.print_results("Mixed Workload");

    // Performance comparison vs regular tell/ask
    println!("\n🔸 Test 6: Performance Comparison vs Tell/Ask");
    test_performance_comparison(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All streaming tests completed successfully!");
}

/// Test small message latency - optimized for minimum latency
async fn test_small_message_latency(
    conn: &crate::connection_pool::ConnectionHandle,
) -> StreamingMetrics {
    let mut metrics = StreamingMetrics::new();
    let iterations = 1000;
    let mut total_latency = Duration::from_nanos(0);

    for i in 0..iterations {
        let data = StreamData::small(1, i);
        let start_time = Instant::now();

        // Send using streaming API
        conn.stream_send(&data).await.unwrap();

        let latency = start_time.elapsed();
        metrics.add_latency(latency);
        total_latency += latency;

        metrics.total_bytes_sent += data.size() as u64;
        metrics.total_messages_sent += 1;
    }

    metrics.total_duration = total_latency;
    metrics.avg_latency = total_latency / iterations;
    metrics.finalize();

    metrics
}

/// Test large message throughput - optimized for maximum throughput
async fn test_large_message_throughput(
    conn: &crate::connection_pool::ConnectionHandle,
) -> StreamingMetrics {
    let mut metrics = StreamingMetrics::new();
    let iterations = 100;
    let mut total_latency = Duration::from_nanos(0);

    let start_time = Instant::now();

    for i in 0..iterations {
        let data = StreamData::large(2, i);
        let iter_start = Instant::now();

        // Send using streaming API
        conn.stream_send(&data).await.unwrap();

        let latency = iter_start.elapsed();
        metrics.add_latency(latency);
        total_latency += latency;

        metrics.total_bytes_sent += data.size() as u64;
        metrics.total_messages_sent += 1;
    }

    metrics.total_duration = start_time.elapsed();
    metrics.avg_latency = total_latency / iterations;
    metrics.finalize();

    metrics
}

/// Test batch streaming performance
async fn test_batch_streaming(conn: &crate::connection_pool::ConnectionHandle) -> StreamingMetrics {
    let mut metrics = StreamingMetrics::new();
    let batch_size = 50;
    let batches = 20;

    let start_time = Instant::now();

    for batch_id in 0..batches {
        let mut batch = Vec::new();
        for i in 0..batch_size {
            let data = StreamData::medium(3, batch_id * batch_size + i);
            metrics.total_bytes_sent += data.size() as u64;
            batch.push(data);
        }

        let batch_start = Instant::now();

        // Send entire batch at once
        conn.stream_send_batch(&batch).await.unwrap();

        let batch_latency = batch_start.elapsed();
        metrics.add_latency(batch_latency);

        metrics.total_messages_sent += batch_size as u64;
    }

    metrics.total_duration = start_time.elapsed();
    metrics.avg_latency = metrics.total_duration / batches;
    metrics.finalize();

    metrics
}

/// Test raw stream handle performance for zero-copy operations
async fn test_raw_stream_performance(
    conn: &crate::connection_pool::ConnectionHandle,
) -> StreamingMetrics {
    let mut metrics = StreamingMetrics::new();
    let iterations = 1000;

    let stream_handle = conn.get_stream_handle();
    let test_data = vec![0u8; 1024]; // 1KB of raw data

    let start_time = Instant::now();

    for _ in 0..iterations {
        let iter_start = Instant::now();

        // Send raw data using zero-copy stream handle
        stream_handle.write_raw(&test_data).await.unwrap();

        let latency = iter_start.elapsed();
        metrics.add_latency(latency);

        metrics.total_bytes_sent += test_data.len() as u64;
        metrics.total_messages_sent += 1;
    }

    metrics.total_duration = start_time.elapsed();
    metrics.avg_latency = metrics.total_duration / iterations;
    metrics.finalize();

    metrics
}

/// Test mixed workload with different message sizes
async fn test_mixed_workload(conn: &crate::connection_pool::ConnectionHandle) -> StreamingMetrics {
    let mut metrics = StreamingMetrics::new();
    let iterations_per_size = 100;

    let start_time = Instant::now();

    // Mix of small, medium, and large messages
    for i in 0..iterations_per_size {
        // Small message
        let small_data = StreamData::small(4, i);
        let small_start = Instant::now();
        conn.stream_send(&small_data).await.unwrap();
        metrics.add_latency(small_start.elapsed());
        metrics.total_bytes_sent += small_data.size() as u64;
        metrics.total_messages_sent += 1;

        // Medium message
        let medium_data = StreamData::medium(5, i);
        let medium_start = Instant::now();
        conn.stream_send(&medium_data).await.unwrap();
        metrics.add_latency(medium_start.elapsed());
        metrics.total_bytes_sent += medium_data.size() as u64;
        metrics.total_messages_sent += 1;

        // Large message
        let large_data = StreamData::large(6, i);
        let large_start = Instant::now();
        conn.stream_send(&large_data).await.unwrap();
        metrics.add_latency(large_start.elapsed());
        metrics.total_bytes_sent += large_data.size() as u64;
        metrics.total_messages_sent += 1;
    }

    metrics.total_duration = start_time.elapsed();
    metrics.avg_latency = metrics.total_duration / (iterations_per_size * 3);
    metrics.finalize();

    metrics
}

/// Compare streaming performance vs regular tell/ask operations
async fn test_performance_comparison(conn: &crate::connection_pool::ConnectionHandle) {
    let iterations = 500;
    let test_data = vec![0u8; 1024]; // 1KB test data

    // Test regular tell() performance
    let tell_start = Instant::now();
    for _ in 0..iterations {
        conn.tell(&test_data).await.unwrap();
    }
    let tell_duration = tell_start.elapsed();

    // Test streaming send performance
    let stream_data = StreamData::medium(7, 0);
    let stream_start = Instant::now();
    for _ in 0..iterations {
        conn.stream_send(&stream_data).await.unwrap();
    }
    let stream_duration = stream_start.elapsed();

    // Test raw stream handle performance
    let raw_handle = conn.get_stream_handle();
    let raw_start = Instant::now();
    for _ in 0..iterations {
        raw_handle.write_raw(&test_data).await.unwrap();
    }
    let raw_duration = raw_start.elapsed();

    println!("\n📊 Performance Comparison Results:");
    println!(
        "   📤 Regular tell():     {:?} ({:.3} μs avg)",
        tell_duration,
        tell_duration.as_nanos() as f64 / iterations as f64 / 1000.0
    );
    println!(
        "   🔄 Streaming send():   {:?} ({:.3} μs avg)",
        stream_duration,
        stream_duration.as_nanos() as f64 / iterations as f64 / 1000.0
    );
    println!(
        "   ⚡ Raw stream handle: {:?} ({:.3} μs avg)",
        raw_duration,
        raw_duration.as_nanos() as f64 / iterations as f64 / 1000.0
    );

    let stream_overhead = stream_duration.as_nanos() as f64 / tell_duration.as_nanos() as f64;
    let raw_improvement = tell_duration.as_nanos() as f64 / raw_duration.as_nanos() as f64;

    println!(
        "   📈 Streaming overhead: {:.2}x vs tell()",
        stream_overhead
    );
    println!(
        "   🚀 Raw stream speedup: {:.2}x vs tell()",
        raw_improvement
    );

    // Throughput calculations
    let tell_throughput =
        (iterations * test_data.len()) as f64 / tell_duration.as_secs_f64() / 1_000_000.0;
    let stream_throughput =
        (iterations * stream_data.size()) as f64 / stream_duration.as_secs_f64() / 1_000_000.0;
    let raw_throughput =
        (iterations * test_data.len()) as f64 / raw_duration.as_secs_f64() / 1_000_000.0;

    println!("   📊 Tell throughput:    {:.2} MB/s", tell_throughput);
    println!("   📊 Stream throughput:  {:.2} MB/s", stream_throughput);
    println!("   📊 Raw throughput:     {:.2} MB/s", raw_throughput);
}

/// Simple streaming functionality test
#[tokio::test]
async fn test_streaming_basic_functionality() {
    println!("🚀 Basic Streaming Functionality Test");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:36001".parse().unwrap();
    let node2_addr = "127.0.0.1:36002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test basic streaming send
    let test_data = StreamData::small(1, 0);
    println!("📤 Sending stream data: {:?}", test_data);

    let result = conn.stream_send(&test_data).await;
    assert!(result.is_ok(), "Stream send failed: {:?}", result.err());

    println!("✅ Stream send successful");

    // Test batch streaming
    let batch = vec![
        StreamData::small(2, 0),
        StreamData::small(2, 1),
        StreamData::small(2, 2),
    ];

    println!("📤 Sending batch of {} items", batch.len());
    let result = conn.stream_send_batch(&batch).await;
    assert!(
        result.is_ok(),
        "Batch stream send failed: {:?}",
        result.err()
    );

    println!("✅ Batch stream send successful");

    // Test raw stream handle
    let stream_handle = conn.get_stream_handle();
    let raw_data = b"Hello, raw streaming!";

    println!("📤 Sending raw data: {:?}", std::str::from_utf8(raw_data));
    let result = stream_handle.write_raw(raw_data).await;
    assert!(
        result.is_ok(),
        "Raw stream write failed: {:?}",
        result.err()
    );

    println!("✅ Raw stream write successful");

    // Test vectored write
    let bufs = vec![
        b"Hello, ".as_slice(),
        b"vectored ".as_slice(),
        b"write!".as_slice(),
    ];
    let result = stream_handle.write_vectored(&bufs).await;
    assert!(result.is_ok(), "Vectored write failed: {:?}", result.err());

    println!("✅ Vectored write successful");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Basic streaming functionality test completed!");
}
