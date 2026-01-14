use kameo_remote::*;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test data structure for streaming serialization comparison
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamDataBincode {
    pub id: u64,
    pub timestamp: u64,
    pub sequence: u32,
    pub payload: Vec<u8>,
    pub metadata: StreamMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamMetadata {
    pub source_node: String,
    pub priority: u8,
    pub compression: bool,
    pub encryption: bool,
    pub tags: Vec<String>,
}

/// rkyv equivalent structures
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct StreamDataRkyv {
    pub id: u64,
    pub timestamp: u64,
    pub sequence: u32,
    pub payload: Vec<u8>,
    pub metadata: StreamMetadataRkyv,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq)]
pub struct StreamMetadataRkyv {
    pub source_node: String,
    pub priority: u8,
    pub compression: bool,
    pub encryption: bool,
    pub tags: Vec<String>,
}

impl StreamDataBincode {
    pub fn new(id: u64, sequence: u32, payload_size: usize) -> Self {
        Self {
            id,
            timestamp: kameo_remote::current_timestamp_nanos(),
            sequence,
            payload: vec![0u8; payload_size],
            metadata: StreamMetadata {
                source_node: "node_1".to_string(),
                priority: 1,
                compression: false,
                encryption: true,
                tags: vec!["stream".to_string(), "test".to_string(), "data".to_string()],
            },
        }
    }
}

impl StreamDataRkyv {
    pub fn new(id: u64, sequence: u32, payload_size: usize) -> Self {
        Self {
            id,
            timestamp: kameo_remote::current_timestamp_nanos(),
            sequence,
            payload: vec![0u8; payload_size],
            metadata: StreamMetadataRkyv {
                source_node: "node_1".to_string(),
                priority: 1,
                compression: false,
                encryption: true,
                tags: vec!["stream".to_string(), "test".to_string(), "data".to_string()],
            },
        }
    }
}

/// Streaming performance metrics
#[derive(Debug, Default)]
pub struct StreamingSerializationMetrics {
    pub total_messages: u64,
    pub total_bytes_original: u64,
    pub total_bytes_serialized: u64,
    pub total_duration: Duration,
    pub serialization_time: Duration,
    pub transmission_time: Duration,
    pub throughput_mbps: f64,
    pub messages_per_second: f64,
    pub compression_ratio: f64,
    pub latencies: Vec<Duration>,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub avg_latency: Duration,
    pub p50_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
}

impl StreamingSerializationMetrics {
    pub fn new() -> Self {
        Self {
            min_latency: Duration::from_secs(u64::MAX),
            max_latency: Duration::from_nanos(0),
            latencies: Vec::new(),
            ..Default::default()
        }
    }

    pub fn add_latency(&mut self, latency: Duration) {
        self.latencies.push(latency);

        if latency < self.min_latency {
            self.min_latency = latency;
        }
        if latency > self.max_latency {
            self.max_latency = latency;
        }
    }

    pub fn finalize(&mut self) {
        if self.total_duration.as_nanos() > 0 {
            self.throughput_mbps = (self.total_bytes_serialized as f64 * 8.0)
                / (self.total_duration.as_nanos() as f64 / 1_000_000_000.0)
                / 1_000_000.0;
            self.messages_per_second =
                self.total_messages as f64 / self.total_duration.as_secs_f64();
        }

        if self.total_bytes_original > 0 {
            self.compression_ratio =
                self.total_bytes_serialized as f64 / self.total_bytes_original as f64;
        }

        // Calculate latency statistics
        if !self.latencies.is_empty() {
            let mut sorted_latencies = self.latencies.clone();
            sorted_latencies.sort();

            let total_latency: Duration = sorted_latencies.iter().sum();
            self.avg_latency = total_latency / sorted_latencies.len() as u32;

            let len = sorted_latencies.len();
            self.p50_latency = sorted_latencies[len / 2];
            self.p95_latency = sorted_latencies[len * 95 / 100];
            self.p99_latency = sorted_latencies[len * 99 / 100];
        } else {
            self.min_latency = Duration::from_nanos(0);
        }
    }

    pub fn print_results(&self, test_name: &str) {
        println!("\n📊 {} Results:", test_name);
        println!("   📨 Messages: {}", self.total_messages);
        println!(
            "   📤 Original Size: {} bytes ({:.2} MB)",
            self.total_bytes_original,
            self.total_bytes_original as f64 / 1_000_000.0
        );
        println!(
            "   📦 Serialized Size: {} bytes ({:.2} MB)",
            self.total_bytes_serialized,
            self.total_bytes_serialized as f64 / 1_000_000.0
        );
        println!("   🗜️  Compression Ratio: {:.2}x", self.compression_ratio);
        println!("   ⏱️  Total Duration: {:?}", self.total_duration);
        println!(
            "   🧮 Serialization Time: {:?} ({:.1}%)",
            self.serialization_time,
            self.serialization_time.as_nanos() as f64 / self.total_duration.as_nanos() as f64
                * 100.0
        );
        println!(
            "   📡 Transmission Time: {:?} ({:.1}%)",
            self.transmission_time,
            self.transmission_time.as_nanos() as f64 / self.total_duration.as_nanos() as f64
                * 100.0
        );
        println!(
            "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
            self.throughput_mbps,
            self.throughput_mbps / 8.0
        );
        println!("   🔄 Messages/sec: {:.2}", self.messages_per_second);

        // Latency statistics
        if !self.latencies.is_empty() {
            println!("   ⚡ Latency Stats:");
            println!(
                "     - Min: {:?} ({:.3} μs)",
                self.min_latency,
                self.min_latency.as_nanos() as f64 / 1000.0
            );
            println!(
                "     - Avg: {:?} ({:.3} μs)",
                self.avg_latency,
                self.avg_latency.as_nanos() as f64 / 1000.0
            );
            println!(
                "     - Max: {:?} ({:.3} μs)",
                self.max_latency,
                self.max_latency.as_nanos() as f64 / 1000.0
            );
            println!(
                "     - P50: {:?} ({:.3} μs)",
                self.p50_latency,
                self.p50_latency.as_nanos() as f64 / 1000.0
            );
            println!(
                "     - P95: {:?} ({:.3} μs)",
                self.p95_latency,
                self.p95_latency.as_nanos() as f64 / 1000.0
            );
            println!(
                "     - P99: {:?} ({:.3} μs)",
                self.p99_latency,
                self.p99_latency.as_nanos() as f64 / 1000.0
            );
        }
    }
}

/// Test bincode serialization in streaming scenario
async fn test_bincode_streaming(
    conn: &crate::connection_pool::ConnectionHandle,
    message_count: usize,
    payload_size: usize,
) -> StreamingSerializationMetrics {
    let mut metrics = StreamingSerializationMetrics::new();
    let start_time = Instant::now();

    println!(
        "🔸 Testing bincode streaming: {} messages, {} bytes payload",
        message_count, payload_size
    );

    for i in 0..message_count {
        let data = StreamDataBincode::new(i as u64, i as u32, payload_size);
        let original_size = 8 + 8 + 4 + payload_size + 50; // Rough estimate

        // Measure full end-to-end latency
        let msg_start = Instant::now();

        // Measure serialization time
        let serialize_start = Instant::now();
        let serialized = bincode::serialize(&data).unwrap();
        let serialize_time = serialize_start.elapsed();

        // Measure transmission time
        let transmit_start = Instant::now();
        conn.tell(serialized.as_slice()).await.unwrap();
        let transmit_time = transmit_start.elapsed();

        // Calculate full latency
        let full_latency = msg_start.elapsed();

        metrics.total_messages += 1;
        metrics.total_bytes_original += original_size as u64;
        metrics.total_bytes_serialized += serialized.len() as u64;
        metrics.serialization_time += serialize_time;
        metrics.transmission_time += transmit_time;
        metrics.add_latency(full_latency);

        // Progress indicator
        if i % 100 == 0 && i > 0 {
            println!("   Progress: {}/{} messages sent", i, message_count);
        }
    }

    metrics.total_duration = start_time.elapsed();
    metrics.finalize();
    metrics
}

/// Test rkyv serialization in streaming scenario
async fn test_rkyv_streaming(
    conn: &crate::connection_pool::ConnectionHandle,
    message_count: usize,
    payload_size: usize,
) -> StreamingSerializationMetrics {
    let mut metrics = StreamingSerializationMetrics::new();
    let start_time = Instant::now();

    println!(
        "🔸 Testing rkyv streaming: {} messages, {} bytes payload",
        message_count, payload_size
    );

    for i in 0..message_count {
        let data = StreamDataRkyv::new(i as u64, i as u32, payload_size);
        let original_size = 8 + 8 + 4 + payload_size + 50; // Rough estimate

        // Measure full end-to-end latency
        let msg_start = Instant::now();

        // Measure serialization time
        let serialize_start = Instant::now();
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&data).unwrap();
        let serialize_time = serialize_start.elapsed();

        // Measure transmission time
        let transmit_start = Instant::now();
        conn.tell(serialized.as_slice()).await.unwrap();
        let transmit_time = transmit_start.elapsed();

        // Calculate full latency
        let full_latency = msg_start.elapsed();

        metrics.total_messages += 1;
        metrics.total_bytes_original += original_size as u64;
        metrics.total_bytes_serialized += serialized.len() as u64;
        metrics.serialization_time += serialize_time;
        metrics.transmission_time += transmit_time;
        metrics.add_latency(full_latency);

        // Progress indicator
        if i % 100 == 0 && i > 0 {
            println!("   Progress: {}/{} messages sent", i, message_count);
        }
    }

    metrics.total_duration = start_time.elapsed();
    metrics.finalize();
    metrics
}

/// Test batch streaming with serialization
async fn test_batch_streaming_bincode(
    conn: &crate::connection_pool::ConnectionHandle,
    message_count: usize,
    payload_size: usize,
    batch_size: usize,
) -> StreamingSerializationMetrics {
    let mut metrics = StreamingSerializationMetrics::new();
    let start_time = Instant::now();

    println!(
        "🔸 Testing bincode batch streaming: {} messages, {} bytes payload, batch size {}",
        message_count, payload_size, batch_size
    );

    let mut batch = Vec::new();
    let mut batch_serialized = Vec::new();

    for i in 0..message_count {
        let data = StreamDataBincode::new(i as u64, i as u32, payload_size);
        let original_size = 8 + 8 + 4 + payload_size + 50; // Rough estimate

        // Measure serialization time
        let serialize_start = Instant::now();
        let serialized = bincode::serialize(&data).unwrap();
        let serialize_time = serialize_start.elapsed();

        batch.push(data);
        batch_serialized.push(serialized);

        metrics.total_messages += 1;
        metrics.total_bytes_original += original_size as u64;
        metrics.total_bytes_serialized += batch_serialized.last().unwrap().len() as u64;
        metrics.serialization_time += serialize_time;

        // Send batch when full
        if batch_serialized.len() >= batch_size || i == message_count - 1 {
            let transmit_start = Instant::now();
            let batch_data: Vec<&[u8]> = batch_serialized.iter().map(|s| s.as_slice()).collect();
            conn.tell(kameo_remote::connection_pool::TellMessage::batch(
                batch_data,
            ))
            .await
            .unwrap();
            let transmit_time = transmit_start.elapsed();

            metrics.transmission_time += transmit_time;

            // Clear batch
            batch.clear();
            batch_serialized.clear();

            if i % 500 == 0 && i > 0 {
                println!("   Progress: {}/{} messages sent", i, message_count);
            }
        }
    }

    metrics.total_duration = start_time.elapsed();
    metrics.finalize();
    metrics
}

/// Test batch streaming with rkyv
async fn test_batch_streaming_rkyv(
    conn: &crate::connection_pool::ConnectionHandle,
    message_count: usize,
    payload_size: usize,
    batch_size: usize,
) -> StreamingSerializationMetrics {
    let mut metrics = StreamingSerializationMetrics::new();
    let start_time = Instant::now();

    println!(
        "🔸 Testing rkyv batch streaming: {} messages, {} bytes payload, batch size {}",
        message_count, payload_size, batch_size
    );

    let mut batch = Vec::new();
    let mut batch_serialized = Vec::new();

    for i in 0..message_count {
        let data = StreamDataRkyv::new(i as u64, i as u32, payload_size);
        let original_size = 8 + 8 + 4 + payload_size + 50; // Rough estimate

        // Measure serialization time
        let serialize_start = Instant::now();
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&data).unwrap();
        let serialize_time = serialize_start.elapsed();

        batch.push(data);
        batch_serialized.push(serialized);

        metrics.total_messages += 1;
        metrics.total_bytes_original += original_size as u64;
        metrics.total_bytes_serialized += batch_serialized.last().unwrap().len() as u64;
        metrics.serialization_time += serialize_time;

        // Send batch when full
        if batch_serialized.len() >= batch_size || i == message_count - 1 {
            let transmit_start = Instant::now();
            let batch_data: Vec<&[u8]> = batch_serialized.iter().map(|s| s.as_slice()).collect();
            conn.tell(kameo_remote::connection_pool::TellMessage::batch(
                batch_data,
            ))
            .await
            .unwrap();
            let transmit_time = transmit_start.elapsed();

            metrics.transmission_time += transmit_time;

            // Clear batch
            batch.clear();
            batch_serialized.clear();

            if i % 500 == 0 && i > 0 {
                println!("   Progress: {}/{} messages sent", i, message_count);
            }
        }
    }

    metrics.total_duration = start_time.elapsed();
    metrics.finalize();
    metrics
}

/// Comprehensive streaming serialization comparison test
#[tokio::test]
async fn test_streaming_serialization_comparison() {
    println!("🚀 STREAMING SERIALIZATION COMPARISON: bincode vs rkyv");
    println!("======================================================");

    // Setup nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:38001".parse().unwrap();
    let node2_addr = "127.0.0.1:38002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test configurations
    let test_configs = vec![
        (100, 100, "Small messages (100 bytes)"),
        (100, 1000, "Medium messages (1KB)"),
        (100, 10000, "Large messages (10KB)"),
        (1000, 100, "Many small messages"),
        (1000, 1000, "Many medium messages"),
    ];

    for (message_count, payload_size, description) in test_configs {
        println!(
            "\n📊 Testing {} - {} messages, {} bytes payload",
            description, message_count, payload_size
        );
        println!("{}=", "=".repeat(60));

        // Test individual message streaming
        let bincode_metrics = test_bincode_streaming(&conn, message_count, payload_size).await;
        let rkyv_metrics = test_rkyv_streaming(&conn, message_count, payload_size).await;

        bincode_metrics.print_results("bincode Individual Streaming");
        rkyv_metrics.print_results("rkyv Individual Streaming");

        // Performance comparison
        let total_time_improvement = bincode_metrics.total_duration.as_nanos() as f64
            / rkyv_metrics.total_duration.as_nanos() as f64;
        let serialization_improvement = bincode_metrics.serialization_time.as_nanos() as f64
            / rkyv_metrics.serialization_time.as_nanos() as f64;
        let throughput_improvement = rkyv_metrics.throughput_mbps / bincode_metrics.throughput_mbps;
        let size_ratio = bincode_metrics.total_bytes_serialized as f64
            / rkyv_metrics.total_bytes_serialized as f64;

        // Latency comparison
        let avg_latency_improvement = bincode_metrics.avg_latency.as_nanos() as f64
            / rkyv_metrics.avg_latency.as_nanos() as f64;
        let p95_latency_improvement = bincode_metrics.p95_latency.as_nanos() as f64
            / rkyv_metrics.p95_latency.as_nanos() as f64;
        let p99_latency_improvement = bincode_metrics.p99_latency.as_nanos() as f64
            / rkyv_metrics.p99_latency.as_nanos() as f64;

        println!("\n🎯 Individual Streaming Comparison:");
        println!(
            "   - Total time: rkyv is {:.2}x faster than bincode",
            total_time_improvement
        );
        println!(
            "   - Serialization: rkyv is {:.2}x faster than bincode",
            serialization_improvement
        );
        println!(
            "   - Throughput: rkyv is {:.2}x faster than bincode",
            throughput_improvement
        );
        println!(
            "   - Size efficiency: bincode is {:.2}x smaller than rkyv",
            size_ratio
        );
        println!(
            "   - Average latency: rkyv is {:.2}x faster than bincode",
            avg_latency_improvement
        );
        println!(
            "   - P95 latency: rkyv is {:.2}x faster than bincode",
            p95_latency_improvement
        );
        println!(
            "   - P99 latency: rkyv is {:.2}x faster than bincode",
            p99_latency_improvement
        );

        // Test batch streaming
        let batch_size = 10;
        let bincode_batch_metrics =
            test_batch_streaming_bincode(&conn, message_count, payload_size, batch_size).await;
        let rkyv_batch_metrics =
            test_batch_streaming_rkyv(&conn, message_count, payload_size, batch_size).await;

        bincode_batch_metrics.print_results("bincode Batch Streaming");
        rkyv_batch_metrics.print_results("rkyv Batch Streaming");

        // Batch comparison
        let batch_total_improvement = bincode_batch_metrics.total_duration.as_nanos() as f64
            / rkyv_batch_metrics.total_duration.as_nanos() as f64;
        let batch_serialization_improvement = bincode_batch_metrics.serialization_time.as_nanos()
            as f64
            / rkyv_batch_metrics.serialization_time.as_nanos() as f64;
        let batch_throughput_improvement =
            rkyv_batch_metrics.throughput_mbps / bincode_batch_metrics.throughput_mbps;

        println!("\n🎯 Batch Streaming Comparison:");
        println!(
            "   - Total time: rkyv is {:.2}x faster than bincode",
            batch_total_improvement
        );
        println!(
            "   - Serialization: rkyv is {:.2}x faster than bincode",
            batch_serialization_improvement
        );
        println!(
            "   - Throughput: rkyv is {:.2}x faster than bincode",
            batch_throughput_improvement
        );

        // Individual vs Batch comparison
        let bincode_batch_improvement = bincode_metrics.total_duration.as_nanos() as f64
            / bincode_batch_metrics.total_duration.as_nanos() as f64;
        let rkyv_batch_improvement = rkyv_metrics.total_duration.as_nanos() as f64
            / rkyv_batch_metrics.total_duration.as_nanos() as f64;

        println!("\n🎯 Individual vs Batch Performance:");
        println!(
            "   - bincode: batching is {:.2}x faster",
            bincode_batch_improvement
        );
        println!(
            "   - rkyv: batching is {:.2}x faster",
            rkyv_batch_improvement
        );

        // Wait between tests
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n🎯 FINAL RECOMMENDATIONS FOR STREAMING:");
    println!("======================================");
    println!("✅ Use rkyv for streaming when:");
    println!("   - CPU-bound serialization is the bottleneck");
    println!("   - You need maximum throughput");
    println!("   - Network bandwidth is not constrained");
    println!("   - You can afford larger message sizes");

    println!("\n✅ Use bincode for streaming when:");
    println!("   - Network bandwidth is limited");
    println!("   - Message size matters more than CPU performance");
    println!("   - You need maximum compression");
    println!("   - Simple integration is preferred");

    println!("\n💡 Key Insights:");
    println!("   - Both benefit significantly from batching");
    println!("   - rkyv serialization overhead is consistently lower");
    println!("   - bincode produces smaller messages (better for bandwidth)");
    println!("   - rkyv provides better CPU utilization");

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ Streaming serialization comparison completed!");
}

/// High-volume streaming test focusing on serialization overhead
#[tokio::test]
async fn test_high_volume_streaming_serialization() {
    println!("🚀 HIGH-VOLUME STREAMING SERIALIZATION TEST");
    println!("===========================================");

    // Setup nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:39001".parse().unwrap();
    let node2_addr = "127.0.0.1:39002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // High-volume test: 10,000 messages with 1KB payload each
    let message_count = 10000;
    let payload_size = 1024;

    println!(
        "📊 Testing high-volume streaming: {} messages, {} bytes each",
        message_count, payload_size
    );
    println!(
        "Total data: {:.2} MB",
        (message_count * payload_size) as f64 / 1_000_000.0
    );

    // Test with large batches for maximum efficiency
    let batch_size = 100;

    let bincode_metrics =
        test_batch_streaming_bincode(&conn, message_count, payload_size, batch_size).await;
    let rkyv_metrics =
        test_batch_streaming_rkyv(&conn, message_count, payload_size, batch_size).await;

    bincode_metrics.print_results("bincode High-Volume");
    rkyv_metrics.print_results("rkyv High-Volume");

    // Detailed analysis
    let serialization_cpu_overhead_bincode = bincode_metrics.serialization_time.as_nanos() as f64
        / bincode_metrics.total_duration.as_nanos() as f64
        * 100.0;
    let serialization_cpu_overhead_rkyv = rkyv_metrics.serialization_time.as_nanos() as f64
        / rkyv_metrics.total_duration.as_nanos() as f64
        * 100.0;

    println!("\n🔍 Detailed Analysis:");
    println!(
        "   bincode serialization CPU overhead: {:.1}%",
        serialization_cpu_overhead_bincode
    );
    println!(
        "   rkyv serialization CPU overhead: {:.1}%",
        serialization_cpu_overhead_rkyv
    );
    println!(
        "   CPU overhead reduction with rkyv: {:.1}%",
        serialization_cpu_overhead_bincode - serialization_cpu_overhead_rkyv
    );

    let total_improvement = bincode_metrics.total_duration.as_nanos() as f64
        / rkyv_metrics.total_duration.as_nanos() as f64;
    let bandwidth_usage_ratio =
        rkyv_metrics.total_bytes_serialized as f64 / bincode_metrics.total_bytes_serialized as f64;

    println!("\n🎯 High-Volume Performance Summary:");
    println!(
        "   - Overall performance: rkyv is {:.2}x faster",
        total_improvement
    );
    println!(
        "   - Bandwidth usage: rkyv uses {:.2}x more bandwidth",
        bandwidth_usage_ratio
    );
    println!(
        "   - Trade-off: {:.2}x performance improvement costs {:.2}x bandwidth",
        total_improvement, bandwidth_usage_ratio
    );

    // Performance assertions
    assert!(
        total_improvement > 1.0,
        "rkyv should be faster than bincode for high-volume streaming"
    );
    assert!(
        rkyv_metrics.throughput_mbps > bincode_metrics.throughput_mbps,
        "rkyv should have higher throughput"
    );

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ High-volume streaming test completed!");
}
