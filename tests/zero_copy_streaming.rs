use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Ultra-high-performance zero-copy streaming test targeting line-rate performance
#[tokio::test]
async fn test_zero_copy_line_rate_performance() {
    println!("🚀 Zero-Copy Line-Rate Performance Test");
    println!("🎯 Target: >5 Gbps throughput, <1 μs latency");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:37001".parse().unwrap();
    let node2_addr = "127.0.0.1:37002".parse().unwrap();

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

    // Test 1: Maximum throughput with large buffers and no flush
    println!("\n🔸 Test 1: Zero-Copy Maximum Throughput (No Flush)");
    test_zero_copy_max_throughput(&conn).await;

    // Test 2: Ultra-low latency with small buffers
    println!("\n🔸 Test 2: Zero-Copy Ultra-Low Latency");
    test_zero_copy_ultra_low_latency(&conn).await;

    // Test 3: Vectored I/O performance
    println!("\n🔸 Test 3: Zero-Copy Vectored I/O Performance");
    test_zero_copy_vectored_io(&conn).await;

    // Test 4: Batch processing with controlled flushing
    println!("\n🔸 Test 4: Zero-Copy Batch Processing");
    test_zero_copy_batch_processing(&conn).await;

    // Test 5: Continuous streaming test
    println!("\n🔸 Test 5: Zero-Copy Continuous Streaming");
    test_zero_copy_continuous_streaming(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All zero-copy tests completed!");
}

/// Test maximum throughput with zero-copy streaming
async fn test_zero_copy_max_throughput(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffers
    const BATCH_SIZE: usize = 1024 * 1024; // 1MB batch size
    const ITERATIONS: usize = 1000;

    // Create zero-copy handle with large batch size
    let zero_copy = conn.get_zero_copy_handle(BATCH_SIZE);

    // Pre-allocate buffer to avoid allocations in hot path
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing with {}KB buffers, {}MB batch size, {} iterations",
        BUFFER_SIZE / 1024,
        BATCH_SIZE / 1024 / 1024,
        ITERATIONS
    );

    let start_time = Instant::now();
    let mut total_bytes = 0u64;

    for i in 0..ITERATIONS {
        // Write without flush for maximum performance
        zero_copy.write_raw_no_flush(&buffer).await.unwrap();
        total_bytes += buffer.len() as u64;

        // Only flush every 16 iterations to minimize syscalls
        if i % 16 == 15 {
            zero_copy.flush().await.unwrap();
        }
    }

    // Final flush
    zero_copy.flush().await.unwrap();

    let elapsed = start_time.elapsed();
    let throughput_gbps = (total_bytes as f64 * 8.0)
        / (elapsed.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let throughput_mbps = throughput_gbps * 1000.0;

    println!("📈 Maximum Throughput Results:");
    println!(
        "   📤 Total Bytes: {} bytes ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_mbps / 8.0
    );
    println!(
        "   ⚡ Avg per write: {:.2} ns",
        elapsed.as_nanos() as f64 / ITERATIONS as f64
    );

    if throughput_gbps > 5.0 {
        println!("   ✅ LINE-RATE ACHIEVED! >5 Gbps");
    } else {
        println!(
            "   ⚠️  Below line-rate target ({:.2} Gbps)",
            throughput_gbps
        );
    }
}

/// Test ultra-low latency with zero-copy streaming
async fn test_zero_copy_ultra_low_latency(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 1024; // 1KB buffers
    const ITERATIONS: usize = 10000;

    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * 10);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing ultra-low latency with {}B buffers, {} iterations",
        BUFFER_SIZE, ITERATIONS
    );

    let mut min_latency = Duration::from_secs(1);
    let mut max_latency = Duration::from_nanos(0);
    let mut total_latency = Duration::from_nanos(0);

    // Warm up
    for _ in 0..100 {
        zero_copy.write_raw_no_flush(&buffer).await.unwrap();
    }
    zero_copy.flush().await.unwrap();

    // Measure latency
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        zero_copy.write_raw_no_flush(&buffer).await.unwrap();
        let latency = start.elapsed();

        if latency < min_latency {
            min_latency = latency;
        }
        if latency > max_latency {
            max_latency = latency;
        }
        total_latency += latency;
    }

    // Final flush
    zero_copy.flush().await.unwrap();

    let avg_latency = total_latency / ITERATIONS as u32;

    println!("⚡ Ultra-Low Latency Results:");
    println!(
        "   🏃 Min Latency: {:?} ({:.2} ns)",
        min_latency,
        min_latency.as_nanos()
    );
    println!(
        "   📊 Avg Latency: {:?} ({:.2} ns)",
        avg_latency,
        avg_latency.as_nanos()
    );
    println!(
        "   🐌 Max Latency: {:?} ({:.2} ns)",
        max_latency,
        max_latency.as_nanos()
    );

    if avg_latency.as_nanos() < 1000 {
        println!("   ✅ SUB-MICROSECOND LATENCY ACHIEVED!");
    } else {
        println!("   ⚠️  Above 1μs target ({:.2} ns)", avg_latency.as_nanos());
    }
}

/// Test vectored I/O performance
async fn test_zero_copy_vectored_io(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 4096; // 4KB per buffer
    const VECTORS_PER_WRITE: usize = 16; // 16 buffers per vectored write
    const ITERATIONS: usize = 500;

    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * VECTORS_PER_WRITE * 4);

    // Pre-allocate buffers
    let buffers: Vec<Vec<u8>> = (0..VECTORS_PER_WRITE)
        .map(|i| vec![i as u8; BUFFER_SIZE])
        .collect();

    let buffer_refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_slice()).collect();

    println!(
        "📊 Testing vectored I/O with {} buffers of {}KB each per write",
        VECTORS_PER_WRITE,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut total_bytes = 0u64;

    for i in 0..ITERATIONS {
        // Use vectored write for maximum efficiency
        zero_copy.write_raw_vectored(&buffer_refs).await.unwrap();
        total_bytes += (BUFFER_SIZE * VECTORS_PER_WRITE) as u64;

        // Flush every 8 iterations
        if i % 8 == 7 {
            zero_copy.flush().await.unwrap();
        }
    }

    zero_copy.flush().await.unwrap();

    let elapsed = start_time.elapsed();
    let throughput_gbps = (total_bytes as f64 * 8.0)
        / (elapsed.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;

    println!("📈 Vectored I/O Results:");
    println!(
        "   📤 Total Bytes: {} bytes ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_gbps * 1000.0 / 8.0
    );
    println!(
        "   ⚡ Avg per vectored write: {:.2} μs",
        elapsed.as_micros() as f64 / ITERATIONS as f64
    );
}

/// Test batch processing with controlled flushing
async fn test_zero_copy_batch_processing(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 8192; // 8KB buffers
    const BATCH_SIZE: usize = 512 * 1024; // 512KB batch size
    const ITERATIONS: usize = 2000;

    let zero_copy = conn.get_zero_copy_handle(BATCH_SIZE);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing batch processing with {}KB buffers, {}KB batch size",
        BUFFER_SIZE / 1024,
        BATCH_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut total_bytes = 0u64;
    let mut flush_count = 0;

    for _ in 0..ITERATIONS {
        // Use automatic batching
        zero_copy.write_with_batching(&buffer).await.unwrap();
        total_bytes += buffer.len() as u64;

        // Count flushes
        if zero_copy.pending_bytes() == 0 {
            flush_count += 1;
        }
    }

    // Final flush if needed
    if zero_copy.pending_bytes() > 0 {
        zero_copy.flush().await.unwrap();
        flush_count += 1;
    }

    let elapsed = start_time.elapsed();
    let throughput_gbps = (total_bytes as f64 * 8.0)
        / (elapsed.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;

    println!("📈 Batch Processing Results:");
    println!(
        "   📤 Total Bytes: {} bytes ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_gbps * 1000.0 / 8.0
    );
    println!(
        "   🔄 Flush Count: {} (efficiency: {:.2}%)",
        flush_count,
        100.0 * total_bytes as f64 / (flush_count as f64 * BATCH_SIZE as f64)
    );
}

/// Test continuous streaming performance
async fn test_zero_copy_continuous_streaming(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 32 * 1024; // 32KB buffers
    const STREAM_DURATION: Duration = Duration::from_secs(2); // 2 seconds
    const BATCH_SIZE: usize = 2 * 1024 * 1024; // 2MB batch

    let zero_copy = conn.get_zero_copy_handle(BATCH_SIZE);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing continuous streaming for {:?} with {}KB buffers",
        STREAM_DURATION,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut total_bytes = 0u64;
    let mut iteration_count = 0;

    // Stream continuously for the specified duration
    while start_time.elapsed() < STREAM_DURATION {
        zero_copy.write_raw_no_flush(&buffer).await.unwrap();
        total_bytes += buffer.len() as u64;
        iteration_count += 1;

        // Flush every 64 iterations for sustained performance
        if iteration_count % 64 == 0 {
            zero_copy.flush().await.unwrap();
        }
    }

    // Final flush
    zero_copy.flush().await.unwrap();

    let elapsed = start_time.elapsed();
    let throughput_gbps = (total_bytes as f64 * 8.0)
        / (elapsed.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let messages_per_second = iteration_count as f64 / elapsed.as_secs_f64();

    println!("📈 Continuous Streaming Results:");
    println!(
        "   📤 Total Bytes: {} bytes ({:.2} GB)",
        total_bytes,
        total_bytes as f64 / 1_000_000_000.0
    );
    println!("   🔄 Iterations: {}", iteration_count);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Sustained Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_gbps * 1000.0 / 8.0
    );
    println!("   📊 Messages/sec: {:.2}", messages_per_second);
    println!(
        "   ⚡ Avg per message: {:.2} ns",
        elapsed.as_nanos() as f64 / iteration_count as f64
    );

    if throughput_gbps > 10.0 {
        println!("   ✅ EXCEPTIONAL PERFORMANCE! >10 Gbps sustained");
    } else if throughput_gbps > 5.0 {
        println!("   ✅ LINE-RATE ACHIEVED! >5 Gbps sustained");
    } else {
        println!(
            "   ⚠️  Below line-rate target ({:.2} Gbps)",
            throughput_gbps
        );
    }
}

/// Compare zero-copy vs regular streaming performance
#[tokio::test]
async fn test_zero_copy_vs_regular_comparison() {
    println!("🚀 Zero-Copy vs Regular Streaming Comparison");

    // Setup
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

    const BUFFER_SIZE: usize = 16 * 1024; // 16KB
    const ITERATIONS: usize = 1000;

    let buffer = vec![0u8; BUFFER_SIZE];

    // Test regular stream handle
    let regular_handle = conn.get_stream_handle();
    let regular_start = Instant::now();
    for _ in 0..ITERATIONS {
        regular_handle.write_raw(&buffer).await.unwrap();
    }
    let regular_duration = regular_start.elapsed();

    // Test zero-copy handle
    let zero_copy_handle = conn.get_zero_copy_handle(BUFFER_SIZE * 32);
    let zero_copy_start = Instant::now();
    for i in 0..ITERATIONS {
        zero_copy_handle.write_raw_no_flush(&buffer).await.unwrap();
        if i % 32 == 31 {
            zero_copy_handle.flush().await.unwrap();
        }
    }
    zero_copy_handle.flush().await.unwrap();
    let zero_copy_duration = zero_copy_start.elapsed();

    // Test zero-copy vectored
    let buffers: Vec<&[u8]> = (0..8).map(|_| buffer.as_slice()).collect();
    let vectored_start = Instant::now();
    for i in 0..(ITERATIONS / 8) {
        zero_copy_handle.write_raw_vectored(&buffers).await.unwrap();
        if i % 4 == 3 {
            zero_copy_handle.flush().await.unwrap();
        }
    }
    zero_copy_handle.flush().await.unwrap();
    let vectored_duration = vectored_start.elapsed();

    let total_bytes = (BUFFER_SIZE * ITERATIONS) as f64;
    let regular_throughput = (total_bytes * 8.0)
        / (regular_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let zero_copy_throughput = (total_bytes * 8.0)
        / (zero_copy_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let vectored_throughput = (total_bytes * 8.0)
        / (vectored_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;

    println!("\n📊 Performance Comparison Results:");
    println!(
        "   📤 Regular Stream:    {:?} ({:.2} Gbps)",
        regular_duration, regular_throughput
    );
    println!(
        "   🚀 Zero-Copy:         {:?} ({:.2} Gbps)",
        zero_copy_duration, zero_copy_throughput
    );
    println!(
        "   ⚡ Zero-Copy Vectored: {:?} ({:.2} Gbps)",
        vectored_duration, vectored_throughput
    );

    let zero_copy_speedup = regular_throughput / zero_copy_throughput;
    let vectored_speedup = regular_throughput / vectored_throughput;

    println!("   📈 Zero-Copy Speedup: {:.2}x", 1.0 / zero_copy_speedup);
    println!("   📈 Vectored Speedup:  {:.2}x", 1.0 / vectored_speedup);

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Comparison test completed!");
}
