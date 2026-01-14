use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Realistic TCP streaming performance test
#[tokio::test]
async fn test_realistic_tcp_streaming() {
    println!("🚀 Realistic TCP Streaming Performance Test");
    println!("🎯 Target: Maximize TCP throughput within physical limits");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:41001".parse().unwrap();
    let node2_addr = "127.0.0.1:41002".parse().unwrap();

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

    // Test 1: Baseline TCP performance
    println!("\n🔸 Test 1: Baseline TCP Performance");
    test_baseline_tcp_performance(&conn).await;

    // Test 2: Optimized batching
    println!("\n🔸 Test 2: Optimized TCP Batching");
    test_optimized_tcp_batching(&conn).await;

    // Test 3: Large buffer streaming
    println!("\n🔸 Test 3: Large Buffer Streaming");
    test_large_buffer_streaming(&conn).await;

    // Test 4: Sustained throughput test
    println!("\n🔸 Test 4: Sustained TCP Throughput");
    test_sustained_tcp_throughput(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All realistic TCP streaming tests completed!");
}

/// Test baseline TCP performance with proper error handling
async fn test_baseline_tcp_performance(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB - good for TCP
    const ITERATIONS: usize = 100;

    let buffer = vec![0u8; BUFFER_SIZE];
    let stream_handle = conn.get_stream_handle();

    println!(
        "📊 Testing baseline TCP with {}KB buffers, {} iterations",
        BUFFER_SIZE / 1024,
        ITERATIONS
    );

    let start_time = Instant::now();
    let mut successful_writes = 0;

    for _ in 0..ITERATIONS {
        match stream_handle.write_raw(&buffer).await {
            Ok(()) => successful_writes += 1,
            Err(e) => println!("Write error: {:?}", e),
        }
    }

    let elapsed = start_time.elapsed();
    let total_bytes = (successful_writes * BUFFER_SIZE) as f64;
    let throughput_mbps = (total_bytes * 8.0) / (elapsed.as_secs_f64()) / 1_000_000.0;

    println!("📈 Baseline TCP Results:");
    println!(
        "   📤 Successful writes: {}/{}",
        successful_writes, ITERATIONS
    );
    println!("   📤 Total bytes: {:.2} MB", total_bytes / 1_000_000.0);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!(
        "   ⚡ Avg per write: {:.2} ms",
        elapsed.as_millis() as f64 / successful_writes as f64
    );

    // Realistic TCP expectations for localhost
    if throughput_mbps > 8000.0 {
        println!("   ✅ EXCELLENT TCP PERFORMANCE! >8 Gbps");
    } else if throughput_mbps > 1000.0 {
        println!("   ✅ GOOD TCP PERFORMANCE! >1 Gbps");
    } else {
        println!("   ℹ️  TCP Performance: {:.2} Mbps", throughput_mbps);
    }
}

/// Test optimized TCP batching with vectored I/O
async fn test_optimized_tcp_batching(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 32 * 1024; // 32KB per buffer
    const BUFFERS_PER_BATCH: usize = 8; // 8 buffers per batch
    const ITERATIONS: usize = 50;

    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * BUFFERS_PER_BATCH * 2);

    // Pre-allocate buffers
    let buffers: Vec<Vec<u8>> = (0..BUFFERS_PER_BATCH)
        .map(|i| vec![i as u8; BUFFER_SIZE])
        .collect();

    let buffer_refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_slice()).collect();

    println!(
        "📊 Testing TCP batching with {} buffers of {}KB each per batch",
        BUFFERS_PER_BATCH,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut successful_batches = 0;

    for i in 0..ITERATIONS {
        match zero_copy.write_batch_no_flush(&buffer_refs).await {
            Ok(()) => {
                successful_batches += 1;
                // Flush every 4 batches to balance latency and throughput
                if i % 4 == 3 {
                    let _ = zero_copy.flush().await;
                }
            }
            Err(e) => println!("Batch write error: {:?}", e),
        }
    }

    // Final flush
    let _ = zero_copy.flush().await;

    let elapsed = start_time.elapsed();
    let total_bytes = (successful_batches * BUFFER_SIZE * BUFFERS_PER_BATCH) as f64;
    let throughput_mbps = (total_bytes * 8.0) / (elapsed.as_secs_f64()) / 1_000_000.0;

    println!("📈 TCP Batching Results:");
    println!(
        "   📤 Successful batches: {}/{}",
        successful_batches, ITERATIONS
    );
    println!("   📤 Total bytes: {:.2} MB", total_bytes / 1_000_000.0);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!(
        "   ⚡ Avg per batch: {:.2} ms",
        elapsed.as_millis() as f64 / successful_batches as f64
    );
}

/// Test large buffer streaming for maximum TCP throughput
async fn test_large_buffer_streaming(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 256 * 1024; // 256KB - large TCP buffers
    const ITERATIONS: usize = 50;

    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * 4);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing large buffer streaming with {}KB buffers",
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut successful_writes = 0;

    for i in 0..ITERATIONS {
        match zero_copy.write_raw_no_flush(&buffer).await {
            Ok(()) => {
                successful_writes += 1;
                // Flush every 8 writes to allow kernel buffering
                if i % 8 == 7 {
                    let _ = zero_copy.flush().await;
                }
            }
            Err(e) => println!("Large buffer write error: {:?}", e),
        }
    }

    // Final flush
    let _ = zero_copy.flush().await;

    let elapsed = start_time.elapsed();
    let total_bytes = (successful_writes * BUFFER_SIZE) as f64;
    let throughput_mbps = (total_bytes * 8.0) / (elapsed.as_secs_f64()) / 1_000_000.0;

    println!("📈 Large Buffer Streaming Results:");
    println!(
        "   📤 Successful writes: {}/{}",
        successful_writes, ITERATIONS
    );
    println!("   📤 Total bytes: {:.2} MB", total_bytes / 1_000_000.0);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!(
        "   ⚡ Avg per write: {:.2} ms",
        elapsed.as_millis() as f64 / successful_writes as f64
    );
}

/// Test sustained TCP throughput over time
async fn test_sustained_tcp_throughput(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 128 * 1024; // 128KB
    const TEST_DURATION: Duration = Duration::from_secs(5); // 5 seconds

    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * 16);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing sustained TCP throughput for {:?} with {}KB buffers",
        TEST_DURATION,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut successful_writes = 0;
    let mut total_errors = 0;

    while start_time.elapsed() < TEST_DURATION {
        match zero_copy.write_raw_no_flush(&buffer).await {
            Ok(()) => {
                successful_writes += 1;
                // Flush every 16 writes for sustained performance
                if successful_writes % 16 == 0 {
                    let _ = zero_copy.flush().await;
                }
            }
            Err(_) => {
                total_errors += 1;
                // If we're getting errors, slow down a bit
                if total_errors % 10 == 0 {
                    sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    // Final flush
    let _ = zero_copy.flush().await;

    let elapsed = start_time.elapsed();
    let total_bytes = (successful_writes * BUFFER_SIZE) as f64;
    let throughput_mbps = (total_bytes * 8.0) / (elapsed.as_secs_f64()) / 1_000_000.0;
    let writes_per_second = successful_writes as f64 / elapsed.as_secs_f64();

    println!("📈 Sustained TCP Throughput Results:");
    println!("   📤 Successful writes: {}", successful_writes);
    println!("   ❌ Total errors: {}", total_errors);
    println!("   📤 Total bytes: {:.2} MB", total_bytes / 1_000_000.0);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Sustained Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!("   📊 Writes/sec: {:.2}", writes_per_second);
    println!(
        "   ⚡ Avg per write: {:.2} ms",
        elapsed.as_millis() as f64 / successful_writes as f64
    );
    println!(
        "   📊 Success rate: {:.1}%",
        (successful_writes as f64 / (successful_writes + total_errors) as f64) * 100.0
    );

    // Realistic sustained TCP throughput expectations
    if throughput_mbps > 5000.0 {
        println!("   ✅ EXCELLENT SUSTAINED TCP! >5 Gbps");
    } else if throughput_mbps > 1000.0 {
        println!("   ✅ GOOD SUSTAINED TCP! >1 Gbps");
    } else if throughput_mbps > 100.0 {
        println!("   ✅ DECENT SUSTAINED TCP! >100 Mbps");
    } else {
        println!("   ℹ️  Sustained TCP: {:.2} Mbps", throughput_mbps);
    }
}

/// Realistic performance comparison test
#[tokio::test]
async fn test_realistic_performance_comparison() {
    println!("🚀 Realistic TCP Performance Comparison");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:42001".parse().unwrap();
    let node2_addr = "127.0.0.1:42002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    const BUFFER_SIZE: usize = 64 * 1024; // 64KB
    const ITERATIONS: usize = 100;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Comparing different TCP streaming approaches with {}KB buffers",
        BUFFER_SIZE / 1024
    );

    // Test 1: Regular tell()
    let tell_start = Instant::now();
    let mut tell_success = 0;
    for _ in 0..ITERATIONS {
        match conn.tell(&buffer).await {
            Ok(()) => tell_success += 1,
            Err(_) => {}
        }
    }
    let tell_duration = tell_start.elapsed();
    let tell_throughput =
        (tell_success * BUFFER_SIZE) as f64 * 8.0 / tell_duration.as_secs_f64() / 1_000_000.0;

    // Test 2: Stream handle
    let stream_handle = conn.get_stream_handle();
    let stream_start = Instant::now();
    let mut stream_success = 0;
    for _ in 0..ITERATIONS {
        match stream_handle.write_raw(&buffer).await {
            Ok(()) => stream_success += 1,
            Err(_) => {}
        }
    }
    let stream_duration = stream_start.elapsed();
    let stream_throughput =
        (stream_success * BUFFER_SIZE) as f64 * 8.0 / stream_duration.as_secs_f64() / 1_000_000.0;

    // Test 3: Zero-copy handle
    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * 8);
    let zero_copy_start = Instant::now();
    let mut zero_copy_success = 0;
    for i in 0..ITERATIONS {
        match zero_copy.write_raw_no_flush(&buffer).await {
            Ok(()) => {
                zero_copy_success += 1;
                if i % 8 == 7 {
                    let _ = zero_copy.flush().await;
                }
            }
            Err(_) => {}
        }
    }
    let _ = zero_copy.flush().await;
    let zero_copy_duration = zero_copy_start.elapsed();
    let zero_copy_throughput = (zero_copy_success * BUFFER_SIZE) as f64 * 8.0
        / zero_copy_duration.as_secs_f64()
        / 1_000_000.0;

    println!("\n📊 Realistic TCP Performance Comparison:");
    println!(
        "   📤 Regular tell():     {:?} ({:.2} Mbps) - {}/{} success",
        tell_duration, tell_throughput, tell_success, ITERATIONS
    );
    println!(
        "   🔄 Stream handle:      {:?} ({:.2} Mbps) - {}/{} success",
        stream_duration, stream_throughput, stream_success, ITERATIONS
    );
    println!(
        "   ⚡ Zero-copy:          {:?} ({:.2} Mbps) - {}/{} success",
        zero_copy_duration, zero_copy_throughput, zero_copy_success, ITERATIONS
    );

    let best_throughput = tell_throughput
        .max(stream_throughput)
        .max(zero_copy_throughput);
    println!(
        "   🏆 Best TCP throughput: {:.2} Mbps ({:.2} MB/s)",
        best_throughput,
        best_throughput / 8.0
    );

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Realistic comparison completed!");
}
