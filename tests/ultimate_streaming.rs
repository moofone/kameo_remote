use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Ultimate performance streaming test - no acknowledgment, maximum throughput
#[tokio::test]
async fn test_ultimate_streaming_no_ack() {
    println!("🚀 Ultimate Performance Streaming Test - NO ACKNOWLEDGMENT MODE");
    println!("🎯 Target: >50 Gbps throughput, absolute maximum performance");

    // Setup two nodes
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

    println!(
        "✅ Connection established: {} -> {}",
        node1_addr, node2_addr
    );

    // Test 1: Ultimate throughput with no acknowledgment
    println!("\n🔸 Test 1: Ultimate No-Ack Throughput");
    test_ultimate_no_ack_throughput(&conn).await;

    // Test 2: Ultimate vectored I/O with no acknowledgment
    println!("\n🔸 Test 2: Ultimate Vectored No-Ack");
    test_ultimate_vectored_no_ack(&conn).await;

    // Test 3: Ultimate bulk write with no acknowledgment
    println!("\n🔸 Test 3: Ultimate Bulk No-Ack");
    test_ultimate_bulk_no_ack(&conn).await;

    // Test 4: Ultimate continuous streaming
    println!("\n🔸 Test 4: Ultimate Continuous Streaming");
    test_ultimate_continuous_streaming(&conn).await;

    // Test 5: Ultimate performance comparison
    println!("\n🔸 Test 5: Ultimate vs Zero-Copy Comparison");
    test_ultimate_vs_zero_copy(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All ultimate performance tests completed!");
}

/// Test ultimate throughput with no acknowledgment
async fn test_ultimate_no_ack_throughput(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 128 * 1024; // 128KB buffers
    const ITERATIONS: usize = 2000;

    // Create ultimate handle with large buffer pool
    let ultimate = conn.get_ultimate_handle(32, BUFFER_SIZE);

    // Pre-allocate buffer to avoid allocations
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing no-ack mode with {}KB buffers, {} iterations",
        BUFFER_SIZE / 1024,
        ITERATIONS
    );

    let start_time = Instant::now();

    // Write as fast as possible with no acknowledgment
    for _ in 0..ITERATIONS {
        // Fire and forget - no error checking
        let _ = ultimate.write_no_ack(&buffer).await;
    }

    // Never flush - let kernel handle everything
    let _ = ultimate.never_flush().await;

    let elapsed = start_time.elapsed();
    let total_bytes = ultimate.bytes_written() as f64;
    let throughput_gbps =
        (total_bytes * 8.0) / (elapsed.as_nanos() as f64 / 1_000_000_000.0) / 1_000_000_000.0;

    println!("📈 Ultimate No-Ack Throughput Results:");
    println!(
        "   📤 Bytes Written: {} bytes ({:.2} MB)",
        ultimate.bytes_written(),
        total_bytes / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_gbps * 1000.0 / 8.0
    );
    println!(
        "   ⚡ Avg per write: {:.2} ns",
        elapsed.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "   📊 Success Rate: {:.1}%",
        (total_bytes / (BUFFER_SIZE * ITERATIONS) as f64) * 100.0
    );

    if throughput_gbps > 50.0 {
        println!("   ✅ ULTIMATE PERFORMANCE ACHIEVED! >50 Gbps");
    } else if throughput_gbps > 20.0 {
        println!("   ✅ EXCEPTIONAL PERFORMANCE! >20 Gbps");
    } else {
        println!("   ⚠️  Performance: {:.2} Gbps", throughput_gbps);
    }
}

/// Test ultimate vectored I/O with no acknowledgment
async fn test_ultimate_vectored_no_ack(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 8192; // 8KB per buffer
    const VECTORS_PER_WRITE: usize = 32; // 32 buffers per vectored write
    const ITERATIONS: usize = 1000;

    let ultimate = conn.get_ultimate_handle(64, BUFFER_SIZE * VECTORS_PER_WRITE);

    // Pre-allocate buffers
    let buffers: Vec<Vec<u8>> = (0..VECTORS_PER_WRITE)
        .map(|i| vec![i as u8; BUFFER_SIZE])
        .collect();

    let buffer_refs: Vec<&[u8]> = buffers.iter().map(|b| b.as_slice()).collect();

    println!(
        "📊 Testing ultimate vectored no-ack with {} buffers of {}KB each",
        VECTORS_PER_WRITE,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();

    for _ in 0..ITERATIONS {
        // Fire and forget vectored write
        let _ = ultimate.write_vectored_no_ack(&buffer_refs).await;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = ultimate.bytes_written() as f64;
    let throughput_gbps =
        (total_bytes * 8.0) / (elapsed.as_nanos() as f64 / 1_000_000_000.0) / 1_000_000_000.0;

    println!("📈 Ultimate Vectored No-Ack Results:");
    println!(
        "   📤 Bytes Written: {} bytes ({:.2} MB)",
        ultimate.bytes_written(),
        total_bytes / 1_000_000.0
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

/// Test ultimate bulk write with no acknowledgment
async fn test_ultimate_bulk_no_ack(conn: &crate::connection_pool::ConnectionHandle) {
    const CHUNK_SIZE: usize = 16 * 1024; // 16KB chunks
    const CHUNKS_PER_BULK: usize = 64; // 64 chunks per bulk write
    const ITERATIONS: usize = 500;

    let ultimate = conn.get_ultimate_handle(128, CHUNK_SIZE * CHUNKS_PER_BULK);

    // Pre-allocate chunks
    let chunks: Vec<Vec<u8>> = (0..CHUNKS_PER_BULK)
        .map(|i| vec![i as u8; CHUNK_SIZE])
        .collect();

    let chunk_refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();

    println!(
        "📊 Testing ultimate bulk no-ack with {} chunks of {}KB each",
        CHUNKS_PER_BULK,
        CHUNK_SIZE / 1024
    );

    let start_time = Instant::now();

    for _ in 0..ITERATIONS {
        // Fire and forget bulk write
        let _ = ultimate.bulk_write_no_ack(&chunk_refs).await;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = ultimate.bytes_written() as f64;
    let throughput_gbps =
        (total_bytes * 8.0) / (elapsed.as_nanos() as f64 / 1_000_000_000.0) / 1_000_000_000.0;

    println!("📈 Ultimate Bulk No-Ack Results:");
    println!(
        "   📤 Bytes Written: {} bytes ({:.2} MB)",
        ultimate.bytes_written(),
        total_bytes / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Gbps ({:.2} MB/s)",
        throughput_gbps,
        throughput_gbps * 1000.0 / 8.0
    );
    println!(
        "   ⚡ Avg per bulk write: {:.2} μs",
        elapsed.as_micros() as f64 / ITERATIONS as f64
    );
}

/// Test ultimate continuous streaming
async fn test_ultimate_continuous_streaming(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffers
    const STREAM_DURATION: Duration = Duration::from_secs(3); // 3 seconds

    let ultimate = conn.get_ultimate_handle(256, BUFFER_SIZE);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing ultimate continuous streaming for {:?} with {}KB buffers",
        STREAM_DURATION,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut iteration_count = 0;

    // Stream continuously at maximum speed
    while start_time.elapsed() < STREAM_DURATION {
        // Fire and forget - no error checking, no waiting
        let _ = ultimate.write_no_ack(&buffer).await;
        iteration_count += 1;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = ultimate.bytes_written() as f64;
    let throughput_gbps =
        (total_bytes * 8.0) / (elapsed.as_nanos() as f64 / 1_000_000_000.0) / 1_000_000_000.0;
    let messages_per_second = iteration_count as f64 / elapsed.as_secs_f64();

    println!("📈 Ultimate Continuous Streaming Results:");
    println!(
        "   📤 Bytes Written: {} bytes ({:.2} GB)",
        ultimate.bytes_written(),
        total_bytes / 1_000_000_000.0
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
    println!(
        "   📊 Success Rate: {:.1}%",
        (total_bytes / (BUFFER_SIZE * iteration_count) as f64) * 100.0
    );

    if throughput_gbps > 100.0 {
        println!("   ✅ PHENOMENAL PERFORMANCE! >100 Gbps sustained");
    } else if throughput_gbps > 50.0 {
        println!("   ✅ ULTIMATE PERFORMANCE! >50 Gbps sustained");
    } else if throughput_gbps > 20.0 {
        println!("   ✅ EXCEPTIONAL PERFORMANCE! >20 Gbps sustained");
    } else {
        println!("   ⚠️  Performance: {:.2} Gbps", throughput_gbps);
    }
}

/// Compare ultimate vs zero-copy performance
async fn test_ultimate_vs_zero_copy(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 32 * 1024; // 32KB
    const ITERATIONS: usize = 1000;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!("📊 Comparing Ultimate vs Zero-Copy performance");

    // Test zero-copy handle
    let zero_copy = conn.get_zero_copy_handle(BUFFER_SIZE * 16);
    let zero_copy_start = Instant::now();
    for i in 0..ITERATIONS {
        zero_copy.write_raw_no_flush(&buffer).await.unwrap();
        if i % 16 == 15 {
            zero_copy.flush().await.unwrap();
        }
    }
    zero_copy.flush().await.unwrap();
    let zero_copy_duration = zero_copy_start.elapsed();

    // Test ultimate handle
    let ultimate = conn.get_ultimate_handle(64, BUFFER_SIZE * 32);
    let ultimate_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = ultimate.write_no_ack(&buffer).await;
    }
    let ultimate_duration = ultimate_start.elapsed();

    // Test ultimate vectored
    let buffers: Vec<&[u8]> = (0..16).map(|_| buffer.as_slice()).collect();
    let ultimate_vectored_start = Instant::now();
    for _ in 0..(ITERATIONS / 16) {
        let _ = ultimate.write_vectored_no_ack(&buffers).await;
    }
    let ultimate_vectored_duration = ultimate_vectored_start.elapsed();

    let total_bytes = (BUFFER_SIZE * ITERATIONS) as f64;
    let zero_copy_throughput = (total_bytes * 8.0)
        / (zero_copy_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let ultimate_throughput = (ultimate.bytes_written() as f64 * 8.0)
        / (ultimate_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;
    let ultimate_vectored_throughput = (ultimate.bytes_written() as f64 * 8.0)
        / (ultimate_vectored_duration.as_nanos() as f64 / 1_000_000_000.0)
        / 1_000_000_000.0;

    println!("\n📊 Ultimate vs Zero-Copy Results:");
    println!(
        "   📤 Zero-Copy:           {:?} ({:.2} Gbps)",
        zero_copy_duration, zero_copy_throughput
    );
    println!(
        "   🚀 Ultimate No-Ack:     {:?} ({:.2} Gbps)",
        ultimate_duration, ultimate_throughput
    );
    println!(
        "   ⚡ Ultimate Vectored:    {:?} ({:.2} Gbps)",
        ultimate_vectored_duration, ultimate_vectored_throughput
    );

    let ultimate_speedup = ultimate_throughput / zero_copy_throughput;
    let vectored_speedup = ultimate_vectored_throughput / zero_copy_throughput;

    println!("   📈 Ultimate Speedup:    {:.2}x", ultimate_speedup);
    println!("   📈 Vectored Speedup:    {:.2}x", vectored_speedup);

    if ultimate_speedup > 2.0 {
        println!("   ✅ SIGNIFICANT IMPROVEMENT! >2x speedup");
    } else if ultimate_speedup > 1.5 {
        println!("   ✅ NOTABLE IMPROVEMENT! >1.5x speedup");
    } else {
        println!("   ℹ️  Performance: {:.2}x", ultimate_speedup);
    }
}

/// Simple ultimate streaming functionality test
#[tokio::test]
async fn test_ultimate_streaming_basic() {
    println!("🚀 Basic Ultimate Streaming Test");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:40001".parse().unwrap();
    let node2_addr = "127.0.0.1:40002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test ultimate handle creation
    let ultimate = conn.get_ultimate_handle(16, 4096);
    let test_data = b"Hello, ultimate streaming!";

    println!(
        "📤 Testing ultimate no-ack write: {:?}",
        std::str::from_utf8(test_data)
    );
    let result = ultimate.write_no_ack(test_data).await;
    assert!(result.is_ok(), "Ultimate write failed: {:?}", result.err());

    println!("✅ Ultimate no-ack write successful");

    // Test vectored write
    let bufs = vec![
        b"Hello, ".as_slice(),
        b"ultimate ".as_slice(),
        b"vectored!".as_slice(),
    ];
    let result = ultimate.write_vectored_no_ack(&bufs).await;
    assert!(
        result.is_ok(),
        "Ultimate vectored write failed: {:?}",
        result.err()
    );

    println!("✅ Ultimate vectored write successful");

    // Test bulk write
    let chunks = vec![
        b"Chunk1".as_slice(),
        b"Chunk2".as_slice(),
        b"Chunk3".as_slice(),
    ];
    let result = ultimate.bulk_write_no_ack(&chunks).await;
    assert!(
        result.is_ok(),
        "Ultimate bulk write failed: {:?}",
        result.err()
    );

    println!("✅ Ultimate bulk write successful");

    println!("📊 Total bytes written: {}", ultimate.bytes_written());

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Basic ultimate streaming test completed!");
}
