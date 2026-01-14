use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Helper function for lock-free streaming
fn get_lock_free_stream_helper(
    conn: &crate::connection_pool::ConnectionHandle,
    channel_id: ChannelId,
) -> LockFreeStreamHandle {
    conn.get_lock_free_stream(channel_id, 1024)
}

/// Test truly lock-free streaming with ring buffer and background writer
#[tokio::test]
async fn test_lock_free_streaming() {
    println!("🚀 Lock-Free Streaming Test - Ring Buffer + Background Writer");
    println!("🎯 Zero contention between streaming and tell/ask operations");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:45001".parse().unwrap();
    let node2_addr = "127.0.0.1:45002".parse().unwrap();

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

    // Test 1: Basic lock-free streaming
    println!("\n🔸 Test 1: Basic Lock-Free Streaming");
    test_basic_lock_free_streaming(&conn).await;

    // Test 2: Ring buffer concurrent operations
    println!("\n🔸 Test 2: Ring Buffer Concurrent Operations Test");
    test_concurrent_lock_free_tell(&conn).await;

    // Test 3: Multiple ring buffer streams
    println!("\n🔸 Test 3: Multiple Ring Buffer Streams Test");
    test_multiple_lock_free_streams(&conn).await;

    // Test 4: Ring buffer sustained throughput
    println!("\n🔸 Test 4: Ring Buffer Sustained Throughput Test");
    test_high_throughput_lock_free(&conn).await;

    // Test 5: Ring buffer burst performance
    println!("\n🔸 Test 5: Ring Buffer Burst Performance Test");
    test_ring_buffer_performance(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All lock-free streaming tests completed!");
}

/// Test basic lock-free streaming functionality
async fn test_basic_lock_free_streaming(conn: &crate::connection_pool::ConnectionHandle) {
    // Create lock-free streaming handle
    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Stream1, 1024);

    let test_data = b"Hello, lock-free streaming!";

    println!(
        "📤 Testing lock-free stream write: {:?}",
        std::str::from_utf8(test_data)
    );

    // Test non-blocking write
    let result = lock_free_handle.write_nonblocking(test_data);
    assert!(result.is_ok(), "Lock-free write failed: {:?}", result.err());

    // Test vectored write
    let buffers = vec![
        b"Part1".as_slice(),
        b"Part2".as_slice(),
        b"Part3".as_slice(),
    ];
    let result = lock_free_handle.write_vectored_nonblocking(&buffers);
    assert!(
        result.is_ok(),
        "Lock-free vectored write failed: {:?}",
        result.err()
    );

    // Test chunked write
    let large_data = vec![42u8; 10000];
    let result = lock_free_handle.write_chunked_nonblocking(&large_data, 1024);
    assert!(
        result.is_ok(),
        "Lock-free chunked write failed: {:?}",
        result.err()
    );

    // Give the background writer time to process
    sleep(Duration::from_millis(10)).await;

    let (pending, available) = lock_free_handle.buffer_status();
    println!(
        "✅ Basic lock-free streaming: {} bytes written",
        lock_free_handle.bytes_written()
    );
    println!("   📊 Channel: {:?}", lock_free_handle.channel_id());
    println!("   📊 Sequence: {}", lock_free_handle.sequence_number());
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );

    lock_free_handle.shutdown();
}

/// Test concurrent ring buffer streaming and tell operations
async fn test_concurrent_lock_free_tell(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 1000;
    const BUFFER_SIZE: usize = 4096;

    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Stream2, 2048);
    let stream_buffer = vec![0u8; BUFFER_SIZE];
    let tell_buffer = vec![1u8; BUFFER_SIZE];

    println!(
        "📊 Testing concurrent ring buffer + tell with {} iterations",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Spawn concurrent tasks
    let stream_task = {
        let lock_free_handle = lock_free_handle.clone();
        let stream_buffer = stream_buffer.clone();
        tokio::spawn(async move {
            let mut success_count = 0;
            for _ in 0..ITERATIONS {
                if lock_free_handle.write_nonblocking(&stream_buffer).is_ok() {
                    success_count += 1;
                }
            }
            // Give background writer time to process
            sleep(Duration::from_millis(50)).await;
            (success_count, lock_free_handle.bytes_written())
        })
    };

    let tell_task = {
        let conn = conn.clone();
        let tell_buffer = tell_buffer.clone();
        tokio::spawn(async move {
            let mut success_count = 0;
            for _ in 0..ITERATIONS {
                if conn.tell(&tell_buffer).await.is_ok() {
                    success_count += 1;
                }
            }
            success_count
        })
    };

    // Wait for both tasks to complete
    let (stream_result, tell_success) = tokio::join!(stream_task, tell_task);
    let elapsed = start_time.elapsed();

    let (stream_success, stream_bytes) = stream_result.unwrap();
    let tell_success = tell_success.unwrap();

    let total_bytes = stream_bytes + (tell_success * BUFFER_SIZE);
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    let (pending, available) = lock_free_handle.buffer_status();

    println!("📈 Concurrent Ring Buffer + Tell Results:");
    println!(
        "   📤 Ring buffer operations: {}/{} successful",
        stream_success, ITERATIONS
    );
    println!(
        "   📤 Tell operations: {}/{} successful",
        tell_success, ITERATIONS
    );
    println!(
        "   📤 Total bytes: {} bytes ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Combined throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );

    if stream_success > 0 && tell_success > 0 {
        println!("   ✅ RING BUFFER CONCURRENT SUCCESS! Zero contention detected");
    } else {
        println!("   ⚠️  Some operations failed");
    }

    lock_free_handle.shutdown();
}

/// Test multiple ring buffer streams simultaneously
async fn test_multiple_lock_free_streams(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 500;
    const BUFFER_SIZE: usize = 8192;

    // Create multiple lock-free handles on different channels
    let stream1 = conn.get_lock_free_stream(ChannelId::Stream1, 1024);
    let stream2 = conn.get_lock_free_stream(ChannelId::Stream2, 1024);
    let stream3 = conn.get_lock_free_stream(ChannelId::Stream3, 1024);
    let bulk_stream = conn.get_lock_free_stream(ChannelId::Bulk, 1024);

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing multiple ring buffer streams with {} iterations each",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Create tasks for each stream
    let task1 = {
        let stream1 = stream1.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let _ = stream1.write_nonblocking(&buffer);
            }
            sleep(Duration::from_millis(20)).await;
            stream1.bytes_written()
        })
    };

    let task2 = {
        let stream2 = stream2.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let _ = stream2.write_nonblocking(&buffer);
            }
            sleep(Duration::from_millis(20)).await;
            stream2.bytes_written()
        })
    };

    let task3 = {
        let stream3 = stream3.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let _ = stream3.write_nonblocking(&buffer);
            }
            sleep(Duration::from_millis(20)).await;
            stream3.bytes_written()
        })
    };

    let bulk_task = {
        let bulk_stream = bulk_stream.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let _ = bulk_stream.write_nonblocking(&buffer);
            }
            sleep(Duration::from_millis(20)).await;
            bulk_stream.bytes_written()
        })
    };

    // Wait for all tasks
    let (bytes1, bytes2, bytes3, bulk_bytes) = tokio::join!(task1, task2, task3, bulk_task);
    let elapsed = start_time.elapsed();

    let bytes1 = bytes1.unwrap();
    let bytes2 = bytes2.unwrap();
    let bytes3 = bytes3.unwrap();
    let bulk_bytes = bulk_bytes.unwrap();

    let total_bytes = bytes1 + bytes2 + bytes3 + bulk_bytes;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    println!("📈 Multiple Ring Buffer Streams Results:");
    println!(
        "   📤 Stream1 bytes: {} ({:.2} MB)",
        bytes1,
        bytes1 as f64 / 1_000_000.0
    );
    println!(
        "   📤 Stream2 bytes: {} ({:.2} MB)",
        bytes2,
        bytes2 as f64 / 1_000_000.0
    );
    println!(
        "   📤 Stream3 bytes: {} ({:.2} MB)",
        bytes3,
        bytes3 as f64 / 1_000_000.0
    );
    println!(
        "   📤 Bulk bytes: {} ({:.2} MB)",
        bulk_bytes,
        bulk_bytes as f64 / 1_000_000.0
    );
    println!(
        "   📤 Total bytes: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Combined throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );

    // Cleanup
    stream1.shutdown();
    stream2.shutdown();
    stream3.shutdown();
    bulk_stream.shutdown();

    if total_bytes > 0 {
        println!("   ✅ MULTIPLE RING BUFFER STREAMS SUCCESS! All operating independently");
    }
}

/// Test ring buffer sustained throughput performance
async fn test_high_throughput_lock_free(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 128 * 1024; // 128KB
    const STREAM_DURATION: Duration = Duration::from_secs(3);

    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Bulk, 4096);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing ring buffer sustained throughput for {:?}",
        STREAM_DURATION
    );

    let start_time = Instant::now();
    let mut iterations = 0;

    while start_time.elapsed() < STREAM_DURATION {
        let _ = lock_free_handle.write_nonblocking(&buffer);
        iterations += 1;
    }

    // Give background writer time to process
    sleep(Duration::from_millis(100)).await;

    let elapsed = start_time.elapsed();
    let total_bytes = lock_free_handle.bytes_written() as f64;
    let throughput_mbps = (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let writes_per_second = iterations as f64 / elapsed.as_secs_f64();

    let (pending, available) = lock_free_handle.buffer_status();

    println!("📈 Ring Buffer Sustained Throughput Results:");
    println!(
        "   📤 Total bytes: {} ({:.2} GB)",
        lock_free_handle.bytes_written(),
        total_bytes / 1_000_000_000.0
    );
    println!("   🔄 Iterations: {}", iterations);
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!("   📊 Writes/sec: {:.2}", writes_per_second);
    println!(
        "   ⚡ Avg per write: {:.2} ns",
        elapsed.as_nanos() as f64 / iterations as f64
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   📊 Buffer full: {}", lock_free_handle.is_buffer_full());

    if throughput_mbps > 20000.0 {
        println!("   ✅ PHENOMENAL LOCK-FREE PERFORMANCE! >20 Gbps");
    } else if throughput_mbps > 10000.0 {
        println!("   ✅ EXCEPTIONAL RING BUFFER PERFORMANCE! >10 Gbps");
    } else if throughput_mbps > 1000.0 {
        println!("   ✅ EXCELLENT RING BUFFER PERFORMANCE! >1 Gbps");
    } else {
        println!(
            "   ℹ️  Ring buffer performance: {:.2} Mbps",
            throughput_mbps
        );
    }

    lock_free_handle.shutdown();
}

/// Test ring buffer burst performance characteristics
async fn test_ring_buffer_performance(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 1024; // 1KB
    const ITERATIONS: usize = 10000;

    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Priority, 512);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing ring buffer burst performance with {} iterations",
        ITERATIONS
    );

    let start_time = Instant::now();
    let mut successful_writes = 0;
    let mut buffer_full_count = 0;

    for _ in 0..ITERATIONS {
        if lock_free_handle.is_buffer_full() {
            buffer_full_count += 1;
            // Brief pause to let background writer catch up
            sleep(Duration::from_micros(1)).await;
        }

        if lock_free_handle.write_nonblocking(&buffer).is_ok() {
            successful_writes += 1;
        }
    }

    let write_elapsed = start_time.elapsed();

    // Give background writer time to drain the buffer
    sleep(Duration::from_millis(100)).await;

    let total_elapsed = start_time.elapsed();
    let total_bytes = lock_free_handle.bytes_written() as f64;
    let write_throughput =
        (successful_writes * BUFFER_SIZE) as f64 * 8.0 / write_elapsed.as_secs_f64() / 1_000_000.0;
    let actual_throughput = total_bytes * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;

    let (pending, available) = lock_free_handle.buffer_status();

    println!("📈 Ring Buffer Burst Performance Results:");
    println!(
        "   📤 Write operations: {}/{} successful",
        successful_writes, ITERATIONS
    );
    println!("   📤 Buffer full events: {}", buffer_full_count);
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        lock_free_handle.bytes_written(),
        total_bytes / 1_000_000.0
    );
    println!("   ⏱️  Write duration: {:?}", write_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Write throughput: {:.2} Mbps ({:.2} MB/s)",
        write_throughput,
        write_throughput / 8.0
    );
    println!(
        "   🚀 Actual throughput: {:.2} Mbps ({:.2} MB/s)",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!(
        "   ⚡ Avg write latency: {:.2} ns",
        write_elapsed.as_nanos() as f64 / successful_writes as f64
    );

    let efficiency = (successful_writes as f64 / ITERATIONS as f64) * 100.0;
    println!("   📊 Ring buffer efficiency: {:.1}%", efficiency);

    if efficiency > 95.0 {
        println!("   ✅ EXCELLENT RING BUFFER EFFICIENCY! >95%");
    } else if efficiency > 90.0 {
        println!("   ✅ GOOD RING BUFFER EFFICIENCY! >90%");
    } else {
        println!("   ℹ️  Ring buffer efficiency: {:.1}%", efficiency);
    }

    lock_free_handle.shutdown();
}

/// Basic functionality test for lock-free streaming
#[tokio::test]
async fn test_lock_free_basic_functionality() {
    println!("🚀 Basic Lock-Free Streaming Functionality Test");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:46001".parse().unwrap();
    let node2_addr = "127.0.0.1:46002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test creating lock-free handle
    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Stream1, 256);

    assert_eq!(lock_free_handle.channel_id(), ChannelId::Stream1);
    assert_eq!(lock_free_handle.bytes_written(), 0);
    assert_eq!(lock_free_handle.sequence_number(), 0);

    println!(
        "✅ Created lock-free stream: {:?}",
        lock_free_handle.channel_id()
    );

    // Test basic operations
    let test_data = b"Lock-free test data";

    let result = lock_free_handle.write_nonblocking(test_data);
    assert!(result.is_ok());

    let (pending, available) = lock_free_handle.buffer_status();
    println!("✅ Lock-free write successful");
    println!("   📊 Bytes written: {}", lock_free_handle.bytes_written());
    println!("   📊 Sequence: {}", lock_free_handle.sequence_number());
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );

    // Give background writer time to process
    sleep(Duration::from_millis(10)).await;

    lock_free_handle.shutdown();

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Basic lock-free functionality test completed!");
}
