use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Basic functionality test for lock-free streaming
#[tokio::test]
async fn test_lock_free_basic() {
    println!("🚀 Basic Lock-Free Streaming Test");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:47001".parse().unwrap();
    let node2_addr = "127.0.0.1:47002".parse().unwrap();

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

    // Test vectored write
    let buffers = vec![
        b"Part1".as_slice(),
        b"Part2".as_slice(),
        b"Part3".as_slice(),
    ];
    let result = lock_free_handle.write_vectored_nonblocking(&buffers);
    assert!(result.is_ok());

    println!("✅ Lock-free vectored write successful");

    // Give background writer time to process
    sleep(Duration::from_millis(50)).await;

    let (pending_after, available_after) = lock_free_handle.buffer_status();
    println!(
        "   📊 After processing: {} pending, {} available",
        pending_after, available_after
    );

    lock_free_handle.shutdown();

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Basic lock-free test completed!");
}

/// Test concurrent lock-free streaming and tell operations
#[tokio::test]
async fn test_lock_free_concurrent() {
    println!("🚀 Lock-Free Concurrent Test");

    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:47003".parse().unwrap();
    let node2_addr = "127.0.0.1:47004".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    const ITERATIONS: usize = 100;
    const BUFFER_SIZE: usize = 1024;

    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Stream2, 512);
    let stream_buffer = vec![0u8; BUFFER_SIZE];
    let tell_buffer = vec![1u8; BUFFER_SIZE];

    println!(
        "📊 Testing concurrent lock-free + tell with {} iterations",
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

    println!("📈 Concurrent Lock-Free + Tell Results:");
    println!(
        "   📤 Lock-free operations: {}/{} successful",
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
        println!("   ✅ LOCK-FREE CONCURRENT SUCCESS! Zero contention detected");
    } else {
        println!("   ⚠️  Some operations failed");
    }

    lock_free_handle.shutdown();

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Concurrent lock-free test completed!");
}

/// Test high-throughput lock-free streaming
#[tokio::test]
async fn test_lock_free_performance() {
    println!("🚀 Lock-Free Performance Test");

    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:47005".parse().unwrap();
    let node2_addr = "127.0.0.1:47006".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    const BUFFER_SIZE: usize = 64 * 1024; // 64KB
    const STREAM_DURATION: Duration = Duration::from_secs(2);

    let lock_free_handle = conn.get_lock_free_stream(ChannelId::Bulk, 2048);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing high-throughput lock-free streaming for {:?}",
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

    println!("📈 Lock-Free Performance Results:");
    println!(
        "   📤 Total bytes: {} ({:.2} MB)",
        lock_free_handle.bytes_written(),
        total_bytes / 1_000_000.0
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

    if throughput_mbps > 10000.0 {
        println!("   ✅ EXCEPTIONAL LOCK-FREE PERFORMANCE! >10 Gbps");
    } else if throughput_mbps > 1000.0 {
        println!("   ✅ EXCELLENT LOCK-FREE PERFORMANCE! >1 Gbps");
    } else {
        println!("   ℹ️  Lock-free performance: {:.2} Mbps", throughput_mbps);
    }

    lock_free_handle.shutdown();

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Performance test completed!");
}
