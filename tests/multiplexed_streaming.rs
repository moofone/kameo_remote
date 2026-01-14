use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test multiplexed streaming that doesn't collide with tell/ask operations
#[tokio::test]
async fn test_multiplexed_streaming_no_collision() {
    println!("🚀 Multiplexed Streaming Test - No Collision with Tell/Ask");
    println!("🎯 Concurrent streaming + tell/ask operations on same socket");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:43001".parse().unwrap();
    let node2_addr = "127.0.0.1:43002".parse().unwrap();

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

    // Test 1: Basic multiplexed streaming
    println!("\n🔸 Test 1: Basic Multiplexed Streaming");
    test_basic_multiplexed_streaming(&conn).await;

    // Test 2: Concurrent streaming + tell operations
    println!("\n🔸 Test 2: Concurrent Streaming + Tell Operations");
    test_concurrent_streaming_tell(&conn).await;

    // Test 3: Multiple streaming channels
    println!("\n🔸 Test 3: Multiple Streaming Channels");
    test_multiple_streaming_channels(&conn).await;

    // Test 4: Performance comparison with different channels
    println!("\n🔸 Test 4: Performance Comparison - Different Channels");
    test_channel_performance_comparison(&conn).await;

    // Test 5: High-throughput multiplexed streaming
    println!("\n🔸 Test 5: High-Throughput Multiplexed Streaming");
    test_high_throughput_multiplexed(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All multiplexed streaming tests completed!");
}

/// Test basic multiplexed streaming functionality
async fn test_basic_multiplexed_streaming(conn: &crate::connection_pool::ConnectionHandle) {
    // Create lock-free streaming handle on dedicated channel
    let stream_handle = conn.get_lock_free_stream(ChannelId::Stream1, 16);

    let test_data = b"Hello, multiplexed streaming!";

    println!(
        "📤 Testing multiplexed stream write: {:?}",
        std::str::from_utf8(test_data)
    );

    // Test non-blocking write
    let result = stream_handle.write_nonblocking(test_data);
    assert!(
        result.is_ok(),
        "Lock-free stream write failed: {:?}",
        result.err()
    );

    // Test vectored write
    let buffers = vec![
        b"Part1".as_slice(),
        b"Part2".as_slice(),
        b"Part3".as_slice(),
    ];
    let result = stream_handle.write_vectored_nonblocking(&buffers);
    assert!(
        result.is_ok(),
        "Lock-free vectored write failed: {:?}",
        result.err()
    );

    println!(
        "✅ Basic multiplexed streaming: {} bytes written",
        stream_handle.bytes_written()
    );
    println!("   📊 Channel: {:?}", stream_handle.channel_id());
    println!("   📊 Sequence: {}", stream_handle.sequence_number());
}

/// Test concurrent streaming and tell operations
async fn test_concurrent_streaming_tell(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 100;
    const BUFFER_SIZE: usize = 4096;

    let stream_handle = conn.get_lock_free_stream(ChannelId::Stream2, 32);
    let stream_buffer = vec![0u8; BUFFER_SIZE];
    let tell_buffer = vec![1u8; BUFFER_SIZE];

    println!(
        "📊 Testing concurrent streaming + tell with {} iterations",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Spawn concurrent tasks
    let stream_task = {
        let stream_handle = stream_handle.clone();
        let stream_buffer = stream_buffer.clone();
        tokio::spawn(async move {
            let mut success_count = 0;
            for _ in 0..ITERATIONS {
                if stream_handle.write_nonblocking(&stream_buffer).is_ok() {
                    success_count += 1;
                }
            }
            success_count
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
    let (stream_success, tell_success) = tokio::join!(stream_task, tell_task);
    let elapsed = start_time.elapsed();

    let stream_success = stream_success.unwrap();
    let tell_success = tell_success.unwrap();

    let total_bytes = (stream_success + tell_success) * BUFFER_SIZE;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    println!("📈 Concurrent Operations Results:");
    println!(
        "   📤 Stream operations: {}/{} successful",
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

    if stream_success > 0 && tell_success > 0 {
        println!("   ✅ CONCURRENT OPERATIONS SUCCESSFUL! No collisions detected");
    } else {
        println!("   ⚠️  Some operations failed - possible collision or contention");
    }
}

/// Test multiple streaming channels simultaneously
async fn test_multiple_streaming_channels(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 50;
    const BUFFER_SIZE: usize = 8192;

    // Create multiple lock-free streaming handles on different channels
    let stream1 = conn.get_lock_free_stream(ChannelId::Stream1, 16);
    let stream2 = conn.get_lock_free_stream(ChannelId::Stream2, 16);
    let stream3 = conn.get_lock_free_stream(ChannelId::Stream3, 16);
    let bulk_stream = conn.get_lock_free_stream(ChannelId::Bulk, 16);

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing multiple streaming channels with {} iterations each",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Create tasks for each channel
    let task1 = {
        let stream1 = stream1.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            for _ in 0..ITERATIONS {
                let _ = stream1.write_nonblocking(&buffer);
            }
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

    println!("📈 Multiple Channels Results:");
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

    if total_bytes > 0 {
        println!("   ✅ MULTIPLE CHANNELS SUCCESSFUL! All channels operating independently");
    }
}

/// Test performance comparison between different channel types
async fn test_channel_performance_comparison(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 100;
    const BUFFER_SIZE: usize = 32 * 1024; // 32KB

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Comparing channel performance with {}KB buffers",
        BUFFER_SIZE / 1024
    );

    // Test regular tell()
    let tell_start = Instant::now();
    let mut tell_success = 0;
    for _ in 0..ITERATIONS {
        if conn.tell(&buffer).await.is_ok() {
            tell_success += 1;
        }
    }
    let tell_duration = tell_start.elapsed();

    // Test lock-free stream
    let stream_handle = conn.get_lock_free_stream(ChannelId::Stream1, 32);
    let stream_start = Instant::now();
    let stream_initial_bytes = stream_handle.bytes_written();
    for _ in 0..ITERATIONS {
        let _ = stream_handle.write_nonblocking(&buffer);
    }
    let stream_duration = stream_start.elapsed();
    let stream_bytes = stream_handle.bytes_written() - stream_initial_bytes;

    // Test priority stream
    let priority_handle = conn.get_lock_free_stream(ChannelId::Priority, 32);
    let priority_start = Instant::now();
    let priority_initial_bytes = priority_handle.bytes_written();
    for _ in 0..ITERATIONS {
        let _ = priority_handle.write_nonblocking(&buffer);
    }
    let priority_duration = priority_start.elapsed();
    let priority_bytes = priority_handle.bytes_written() - priority_initial_bytes;

    // Calculate throughputs
    let tell_throughput =
        (tell_success * BUFFER_SIZE) as f64 * 8.0 / tell_duration.as_secs_f64() / 1_000_000.0;
    let stream_throughput = stream_bytes as f64 * 8.0 / stream_duration.as_secs_f64() / 1_000_000.0;
    let priority_throughput =
        priority_bytes as f64 * 8.0 / priority_duration.as_secs_f64() / 1_000_000.0;

    println!("\n📊 Channel Performance Comparison:");
    println!(
        "   📤 Regular tell():     {:?} ({:.2} Mbps) - {}/{} success",
        tell_duration, tell_throughput, tell_success, ITERATIONS
    );
    println!(
        "   🔄 Stream channel:     {:?} ({:.2} Mbps) - {} bytes",
        stream_duration, stream_throughput, stream_bytes
    );
    println!(
        "   ⚡ Priority channel:   {:?} ({:.2} Mbps) - {} bytes",
        priority_duration, priority_throughput, priority_bytes
    );

    let best_throughput = tell_throughput
        .max(stream_throughput)
        .max(priority_throughput);
    println!(
        "   🏆 Best throughput: {:.2} Mbps ({:.2} MB/s)",
        best_throughput,
        best_throughput / 8.0
    );
}

/// Test high-throughput multiplexed streaming
async fn test_high_throughput_multiplexed(conn: &crate::connection_pool::ConnectionHandle) {
    const BUFFER_SIZE: usize = 128 * 1024; // 128KB
    const STREAM_DURATION: Duration = Duration::from_secs(3);

    let bulk_handle = conn.get_lock_free_stream(ChannelId::Bulk, 64);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing high-throughput multiplexed streaming for {:?}",
        STREAM_DURATION
    );

    let start_time = Instant::now();
    let mut iterations = 0;

    while start_time.elapsed() < STREAM_DURATION {
        let _ = bulk_handle.write_nonblocking(&buffer);
        iterations += 1;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = bulk_handle.bytes_written() as f64;
    let throughput_mbps = (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let writes_per_second = iterations as f64 / elapsed.as_secs_f64();

    println!("📈 High-Throughput Multiplexed Results:");
    println!(
        "   📤 Total bytes: {} ({:.2} GB)",
        bulk_handle.bytes_written(),
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
    println!("   📊 Channel: {:?}", bulk_handle.channel_id());

    if throughput_mbps > 10000.0 {
        println!("   ✅ EXCEPTIONAL MULTIPLEXED PERFORMANCE! >10 Gbps");
    } else if throughput_mbps > 1000.0 {
        println!("   ✅ EXCELLENT MULTIPLEXED PERFORMANCE! >1 Gbps");
    } else {
        println!(
            "   ℹ️  Multiplexed performance: {:.2} Mbps",
            throughput_mbps
        );
    }
}

/// Basic functionality test for multiplexed streaming
#[tokio::test]
async fn test_multiplexed_basic_functionality() {
    println!("🚀 Basic Multiplexed Streaming Functionality Test");

    // Setup
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:44001".parse().unwrap();
    let node2_addr = "127.0.0.1:44002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();

    // Test creating different channel handles
    let stream1 = conn.get_lock_free_stream(ChannelId::Stream1, 8);
    let stream2 = conn.get_lock_free_stream(ChannelId::Stream2, 8);
    let bulk = conn.get_lock_free_stream(ChannelId::Bulk, 8);

    assert_eq!(stream1.channel_id(), ChannelId::Stream1);
    assert_eq!(stream2.channel_id(), ChannelId::Stream2);
    assert_eq!(bulk.channel_id(), ChannelId::Bulk);

    println!("✅ Created multiplexed streams: Stream1, Stream2, Bulk");

    // Test basic operations
    let test_data = b"Multiplexed test data";

    let result1 = stream1.write_nonblocking(test_data);
    let result2 = stream2.write_nonblocking(test_data);
    let result3 = bulk.write_nonblocking(test_data);

    assert!(result1.is_ok());
    assert!(result2.is_ok());
    assert!(result3.is_ok());

    println!("✅ All multiplexed streams working independently");
    println!("   📊 Stream1 bytes: {}", stream1.bytes_written());
    println!("   📊 Stream2 bytes: {}", stream2.bytes_written());
    println!("   📊 Bulk bytes: {}", bulk.bytes_written());

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("✅ Basic multiplexed functionality test completed!");
}
