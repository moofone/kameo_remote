use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Compare ring buffer streaming vs direct tell() performance
#[tokio::test]
async fn test_ring_buffer_vs_direct_tell_comparison() {
    println!("🚀 Ring Buffer vs Direct Tell Performance Comparison");
    println!("📊 Testing lock-free ring buffer streaming vs mutex-protected tell()");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:48001".parse().unwrap();
    let node2_addr = "127.0.0.1:48002".parse().unwrap();

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

    // Test 1: Direct tell() performance (mutex-protected)
    println!("\n🔸 Test 1: Direct Tell Performance (Mutex-Protected TCP)");
    test_direct_tell_performance(&conn).await;

    // Test 2: Ring buffer streaming performance (lock-free)
    println!("\n🔸 Test 2: Ring Buffer Streaming Performance (Lock-Free)");
    test_ring_buffer_streaming_performance(&conn).await;

    // Test 3: Concurrent tell() operations (mutex contention)
    println!("\n🔸 Test 3: Concurrent Tell Operations (Mutex Contention)");
    test_concurrent_tell_operations(&conn).await;

    // Test 4: Concurrent ring buffer operations (lock-free)
    println!("\n🔸 Test 4: Concurrent Ring Buffer Operations (Lock-Free)");
    test_concurrent_ring_buffer_operations(&conn).await;

    // Test 5: Mixed workload comparison
    println!("\n🔸 Test 5: Mixed Workload Comparison");
    test_mixed_workload_comparison(&conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All ring buffer vs direct tell comparison tests completed!");
}

/// Test direct tell() performance using mutex-protected TCP socket
async fn test_direct_tell_performance(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 1000;
    const BUFFER_SIZE: usize = 4096;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing direct tell() with {} iterations of {}KB",
        ITERATIONS,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut successful_operations = 0;

    for _ in 0..ITERATIONS {
        if conn.tell(&buffer).await.is_ok() {
            successful_operations += 1;
        }
    }

    let elapsed = start_time.elapsed();
    let total_bytes = successful_operations * BUFFER_SIZE;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = successful_operations as f64 / elapsed.as_secs_f64();

    println!("📈 Direct Tell Performance Results:");
    println!(
        "   📤 Successful operations: {}/{}",
        successful_operations, ITERATIONS
    );
    println!(
        "   📤 Total bytes: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg per operation: {:.2} μs",
        elapsed.as_micros() as f64 / successful_operations as f64
    );
    println!("   🔒 Method: Mutex-protected TCP socket");
}

/// Test ring buffer streaming performance using lock-free operations
async fn test_ring_buffer_streaming_performance(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 1000;
    const BUFFER_SIZE: usize = 4096;

    let ring_buffer_handle = conn.get_lock_free_stream(ChannelId::Stream1, 512);
    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing ring buffer streaming with {} iterations of {}KB",
        ITERATIONS,
        BUFFER_SIZE / 1024
    );

    let start_time = Instant::now();
    let mut successful_operations = 0;

    for _ in 0..ITERATIONS {
        if ring_buffer_handle.write_nonblocking(&buffer).is_ok() {
            successful_operations += 1;
        }
    }

    let write_elapsed = start_time.elapsed();

    // Give background writer time to process
    sleep(Duration::from_millis(50)).await;

    let total_elapsed = start_time.elapsed();
    let total_bytes = ring_buffer_handle.bytes_written();
    let submit_throughput = (successful_operations * BUFFER_SIZE) as f64 * 8.0
        / write_elapsed.as_secs_f64()
        / 1_000_000.0;
    let actual_throughput = total_bytes as f64 * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = successful_operations as f64 / write_elapsed.as_secs_f64();

    let (pending, available) = ring_buffer_handle.buffer_status();

    println!("📈 Ring Buffer Streaming Performance Results:");
    println!(
        "   📤 Successful operations: {}/{}",
        successful_operations, ITERATIONS
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Write duration: {:?}", write_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Submit rate: {:.2} Mbps ({:.2} MB/s) - how fast data enters ring buffer",
        submit_throughput,
        submit_throughput / 8.0
    );
    println!(
        "   🚀 TCP throughput: {:.2} Mbps ({:.2} MB/s) - actual network transmission",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg submit latency: {:.2} μs",
        write_elapsed.as_micros() as f64 / successful_operations as f64
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔓 Method: Lock-free ring buffer");

    // Show comparison note
    println!("   📝 Note: Submit rate shows memory speed, TCP throughput shows network speed");

    ring_buffer_handle.shutdown();
}

/// Test concurrent tell() operations to show mutex contention
async fn test_concurrent_tell_operations(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 500;
    const BUFFER_SIZE: usize = 4096;
    const CONCURRENT_TASKS: usize = 4;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing {} concurrent tell() tasks with {} iterations each",
        CONCURRENT_TASKS, ITERATIONS
    );

    let start_time = Instant::now();

    // Spawn multiple concurrent tell tasks
    let mut tasks = Vec::new();
    for task_id in 0..CONCURRENT_TASKS {
        let conn = conn.clone();
        let buffer = buffer.clone();
        let task = tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if conn.tell(&buffer).await.is_ok() {
                    successful_operations += 1;
                }
            }
            (task_id, successful_operations)
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut total_successful = 0;
    for task in tasks {
        let (task_id, successful) = task.await.unwrap();
        println!(
            "   🔄 Task {}: {}/{} successful",
            task_id, successful, ITERATIONS
        );
        total_successful += successful;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = total_successful * BUFFER_SIZE;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = total_successful as f64 / elapsed.as_secs_f64();

    println!("📈 Concurrent Tell Operations Results:");
    println!(
        "   📤 Total successful operations: {}/{}",
        total_successful,
        CONCURRENT_TASKS * ITERATIONS
    );
    println!(
        "   📤 Total bytes: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg per operation: {:.2} μs",
        elapsed.as_micros() as f64 / total_successful as f64
    );
    println!("   🔒 Method: Mutex-protected TCP socket (contention expected)");
}

/// Test concurrent ring buffer operations to show lock-free performance
async fn test_concurrent_ring_buffer_operations(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 500;
    const BUFFER_SIZE: usize = 4096;
    const CONCURRENT_TASKS: usize = 4;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing {} concurrent ring buffer tasks with {} iterations each",
        CONCURRENT_TASKS, ITERATIONS
    );

    let start_time = Instant::now();

    // Spawn multiple concurrent ring buffer tasks
    let mut tasks = Vec::new();
    for task_id in 0..CONCURRENT_TASKS {
        let ring_buffer_handle = conn.get_lock_free_stream(
            match task_id {
                0 => ChannelId::Stream1,
                1 => ChannelId::Stream2,
                2 => ChannelId::Stream3,
                _ => ChannelId::Bulk,
            },
            512,
        );
        let buffer = buffer.clone();
        let task = tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if ring_buffer_handle.write_nonblocking(&buffer).is_ok() {
                    successful_operations += 1;
                }
            }
            // Give background writer time to process
            sleep(Duration::from_millis(20)).await;
            (
                task_id,
                successful_operations,
                ring_buffer_handle.bytes_written(),
            )
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut total_successful = 0;
    let mut total_bytes_written = 0;
    for task in tasks {
        let (task_id, successful, bytes_written) = task.await.unwrap();
        println!(
            "   🔄 Task {}: {}/{} successful, {} bytes written",
            task_id, successful, ITERATIONS, bytes_written
        );
        total_successful += successful;
        total_bytes_written += bytes_written;
    }

    let elapsed = start_time.elapsed();
    let throughput_mbps = (total_bytes_written as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = total_successful as f64 / elapsed.as_secs_f64();

    println!("📈 Concurrent Ring Buffer Operations Results:");
    println!(
        "   📤 Total successful operations: {}/{}",
        total_successful,
        CONCURRENT_TASKS * ITERATIONS
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        total_bytes_written,
        total_bytes_written as f64 / 1_000_000.0
    );
    println!("   ⏱️  Duration: {:?}", elapsed);
    println!(
        "   🚀 Throughput: {:.2} Mbps ({:.2} MB/s)",
        throughput_mbps,
        throughput_mbps / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg per operation: {:.2} μs",
        elapsed.as_micros() as f64 / total_successful as f64
    );
    println!("   🔓 Method: Lock-free ring buffer (no contention expected)");
}

/// Test mixed workload of tell() and ring buffer operations
async fn test_mixed_workload_comparison(conn: &crate::connection_pool::ConnectionHandle) {
    const ITERATIONS: usize = 250;
    const BUFFER_SIZE: usize = 4096;

    let buffer = vec![0u8; BUFFER_SIZE];
    let ring_buffer_handle = conn.get_lock_free_stream(ChannelId::Stream1, 512);

    println!(
        "📊 Testing mixed workload: tell() + ring buffer operations with {} iterations each",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Spawn concurrent tasks
    let tell_task = {
        let conn = conn.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if conn.tell(&buffer).await.is_ok() {
                    successful_operations += 1;
                }
            }
            successful_operations
        })
    };

    let ring_buffer_task = {
        let ring_buffer_handle = ring_buffer_handle.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if ring_buffer_handle.write_nonblocking(&buffer).is_ok() {
                    successful_operations += 1;
                }
            }
            // Give background writer time to process
            sleep(Duration::from_millis(30)).await;
            (successful_operations, ring_buffer_handle.bytes_written())
        })
    };

    // Wait for both tasks to complete
    let (tell_result, ring_buffer_result) = tokio::join!(tell_task, ring_buffer_task);

    let elapsed = start_time.elapsed();
    let tell_successful = tell_result.unwrap();
    let (ring_buffer_successful, ring_buffer_bytes) = ring_buffer_result.unwrap();

    let total_bytes = (tell_successful * BUFFER_SIZE) + ring_buffer_bytes;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
    let total_ops = tell_successful + ring_buffer_successful;
    let ops_per_second = total_ops as f64 / elapsed.as_secs_f64();

    let (pending, available) = ring_buffer_handle.buffer_status();

    println!("📈 Mixed Workload Results:");
    println!(
        "   📤 Tell operations: {}/{} successful",
        tell_successful, ITERATIONS
    );
    println!(
        "   📤 Ring buffer operations: {}/{} successful",
        ring_buffer_successful, ITERATIONS
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
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg per operation: {:.2} μs",
        elapsed.as_micros() as f64 / total_ops as f64
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔄 Method: Mixed mutex + lock-free operations");

    ring_buffer_handle.shutdown();

    if tell_successful > 0 && ring_buffer_successful > 0 {
        println!("   ✅ MIXED WORKLOAD SUCCESS! Both methods operating concurrently");
    } else {
        println!("   ⚠️  Some operations failed in mixed workload");
    }
}
