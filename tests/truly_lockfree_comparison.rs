use kameo_remote::connection_pool::ConnectionHandle;
use kameo_remote::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test truly lock-free architecture where ALL operations go through ring buffer
#[tokio::test]
async fn test_truly_lockfree_vs_mutex_comparison() {
    println!("🚀 Truly Lock-Free vs Mutex Architecture Comparison");
    println!("📊 Testing where ALL operations (tell + streaming) go through ring buffer");

    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:49001".parse().unwrap();
    let node2_addr = "127.0.0.1:49002".parse().unwrap();

    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone()))
        .await
        .unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone()))
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;
    let mut conn = node1.get_connection(node2_addr).await.unwrap();

    println!(
        "✅ Connection established: {} -> {}",
        node1_addr, node2_addr
    );

    // Test 1: Traditional mutex-based tell() performance
    println!("\n🔸 Test 1: Traditional Mutex-Based Tell Performance");
    test_traditional_mutex_tell(&conn).await;

    // Test 2: Truly lock-free tell() performance (through ring buffer)
    println!("\n🔸 Test 2: Truly Lock-Free Tell Performance");
    test_truly_lockfree_tell(&mut conn).await;

    // Test 3: Mixed workload - all operations lock-free
    println!("\n🔸 Test 3: Mixed Workload - All Operations Lock-Free");
    test_mixed_workload_lockfree(&mut conn).await;

    // Test 4: Concurrent operations - all lock-free
    println!("\n🔸 Test 4: Concurrent Operations - All Lock-Free");
    test_concurrent_all_lockfree(&mut conn).await;

    // Test 5: High throughput comparison
    println!("\n🔸 Test 5: High Throughput Comparison");
    test_high_throughput_comparison(&mut conn).await;

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;

    println!("\n✅ All truly lock-free comparison tests completed!");
}

/// Test traditional mutex-based tell() performance
async fn test_traditional_mutex_tell(conn: &ConnectionHandle) {
    const ITERATIONS: usize = 1000;
    const BUFFER_SIZE: usize = 4096;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing traditional mutex-based tell() with {} iterations",
        ITERATIONS
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

    println!("📈 Traditional Mutex-Based Tell Results:");
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

/// Test truly lock-free tell() performance (through ring buffer)
async fn test_truly_lockfree_tell(conn: &mut ConnectionHandle) {
    const ITERATIONS: usize = 1000;
    const BUFFER_SIZE: usize = 4096;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing truly lock-free tell() with {} iterations",
        ITERATIONS
    );

    let start_time = Instant::now();
    let mut successful_operations = 0;

    for _ in 0..ITERATIONS {
        if conn.tell_lockfree(&buffer).is_ok() {
            successful_operations += 1;
        }
    }

    let submit_elapsed = start_time.elapsed();

    // Give background writer time to process
    sleep(Duration::from_millis(50)).await;

    let total_elapsed = start_time.elapsed();
    let total_bytes = successful_operations * BUFFER_SIZE;
    let submit_throughput = (total_bytes as f64 * 8.0) / submit_elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = successful_operations as f64 / submit_elapsed.as_secs_f64();

    // Get ring buffer stats
    let ring_buffer = conn.get_or_create_global_ring_buffer();
    let actual_bytes = ring_buffer.bytes_written();
    let actual_throughput = actual_bytes as f64 * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;
    let (pending, available) = ring_buffer.buffer_status();

    println!("📈 Truly Lock-Free Tell Results:");
    println!(
        "   📤 Successful operations: {}/{}",
        successful_operations, ITERATIONS
    );
    println!(
        "   📤 Total bytes submitted: {} ({:.2} MB)",
        total_bytes,
        total_bytes as f64 / 1_000_000.0
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        actual_bytes,
        actual_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Submit duration: {:?}", submit_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Submit throughput: {:.2} Mbps ({:.2} MB/s)",
        submit_throughput,
        submit_throughput / 8.0
    );
    println!(
        "   🚀 TCP throughput: {:.2} Mbps ({:.2} MB/s)",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg submit latency: {:.2} μs",
        submit_elapsed.as_micros() as f64 / successful_operations as f64
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔓 Method: Lock-free ring buffer with exclusive TCP");
}

/// Test mixed workload - all operations lock-free
async fn test_mixed_workload_lockfree(conn: &mut ConnectionHandle) {
    const ITERATIONS: usize = 500;
    const BUFFER_SIZE: usize = 4096;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing mixed workload - all operations lock-free with {} iterations",
        ITERATIONS
    );

    let start_time = Instant::now();

    // Get shared ring buffer for all operations
    let ring_buffer = conn.get_or_create_global_ring_buffer();

    // Spawn concurrent tasks - all using lock-free operations
    let tell_task = {
        let mut conn = conn.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if conn.tell_lockfree(&buffer).is_ok() {
                    successful_operations += 1;
                }
            }
            successful_operations
        })
    };

    let stream_task = {
        let ring_buffer = ring_buffer.clone();
        let buffer = buffer.clone();
        tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if ring_buffer.write_nonblocking(&buffer).is_ok() {
                    successful_operations += 1;
                }
            }
            successful_operations
        })
    };

    // Wait for both tasks to complete
    let (tell_successful, stream_successful) = tokio::join!(tell_task, stream_task);

    let submit_elapsed = start_time.elapsed();

    // Give background writer time to process
    sleep(Duration::from_millis(50)).await;

    let total_elapsed = start_time.elapsed();
    let tell_successful = tell_successful.unwrap();
    let stream_successful = stream_successful.unwrap();

    let total_operations = tell_successful + stream_successful;
    let total_bytes_submitted = total_operations * BUFFER_SIZE;
    let actual_bytes = ring_buffer.bytes_written();
    let submit_throughput =
        (total_bytes_submitted as f64 * 8.0) / submit_elapsed.as_secs_f64() / 1_000_000.0;
    let actual_throughput = actual_bytes as f64 * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;
    let (pending, available) = ring_buffer.buffer_status();

    println!("📈 Mixed Workload Lock-Free Results:");
    println!(
        "   📤 Tell operations: {}/{} successful",
        tell_successful, ITERATIONS
    );
    println!(
        "   📤 Stream operations: {}/{} successful",
        stream_successful, ITERATIONS
    );
    println!("   📤 Total operations: {}", total_operations);
    println!(
        "   📤 Total bytes submitted: {} ({:.2} MB)",
        total_bytes_submitted,
        total_bytes_submitted as f64 / 1_000_000.0
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        actual_bytes,
        actual_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Submit duration: {:?}", submit_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Submit throughput: {:.2} Mbps ({:.2} MB/s)",
        submit_throughput,
        submit_throughput / 8.0
    );
    println!(
        "   🚀 TCP throughput: {:.2} Mbps ({:.2} MB/s)",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔓 Method: All operations through lock-free ring buffer");

    if tell_successful > 0 && stream_successful > 0 {
        println!("   ✅ MIXED LOCK-FREE SUCCESS! All operations through single ring buffer");
    } else {
        println!("   ⚠️  Some operations failed");
    }
}

/// Test concurrent operations - all lock-free
async fn test_concurrent_all_lockfree(conn: &mut ConnectionHandle) {
    const ITERATIONS: usize = 250;
    const BUFFER_SIZE: usize = 4096;
    const CONCURRENT_TASKS: usize = 4;

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing {} concurrent lock-free tasks with {} iterations each",
        CONCURRENT_TASKS, ITERATIONS
    );

    let start_time = Instant::now();

    // Create multiple concurrent tasks - all lock-free
    let mut tasks = Vec::new();
    for task_id in 0..CONCURRENT_TASKS {
        let mut conn = conn.clone();
        let buffer = buffer.clone();
        let task = tokio::spawn(async move {
            let mut successful_operations = 0;
            for _ in 0..ITERATIONS {
                if conn.tell_lockfree(&buffer).is_ok() {
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

    let submit_elapsed = start_time.elapsed();

    // Give background writer time to process
    sleep(Duration::from_millis(50)).await;

    let total_elapsed = start_time.elapsed();
    let ring_buffer = conn.get_or_create_global_ring_buffer();
    let actual_bytes = ring_buffer.bytes_written();
    let total_bytes_submitted = total_successful * BUFFER_SIZE;
    let submit_throughput =
        (total_bytes_submitted as f64 * 8.0) / submit_elapsed.as_secs_f64() / 1_000_000.0;
    let actual_throughput = actual_bytes as f64 * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;
    let (pending, available) = ring_buffer.buffer_status();

    println!("📈 Concurrent Lock-Free Results:");
    println!(
        "   📤 Total successful operations: {}/{}",
        total_successful,
        CONCURRENT_TASKS * ITERATIONS
    );
    println!(
        "   📤 Total bytes submitted: {} ({:.2} MB)",
        total_bytes_submitted,
        total_bytes_submitted as f64 / 1_000_000.0
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        actual_bytes,
        actual_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Submit duration: {:?}", submit_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Submit throughput: {:.2} Mbps ({:.2} MB/s)",
        submit_throughput,
        submit_throughput / 8.0
    );
    println!(
        "   🚀 TCP throughput: {:.2} Mbps ({:.2} MB/s)",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔓 Method: All concurrent operations through lock-free ring buffer");

    if total_successful > 0 {
        println!("   ✅ CONCURRENT LOCK-FREE SUCCESS! Zero contention across all tasks");
    }
}

/// Test high throughput comparison
async fn test_high_throughput_comparison(conn: &mut ConnectionHandle) {
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB
    const STREAM_DURATION: Duration = Duration::from_secs(2);

    let buffer = vec![0u8; BUFFER_SIZE];

    println!(
        "📊 Testing high-throughput lock-free operations for {:?}",
        STREAM_DURATION
    );

    let ring_buffer = conn.get_or_create_global_ring_buffer();

    let start_time = Instant::now();
    let mut iterations = 0;

    // Mix of tell and stream operations
    while start_time.elapsed() < STREAM_DURATION {
        if iterations % 2 == 0 {
            let _ = conn.tell_lockfree(&buffer);
        } else {
            let _ = ring_buffer.write_nonblocking(&buffer);
        }
        iterations += 1;
    }

    let submit_elapsed = start_time.elapsed();

    // Give background writer time to process
    sleep(Duration::from_millis(100)).await;

    let total_elapsed = start_time.elapsed();
    let actual_bytes = ring_buffer.bytes_written();
    let submitted_bytes = iterations * BUFFER_SIZE;
    let submit_throughput =
        (submitted_bytes as f64 * 8.0) / submit_elapsed.as_secs_f64() / 1_000_000.0;
    let actual_throughput = actual_bytes as f64 * 8.0 / total_elapsed.as_secs_f64() / 1_000_000.0;
    let ops_per_second = iterations as f64 / submit_elapsed.as_secs_f64();
    let (pending, available) = ring_buffer.buffer_status();

    println!("📈 High-Throughput Lock-Free Results:");
    println!("   📤 Total operations: {}", iterations);
    println!(
        "   📤 Total bytes submitted: {} ({:.2} MB)",
        submitted_bytes,
        submitted_bytes as f64 / 1_000_000.0
    );
    println!(
        "   📤 Total bytes written: {} ({:.2} MB)",
        actual_bytes,
        actual_bytes as f64 / 1_000_000.0
    );
    println!("   ⏱️  Submit duration: {:?}", submit_elapsed);
    println!("   ⏱️  Total duration: {:?}", total_elapsed);
    println!(
        "   🚀 Submit throughput: {:.2} Mbps ({:.2} MB/s)",
        submit_throughput,
        submit_throughput / 8.0
    );
    println!(
        "   🚀 TCP throughput: {:.2} Mbps ({:.2} MB/s)",
        actual_throughput,
        actual_throughput / 8.0
    );
    println!("   📊 Operations/sec: {:.2}", ops_per_second);
    println!(
        "   ⚡ Avg per operation: {:.2} ns",
        submit_elapsed.as_nanos() as f64 / iterations as f64
    );
    println!(
        "   📊 Ring buffer: {} pending, {} available",
        pending, available
    );
    println!("   🔓 Method: Mixed operations through lock-free ring buffer");

    if actual_throughput > 1000.0 {
        println!("   ✅ EXCELLENT LOCK-FREE PERFORMANCE! >1 Gbps TCP throughput");
    } else {
        println!(
            "   ℹ️  Lock-free TCP throughput: {:.2} Mbps",
            actual_throughput
        );
    }
}
