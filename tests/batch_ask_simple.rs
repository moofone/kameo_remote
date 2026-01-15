use kameo_remote::{GossipConfig, GossipRegistryHandle, MessageType, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

/// Simple test echo server that adds 1 to received integers
async fn start_echo_server() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            tokio::spawn(handle_echo_client(stream, peer));
        }
    });

    Ok(addr)
}

async fn handle_echo_client(mut stream: TcpStream, peer: SocketAddr) {
    info!("Echo server: new client from {}", peer);
    let mut buffer = vec![0u8; 4096];

    while (stream.read_exact(&mut buffer[..4]).await).is_ok() {
        let msg_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        if msg_len > buffer.len() {
            buffer.resize(msg_len, 0);
        }

        // Read message
        if stream.read_exact(&mut buffer[..msg_len]).await.is_err() {
            break;
        }

        // Parse header
        if msg_len < 8 {
            continue;
        }

        let msg_type = buffer[0];
        let correlation_id = u16::from_be_bytes([buffer[1], buffer[2]]);
        let payload = &buffer[8..msg_len];

        if msg_type == MessageType::Ask as u8 && payload.len() >= 4 {
            // Parse integer
            let value = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let response_value = value + 1;

            // Build response
            let response_payload = response_value.to_be_bytes();
            let response_size = 8 + response_payload.len();
            let mut response = Vec::with_capacity(4 + response_size);

            response.extend_from_slice(&(response_size as u32).to_be_bytes());
            response.push(MessageType::Response as u8);
            response.extend_from_slice(&correlation_id.to_be_bytes());
            response.extend_from_slice(&[0u8; 5]);
            response.extend_from_slice(&response_payload);

            if stream.write_all(&response).await.is_err() {
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_batch_ask_simple() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info,batch_ask_simple=debug")
        .try_init()
        .ok();

    // Start echo server
    let echo_addr = start_echo_server().await?;
    info!("Echo server started on {}", echo_addr);

    // Start gossip registry
    let registry = GossipRegistryHandle::new(
        "127.0.0.1:0".parse().unwrap(),
        vec![],
        Some(GossipConfig::default()),
    )
    .await?;

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection
    let conn = registry.get_connection(echo_addr).await?;
    info!("Connected to echo server");

    // Test 1: Single ask
    {
        let request = 42u32.to_be_bytes();
        let start = Instant::now();
        let response = conn.ask(&request).await?;
        let elapsed = start.elapsed();

        assert_eq!(response.len(), 4);
        let value = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(value, 43);
        info!("Single ask: {} -> {} in {:?}", 42, value, elapsed);
    }

    // Test 2: Small batch
    {
        let requests: Vec<Vec<u8>> = (0..10u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

        let start = Instant::now();
        let receivers = conn.ask_batch(&request_refs).await?;

        let mut responses = Vec::new();
        for receiver in receivers {
            match receiver.await {
                Ok(response) => responses.push(response),
                Err(_) => panic!("Failed to receive response"),
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(responses.len(), 10);
        for (i, response) in responses.iter().enumerate() {
            assert_eq!(response.len(), 4);
            let value = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
            assert_eq!(value, i as u32 + 1);
        }
        info!("Batch ask (10): completed in {:?}", elapsed);
    }

    // Test 3: Large batch
    {
        const BATCH_SIZE: usize = 100;
        let requests: Vec<Vec<u8>> = (0..BATCH_SIZE as u32)
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

        let start = Instant::now();
        let receivers = conn.ask_batch(&request_refs).await?;

        let mut responses = Vec::new();
        for receiver in receivers {
            match receiver.await {
                Ok(response) => responses.push(response),
                Err(_) => panic!("Failed to receive response"),
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(responses.len(), BATCH_SIZE);
        let mut success_count = 0;
        for (i, response) in responses.iter().enumerate() {
            if response.len() == 4 {
                let value =
                    u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
                if value == i as u32 + 1 {
                    success_count += 1;
                }
            }
        }
        assert_eq!(success_count, BATCH_SIZE);
        info!(
            "Batch ask ({}): completed in {:?} ({:.2} req/sec)",
            BATCH_SIZE,
            elapsed,
            BATCH_SIZE as f64 / elapsed.as_secs_f64()
        );
    }

    // Test 4: Concurrent batches
    {
        const NUM_CONCURRENT: usize = 5;
        const BATCH_SIZE: usize = 20;

        let conn = Arc::new(conn);
        let mut tasks = Vec::new();

        let start = Instant::now();

        for task_id in 0..NUM_CONCURRENT {
            let conn_clone = conn.clone();
            let base = (task_id * BATCH_SIZE) as u32;

            let task = tokio::spawn(async move {
                let requests: Vec<Vec<u8>> = (base..base + BATCH_SIZE as u32)
                    .map(|i| i.to_be_bytes().to_vec())
                    .collect();
                let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

                let receivers = conn_clone.ask_batch(&request_refs).await?;

                let mut responses = Vec::new();
                for receiver in receivers {
                    match receiver.await {
                        Ok(response) => responses.push(response),
                        Err(_) => {
                            return Err(kameo_remote::GossipError::Network(std::io::Error::other(
                                "Failed to receive response",
                            )))
                        }
                    }
                }

                // Verify responses
                let mut success = 0;
                for (i, response) in responses.iter().enumerate() {
                    if response.len() == 4 {
                        let value = u32::from_be_bytes([
                            response[0],
                            response[1],
                            response[2],
                            response[3],
                        ]);
                        if value == base + i as u32 + 1 {
                            success += 1;
                        }
                    }
                }

                Ok::<usize, kameo_remote::GossipError>(success)
            });

            tasks.push(task);
        }

        let mut total_success = 0;
        for task in tasks {
            total_success += task.await.unwrap()?;
        }

        let elapsed = start.elapsed();
        let total_requests = NUM_CONCURRENT * BATCH_SIZE;

        assert_eq!(total_success, total_requests);
        info!(
            "Concurrent batch asks ({} tasks x {} requests): completed in {:?} ({:.2} req/sec)",
            NUM_CONCURRENT,
            BATCH_SIZE,
            elapsed,
            total_requests as f64 / elapsed.as_secs_f64()
        );
    }

    // Cleanup
    registry.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn test_batch_ask_with_timeout() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("kameo_remote=info")
        .try_init()
        .ok();

    // Start echo server
    let echo_addr = start_echo_server().await?;

    // Start registry
    let registry = GossipRegistryHandle::new("127.0.0.1:0".parse().unwrap(), vec![], None).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get connection
    let conn = registry.get_connection(echo_addr).await?;

    // Test batch with timeout
    let requests: Vec<Vec<u8>> = (0..5u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let request_refs: Vec<&[u8]> = requests.iter().map(|r| r.as_slice()).collect();

    let results = conn
        .ask_batch_with_timeout(&request_refs, Duration::from_secs(1))
        .await?;

    assert_eq!(results.len(), 5);
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(response) => {
                assert_eq!(response.len(), 4);
                let value =
                    u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
                assert_eq!(value, i as u32 + 1);
            }
            Err(e) => panic!("Request {} failed: {}", i, e),
        }
    }

    info!("Batch ask with timeout: all requests succeeded");

    // Cleanup
    registry.shutdown().await;

    Ok(())
}
