use std::{
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::sleep,
};

use kameo_remote::{
    handshake::Hello,
    registry::{ActorMessageFuture, ActorMessageHandler},
    tls::TlsConfig,
    typed, GossipConfig, GossipRegistryHandle, KeyPair,
};

const TEST_DATA_SIZE: usize = 100 * 1024 * 1024; // 100MB
const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
const MESSAGE_COUNT: usize = 10000;
const ASK_CONCURRENCY: usize = 256;
const TELL_LARGE_MESSAGE_COUNT: usize = 1024;
const BENCH_ACTOR_NAME: &str = "ask_actor";
const BENCH_ACTOR_ID: u64 = typed::fnv1a_hash(BENCH_ACTOR_NAME);
const BENCH_TYPE_HASH: u32 = (BENCH_ACTOR_ID & 0xFFFF_FFFF) as u32;

fn throughput_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    GUARD
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap()
}

struct BenchActorHandler;

impl ActorMessageHandler for BenchActorHandler {
    fn handle_actor_message(
        &self,
        _actor_id: &str,
        _type_hash: u32,
        _payload: &[u8],
        correlation_id: Option<u16>,
    ) -> ActorMessageFuture<'_> {
        let reply = correlation_id.map(|_| Bytes::from_static(b"ok"));
        Box::pin(async move { Ok(reply) })
    }
}

struct ReceiverProcess {
    peer_id: kameo_remote::PeerId,
    bind_addr: SocketAddr,
    shutdown: tokio::sync::oneshot::Sender<()>,
    join: std::thread::JoinHandle<()>,
}

fn spawn_receiver_process(name: &str, ask_inflight_limit: usize) -> ReceiverProcess {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let name = name.to_string();

    let join = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let mut config = GossipConfig::default();
            config.ask_inflight_limit = ask_inflight_limit;
            let receiver_addr = "127.0.0.1:0".parse().unwrap();
            let receiver_keypair = KeyPair::new_for_testing(name);

            let receiver_handle = GossipRegistryHandle::new_with_keypair(
                receiver_addr,
                receiver_keypair.clone(),
                Some(config),
            )
            .await
            .unwrap();

            receiver_handle
                .registry
                .set_actor_message_handler(Arc::new(BenchActorHandler))
                .await;

            let bind_addr = receiver_handle.registry.bind_addr;
            receiver_handle
                .register(BENCH_ACTOR_NAME.to_string(), bind_addr)
                .await
                .unwrap();

            // Actor messages are handled by the handler directly; bind addr is enough.
            let peer_id = receiver_keypair.peer_id();
            ready_tx.send((peer_id, bind_addr)).ok();

            let _ = shutdown_rx.await;
            receiver_handle.shutdown().await;
        });
    });

    let (peer_id, bind_addr) = ready_rx.recv().unwrap();
    ReceiverProcess {
        peer_id,
        bind_addr,
        shutdown: shutdown_tx,
        join,
    }
}

async fn stop_receiver_process(proc: ReceiverProcess) {
    let _ = proc.shutdown.send(());
    let _ = tokio::task::spawn_blocking(move || proc.join.join()).await;
}

/// Generate random test data
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    data
}

/// Calculate throughput in MB/s
fn calculate_throughput(bytes: usize, duration: Duration) -> f64 {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    let seconds = duration.as_secs_f64();
    mb / seconds
}

async fn perform_server_hello<S>(stream: &mut S) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut buf).await?;
    }
    let hello = Hello::new();
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&hello)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "hello serialize"))?;
    let out_len = serialized.len() as u32;
    stream.write_all(&out_len.to_be_bytes()).await?;
    stream.write_all(&serialized).await?;
    stream.flush().await?;
    Ok(())
}

/// TLS server that reads length-prefixed payloads and optionally counts messages.
fn start_tls_len_prefixed_server(
    tls: TlsConfig,
    counter: Option<Arc<AtomicU64>>,
) -> (std::thread::JoinHandle<()>, SocketAddr) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    std_listener.set_nonblocking(true).unwrap();

    let handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            let acceptor = tls.acceptor();
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let acceptor = acceptor.clone();
                    let counter = counter.clone();
                    tokio::spawn(async move {
                        let mut stream = match acceptor.accept(stream).await {
                            Ok(stream) => stream,
                            Err(_) => return,
                        };
                        if perform_server_hello(&mut stream).await.is_err() {
                            return;
                        }
                        let mut buffer = vec![0u8; CHUNK_SIZE];
                        let mut len_buf = [0u8; 4];

                        loop {
                            if stream.read_exact(&mut len_buf).await.is_err() {
                                break;
                            }
                            let msg_len = u32::from_be_bytes(len_buf) as usize;
                            if msg_len > buffer.len() {
                                buffer.resize(msg_len, 0);
                            }
                            if stream.read_exact(&mut buffer[..msg_len]).await.is_err() {
                                break;
                            }
                            if let Some(counter) = counter.as_ref() {
                                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    });
                }
            }
        });
    });
    (handle, addr)
}

/// Test 1: Reuse existing connection pool for direct throughput
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_direct_connection_throughput() {
    let _guard = throughput_guard();
    println!("🚀 Test 1: Direct Connection Pool Throughput");

    // Setup gossip registry
    let mut config = GossipConfig::default();
    config.ask_inflight_limit = 256;
    let receiver_addr = "127.0.0.1:0".parse().unwrap();
    let sender_addr = "127.0.0.1:0".parse().unwrap();

    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_1");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_1");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Register a throughput test actor on receiver
    let tls = TlsConfig::new(receiver_keypair.to_secret_key()).unwrap();
    let (_echo_server, actor_addr) = start_tls_len_prefixed_server(tls, None);
    receiver_handle
        .register("throughput_actor".to_string(), actor_addr)
        .await
        .unwrap();

    // Connect nodes - this establishes the connection pool
    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender
        .connect(&sender_handle.registry.bind_addr)
        .await
        .unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver
        .connect(&receiver_handle.registry.bind_addr)
        .await
        .unwrap();

    // Wait for gossip propagation
    sleep(Duration::from_millis(100)).await;

    // Use lookup() to find the remote actor
    let found_actor = sender_handle.lookup("throughput_actor").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");

    let actor_location = found_actor.unwrap();
    let actor_addr: SocketAddr = actor_location.address.parse().unwrap();
    println!("✅ Found remote actor at: {}", actor_location.address);

    sleep(Duration::from_millis(50)).await; // Let server start

    // REUSE existing connection from pool instead of creating new one
    let connection_handle = sender_handle.get_connection(actor_addr).await.unwrap();
    println!(
        "🔗 Reusing existing connection from pool to: {}",
        connection_handle.addr
    );

    // Generate test data
    let test_data = generate_test_data(TEST_DATA_SIZE);
    println!(
        "📊 Generated {} MB of test data",
        TEST_DATA_SIZE / (1024 * 1024)
    );

    // Measure throughput using existing connection
    let start_time = Instant::now();
    let mut total_bytes = 0;
    let mut chunks_sent = 0usize;

    // Send data in chunks using the existing connection
    for chunk in test_data.chunks(CHUNK_SIZE) {
        // Send chunk via existing connection pool
        connection_handle
            .tell_bytes(Bytes::copy_from_slice(chunk))
            .await
            .unwrap();
        total_bytes += chunk.len();
        chunks_sent += 1;
    }

    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);

    let chunks_per_sec = chunks_sent as f64 / duration.as_secs_f64();
    let bytes_per_sec = total_bytes as f64 / duration.as_secs_f64();
    println!("📈 Connection Pool Results:");
    println!("   - Total bytes: {} MB", total_bytes / (1024 * 1024));
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Chunks/sec (64KB): {:.0}", chunks_per_sec);
    println!("   - Bytes/sec: {:.0}", bytes_per_sec);

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(throughput > 10.0, "Throughput should be > 10 MB/s");
}

// Connection pool-based implementations are now used directly through ConnectionHandle

/// Test 2: tell() fire-and-forget performance using connection pool
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tell_throughput() {
    let _guard = throughput_guard();
    println!("🚀 Test 2: tell() Fire-and-Forget Throughput");

    // Setup gossip registry
    let mut config = GossipConfig::default();
    config.ask_inflight_limit = 256;
    let receiver_addr = "127.0.0.1:0".parse().unwrap();
    let sender_addr = "127.0.0.1:0".parse().unwrap();

    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_2");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_2");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Register actor
    let message_count = Arc::new(AtomicU64::new(0));
    let count_clone = message_count.clone();

    let tls = TlsConfig::new(receiver_keypair.to_secret_key()).unwrap();
    let (_server, actor_addr) = start_tls_len_prefixed_server(tls, Some(count_clone));
    receiver_handle
        .register("tell_actor".to_string(), actor_addr)
        .await
        .unwrap();

    // Connect nodes - establishes connection pool
    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender
        .connect(&sender_handle.registry.bind_addr)
        .await
        .unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver
        .connect(&receiver_handle.registry.bind_addr)
        .await
        .unwrap();

    // Wait for propagation
    sleep(Duration::from_millis(100)).await;

    // Lookup actor
    let found_actor = sender_handle.lookup("tell_actor").await;
    assert!(found_actor.is_some());

    let actor_location = found_actor.unwrap();
    let actor_addr: SocketAddr = actor_location.address.parse().unwrap();
    println!("✅ Found tell actor at: {}", actor_location.address);

    sleep(Duration::from_millis(50)).await;

    // REUSE existing connection from pool
    let connection_handle = sender_handle.get_connection(actor_addr).await.unwrap();
    println!(
        "🔗 Reusing existing connection for tell() to: {}",
        connection_handle.addr
    );

    // Generate test message
    let test_message = Bytes::from_static(
        b"Hello from tell() - this is a test message for fire-and-forget performance testing!",
    );

    println!("📊 Sending {} messages via tell()", MESSAGE_COUNT);

    // Measure tell() performance using connection pool
    let start_time = Instant::now();

    for _ in 0..MESSAGE_COUNT {
        connection_handle
            .tell_bytes(test_message.clone())
            .await
            .unwrap();
    }

    let duration = start_time.elapsed();
    let total_bytes = MESSAGE_COUNT * test_message.len();
    let throughput = calculate_throughput(total_bytes, duration);
    let messages_per_sec = MESSAGE_COUNT as f64 / duration.as_secs_f64();

    // Wait a bit for messages to be processed
    sleep(Duration::from_millis(100)).await;

    println!("📈 tell() Results:");
    println!("   - Messages sent: {}", MESSAGE_COUNT);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Messages/sec: {:.0}", messages_per_sec);

    // Cleanup
    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;

    assert!(
        messages_per_sec > 1000.0,
        "Should handle >1000 messages/sec"
    );
}

/// Test 2b: tell() throughput with 64KB payloads (streaming-size comparison)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tell_large_payload_throughput() {
    println!("🚀 Test 2b: tell() 64KB Throughput");
    let _guard = throughput_guard();

    let mut config = GossipConfig::default();
    config.ask_inflight_limit = 256;
    let receiver_addr = "127.0.0.1:0".parse().unwrap();
    let sender_addr = "127.0.0.1:0".parse().unwrap();

    let receiver_keypair = KeyPair::new_for_testing("throughput_receiver_2b");
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_2b");
    let receiver_peer_id = receiver_keypair.peer_id();
    let sender_peer_id = sender_keypair.peer_id();

    let receiver_handle = GossipRegistryHandle::new_with_keypair(
        receiver_addr,
        receiver_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();

    receiver_handle
        .register(
            "throughput_actor_64kb".to_string(),
            receiver_handle.registry.bind_addr,
        )
        .await
        .unwrap();

    let peer_sender = receiver_handle.add_peer(&sender_peer_id).await;
    peer_sender
        .connect(&sender_handle.registry.bind_addr)
        .await
        .unwrap();
    let peer_receiver = sender_handle.add_peer(&receiver_peer_id).await;
    peer_receiver
        .connect(&receiver_handle.registry.bind_addr)
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    let found_actor = sender_handle.lookup("throughput_actor_64kb").await;
    assert!(found_actor.is_some(), "Actor should be found via lookup");

    let actor_location = found_actor.unwrap();
    let actor_addr: SocketAddr = actor_location.address.parse().unwrap();
    println!("✅ Found tell actor at: {}", actor_location.address);

    let connection_handle = sender_handle.get_connection(actor_addr).await.unwrap();
    println!(
        "🔗 Reusing existing connection for tell() to: {}",
        connection_handle.addr
    );

    let test_message = Bytes::copy_from_slice(&generate_test_data(CHUNK_SIZE));
    println!(
        "📊 Sending {} messages via tell() (64KB payload)",
        TELL_LARGE_MESSAGE_COUNT
    );

    let start_time = Instant::now();
    for _ in 0..TELL_LARGE_MESSAGE_COUNT {
        connection_handle
            .tell_bytes(test_message.clone())
            .await
            .unwrap();
    }

    let duration = start_time.elapsed();
    let total_bytes = TELL_LARGE_MESSAGE_COUNT * test_message.len();
    let throughput = calculate_throughput(total_bytes, duration);
    let messages_per_sec = TELL_LARGE_MESSAGE_COUNT as f64 / duration.as_secs_f64();

    sleep(Duration::from_millis(100)).await;

    println!("📈 tell() 64KB Results:");
    println!("   - Messages sent: {}", TELL_LARGE_MESSAGE_COUNT);
    println!("   - Total bytes: {} MB", total_bytes / (1024 * 1024));
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Messages/sec: {:.0}", messages_per_sec);

    receiver_handle.shutdown().await;
    sender_handle.shutdown().await;
}

/// Test 3: ask() request-response performance using connection pool
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ask_throughput() {
    let _guard = throughput_guard();
    println!("🚀 Test 3: ask() Request-Response Throughput");

    // Setup gossip registry
    let mut config = GossipConfig::default();
    config.ask_inflight_limit = ASK_CONCURRENCY;
    let sender_addr = "127.0.0.1:0".parse().unwrap();

    let receiver_proc = spawn_receiver_process("throughput_receiver_3", ASK_CONCURRENCY);
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_3");
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();

    // Connect nodes - establishes connection pool
    let peer_receiver = sender_handle.add_peer(&receiver_proc.peer_id).await;
    peer_receiver
        .connect(&receiver_proc.bind_addr)
        .await
        .unwrap();

    // Wait for propagation
    sleep(Duration::from_millis(100)).await;

    println!("✅ Found ask actor at: {}", receiver_proc.bind_addr);

    // REUSE existing connection from pool
    let connection_handle = sender_handle
        .get_connection_to_peer(&receiver_proc.peer_id)
        .await
        .unwrap();
    println!(
        "🔗 Reusing existing connection for ask() to: {}",
        connection_handle.addr
    );

    let ask_payload = Bytes::from_static(b"REQUEST");

    let total_requests = (MESSAGE_COUNT / 10).max(ASK_CONCURRENCY * 4);
    println!(
        "📊 Sending {} request-response pairs via ask()",
        total_requests
    );

    // Measure ask() performance using connection pool
    let start_time = Instant::now();
    let mut total_bytes = 0;
    let mut latencies = Vec::with_capacity(total_requests);
    let mut in_flight: FuturesUnordered<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<(Duration, bytes::Bytes), kameo_remote::GossipError>,
                    > + Send,
            >,
        >,
    > = FuturesUnordered::new();
    let mut remaining = total_requests;

    let initial = ASK_CONCURRENCY.min(remaining);
    for _ in 0..initial {
        let conn = connection_handle.clone();
        let payload = ask_payload.clone();
        in_flight.push(Box::pin(async move {
            let request_start = Instant::now();
            let response = conn
                .ask_actor(
                    BENCH_ACTOR_ID,
                    BENCH_TYPE_HASH,
                    payload,
                    Duration::from_secs(5),
                )
                .await?;
            let latency = request_start.elapsed();
            Ok::<(Duration, bytes::Bytes), kameo_remote::GossipError>((latency, response))
        }));
        remaining -= 1;
    }

    while let Some(result) = in_flight.next().await {
        let (latency, response) = result.unwrap();
        latencies.push(latency);
        total_bytes += ask_payload.len() + response.len();

        if remaining > 0 {
            let conn = connection_handle.clone();
            let payload = ask_payload.clone();
            in_flight.push(Box::pin(async move {
                let request_start = Instant::now();
                let response = conn
                    .ask_actor(
                        BENCH_ACTOR_ID,
                        BENCH_TYPE_HASH,
                        payload,
                        Duration::from_secs(5),
                    )
                    .await?;
                let latency = request_start.elapsed();
                Ok::<(Duration, bytes::Bytes), kameo_remote::GossipError>((latency, response))
            }));
            remaining -= 1;
        }
    }

    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);
    let requests_per_sec = total_requests as f64 / duration.as_secs_f64();

    // Calculate latency statistics
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];

    println!("📈 ask() Results:");
    println!("   - Requests completed: {}", total_requests);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Requests/sec: {:.0}", requests_per_sec);
    println!("   - Latency P50: {:?}", p50);
    println!("   - Latency P95: {:?}", p95);
    println!("   - Latency P99: {:?}", p99);

    // Cleanup
    sender_handle.shutdown().await;
    stop_receiver_process(receiver_proc).await;

    assert!(requests_per_sec > 100.0, "Should handle >100 requests/sec");
    assert!(
        p95 < Duration::from_millis(100),
        "P95 latency should be <100ms"
    );
}

/// Test 4: ask() with exactly MAX_INFLIGHT requests in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ask_max_inflight() {
    let _guard = throughput_guard();
    println!("🚀 Test 4: ask() MAX_INFLIGHT Throughput");

    let mut config = GossipConfig::default();
    let max_inflight = 256usize;
    config.ask_inflight_limit = max_inflight;
    let sender_addr = "127.0.0.1:0".parse().unwrap();

    let receiver_proc = spawn_receiver_process("throughput_receiver_4", max_inflight);
    let sender_keypair = KeyPair::new_for_testing("throughput_sender_4");
    let sender_handle = GossipRegistryHandle::new_with_keypair(
        sender_addr,
        sender_keypair.clone(),
        Some(config.clone()),
    )
    .await
    .unwrap();

    let peer_receiver = sender_handle.add_peer(&receiver_proc.peer_id).await;
    peer_receiver
        .connect(&receiver_proc.bind_addr)
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    println!("✅ Found ask actor at: {}", receiver_proc.bind_addr);

    let connection_handle = sender_handle
        .get_connection_to_peer(&receiver_proc.peer_id)
        .await
        .unwrap();
    println!(
        "🔗 Reusing existing connection for ask() to: {}",
        connection_handle.addr
    );

    let ask_payload = Bytes::from_static(b"REQUEST");

    let total_requests = max_inflight * 4;
    println!(
        "📊 Sending {} request-response pairs via ask() (max inflight {})",
        total_requests, max_inflight
    );

    let start_time = Instant::now();
    let mut in_flight: FuturesUnordered<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<(Duration, bytes::Bytes), kameo_remote::GossipError>,
                    > + Send,
            >,
        >,
    > = FuturesUnordered::new();

    let mut remaining = total_requests;
    for _ in 0..max_inflight {
        let conn = connection_handle.clone();
        let payload = ask_payload.clone();
        in_flight.push(Box::pin(async move {
            let request_start = Instant::now();
            let response = conn
                .ask_actor(
                    BENCH_ACTOR_ID,
                    BENCH_TYPE_HASH,
                    payload,
                    Duration::from_secs(5),
                )
                .await?;
            let latency = request_start.elapsed();
            Ok::<(Duration, bytes::Bytes), kameo_remote::GossipError>((latency, response))
        }));
        remaining -= 1;
    }

    let mut latencies = Vec::with_capacity(total_requests);
    let mut total_bytes = 0usize;
    while let Some(result) = in_flight.next().await {
        let (latency, response) = result.unwrap();
        latencies.push(latency);
        total_bytes += ask_payload.len() + response.len();
        if remaining > 0 {
            let conn = connection_handle.clone();
            let payload = ask_payload.clone();
            in_flight.push(Box::pin(async move {
                let request_start = Instant::now();
                let response = conn
                    .ask_actor(
                        BENCH_ACTOR_ID,
                        BENCH_TYPE_HASH,
                        payload,
                        Duration::from_secs(5),
                    )
                    .await?;
                let latency = request_start.elapsed();
                Ok::<(Duration, bytes::Bytes), kameo_remote::GossipError>((latency, response))
            }));
            remaining -= 1;
        }
    }

    let duration = start_time.elapsed();
    let throughput = calculate_throughput(total_bytes, duration);
    let requests_per_sec = total_requests as f64 / duration.as_secs_f64();

    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];

    println!("📈 ask() MAX_INFLIGHT Results:");
    println!("   - Requests completed: {}", total_requests);
    println!("   - Total bytes: {} KB", total_bytes / 1024);
    println!("   - Duration: {:?}", duration);
    println!("   - Throughput: {:.2} MB/s", throughput);
    println!("   - Requests/sec: {:.0}", requests_per_sec);
    println!("   - Latency P50: {:?}", p50);
    println!("   - Latency P95: {:?}", p95);
    println!("   - Latency P99: {:?}", p99);

    sender_handle.shutdown().await;
    stop_receiver_process(receiver_proc).await;
}

/// Combined performance comparison test
#[tokio::test]
async fn test_performance_comparison() {
    println!("🚀 Performance Comparison Summary");
    println!("This test provides a comprehensive comparison of all three approaches:");
    println!("1. Direct TcpStream");
    println!("2. tell() fire-and-forget");
    println!("3. ask() request-response");
    println!("");
    println!("Run each test individually to see detailed performance metrics.");
    println!("Expected performance hierarchy: TcpStream > tell() > ask()");
}
