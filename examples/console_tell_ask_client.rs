use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use kameo_remote::{typed, wire_type, GossipConfig, GossipRegistryHandle, NodeId, SecretKey};
use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
struct EchoRequest {
    payload: Vec<u8>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
struct EchoResponse {
    payload: Vec<u8>,
}

wire_type!(EchoRequest, "kameo.remote.EchoRequest");
wire_type!(EchoResponse, "kameo.remote.EchoResponse");

const CONSOLE_ACTOR_NAME: &str = "console_echo";
const CONSOLE_ACTOR_ID: u64 = 0x342F_2FDD_5C5A_4F98;
const CONSOLE_TYPE_HASH: u32 = 0xDF5A_E1BC;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Console tell/ask client (TLS).
///
/// Usage:
///   cargo run --example console_tell_ask_client /tmp/kameo_tls/console_tell_ask_server.pub [tell_count] [ask_count] [ask_concurrency] [--typed] [--streaming-ask] [--ask-payload-bytes N]
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt().init();

    let args: Vec<String> = env::args().collect();
    let server_pub_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/tmp/kameo_tls/console_tell_ask_server.pub".to_string());
    let tell_count: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let ask_count: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(100);
    let ask_concurrency_requested: usize = args
        .get(4)
        .and_then(|v| v.parse().ok())
        .unwrap_or(kameo_remote::config::DEFAULT_ASK_INFLIGHT_LIMIT);
    let run_typed = args.iter().any(|arg| arg == "--typed");
    let ask_payload_bytes = args
        .iter()
        .position(|arg| arg == "--ask-payload-bytes")
        .and_then(|idx| args.get(idx + 1))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);

    println!("🔐 Console Tell/Ask Client (TLS)");
    println!("================================\n");

    let server_node_id = load_node_id(&server_pub_path)?;
    println!("Server NodeId: {}", server_node_id.fmt_short());
    println!("Server key: {}\n", server_pub_path);

    let client_key_path = "/tmp/kameo_tls/console_tell_ask_client.key";
    let client_secret = load_or_generate_key(client_key_path)?;
    let client_node_id = client_secret.public();

    println!("Client NodeId: {}", client_node_id.fmt_short());
    println!("Client key: {}\n", client_key_path);

    let mut config = GossipConfig::default();
    config.enable_peer_discovery = true;
    config.ask_inflight_limit = 1024;
    let ask_limit = config.ask_inflight_limit;
    let ask_concurrency = ask_concurrency_requested.min(ask_limit);
    if ask_concurrency_requested != ask_concurrency {
        println!(
            "ℹ️  Requested ask concurrency {} capped at ask_inflight_limit {}",
            ask_concurrency_requested, ask_limit
        );
    }
    let registry =
        GossipRegistryHandle::new_with_tls("0.0.0.0:0".parse()?, client_secret, Some(config))
            .await?;

    let server_addr = "127.0.0.1:29200".parse()?;
    registry
        .registry
        .add_peer_with_node_id(server_addr, Some(server_node_id))
        .await;

    // Establish TLS connection to the server.
    let conn = registry.get_connection(server_addr).await?;

    println!("✅ Connected to {}", server_addr);
    println!(
        "Sending tell + ask via zero-copy actor frames (no gossip fallback)...\nAsk concurrency: {} (limit {})\n",
        ask_concurrency,
        ask_limit
    );

    // Tell (fire-and-forget)
    let tell_payload = Bytes::from_static(b"tell:hello");
    conn.tell_actor(CONSOLE_ACTOR_ID, CONSOLE_TYPE_HASH, tell_payload.clone())
        .await?;
    println!("✅ Tell sent");

    // Ask (request-response)
    let ask_payload = if ask_payload_bytes > 0 {
        Bytes::from(vec![b'x'; ask_payload_bytes])
    } else {
        Bytes::from_static(b"ask:ping")
    };
    let threshold = conn.streaming_threshold();
    let payload_len = ask_payload.len();
    let uses_streaming = payload_len > threshold;

    let response = conn
        .ask_actor(
            CONSOLE_ACTOR_ID,
            CONSOLE_TYPE_HASH,
            ask_payload.clone(),
            Duration::from_secs(5),
        )
        .await?;

    if uses_streaming {
        if response.len() > 256 {
            println!(
                "✅ Ask response (streamed): {} bytes (preview: {:?})",
                response.len(),
                String::from_utf8_lossy(&response[..64.min(response.len())])
            );
        } else {
            println!(
                "✅ Ask response (streamed): {:?}",
                String::from_utf8_lossy(response.as_ref())
            );
        }
    } else {
        println!(
            "✅ Ask response (ring buffer): {:?}",
            String::from_utf8_lossy(response.as_ref())
        );
    }

    if run_typed {
        #[cfg(debug_assertions)]
        {
            // Typed ask (debug-only type hash verification) using zero-copy archived response
            let archived_response = conn
                .ask_typed_archived::<EchoRequest, EchoResponse>(&EchoRequest {
                    payload: b"ask:typed".to_vec(),
                })
                .await?;
            let archived = archived_response.archived()?;
            let payload = archived.payload.as_slice();
            println!(
                "✅ Typed ask response (zero-copy): {:?}",
                String::from_utf8_lossy(payload)
            );
        }
        #[cfg(not(debug_assertions))]
        {
            println!(
                "⚠️  Typed ask is disabled in release builds; use a debug build to verify types."
            );
        }
    }

    if tell_count > 0 {
        println!("\n🔸 Tell benchmark (count = {})", tell_count);
        let before = alloc_snapshot();
        let mut tell_latencies = Vec::with_capacity(tell_count);
        let tell_start = Instant::now();
        for _ in 0..tell_count {
            let start = Instant::now();
            conn.tell_actor(CONSOLE_ACTOR_ID, CONSOLE_TYPE_HASH, tell_payload.clone())
                .await?;
            tell_latencies.push(start.elapsed());
        }
        let tell_total = tell_start.elapsed();
        let tell_rps = tell_count as f64 / tell_total.as_secs_f64();
        let after = alloc_snapshot();
        print_latency_stats("tell()", &tell_latencies);
        println!(
            "tell() throughput: {:.0} msgs/sec (total {:.3}s)",
            tell_rps,
            tell_total.as_secs_f64()
        );
        print_alloc_delta("tell()", before, after);
    }

    if ask_count > 0 {
        println!(
            "\n🔸 Ask benchmark (count = {}, concurrency = {}, payload = {} bytes{})",
            ask_count,
            ask_concurrency,
            payload_len,
            if uses_streaming {
                " (streamed)"
            } else {
                " (ring buffer)"
            }
        );
        let before = alloc_snapshot();
        let benchmark_payload = ask_payload.clone();
        let mut in_flight: FuturesUnordered<BoxFuture<'static, Result<Duration, anyhow::Error>>> =
            FuturesUnordered::new();
        let ask_start = Instant::now();
        let mut remaining = ask_count;

        let initial = ask_concurrency.min(remaining);
        for _ in 0..initial {
            let conn_clone = conn.clone();
            let payload = benchmark_payload.clone();
            in_flight.push(Box::pin(async move {
                let start = Instant::now();
                let _ = conn_clone
                    .ask_actor(
                        CONSOLE_ACTOR_ID,
                        CONSOLE_TYPE_HASH,
                        payload,
                        Duration::from_secs(5),
                    )
                    .await?;
                Ok::<Duration, anyhow::Error>(start.elapsed())
            }));
            remaining -= 1;
        }

        let mut ask_latencies = Vec::with_capacity(ask_count);
        while let Some(result) = in_flight.next().await {
            ask_latencies.push(result?);
            if remaining > 0 {
                let conn_clone = conn.clone();
                let payload = benchmark_payload.clone();
                in_flight.push(Box::pin(async move {
                    let start = Instant::now();
                    let _ = conn_clone
                        .ask_actor(
                            CONSOLE_ACTOR_ID,
                            CONSOLE_TYPE_HASH,
                            payload,
                            Duration::from_secs(5),
                        )
                        .await?;
                    Ok::<Duration, anyhow::Error>(start.elapsed())
                }));
                remaining -= 1;
            }
        }

        let ask_total = ask_start.elapsed();
        let ask_rps = ask_count as f64 / ask_total.as_secs_f64();
        let after = alloc_snapshot();
        print_latency_stats("ask()", &ask_latencies);
        println!(
            "ask() throughput: {:.0} req/sec (total {:.3}s)",
            ask_rps,
            ask_total.as_secs_f64()
        );
        print_alloc_delta("ask()", before, after);
    }

    // Keep the client alive so the server doesn't mark the peer as failed.
    println!("\nPress Enter to exit...");
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    registry.shutdown().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

fn load_node_id(path: &str) -> Result<NodeId> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;

    if pub_key_bytes.len() != 32 {
        return Err(anyhow::anyhow!(
            "Invalid public key length: expected 32, got {}",
            pub_key_bytes.len()
        ));
    }

    NodeId::from_bytes(&pub_key_bytes).map_err(|e| anyhow::anyhow!("Invalid NodeId: {}", e))
}

fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);

    if key_path.exists() {
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 32, got {}",
                key_bytes.len()
            ));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let secret_key = SecretKey::generate();
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        Ok(secret_key)
    }
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    deallocs: u64,
    alloc_bytes: u64,
    dealloc_bytes: u64,
}

fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        allocs: ALLOC_COUNT.load(Ordering::Relaxed),
        deallocs: DEALLOC_COUNT.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn print_alloc_delta(label: &str, before: AllocSnapshot, after: AllocSnapshot) {
    let allocs = after.allocs.saturating_sub(before.allocs);
    let deallocs = after.deallocs.saturating_sub(before.deallocs);
    let alloc_bytes = after.alloc_bytes.saturating_sub(before.alloc_bytes);
    let dealloc_bytes = after.dealloc_bytes.saturating_sub(before.dealloc_bytes);
    let live_bytes = alloc_bytes.saturating_sub(dealloc_bytes);

    println!(
        "{} allocations: allocs {} ({} bytes) | deallocs {} ({} bytes) | net {} bytes",
        label, allocs, alloc_bytes, deallocs, dealloc_bytes, live_bytes
    );
}

fn print_latency_stats(label: &str, latencies: &[Duration]) {
    if latencies.is_empty() {
        return;
    }

    let mut nanos: Vec<u128> = latencies.iter().map(|d| d.as_nanos()).collect();
    nanos.sort_unstable();

    let len = nanos.len() as u128;
    let sum: u128 = nanos.iter().sum();
    let mean = sum / len;

    let p50 = percentile(&nanos, 50);
    let p95 = percentile(&nanos, 95);
    let p99 = percentile(&nanos, 99);
    let min = *nanos.first().unwrap();
    let max = *nanos.last().unwrap();

    println!(
        "{} latency (µs): min {:.2} | p50 {:.2} | p95 {:.2} | p99 {:.2} | max {:.2} | avg {:.2}",
        label,
        nanos_to_micros(min),
        nanos_to_micros(p50),
        nanos_to_micros(p95),
        nanos_to_micros(p99),
        nanos_to_micros(max),
        nanos_to_micros(mean)
    );
}

fn percentile(sorted_nanos: &[u128], pct: u128) -> u128 {
    if sorted_nanos.is_empty() {
        return 0;
    }
    let idx = ((pct * (sorted_nanos.len() as u128 - 1)) / 100) as usize;
    sorted_nanos[idx]
}

fn nanos_to_micros(nanos: u128) -> f64 {
    nanos as f64 / 1000.0
}
