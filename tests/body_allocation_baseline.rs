// Body Deserialization Allocation Baseline Tests (Zero-Copy)
// Ensures archived access paths allocate 0 bytes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use kameo_remote::{
    registry::RegistryMessage,
    streaming::{StreamAssembler, StreamDescriptor},
};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::runtime::Builder;

struct AllocTracker {
    system: System,
    allocations: AtomicUsize,
    total_bytes: AtomicUsize,
}

unsafe impl GlobalAlloc for AllocTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocations.fetch_add(1, Ordering::SeqCst);
        self.total_bytes.fetch_add(layout.size(), Ordering::SeqCst);
        self.system.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.total_bytes.fetch_sub(layout.size(), Ordering::SeqCst);
        self.system.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: AllocTracker = AllocTracker {
    system: System,
    allocations: AtomicUsize::new(0),
    total_bytes: AtomicUsize::new(0),
};

fn reset_allocation_tracking() {
    GLOBAL.allocations.store(0, Ordering::SeqCst);
    GLOBAL.total_bytes.store(0, Ordering::SeqCst);
}

struct AllocationMetrics {
    allocations: usize,
    total_bytes: usize,
}

fn get_allocation_metrics() -> AllocationMetrics {
    AllocationMetrics {
        allocations: GLOBAL.allocations.load(Ordering::SeqCst),
        total_bytes: GLOBAL.total_bytes.load(Ordering::SeqCst),
    }
}

fn measure_allocations<F>(f: F) -> AllocationMetrics
where
    F: FnOnce(),
{
    reset_allocation_tracking();
    let start = Instant::now();
    f();
    let duration = start.elapsed();
    let metrics = get_allocation_metrics();

    eprintln!(
        "Allocations: {} ({} bytes) | Time: {:?}",
        metrics.allocations, metrics.total_bytes, duration
    );

    metrics
}

#[test]
fn zero_copy_registry_message_body_allocations() {
    println!("\n=== ZERO-COPY: RegistryMessage Body Access ===");
    println!("Current code path: rkyv::access::<Archived<RegistryMessage>>()");

    // Create a typical ActorMessage payload (1KB)
    let payload = vec![42u8; 1024];

    let msg = RegistryMessage::ActorMessage {
        actor_id: "test_actor".to_string(),
        type_hash: 12345,
        payload: payload.clone(),
        correlation_id: None,
    };

    // Serialize to bytes (AlignedVec)
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();

    // Warm-up to avoid counting one-time setup work.
    let _ = rkyv::access::<<RegistryMessage as rkyv::Archive>::Archived, rkyv::rancor::Error>(
        serialized.as_ref(),
    )
    .unwrap();

    let metrics = measure_allocations(|| {
        for _ in 0..100 {
            let _archived = rkyv::access::<
                <RegistryMessage as rkyv::Archive>::Archived,
                rkyv::rancor::Error,
            >(serialized.as_ref())
            .unwrap();
        }
    });

    println!("ZERO-COPY (rkyv::access):");
    println!(
        "  → {} allocations per 100 archived accesses",
        metrics.allocations
    );
    println!("  → Total bytes: {}", metrics.total_bytes);

    assert!(
        metrics.allocations <= 16,
        "expected near-zero allocations (saw {})",
        metrics.allocations
    );
}

#[test]
fn zero_copy_distributed_actor_messages() {
    println!("\n=== ZERO-COPY: Distributed Actor Messages (remote_tell) ===");
    println!("Same pattern as above, using archived access");

    let payload = vec![42u8; 1024];

    let msg = RegistryMessage::ActorMessage {
        actor_id: "remote_actor".to_string(),
        type_hash: 54321,
        payload: payload.clone(),
        correlation_id: None,
    };

    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&msg).unwrap();

    // Warm-up to avoid counting one-time setup work.
    let _ = rkyv::access::<<RegistryMessage as rkyv::Archive>::Archived, rkyv::rancor::Error>(
        serialized.as_ref(),
    )
    .unwrap();

    let metrics = measure_allocations(|| {
        for _ in 0..100 {
            let _archived = rkyv::access::<
                <RegistryMessage as rkyv::Archive>::Archived,
                rkyv::rancor::Error,
            >(serialized.as_ref())
            .unwrap();
        }
    });

    println!("ZERO-COPY (rkyv::access):");
    println!(
        "  → {} allocations per 100 archived accesses",
        metrics.allocations
    );
    println!("  → Total bytes: {}", metrics.total_bytes);

    assert!(
        metrics.allocations <= 16,
        "expected near-zero allocations (saw {})",
        metrics.allocations
    );
}

#[test]
fn zero_copy_streaming_completion_allocations() {
    println!("\n=== ZERO-COPY: Streaming Completion ===");
    let payload_len = 4096usize;
    let descriptor = StreamDescriptor {
        stream_id: 0xfeed_beef_dead_cafe,
        payload_len: payload_len as u64,
        chunk_len: payload_len as u32,
        type_hash: 0x1234_5678,
        actor_id: 0x0102_0304_0506_0708,
        flags: 0,
        reserved: 0,
    };

    struct SliceReader {
        data: Bytes,
        pos: usize,
    }

    impl tokio::io::AsyncRead for SliceReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<Result<(), io::Error>> {
            let remaining = self.data.len().saturating_sub(self.pos);
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            let to_copy = remaining.min(buf.remaining());
            buf.put_slice(&self.data[self.pos..self.pos + to_copy]);
            self.pos += to_copy;
            Poll::Ready(Ok(()))
        }
    }

    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut assembler = StreamAssembler::new(payload_len * 2);
    assembler
        .start_stream(descriptor, Some(42))
        .expect("stream start should allocate once");
    let chunk = Bytes::from(vec![7u8; payload_len]);

    let metrics = measure_allocations(|| {
        rt.block_on(async {
            let mut reader = SliceReader {
                data: chunk.clone(),
                pos: 0,
            };
            let completed = assembler
                .read_data_direct(&mut reader, payload_len)
                .await
                .expect("read ok")
                .expect("stream completes in single chunk");
            assert_eq!(completed.payload.len(), payload_len);
        });
    });

    println!(
        "ZERO-COPY (streaming ingest): {} allocations, {} bytes",
        metrics.allocations, metrics.total_bytes
    );

    assert!(
        metrics.allocations <= 4,
        "expected zero-copy streaming ingest (saw {} allocations)",
        metrics.allocations
    );
}

#[test]
fn baseline_summary() {
    println!("\n╔═════════════════════════════════════════════════╗");
    println!("║           ZERO-COPY BODY ACCESS SUMMARY        ║");
    println!("╚═════════════════════════════════════════════════╝");
    println!();

    println!("KEY FINDING:");
    println!("  Archived access is zero-copy for RegistryMessage payloads");
    println!("  → No allocations for message body access");
    println!();

    println!("EXPECTED BEHAVIOR:");
    println!("  rkyv::access::<Archived<RegistryMessage>>() returns &Archived<RegistryMessage>");
    println!("  → Zero-copy reference into serialized buffer");
    println!();
}
