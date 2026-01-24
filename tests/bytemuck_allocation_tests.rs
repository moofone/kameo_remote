// Baseline Allocation Tests - Phase 1 of BYTEMUCK Optimization Sprint
//
// This file establishes baseline allocation metrics BEFORE any optimizations.
// NO CODE CHANGES SHOULD BE MADE UNTIL THESE BASELINES ARE CAPTURED.
//
// Run with:
//   cargo test --test bytemuck_allocation_tests baseline -- --nocapture
//
// Save output to:
//   cargo test --test bytemuck_allocation_tests baseline -- --nocapture > BASELINE_ALLOCATIONS.txt 2>&1

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use kameo_remote::*;

/// Allocator that tracks allocations and deallocations
struct AllocTracker {
    system: System,
    allocations: AtomicUsize,
    total_bytes: AtomicUsize,
}

unsafe impl GlobalAlloc for AllocTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TRACK_ALLOCATIONS.with(|flag| {
            if flag.get() {
                self.allocations.fetch_add(1, Ordering::SeqCst);
                self.total_bytes.fetch_add(layout.size(), Ordering::SeqCst);
            }
        });
        self.system.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        TRACK_ALLOCATIONS.with(|flag| {
            if flag.get() {
                self.total_bytes.fetch_sub(layout.size(), Ordering::SeqCst);
            }
        });
        self.system.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: AllocTracker = AllocTracker {
    system: System,
    allocations: AtomicUsize::new(0),
    total_bytes: AtomicUsize::new(0),
};

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn allocation_test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = Cell::new(false);
}

struct AllocationTrackingGuard {
    previous: bool,
}

impl AllocationTrackingGuard {
    fn new() -> Self {
        let previous = TRACK_ALLOCATIONS.with(|flag| {
            let prev = flag.get();
            flag.set(true);
            prev
        });
        Self { previous }
    }
}

impl Drop for AllocationTrackingGuard {
    fn drop(&mut self) {
        TRACK_ALLOCATIONS.with(|flag| flag.set(self.previous));
    }
}

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

/// Run a function and return allocation metrics
fn measure_allocations<F>(f: F) -> AllocationMetrics
where
    F: FnOnce(),
{
    reset_allocation_tracking();
    let _tracking = AllocationTrackingGuard::new();
    let start = Instant::now();
    f();
    let duration = start.elapsed();
    let metrics = get_allocation_metrics();
    drop(_tracking);

    eprintln!(
        "Allocations: {} ({} bytes) | Time: {:?}",
        metrics.allocations, metrics.total_bytes, duration
    );

    metrics
}

// ============================================================================
// BASELINE TESTS FOR StreamHeader Serialization
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_streamheader_to_bytes_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: StreamHeader::to_bytes ===");
    let header = StreamHeader {
        stream_id: 1234567890,
        total_size: 4096,
        chunk_size: 1024,
        chunk_index: 0,
        type_hash: 0xDEADBEEF,
        actor_id: 9876543210,
    };

    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = header.to_bytes();
        }
    });

    // Baseline: 1000 allocations (1 per to_bytes call)
    assert_eq!(
        metrics.allocations, 1000,
        "Expected 1000 allocations baseline"
    );

    eprintln!(
        "  Result: {} allocations per 1000 calls",
        metrics.allocations
    );
    eprintln!("  Total bytes allocated: {}", metrics.total_bytes);
    eprintln!("  Bytes per call: {}", metrics.total_bytes / 1000);
}

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_streamheader_from_bytes_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: StreamHeader::from_bytes ===");
    let header = StreamHeader {
        stream_id: 1234567890,
        total_size: 4096,
        chunk_size: 1024,
        chunk_index: 0,
        type_hash: 0xDEADBEEF,
        actor_id: 9876543210,
    };
    let bytes = header.to_bytes();

    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = StreamHeader::from_bytes(&bytes).unwrap();
        }
    });

    eprintln!(
        "  Result: {} allocations per 1000 calls",
        metrics.allocations
    );
    eprintln!("  Total bytes allocated: {}", metrics.total_bytes);
}

// ============================================================================
// BASELINE TESTS FOR Hash Extraction
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_hash_extraction_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: Hash Extraction (copy_from_slice) ===");
    let payload = vec![
        0u8, 1, 2, 3, 4, 5, 6, 7, // type hash
        8, 9, 10, 11, 12, 13, 14, 15, // rest of payload
    ];

    let metrics = measure_allocations(|| {
        for _ in 0..10000 {
            let mut hash_bytes = [0u8; 8];
            hash_bytes.copy_from_slice(&payload[..8]);
            let _hash = u64::from_be_bytes(hash_bytes);
        }
    });

    // Baseline: 0 heap allocations (all stack)
    assert_eq!(metrics.allocations, 0, "Expected 0 heap allocations");

    eprintln!(
        "  Result: {} heap allocations (expected 0 - stack only)",
        metrics.allocations
    );
}

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_hash_extraction_performance() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: Hash Extraction Performance ===");
    let payload = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    let metrics = measure_allocations(|| {
        let mut sum = 0u64;
        for _ in 0..100000 {
            let mut hash_bytes = [0u8; 8];
            hash_bytes.copy_from_slice(&payload[..8]);
            sum = sum.wrapping_add(u64::from_be_bytes(hash_bytes));
        }
        // Prevent optimization
        std::hint::black_box(sum);
    });

    eprintln!(
        "  Result: {} heap allocations per 100,000 calls",
        metrics.allocations
    );
}

// ============================================================================
// BASELINE TESTS FOR Key Conversions
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_key_conversion_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: PublicKey::from_bytes ===");
    let key_bytes = [0u8; 32];

    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = PublicKey::from_bytes(&key_bytes).unwrap();
        }
    });

    eprintln!(
        "  Result: {} allocations per 1000 calls",
        metrics.allocations
    );
    eprintln!("  Total bytes allocated: {}", metrics.total_bytes);
}

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_node_id_conversion_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: NodeId::from_bytes ===");
    let id_bytes = [0u8; 32];

    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = NodeId::from_bytes(&id_bytes).unwrap();
        }
    });

    eprintln!(
        "  Result: {} allocations per 1000 calls",
        metrics.allocations
    );
    eprintln!("  Total bytes allocated: {}", metrics.total_bytes);
}

// ============================================================================
// BASELINE TESTS FOR Header Assembly
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_header_assembly_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: Header Assembly (copy_from_slice) ===");
    let data = vec![0u8; 1024];

    let metrics = measure_allocations(|| {
        for _ in 0..10000 {
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());
        }
    });

    // Baseline: 0 heap allocations (all stack)
    assert_eq!(metrics.allocations, 0, "Expected 0 heap allocations");

    eprintln!(
        "  Result: {} heap allocations (expected 0 - stack only)",
        metrics.allocations
    );
}

// ============================================================================
// BASELINE TESTS FOR End-to-End Scenarios
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_full_streamheader_roundtrip_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: StreamHeader Full Roundtrip ===");
    let header = StreamHeader {
        stream_id: 1234567890,
        total_size: 4096,
        chunk_size: 1024,
        chunk_index: 0,
        type_hash: 0xDEADBEEF,
        actor_id: 9876543210,
    };

    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let bytes = header.to_bytes();
            let _parsed = StreamHeader::from_bytes(&bytes).unwrap();
        }
    });

    eprintln!(
        "  Result: {} allocations per 1000 roundtrips",
        metrics.allocations
    );
    eprintln!("  Total bytes allocated: {}", metrics.total_bytes);
}

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_streamheader_various_sizes_allocations() {
    let _guard = allocation_test_guard();
    eprintln!("\n=== BASELINE: StreamHeader Various Sizes ===");

    let test_sizes = vec![64, 256, 1024, 4096, 16384, 65536];

    for size in test_sizes {
        let header = StreamHeader {
            stream_id: 1234567890,
            total_size: size as u64,
            chunk_size: 1024,
            chunk_index: 0,
            type_hash: 0xDEADBEEF,
            actor_id: 9876543210,
        };

        let metrics = measure_allocations(|| {
            for _ in 0..100 {
                let _ = header.to_bytes();
            }
        });

        eprintln!(
            "  Size {}: {} allocations per 100 calls ({} bytes)",
            size, metrics.allocations, metrics.total_bytes
        );
    }
}

// ============================================================================
// BASELINE SUMMARY TEST
// ============================================================================

#[test]
#[ignore = "Run with: cargo test baseline -- --ignored"]
fn baseline_summary_all_tests() {
    let _guard = allocation_test_guard();
    eprintln!("\n");
    eprintln!("╔═══════════════════════════════════════════════════════════════╗");
    eprintln!("║           BASELINE ALLOCATION SUMMARY REPORT                  ║");
    eprintln!("╚═══════════════════════════════════════════════════════════════╝");
    eprintln!();

    // StreamHeader serialization
    let header = StreamHeader {
        stream_id: 1234567890,
        total_size: 4096,
        chunk_size: 1024,
        chunk_index: 0,
        type_hash: 0xDEADBEEF,
        actor_id: 9876543210,
    };

    eprintln!("1. StreamHeader::to_bytes (1000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = header.to_bytes();
        }
    });
    eprintln!(
        "   → {} allocations, {} bytes ({} bytes/call)",
        metrics.allocations,
        metrics.total_bytes,
        metrics.total_bytes / 1000
    );
    eprintln!();

    // StreamHeader deserialization
    let bytes = header.to_bytes();
    eprintln!("2. StreamHeader::from_bytes (1000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = StreamHeader::from_bytes(&bytes).unwrap();
        }
    });
    eprintln!(
        "   → {} allocations, {} bytes",
        metrics.allocations, metrics.total_bytes
    );
    eprintln!();

    // Hash extraction
    let payload = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    eprintln!("3. Hash extraction (10000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..10000 {
            let mut hash_bytes = [0u8; 8];
            hash_bytes.copy_from_slice(&payload[..8]);
            let _hash = u64::from_be_bytes(hash_bytes);
        }
    });
    eprintln!("   → {} heap allocations (stack only)", metrics.allocations);
    eprintln!();

    // PublicKey conversion
    let key_bytes = [0u8; 32];
    eprintln!("4. PublicKey::from_bytes (1000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let _ = PublicKey::from_bytes(&key_bytes).unwrap();
        }
    });
    eprintln!(
        "   → {} allocations, {} bytes",
        metrics.allocations, metrics.total_bytes
    );
    eprintln!();

    // Header assembly
    let data = vec![0u8; 1024];
    eprintln!("5. Header assembly (10000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..10000 {
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());
        }
    });
    eprintln!("   → {} heap allocations (stack only)", metrics.allocations);
    eprintln!();

    // Full roundtrip
    eprintln!("6. StreamHeader roundtrip (1000 calls)");
    let metrics = measure_allocations(|| {
        for _ in 0..1000 {
            let bytes = header.to_bytes();
            let _parsed = StreamHeader::from_bytes(&bytes).unwrap();
        }
    });
    eprintln!(
        "   → {} allocations, {} bytes",
        metrics.allocations, metrics.total_bytes
    );
    eprintln!();

    eprintln!("═══════════════════════════════════════════════════════════════");
    eprintln!("  BASELINE COMPLETE - DO NOT MODIFY UNTIL OPTIMIZATION PHASE");
    eprintln!("═══════════════════════════════════════════════════════════════");
}
