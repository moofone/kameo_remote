use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

pub mod gossip_zero_copy {
    use super::{AtomicU64, OnceLock, Ordering};
    use tracing::{debug, warn};

    #[derive(Default)]
    struct Counters {
        inbound_frames: AtomicU64,
        outbound_frames: AtomicU64,
        alignment_failures: AtomicU64,
    }

    impl Counters {
        fn snapshot(&self) -> Snapshot {
            Snapshot {
                inbound_frames: self.inbound_frames.load(Ordering::Relaxed),
                outbound_frames: self.outbound_frames.load(Ordering::Relaxed),
                alignment_failures: self.alignment_failures.load(Ordering::Relaxed),
            }
        }
    }

    fn counters() -> &'static Counters {
        static COUNTERS: OnceLock<Counters> = OnceLock::new();
        COUNTERS.get_or_init(Counters::default)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Snapshot {
        pub inbound_frames: u64,
        pub outbound_frames: u64,
        pub alignment_failures: u64,
    }

    /// Record a successfully decoded zero-copy gossip frame.
    pub fn record_inbound_frame(context: &'static str, payload_len: usize) {
        let counters = counters();
        let total = counters.inbound_frames.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            target: "kameo::gossip_zero_copy",
            event = "inbound_frame",
            context,
            payload_len,
            gossip_zero_copy_frames_total = total,
            "Zero-copy gossip frame decoded"
        );
    }

    /// Record a zero-copy gossip frame sent over the wire.
    pub fn record_outbound_frame(context: &'static str, payload_len: usize) {
        let counters = counters();
        let total = counters.outbound_frames.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            target: "kameo::gossip_zero_copy",
            event = "outbound_frame",
            context,
            payload_len,
            gossip_zero_copy_frames_total = total,
            "Zero-copy gossip frame emitted"
        );
    }

    /// Record an alignment failure that forced a fallback allocation.
    pub fn record_alignment_failure(context: &'static str) {
        let counters = counters();
        let total = counters.alignment_failures.fetch_add(1, Ordering::Relaxed) + 1;
        warn!(
            target: "kameo::gossip_zero_copy",
            event = "alignment_failure",
            context,
            gossip_zero_copy_alignment_failures_total = total,
            "Gossip payload lost zero-copy alignment"
        );
    }

    /// Snapshot the counters for reporting or tests.
    pub fn snapshot() -> Snapshot {
        counters().snapshot()
    }

    /// Reset counters (primarily for deterministic tests).
    pub fn reset() {
        let counters = counters();
        counters.inbound_frames.store(0, Ordering::Relaxed);
        counters.outbound_frames.store(0, Ordering::Relaxed);
        counters.alignment_failures.store(0, Ordering::Relaxed);
    }
}
