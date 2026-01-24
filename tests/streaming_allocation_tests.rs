use kameo_remote::streaming::{StreamAssembler, StreamDescriptor};
use std::alloc::{GlobalAlloc, Layout, System};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

struct GuardedAllocator;

static GUARD_ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static LAST_ALLOC_SIZE: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL: GuardedAllocator = GuardedAllocator;

unsafe impl GlobalAlloc for GuardedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if GUARD_ACTIVE.load(Ordering::SeqCst) {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            LAST_ALLOC_SIZE.store(layout.size(), Ordering::SeqCst);
        }
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }
}

fn reset_allocations() {
    ALLOCATIONS.store(0, Ordering::SeqCst);
}

fn allocation_count() -> usize {
    ALLOCATIONS.load(Ordering::SeqCst)
}

fn last_allocation_size() -> usize {
    LAST_ALLOC_SIZE.load(Ordering::SeqCst)
}

fn descriptor(payload_len: u64) -> StreamDescriptor {
    StreamDescriptor {
        stream_id: 99,
        payload_len,
        chunk_len: payload_len as u32,
        type_hash: 0xDEAD_BEEF,
        actor_id: 0xA55A,
        flags: 0,
        reserved: 0,
    }
}

#[tokio::test]
async fn zero_allocations_during_direct_stream_receive() {
    let mut assembler = StreamAssembler::new(256);
    let desc = descriptor(64);
    assembler.start_stream(desc, Some(42)).unwrap();

    let payload = vec![0xAA; 64];
    let mut reader = SliceReader::new(payload.clone());

    // Guard the actual TLS read path (without forcing completion) to ensure the copy itself allocates nothing.
    reset_allocations();
    GUARD_ACTIVE.store(true, Ordering::SeqCst);
    assert!(
        assembler
            .read_data_direct(&mut reader, payload.len() - 1)
            .await
            .unwrap()
            .is_none(),
        "stream should remain incomplete while guard is active"
    );
    GUARD_ACTIVE.store(false, Ordering::SeqCst);

    let completed = assembler
        .read_data_direct(&mut reader, 1)
        .await
        .unwrap()
        .expect("stream completes");

    assert_eq!(completed.payload.as_ref(), payload.as_slice());
    assert_eq!(
        allocation_count(),
        0,
        "streaming receive must not allocate during direct placement (last alloc {} bytes)",
        last_allocation_size()
    );
}

struct SliceReader {
    data: Vec<u8>,
    cursor: usize,
}

impl SliceReader {
    fn new(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl AsyncRead for SliceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.cursor >= self.data.len() {
            return Poll::Ready(Ok(()));
        }

        let remaining = self.data.len() - self.cursor;
        let to_copy = remaining.min(buf.remaining());
        buf.put_slice(&self.data[self.cursor..self.cursor + to_copy]);
        self.cursor += to_copy;
        Poll::Ready(Ok(()))
    }
}
