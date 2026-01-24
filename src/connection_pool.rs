use bytes::Buf;
use futures::FutureExt;
use rkyv::Archive;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::{collections::HashMap, io, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
use sha2::{Digest, Sha256};

use crate::{
    current_timestamp, framing,
    registry::{resolve_peer_addr, GossipRegistry, RegistryMessage, RegistryMessageFrame},
    streaming, GossipError, Result,
};

// ==== SINGLE SOURCE OF TRUTH FOR ALL BUFFER SIZES ====
// THIS is the ONLY place we define the master buffer size!
// Change this ONE constant to adjust ALL buffer behavior system-wide.
pub const MASTER_BUFFER_SIZE: usize = 1024 * 1024; // 1MB - THE source of truth

// All other buffer sizes derive from the master constant - NO MAGIC NUMBERS!
pub const TCP_BUFFER_SIZE: usize = MASTER_BUFFER_SIZE; // BufWriter & io_uring buffer
pub const STREAM_CHUNK_SIZE: usize = MASTER_BUFFER_SIZE; // Streaming chunk size
pub const STREAMING_THRESHOLD: usize = MASTER_BUFFER_SIZE.saturating_sub(1024); // Just under buffer limit

// Ring buffer configuration (NUMBER OF SLOTS, not bytes!)
pub const RING_BUFFER_SLOTS: usize = 1024; // Number of WriteCommand slots in ring buffer
                                           // Note: Each slot can hold any size message - this is NOT a byte limit
                                           // Note: Streaming threshold ensures messages fit within TCP_BUFFER_SIZE
const CONTROL_RESERVED_SLOTS: usize = 32; // Reserved permits for control/tell/response lanes
const WRITER_MAX_LATENCY: Duration = Duration::from_micros(100);

#[cfg_attr(
    not(any(test, feature = "test-helpers", debug_assertions)),
    allow(dead_code)
)]
fn typed_tell_capture_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok())
}

/// Tracks spawned tasks for a connection.
///
/// ## Lifecycle Rules (H-004)
/// - `set_writer`/`set_reader` should only be called when establishing NEW connections
/// - Do NOT call set_* during reconnect while messages are in flight
/// - Call `abort_all()` only when the connection is being permanently torn down
/// - For reconnection: let old tasks complete naturally, then set new handles
#[derive(Debug, Default)]
pub struct TaskTracker {
    writer_handle: Option<JoinHandle<()>>,
    reader_handle: Option<JoinHandle<()>>,
}

impl TaskTracker {
    /// Create a new empty TaskTracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set writer handle. Only call for NEW connections, not reconnects.
    pub fn set_writer(&mut self, handle: JoinHandle<()>) {
        if let Some(old) = self.writer_handle.take() {
            old.abort();
        }
        self.writer_handle = Some(handle);
    }

    /// Set reader handle. Only call for NEW connections, not reconnects.
    pub fn set_reader(&mut self, handle: JoinHandle<()>) {
        if let Some(old) = self.reader_handle.take() {
            old.abort();
        }
        self.reader_handle = Some(handle);
    }

    /// Abort all tracked tasks.
    pub fn abort_all(&mut self) {
        if let Some(h) = self.writer_handle.take() {
            h.abort();
        }
        if let Some(h) = self.reader_handle.take() {
            h.abort();
        }
    }

    /// Abort only the reader task.
    pub fn abort_reader(&mut self) {
        if let Some(h) = self.reader_handle.take() {
            h.abort();
        }
    }

    /// Abort only the writer task.
    pub fn abort_writer(&mut self) {
        if let Some(h) = self.writer_handle.take() {
            h.abort();
        }
    }

    /// Detach the writer task without aborting (allow graceful shutdown).
    pub fn detach_writer(&mut self) {
        let _ = self.writer_handle.take();
    }

    /// Detach the reader task without aborting.
    pub fn detach_reader(&mut self) {
        let _ = self.reader_handle.take();
    }
}

impl Drop for TaskTracker {
    fn drop(&mut self) {
        self.abort_all();
    }
}

fn control_reserved_slots(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }

    if capacity >= CONTROL_RESERVED_SLOTS {
        return CONTROL_RESERVED_SLOTS;
    }

    (capacity / 2).max(1)
}

fn should_flush(
    bytes_since_flush: usize,
    ring_empty: bool,
    elapsed: Duration,
    flush_threshold: usize,
    max_latency: Duration,
) -> bool {
    if bytes_since_flush == 0 {
        return false;
    }

    if bytes_since_flush >= flush_threshold {
        return true;
    }

    if ring_empty {
        return true;
    }

    elapsed >= max_latency
}

/// Buffer configuration that encapsulates all buffer-related settings
///
/// This struct ensures that the streaming threshold is always derived from the actual buffer size,
/// preventing the disconnection between buffer size and streaming decisions that causes message loss.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    ring_buffer_slots: usize, // Number of message slots in ring buffer
    tcp_buffer_size: usize,   // Size in bytes for TCP buffers
    ask_inflight_limit: usize,
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub(crate) fn process_mock_request(request: &str) -> Vec<u8> {
    if let Some(payload) = request.strip_prefix("ECHO:") {
        return format!("ECHOED:{}", payload).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("REVERSE:") {
        let reversed: String = payload.chars().rev().collect();
        return format!("REVERSED:{}", reversed).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("COUNT:") {
        return format!("COUNTED:{} chars", payload.chars().count()).into_bytes();
    }
    if let Some(payload) = request.strip_prefix("HASH:") {
        let mut hasher = Sha256::new();
        hasher.update(payload.as_bytes());
        let digest = hasher.finalize();
        return format!("HASHED:{}", hex::encode(digest)).into_bytes();
    }

    format!("RECEIVED:{} bytes, content: '{}'", request.len(), request).into_bytes()
}

#[cfg(any(test, feature = "test-helpers", debug_assertions))]
pub(crate) fn process_mock_request_payload(payload: &[u8]) -> Vec<u8> {
    if payload.len() == 4 {
        let value = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        return (value + 1).to_be_bytes().to_vec();
    }

    let request_str = String::from_utf8_lossy(payload);
    process_mock_request(&request_str)
}

impl BufferConfig {
    /// Create a new BufferConfig with validation
    ///
    /// # Arguments
    /// * `tcp_buffer_size` - The size of TCP buffers in bytes
    ///
    /// # Errors
    /// Returns an error if the buffer size is less than 256KB for safety
    pub fn new(tcp_buffer_size: usize) -> Result<Self> {
        // Enforce minimum size of 256KB for safety
        if tcp_buffer_size < 256 * 1024 {
            return Err(GossipError::InvalidConfig(format!(
                "TCP buffer must be at least 256KB, got {}KB",
                tcp_buffer_size / 1024
            )));
        }
        Ok(Self {
            ring_buffer_slots: RING_BUFFER_SLOTS, // Use constant for slots
            tcp_buffer_size,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        })
    }

    /// Get the ring buffer slot count (NOT bytes!)
    pub fn ring_buffer_slots(&self) -> usize {
        self.ring_buffer_slots
    }

    /// Max in-flight ask permits per connection.
    pub fn ask_inflight_limit(&self) -> usize {
        self.ask_inflight_limit
    }

    /// Override the default ask inflight limit.
    pub fn with_ask_inflight_limit(mut self, limit: usize) -> Self {
        self.ask_inflight_limit = limit;
        self
    }

    /// Calculate the streaming threshold based on buffer size
    ///
    /// The streaming threshold is always derived from the buffer size,
    /// leaving 1KB headroom for headers and serialization overhead.
    pub fn streaming_threshold(&self) -> usize {
        // Always derive from buffer size
        // Leave 1KB headroom for headers/overhead
        self.tcp_buffer_size.saturating_sub(1024)
    }

    /// Get the TCP buffer size for BufWriter and io_uring
    ///
    /// This ensures TCP buffers match the configured size, preventing bottlenecks.
    pub fn tcp_buffer_size(&self) -> usize {
        self.tcp_buffer_size
    }

    /// Create BufferConfig using the master buffer size (recommended)
    pub fn from_master() -> Self {
        Self {
            ring_buffer_slots: RING_BUFFER_SLOTS,
            tcp_buffer_size: MASTER_BUFFER_SIZE,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        }
    }
}

impl Default for BufferConfig {
    fn default() -> Self {
        // Use master constant instead of magic number!
        Self {
            ring_buffer_slots: RING_BUFFER_SLOTS,
            tcp_buffer_size: MASTER_BUFFER_SIZE,
            ask_inflight_limit: crate::config::DEFAULT_ASK_INFLIGHT_LIMIT,
        }
    }
}

/// Stream frame types for high-performance streaming protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameType {
    Data = 0x01,
    Ack = 0x02,
    Close = 0x03,
    Heartbeat = 0x04,
    TellAsk = 0x05,    // Regular tell/ask messages
    StreamData = 0x06, // Dedicated streaming data
}

/// Channel IDs for stream multiplexing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelId {
    TellAsk = 0x00,  // Regular tell/ask channel
    Stream1 = 0x01,  // Dedicated streaming channel 1
    Stream2 = 0x02,  // Dedicated streaming channel 2
    Stream3 = 0x03,  // Dedicated streaming channel 3
    Bulk = 0x04,     // Bulk data channel
    Priority = 0x05, // Priority streaming channel
    Global = 0xFF,   // Global channel for all operations
}

/// Stream frame flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamFrameFlags {
    None = 0x00,
    More = 0x01,       // More frames to follow
    Compressed = 0x02, // Frame is compressed
    Encrypted = 0x04,  // Frame is encrypted
}

/// Stream frame header for structured messaging
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(derive(Debug))]
pub struct StreamFrameHeader {
    pub frame_type: u8,
    pub channel_id: u8,
    pub flags: u8,
    pub sequence_id: u16,
    pub payload_len: u32,
}

/// Lock-free connection state representation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ConnectionState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Failed = 3,
}

impl From<u32> for ConnectionState {
    fn from(value: u32) -> Self {
        match value {
            0 => ConnectionState::Disconnected,
            1 => ConnectionState::Connecting,
            2 => ConnectionState::Connected,
            3 => ConnectionState::Failed,
            _ => ConnectionState::Failed,
        }
    }
}

/// Direction of the TCP connection relative to this node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    Inbound,
    Outbound,
}

/// Lock-free connection metadata
#[derive(Debug)]
pub struct LockFreeConnection {
    pub addr: SocketAddr,
    pub state: AtomicU32,       // ConnectionState
    pub last_used: AtomicUsize, // Timestamp
    pub bytes_written: AtomicUsize,
    pub bytes_read: AtomicUsize,
    pub failure_count: AtomicUsize,
    pub stream_handle: Option<Arc<LockFreeStreamHandle>>,
    pub(crate) correlation: Option<Arc<CorrelationTracker>>,
    pub direction: ConnectionDirection,
    /// Task tracker for background tasks (writer and reader)
    /// Uses parking_lot::Mutex for better performance than std::sync::Mutex
    /// (no poisoning overhead, faster lock acquisition)
    pub task_tracker: parking_lot::Mutex<TaskTracker>,
}

impl Clone for LockFreeConnection {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            state: AtomicU32::new(self.state.load(Ordering::Relaxed)),
            last_used: AtomicUsize::new(self.last_used.load(Ordering::Relaxed)),
            bytes_written: AtomicUsize::new(self.bytes_written.load(Ordering::Relaxed)),
            bytes_read: AtomicUsize::new(self.bytes_read.load(Ordering::Relaxed)),
            failure_count: AtomicUsize::new(self.failure_count.load(Ordering::Relaxed)),
            stream_handle: self.stream_handle.clone(),
            correlation: self.correlation.clone(),
            direction: self.direction,
            // Note: TaskTracker is not cloned - each clone gets a fresh tracker
            // This is intentional: clones are typically used for metadata snapshots,
            // not to transfer task ownership
            task_tracker: parking_lot::Mutex::new(TaskTracker::new()),
        }
    }
}

impl LockFreeConnection {
    pub fn new(addr: SocketAddr, direction: ConnectionDirection) -> Self {
        Self {
            addr,
            state: AtomicU32::new(ConnectionState::Disconnected as u32),
            last_used: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            bytes_read: AtomicUsize::new(0),
            failure_count: AtomicUsize::new(0),
            stream_handle: None,
            correlation: Some(CorrelationTracker::new()),
            direction,
            task_tracker: parking_lot::Mutex::new(TaskTracker::new()),
        }
    }

    /// Abort all tracked background tasks (writer, reader).
    /// Call this when tearing down the connection to prevent resource leaks.
    pub fn abort_tasks(&self) {
        self.task_tracker.lock().abort_all();
    }

    /// Gracefully shutdown writer and abort reader to avoid abrupt TLS close.
    pub fn shutdown_tasks_gracefully(&self) {
        if let Some(ref stream_handle) = self.stream_handle {
            stream_handle.shutdown();
        }
        let mut tracker = self.task_tracker.lock();
        tracker.detach_writer();
        tracker.abort_reader();
    }

    pub fn get_state(&self) -> ConnectionState {
        self.state.load(Ordering::Acquire).into()
    }

    pub fn set_state(&self, state: ConnectionState) {
        self.state.store(state as u32, Ordering::Release);
    }

    pub fn try_set_state(&self, expected: ConnectionState, new: ConnectionState) -> bool {
        self.state
            .compare_exchange(
                expected as u32,
                new as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn update_last_used(&self) {
        self.last_used
            .store(crate::current_timestamp() as usize, Ordering::Release);
    }

    pub fn increment_failure_count(&self) -> usize {
        self.failure_count.fetch_add(1, Ordering::AcqRel)
    }

    pub fn reset_failure_count(&self) {
        self.failure_count.store(0, Ordering::Release);
    }

    pub fn is_connected(&self) -> bool {
        self.get_state() == ConnectionState::Connected
    }

    pub fn is_failed(&self) -> bool {
        self.get_state() == ConnectionState::Failed
    }
}

/// Payloads for ring-buffered writes.
pub enum WritePayload {
    Single(bytes::Bytes),
    HeaderPayload {
        header: bytes::Bytes,
        payload: bytes::Bytes,
    },
    HeaderInline {
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    },
    HeaderPooled {
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    },
    HeaderInlinePooled {
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    },
    HeaderInlineBuf {
        header: [u8; 16],
        header_len: u8,
        payload: Box<dyn Buf + Send>,
    },
    Buf(Box<dyn Buf + Send>),
}

impl std::fmt::Debug for WritePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WritePayload::Single(data) => f.debug_tuple("Single").field(&data.len()).finish(),
            WritePayload::HeaderPayload { header, payload } => f
                .debug_struct("HeaderPayload")
                .field("header_len", &header.len())
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInline {
                header_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInline")
                .field("header_len", &header_len)
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            } => f
                .debug_struct("HeaderPooled")
                .field("header_len", &header.len())
                .field("prefix_len", &prefix.as_ref().map(|p| p.len()).unwrap_or(0))
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInlinePooled {
                header_len,
                prefix,
                prefix_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInlinePooled")
                .field("header_len", &header_len)
                .field(
                    "prefix_len",
                    &if prefix.is_some() { *prefix_len } else { 0 },
                )
                .field("payload_len", &payload.len())
                .finish(),
            WritePayload::HeaderInlineBuf {
                header_len,
                payload,
                ..
            } => f
                .debug_struct("HeaderInlineBuf")
                .field("header_len", &header_len)
                .field("payload_len", &payload.remaining())
                .finish(),
            WritePayload::Buf(_) => f.debug_tuple("Buf").field(&"<buf>").finish(),
        }
    }
}

// Safety: WritePayload is only moved across threads; it is not accessed concurrently.
unsafe impl Send for WritePayload {}
unsafe impl Sync for WritePayload {}

/// Write command for the lock-free ring buffer with zero-copy optimization
pub struct WriteCommand {
    pub channel_id: ChannelId,
    pub data: WritePayload,
    pub sequence: u64,
    pub permit: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for WriteCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCommand")
            .field("channel_id", &self.channel_id)
            .field("data", &self.data)
            .field("sequence", &self.sequence)
            .field("permit", &self.permit.is_some())
            .finish()
    }
}

// Safety: WriteCommand is only moved across threads; it is not accessed concurrently.
unsafe impl Send for WriteCommand {}
unsafe impl Sync for WriteCommand {}

/// Zero-copy write command that references pre-allocated buffer slots
#[derive(Debug)]
pub struct ZeroCopyWriteCommand {
    pub channel_id: ChannelId,
    pub buffer_ptr: *const u8,
    pub len: usize,
    pub sequence: u64,
    pub buffer_id: usize, // For buffer pool management
}

// Safety: ZeroCopyWriteCommand is only used within the single writer task
unsafe impl Send for ZeroCopyWriteCommand {}
unsafe impl Sync for ZeroCopyWriteCommand {}

/// Memory pool for zero-allocation message handling
#[derive(Debug)]
pub struct MessageBufferPool {
    pool: Vec<Vec<u8>>,
    pool_size: usize,
    available_buffers: AtomicUsize,
    next_buffer_idx: AtomicUsize,
}

impl MessageBufferPool {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(Vec::with_capacity(buffer_size));
        }

        Self {
            pool,
            pool_size,
            available_buffers: AtomicUsize::new(pool_size),
            next_buffer_idx: AtomicUsize::new(0),
        }
    }

    /// Get a buffer from the pool - returns None if pool is empty
    pub fn get_buffer(&self) -> Option<Vec<u8>> {
        let available = self.available_buffers.load(Ordering::Acquire);
        if available == 0 {
            return None;
        }

        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;

        // Try to claim this buffer
        if self.available_buffers.fetch_sub(1, Ordering::AcqRel) > 0 {
            // We successfully claimed a buffer
            unsafe {
                let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
                let mut buffer = std::ptr::replace(buffer_ptr, Vec::new());
                buffer.clear(); // Reset length but keep capacity
                Some(buffer)
            }
        } else {
            // No buffers available, restore count
            self.available_buffers.fetch_add(1, Ordering::Release);
            None
        }
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear(); // Reset length but keep capacity

        let idx = self.next_buffer_idx.fetch_add(1, Ordering::AcqRel) % self.pool_size;

        unsafe {
            let buffer_ptr = self.pool.as_ptr().add(idx) as *mut Vec<u8>;
            *buffer_ptr = buffer;
        }

        self.available_buffers.fetch_add(1, Ordering::Release);
    }

    pub fn available_count(&self) -> usize {
        self.available_buffers.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.available_count() == 0
    }
}

/// Lock-free ring buffer for high-performance writes with proper memory ordering
#[derive(Debug)]
pub struct LockFreeRingBuffer {
    buffer: Vec<Option<WriteCommand>>,
    capacity: usize,
    // Cache-line aligned atomic counters to prevent false sharing
    write_index: AtomicUsize,
    read_index: AtomicUsize,
    pending_writes: AtomicUsize,
}

impl LockFreeRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: (0..capacity).map(|_| None).collect(),
            capacity,
            write_index: AtomicUsize::new(0),
            read_index: AtomicUsize::new(0),
            pending_writes: AtomicUsize::new(0),
        }
    }

    /// Try to push a write command - returns true if successful
    /// Uses proper memory ordering to prevent ABA problem
    pub fn try_push(&self, command: WriteCommand) -> bool {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes >= self.capacity {
            return false; // Buffer full
        }

        let write_idx = self.write_index.fetch_add(1, Ordering::AcqRel) % self.capacity;

        // This is unsafe but fast - we're assuming single writer per slot
        // Using proper memory ordering ensures visibility across cores
        unsafe {
            let slot = self.buffer.as_ptr().add(write_idx) as *mut Option<WriteCommand>;
            *slot = Some(command);
        }

        // Release ordering ensures the write above is visible before incrementing pending count
        self.pending_writes.fetch_add(1, Ordering::Release);
        true
    }

    /// Try to pop a write command - returns None if empty
    /// Uses proper memory ordering to prevent race conditions
    pub fn try_pop(&self) -> Option<WriteCommand> {
        let current_writes = self.pending_writes.load(Ordering::Acquire);
        if current_writes == 0 {
            return None;
        }

        let read_idx = self.read_index.fetch_add(1, Ordering::AcqRel) % self.capacity;

        // This is unsafe but fast - we're assuming single reader per slot
        // Using proper memory ordering ensures we see writes from other cores
        unsafe {
            let slot = self.buffer.as_ptr().add(read_idx) as *mut Option<WriteCommand>;
            let command = (*slot).take();
            if command.is_some() {
                // Release ordering ensures the slot is cleared before decrementing pending count
                self.pending_writes.fetch_sub(1, Ordering::Release);
            }
            command
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending_writes.load(Ordering::Acquire)
    }

    /// Get buffer utilization as a percentage (0-100)
    pub fn utilization(&self) -> f32 {
        (self.pending_count() as f32 / self.capacity as f32) * 100.0
    }

    /// Check if buffer is nearly full (>90% capacity)
    pub fn is_nearly_full(&self) -> bool {
        self.pending_count() > (self.capacity * 9) / 10
    }
}

/// Vectored write command for zero-copy header + payload operations
#[derive(Debug)]
pub struct VectoredWriteCommand {
    header: bytes::Bytes,
    payload: bytes::Bytes,
}

/// Commands for streaming operations
#[derive(Debug)]
enum StreamingCommand {
    /// Direct write bytes for streaming
    WriteBytes(bytes::Bytes),
    /// Flush the writer
    Flush,
    /// Vectored write for header + payload (zero-copy)
    VectoredWrite(VectoredWriteCommand),
    /// Batch of owned chunks for streaming (zero-copy)
    OwnedChunks(Vec<bytes::Bytes>),
}

async fn write_bytes_vectored_all<W: AsyncWrite + Unpin>(
    writer: &mut W,
    chunks: &[bytes::Bytes],
) -> std::io::Result<usize> {
    let mut total_written = 0usize;
    let mut idx = 0usize;
    let mut offset = 0usize;

    while idx < chunks.len() {
        let current = &chunks[idx];
        let current_slice = &current[offset..];
        let mut bufs = [
            std::io::IoSlice::new(current_slice),
            std::io::IoSlice::new(&[]),
        ];

        if offset == 0 && idx + 1 < chunks.len() {
            bufs[1] = std::io::IoSlice::new(&chunks[idx + 1]);
        }

        let written = writer.write_vectored(&bufs).await?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write message",
            ));
        }

        total_written += written;

        let mut remaining = written;
        while remaining > 0 && idx < chunks.len() {
            let available = chunks[idx].len().saturating_sub(offset);
            if remaining < available {
                offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                idx += 1;
                offset = 0;
            }
        }
    }

    Ok(total_written)
}

/// Truly lock-free streaming handle with dedicated background writer
#[derive(Clone)]
pub struct LockFreeStreamHandle {
    addr: SocketAddr,
    channel_id: ChannelId,
    sequence_counter: Arc<AtomicUsize>,
    frame_sequence: Arc<AtomicUsize>,
    bytes_written: Arc<AtomicUsize>, // This tracks actual TCP bytes written
    ring_buffer: Arc<LockFreeRingBuffer>,
    shutdown_signal: Arc<AtomicBool>,
    flush_pending: Arc<AtomicBool>,
    writer_idle: Arc<AtomicBool>,
    writer_notify: Arc<Notify>,
    ask_permits: Arc<Semaphore>,
    control_permits: Arc<Semaphore>,
    /// Atomic flag for coordinating streaming mode
    streaming_active: Arc<AtomicBool>,
    /// Channel to send streaming commands to background task
    streaming_tx: mpsc::UnboundedSender<StreamingCommand>,
    /// Buffer configuration that determines sizes and thresholds
    buffer_config: BufferConfig,
}

impl LockFreeStreamHandle {
    /// Create a new lock-free streaming handle with background writer task
    ///
    /// Returns a tuple of (Self, JoinHandle) where the JoinHandle can be used
    /// to track and abort the background writer task (H-004 task tracking).
    pub fn new<W>(
        tcp_writer: W,
        addr: SocketAddr,
        channel_id: ChannelId,
        buffer_config: BufferConfig,
    ) -> (Self, JoinHandle<()>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let ring_buffer = Arc::new(LockFreeRingBuffer::new(buffer_config.ring_buffer_slots()));
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let streaming_active = Arc::new(AtomicBool::new(false));
        let flush_pending = Arc::new(AtomicBool::new(false));
        let writer_idle = Arc::new(AtomicBool::new(true));
        let writer_notify = Arc::new(Notify::new());

        let capacity = buffer_config.ring_buffer_slots();
        let reserved = control_reserved_slots(capacity);
        let available = capacity.saturating_sub(reserved);
        let ask_limit = if available == 0 {
            0
        } else {
            buffer_config.ask_inflight_limit().min(available).max(1)
        };
        let ask_permits = Arc::new(Semaphore::new(ask_limit));
        let control_permits = Arc::new(Semaphore::new(reserved));

        // Create shared counter for actual TCP bytes written
        let bytes_written = Arc::new(AtomicUsize::new(0));

        // Create channel for streaming commands
        let (streaming_tx, streaming_rx) = mpsc::unbounded_channel();

        // Spawn background writer task with exclusive TCP access - NO MUTEX!
        let writer_handle = {
            let ring_buffer = ring_buffer.clone();
            let shutdown_signal = shutdown_signal.clone();
            let bytes_written_for_task = bytes_written.clone();
            let streaming_active_for_task = streaming_active.clone();
            let flush_pending_for_task = flush_pending.clone();
            let writer_idle_for_task = writer_idle.clone();
            let writer_notify_for_task = writer_notify.clone();
            let writer_addr = addr;
            let writer_channel_id = channel_id;

            tokio::spawn(async move {
                info!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "🚀 Background writer task started"
                );
                Self::background_writer_task(
                    tcp_writer,
                    ring_buffer,
                    shutdown_signal,
                    bytes_written_for_task,
                    flush_pending_for_task,
                    streaming_active_for_task,
                    writer_idle_for_task,
                    writer_notify_for_task,
                    streaming_rx,
                )
                .await;
                // CRITICAL: Log when writer exits - this helps diagnose silent writer deaths
                warn!(
                    addr = %writer_addr,
                    channel_id = ?writer_channel_id,
                    "⚠️ Background writer task EXITED - no more writes possible on this connection!"
                );
            })
        };

        (
            Self {
                addr,
                channel_id,
                sequence_counter: Arc::new(AtomicUsize::new(0)),
                frame_sequence: Arc::new(AtomicUsize::new(0)),
                bytes_written, // This now tracks actual TCP bytes written
                ring_buffer,
                shutdown_signal,
                flush_pending,
                writer_idle,
                writer_notify,
                ask_permits,
                control_permits,
                streaming_active,
                streaming_tx,
                buffer_config,
            },
            writer_handle,
        )
    }

    /// Background writer task - truly lock-free with exclusive TCP access
    /// OPTIMIZED FOR MAXIMUM THROUGHPUT - NO MUTEX NEEDED!
    #[allow(clippy::too_many_arguments)]
    async fn background_writer_task<W>(
        tcp_writer: W,
        ring_buffer: Arc<LockFreeRingBuffer>,
        shutdown_signal: Arc<AtomicBool>,
        bytes_written_counter: Arc<AtomicUsize>, // Track ALL bytes written to TCP
        flush_pending: Arc<AtomicBool>,
        streaming_active: Arc<AtomicBool>,
        writer_idle: Arc<AtomicBool>,
        writer_notify: Arc<Notify>,
        mut streaming_rx: mpsc::UnboundedReceiver<StreamingCommand>,
    ) where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        use std::io::IoSlice;
        use tokio::io::AsyncWriteExt;

        // Use direct tokio writer with vectored I/O for now.
        // Future optimization: wire in platform-specific writers (io_uring on Linux 5.1+).
        // This requires either:
        // 1. Modifying StreamWriter trait to work with OwnedWriteHalf
        // 2. Or using a different socket splitting approach that allows io_uring
        // Use full TCP buffer size for large messages (CRITICAL FIX!)
        // Need large buffer to handle 100KB+ BacktestSummary messages
        let mut writer = tokio::io::BufWriter::with_capacity(TCP_BUFFER_SIZE, tcp_writer);

        // Smaller batches for lower latency
        const RING_BATCH_SIZE: usize = 64; // Smaller batches for faster processing
        const FLUSH_THRESHOLD: usize = 4 * 1024; // 4KB before flush (much lower for ask/reply)

        let mut bytes_since_flush = 0;
        let mut last_flush = std::time::Instant::now();

        while !shutdown_signal.load(Ordering::Relaxed) {
            let mut total_bytes_written = 0;
            let mut did_work = false;

            while let Ok(cmd) = streaming_rx.try_recv() {
                did_work = true;
                match cmd {
                    StreamingCommand::WriteBytes(data) => match writer.write_all(&data).await {
                        Ok(_) => {
                            bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                            total_bytes_written += data.len();
                        }
                        Err(e) => {
                            error!("Streaming write error: {}", e);
                            return;
                        }
                    },
                    StreamingCommand::Flush => {
                        let _ = writer.flush().await;
                        flush_pending.store(false, Ordering::Release);
                        last_flush = std::time::Instant::now();
                        bytes_since_flush = 0;
                    }
                    StreamingCommand::VectoredWrite(cmd) => {
                        let VectoredWriteCommand { header, payload } = cmd;
                        let chunks = [header, payload];
                        match write_bytes_vectored_all(&mut writer, &chunks).await {
                            Ok(n) => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Err(e) => {
                                error!("Vectored write error: {}", e);
                                return;
                            }
                        }
                    }
                    StreamingCommand::OwnedChunks(chunks) => {
                        match write_bytes_vectored_all(&mut writer, &chunks).await {
                            Ok(n) => {
                                bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                total_bytes_written += n;
                            }
                            Err(e) => {
                                error!("Chunk batch write error: {}", e);
                                return;
                            }
                        }
                    }
                }
            }

            if !streaming_active.load(Ordering::Acquire) {
                let mut write_chunks = Vec::with_capacity(RING_BATCH_SIZE * 2);
                let mut permits = Vec::with_capacity(RING_BATCH_SIZE);

                for _ in 0..RING_BATCH_SIZE {
                    if let Some(mut command) = ring_buffer.try_pop() {
                        did_work = true;
                        if let Some(permit) = command.permit.take() {
                            permits.push(permit);
                        }
                        match command.data {
                            WritePayload::Single(data) => write_chunks.push(data),
                            WritePayload::HeaderPayload { header, payload } => {
                                write_chunks.push(header);
                                write_chunks.push(payload);
                            }
                            WritePayload::HeaderInline {
                                header,
                                header_len,
                                payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut idx = 0;
                                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                                    for chunk in &chunks {
                                        iov[idx].write(IoSlice::new(chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match writer.write_vectored(slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match writer.write_vectored(slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter
                                                    .fetch_add(bytes_written, Ordering::Relaxed);
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;
                                let mut payload_off = 0usize;
                                let payload_len = payload.len();

                                while header_off < header_len || payload_off < payload_len {
                                    let h = &header[header_off..header_len];
                                    let p = &payload[payload_off..];
                                    let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                    let slice_count = if h.is_empty() {
                                        slices[0] = IoSlice::new(p);
                                        1
                                    } else if p.is_empty() {
                                        slices[0] = IoSlice::new(h);
                                        1
                                    } else {
                                        2
                                    };

                                    match writer.write_vectored(&slices[..slice_count]).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            if header_off < header_len {
                                                let h_rem = header_len - header_off;
                                                if n < h_rem {
                                                    header_off += n;
                                                    continue;
                                                } else {
                                                    header_off = header_len;
                                                    payload_off += n - h_rem;
                                                }
                                            } else {
                                                payload_off += n;
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlineBuf {
                                header,
                                header_len,
                                mut payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut idx = 0;
                                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                                    for chunk in &chunks {
                                        iov[idx].write(IoSlice::new(chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match writer.write_vectored(slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match writer.write_vectored(slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter
                                                    .fetch_add(bytes_written, Ordering::Relaxed);
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                let header_len = header_len as usize;
                                let mut header_off = 0usize;

                                while header_off < header_len {
                                    let h = &header[header_off..header_len];
                                    match writer.write_vectored(&[IoSlice::new(h)]).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                            header_off += n;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                while payload.has_remaining() {
                                    match writer.write_buf(&mut payload).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderPooled {
                                header,
                                prefix,
                                mut payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    const MAX_IOV: usize = 64;
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut idx = 0;
                                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                                    for chunk in &chunks {
                                        iov[idx].write(IoSlice::new(chunk));
                                        idx += 1;
                                        if idx == MAX_IOV {
                                            let slices = unsafe {
                                                std::slice::from_raw_parts(
                                                    iov.as_ptr() as *const IoSlice<'_>,
                                                    idx,
                                                )
                                            };
                                            match writer.write_vectored(slices).await {
                                                Ok(bytes_written) => {
                                                    bytes_written_counter.fetch_add(
                                                        bytes_written,
                                                        Ordering::Relaxed,
                                                    );
                                                    total_bytes_written += bytes_written;
                                                }
                                                Err(_) => return,
                                            }
                                            idx = 0;
                                        }
                                    }

                                    if idx > 0 {
                                        let slices = unsafe {
                                            std::slice::from_raw_parts(
                                                iov.as_ptr() as *const IoSlice<'_>,
                                                idx,
                                            )
                                        };
                                        match writer.write_vectored(slices).await {
                                            Ok(bytes_written) => {
                                                bytes_written_counter
                                                    .fetch_add(bytes_written, Ordering::Relaxed);
                                                total_bytes_written += bytes_written;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                if (writer.write_all(&header).await).is_err() {
                                    return;
                                }
                                bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
                                total_bytes_written += header.len();

                                if let Some(prefix) = prefix {
                                    if (writer.write_all(&prefix).await).is_err() {
                                        return;
                                    }
                                    bytes_written_counter
                                        .fetch_add(prefix.len(), Ordering::Relaxed);
                                    total_bytes_written += prefix.len();
                                }

                                while payload.has_remaining() {
                                    match writer.write_buf(&mut payload).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::HeaderInlinePooled {
                                header,
                                header_len,
                                prefix,
                                prefix_len,
                                mut payload,
                            } => {
                                if !write_chunks.is_empty() {
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut slices = Vec::with_capacity(chunks.len());
                                    for chunk in &chunks {
                                        slices.push(IoSlice::new(chunk));
                                    }
                                    match writer.write_vectored(&slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                let header_len = header_len as usize;
                                let prefix_len = prefix_len as usize;
                                let mut header_off = 0usize;
                                let mut prefix_off = 0usize;

                                if let Some(prefix) = prefix {
                                    while header_off < header_len || prefix_off < prefix_len {
                                        let h = &header[header_off..header_len];
                                        let p = &prefix[prefix_off..prefix_len];
                                        let mut slices = [IoSlice::new(h), IoSlice::new(p)];
                                        let slice_count = if h.is_empty() {
                                            slices[0] = IoSlice::new(p);
                                            1
                                        } else if p.is_empty() {
                                            slices[0] = IoSlice::new(h);
                                            1
                                        } else {
                                            2
                                        };

                                        match writer.write_vectored(&slices[..slice_count]).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                if header_off < header_len {
                                                    let h_rem = header_len - header_off;
                                                    if n < h_rem {
                                                        header_off += n;
                                                        continue;
                                                    } else {
                                                        header_off = header_len;
                                                        prefix_off += n - h_rem;
                                                    }
                                                } else {
                                                    prefix_off += n;
                                                }
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                } else {
                                    while header_off < header_len {
                                        let h = &header[header_off..header_len];
                                        match writer.write_vectored(&[IoSlice::new(h)]).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                bytes_written_counter
                                                    .fetch_add(n, Ordering::Relaxed);
                                                total_bytes_written += n;
                                                header_off += n;
                                            }
                                            Err(_) => return,
                                        }
                                    }
                                }

                                while payload.has_remaining() {
                                    match writer.write_buf(&mut payload).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                            WritePayload::Buf(mut buf) => {
                                if !write_chunks.is_empty() {
                                    let chunks = std::mem::take(&mut write_chunks);
                                    let mut slices = Vec::with_capacity(chunks.len());
                                    for chunk in &chunks {
                                        slices.push(IoSlice::new(chunk));
                                    }
                                    match writer.write_vectored(&slices).await {
                                        Ok(bytes_written) => {
                                            bytes_written_counter
                                                .fetch_add(bytes_written, Ordering::Relaxed);
                                            total_bytes_written += bytes_written;
                                        }
                                        Err(_) => return,
                                    }
                                }

                                while buf.has_remaining() {
                                    match writer.write_buf(&mut buf).await {
                                        Ok(0) => break,
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            total_bytes_written += n;
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }

                if !write_chunks.is_empty() {
                    const MAX_IOV: usize = 64;
                    let chunks = std::mem::take(&mut write_chunks);
                    let mut idx = 0;
                    let mut iov: [std::mem::MaybeUninit<IoSlice<'_>>; MAX_IOV] =
                        unsafe { std::mem::MaybeUninit::uninit().assume_init() };

                    for chunk in &chunks {
                        iov[idx].write(IoSlice::new(chunk));
                        idx += 1;
                        if idx == MAX_IOV {
                            let slices = unsafe {
                                std::slice::from_raw_parts(iov.as_ptr() as *const IoSlice<'_>, idx)
                            };
                            match writer.write_vectored(slices).await {
                                Ok(bytes_written) => {
                                    bytes_written_counter
                                        .fetch_add(bytes_written, Ordering::Relaxed);
                                    total_bytes_written += bytes_written;
                                }
                                Err(_) => return,
                            }
                            idx = 0;
                        }
                    }

                    if idx > 0 {
                        let slices = unsafe {
                            std::slice::from_raw_parts(iov.as_ptr() as *const IoSlice<'_>, idx)
                        };
                        match writer.write_vectored(slices).await {
                            Ok(bytes_written) => {
                                bytes_written_counter.fetch_add(bytes_written, Ordering::Relaxed);
                                total_bytes_written += bytes_written;
                            }
                            Err(_) => return,
                        }
                    }
                }

                drop(permits);
            }

            bytes_since_flush += total_bytes_written;
            let ring_empty = ring_buffer.pending_count() == 0;
            let elapsed = last_flush.elapsed();

            if should_flush(
                bytes_since_flush,
                ring_empty,
                elapsed,
                FLUSH_THRESHOLD,
                WRITER_MAX_LATENCY,
            ) {
                let _ = writer.flush().await;
                bytes_since_flush = 0;
                last_flush = std::time::Instant::now();
                flush_pending.store(false, Ordering::Release);
            }

            if !did_work {
                writer_idle.store(true, Ordering::Release);

                if bytes_since_flush > 0 {
                    tokio::select! {
                        Some(cmd) = streaming_rx.recv() => {
                            writer_idle.store(false, Ordering::Release);
                            match cmd {
                                StreamingCommand::WriteBytes(data) => {
                                    if writer.write_all(&data).await.is_err() {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                                    bytes_since_flush += data.len();
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::Flush => {
                                    let _ = writer.flush().await;
                                    flush_pending.store(false, Ordering::Release);
                                    bytes_since_flush = 0;
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::VectoredWrite(cmd) => {
                                    let VectoredWriteCommand { header, payload } = cmd;
                                    let chunks = [header, payload];
                                    match write_bytes_vectored_all(&mut writer, &chunks).await {
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            bytes_since_flush += n;
                                            last_flush = std::time::Instant::now();
                                        }
                                        Err(_) => return,
                                    }
                                }
                                StreamingCommand::OwnedChunks(chunks) => {
                                    match write_bytes_vectored_all(&mut writer, &chunks).await {
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            bytes_since_flush += n;
                                            last_flush = std::time::Instant::now();
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                        }
                        _ = writer_notify.notified() => {
                            writer_idle.store(false, Ordering::Release);
                        }
                        _ = tokio::time::sleep(WRITER_MAX_LATENCY) => {
                            writer_idle.store(false, Ordering::Release);
                        }
                    }
                } else {
                    tokio::select! {
                        Some(cmd) = streaming_rx.recv() => {
                            writer_idle.store(false, Ordering::Release);
                            match cmd {
                                StreamingCommand::WriteBytes(data) => {
                                    if writer.write_all(&data).await.is_err() {
                                        return;
                                    }
                                    bytes_written_counter.fetch_add(data.len(), Ordering::Relaxed);
                                    bytes_since_flush += data.len();
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::Flush => {
                                    let _ = writer.flush().await;
                                    flush_pending.store(false, Ordering::Release);
                                    bytes_since_flush = 0;
                                    last_flush = std::time::Instant::now();
                                }
                                StreamingCommand::VectoredWrite(cmd) => {
                                    let VectoredWriteCommand { header, payload } = cmd;
                                    let chunks = [header, payload];
                                    match write_bytes_vectored_all(&mut writer, &chunks).await {
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            bytes_since_flush += n;
                                            last_flush = std::time::Instant::now();
                                        }
                                        Err(_) => return,
                                    }
                                }
                                StreamingCommand::OwnedChunks(chunks) => {
                                    match write_bytes_vectored_all(&mut writer, &chunks).await {
                                        Ok(n) => {
                                            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
                                            bytes_since_flush += n;
                                            last_flush = std::time::Instant::now();
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                        }
                        _ = writer_notify.notified() => {
                            writer_idle.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }

        let _ = writer.flush().await;
        let _ = writer.shutdown().await;
    }

    async fn acquire_ask_permit(&self) -> OwnedSemaphorePermit {
        self.ask_permits
            .clone()
            .acquire_owned()
            .await
            .expect("ask permit semaphore closed")
    }

    async fn acquire_control_permit(&self) -> Result<OwnedSemaphorePermit> {
        // Track permit acquisition for debugging stalls
        static ACQUIRE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = ACQUIRE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Add timeout to detect permit stalls - return error instead of panic
        let start = std::time::Instant::now();
        let permit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.control_permits.clone().acquire_owned(),
        )
        .await;

        match permit {
            Ok(Ok(p)) => {
                let elapsed = start.elapsed();
                if elapsed > std::time::Duration::from_millis(100) {
                    warn!(
                        elapsed_ms = elapsed.as_millis(),
                        available_after = self.control_permits.available_permits(),
                        "⚠️ Control permit acquisition was slow"
                    );
                }
                Ok(p)
            }
            Ok(Err(_)) => {
                error!("control permit semaphore closed unexpectedly");
                Err(GossipError::Network(std::io::Error::other(
                    "control permit semaphore closed",
                )))
            }
            Err(_) => {
                // Timeout after 5 seconds - connection likely stalled
                warn!(
                    count = count,
                    available = self.control_permits.available_permits(),
                    "Control permit acquisition timed out after 5s - writer likely stalled, dropping message"
                );
                Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "control permit acquisition timed out - writer stalled",
                )))
            }
        }
    }

    #[cfg(test)]
    fn permit_counts(&self) -> (usize, usize) {
        (
            self.ask_permits.available_permits(),
            self.control_permits.available_permits(),
        )
    }

    #[cfg(test)]
    async fn acquire_ask_permit_for_test(&self) -> OwnedSemaphorePermit {
        self.acquire_ask_permit().await
    }

    #[cfg(test)]
    async fn acquire_control_permit_for_test(&self) -> OwnedSemaphorePermit {
        self.acquire_control_permit()
            .await
            .expect("test: control permit acquisition failed")
    }

    fn enqueue_with_permit(&self, data: WritePayload, permit: OwnedSemaphorePermit) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
        let command = WriteCommand {
            channel_id: self.channel_id,
            data,
            sequence: sequence as u64,
            permit: Some(permit),
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    fn wake_writer_if_idle(&self) {
        if self
            .writer_idle
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.writer_notify.notify_one();
        }
    }

    #[inline]
    pub async fn write_bytes_ask(&self, data: bytes::Bytes) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::Single(data), permit)
    }

    #[inline]
    pub async fn write_bytes_control(&self, data: bytes::Bytes) -> Result<()> {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(WritePayload::Single(data), permit)
    }

    pub async fn write_header_and_payload_control(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(WritePayload::HeaderPayload { header, payload }, permit)
    }

    pub async fn write_header_and_payload_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(
            WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_header_and_payload_ask(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::HeaderPayload { header, payload }, permit)
    }

    pub async fn write_header_and_payload_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_control(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_control_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(
            WritePayload::HeaderInlinePooled {
                header,
                header_len,
                prefix,
                prefix_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_ask(
        &self,
        header: bytes::Bytes,
        prefix: Option<bytes::Bytes>,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderPooled {
                header,
                prefix,
                payload,
            },
            permit,
        )
    }

    pub async fn write_pooled_ask_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload: crate::typed::PooledPayload,
    ) -> Result<()> {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(
            WritePayload::HeaderInlinePooled {
                header,
                header_len,
                prefix,
                prefix_len,
                payload,
            },
            permit,
        )
    }

    pub async fn write_buf_control<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(WritePayload::Buf(Box::new(buf)), permit)
    }

    pub async fn write_header_and_payload_control_inline_buf<B>(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: B,
    ) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let permit = self.acquire_control_permit().await?;
        self.enqueue_with_permit(
            WritePayload::HeaderInlineBuf {
                header,
                header_len,
                payload: Box::new(payload),
            },
            permit,
        )
    }

    pub async fn write_buf_ask<B>(&self, buf: B) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let permit = self.acquire_ask_permit().await;
        self.enqueue_with_permit(WritePayload::Buf(Box::new(buf)), permit)
    }

    /// Write Bytes to the lock-free ring buffer - NO BLOCKING, NO COPY
    pub fn write_bytes_nonblocking(&self, data: bytes::Bytes) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::Single(data),
            sequence: sequence as u64,
            permit: None,
        };

        // CRITICAL FIX: Don't silently drop messages when ring buffer is full!
        // Previously used `let _ =` which ignored try_push failures.
        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            // Don't increment bytes_written here - only increment when actually written to TCP
            Ok(())
        } else {
            warn!(
                channel_id = ?self.channel_id,
                sequence = sequence,
                "Ring buffer full - message dropped!"
            );
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Write header + payload without concatenating (single ring-buffer entry).
    pub fn write_header_and_payload_nonblocking(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::HeaderPayload { header, payload },
            sequence: sequence as u64,
            permit: None,
        };

        // CRITICAL FIX: Don't silently drop messages when ring buffer is full!
        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            warn!(
                channel_id = ?self.channel_id,
                sequence = sequence,
                "Ring buffer full (header+payload) - message dropped!"
            );
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Write header + payload without concatenating, using an inline header to avoid allocations.
    pub fn write_header_and_payload_nonblocking_inline(
        &self,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::HeaderInline {
                header,
                header_len,
                payload,
            },
            sequence: sequence as u64,
            permit: None,
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            warn!(
                channel_id = ?self.channel_id,
                sequence = sequence,
                "Ring buffer full (header inline) - message dropped!"
            );
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Write Bytes to the ring buffer; returns WouldBlock if full.
    pub fn write_bytes_nonblocking_checked(&self, data: bytes::Bytes) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::Single(data),
            sequence: sequence as u64,
            permit: None,
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Write header + payload; returns WouldBlock if full.
    pub fn write_header_and_payload_nonblocking_checked(
        &self,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let sequence = self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let command = WriteCommand {
            channel_id: self.channel_id,
            data: WritePayload::HeaderPayload { header, payload },
            sequence: sequence as u64,
            permit: None,
        };

        if self.ring_buffer.try_push(command) {
            self.wake_writer_if_idle();
            Ok(())
        } else {
            Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "write buffer full",
            )))
        }
    }

    /// Flush the writer immediately - used for low-latency ask operations
    pub fn flush_immediately(&self) -> Result<()> {
        // Coalesce flush requests to avoid flooding the writer task.
        if self
            .flush_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.streaming_tx
                .send(StreamingCommand::Flush)
                .map_err(|_| {
                    self.flush_pending.store(false, Ordering::Release);
                    GossipError::Shutdown
                })
        } else {
            Ok(())
        }
    }

    /// Write data with vectored batching - still no blocking
    pub fn write_vectored_nonblocking(&self, data_chunks: &[&[u8]]) -> Result<()> {
        if data_chunks.is_empty() {
            return Ok(());
        }

        if data_chunks.len() == 1 {
            return self.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(data_chunks[0]));
        }

        let mut chunks = Vec::with_capacity(data_chunks.len());
        for chunk in data_chunks {
            chunks.push(bytes::Bytes::copy_from_slice(chunk));
        }
        self.write_vectored_nonblocking_bytes(chunks)
    }

    /// Write already-owned chunks without concatenation.
    pub fn write_vectored_nonblocking_bytes(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        if chunks.len() == 1 {
            return self.write_bytes_nonblocking(chunks[0].clone());
        }
        self.streaming_tx
            .send(StreamingCommand::OwnedChunks(chunks))
            .map_err(|_| GossipError::Shutdown)
    }

    /// Write large data in chunks to avoid blocking
    pub fn write_chunked_nonblocking(&self, data: &[u8], chunk_size: usize) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let bytes = bytes::Bytes::copy_from_slice(data);
        let mut offset = 0usize;
        while offset < bytes.len() {
            let end = (offset + chunk_size).min(bytes.len());
            let chunk = bytes.slice(offset..end);
            let _ = self.write_bytes_nonblocking(chunk);
            offset = end;
        }

        Ok(())
    }

    /// Get ring buffer status
    pub fn buffer_status(&self) -> (usize, usize) {
        let pending = self.ring_buffer.pending_count();
        (pending, self.ring_buffer.capacity - pending)
    }

    /// Check if ring buffer is near capacity
    pub fn is_buffer_full(&self) -> bool {
        self.ring_buffer.pending_count() >= (self.ring_buffer.capacity * 9 / 10)
        // 90% full
    }

    /// Get channel ID
    pub fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    /// Get total bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get sequence counter
    pub fn sequence_number(&self) -> usize {
        self.sequence_counter.load(Ordering::Relaxed)
    }

    /// Get next stream frame sequence ID (wraps at u16::MAX)
    fn next_frame_sequence_id(&self) -> u16 {
        (self.frame_sequence.fetch_add(1, Ordering::Relaxed) & 0xFFFF) as u16
    }

    /// Get socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the background writer task
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
        self.writer_notify.notify_one();
    }

    /// Get the streaming threshold for this connection
    ///
    /// Returns the threshold above which messages should be streamed rather than
    /// sent through the ring buffer. This is always derived from the buffer size.
    pub fn streaming_threshold(&self) -> usize {
        self.buffer_config.streaming_threshold()
    }

    async fn stream_large_message_zoned(
        &self,
        msg: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        correlation_id: u16,
    ) -> Result<()> {
        use crate::current_timestamp;

        const CHUNK_SIZE: usize = STREAM_CHUNK_SIZE;

        // Acquire streaming mode atomically
        while self
            .streaming_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tokio::task::yield_now().await;
        }

        // Ensure we release streaming mode on exit
        let _guard = StreamingGuard {
            flag: self.streaming_active.clone(),
        };

        // Generate unique stream ID
        let stream_id = current_timestamp();

        // Emit StreamStart descriptor (always using the zero-copy layout)
        let descriptor = streaming::StreamDescriptor {
            stream_id,
            payload_len: msg.len() as u64,
            chunk_len: STREAM_CHUNK_SIZE as u32,
            type_hash,
            actor_id,
            flags: 0,
            reserved: 0,
        };
        let header = streaming::encode_stream_start_frame(&descriptor, correlation_id);
        self.streaming_tx
            .send(StreamingCommand::WriteBytes(bytes::Bytes::copy_from_slice(
                &header,
            )))
            .map_err(|_| GossipError::Shutdown)?;

        // Stream chunks directly (zero-copy payload slices)
        let total_len = msg.len();
        let total_chunks = total_len.div_ceil(CHUNK_SIZE);
        let mut idx = 0u32;
        let mut offset = 0usize;
        let mut data_header = streaming::stream_data_header_template(correlation_id);
        while offset < total_len {
            let chunk_len = std::cmp::min(CHUNK_SIZE, total_len - offset);
            let prefix = (framing::STREAM_HEADER_PREFIX_LEN + chunk_len) as u32;
            data_header[..4].copy_from_slice(&prefix.to_be_bytes());
            let header_bytes = bytes::Bytes::copy_from_slice(&data_header);
            let payload = msg.slice(offset..offset + chunk_len);
            self.streaming_tx
                .send(StreamingCommand::VectoredWrite(VectoredWriteCommand {
                    header: header_bytes,
                    payload,
                }))
                .map_err(|_| GossipError::Shutdown)?;

            // Yield periodically to prevent blocking
            if total_chunks > 1 && idx.is_multiple_of(10) {
                self.streaming_tx
                    .send(StreamingCommand::Flush)
                    .map_err(|_| GossipError::Shutdown)?;
                tokio::task::yield_now().await;
            }
            idx += 1;
            offset += chunk_len;
        }

        let header = streaming::encode_stream_end_header(correlation_id);
        self.streaming_tx
            .send(StreamingCommand::WriteBytes(bytes::Bytes::copy_from_slice(
                &header,
            )))
            .map_err(|_| GossipError::Shutdown)?;
        self.streaming_tx
            .send(StreamingCommand::Flush)
            .map_err(|_| GossipError::Shutdown)?;

        debug!(
            "✅ STREAMING: Successfully streamed {} MB in {} chunks",
            msg.len() as f64 / 1_048_576.0,
            msg.len().div_ceil(CHUNK_SIZE)
        );

        Ok(())
    }

    async fn stream_large_message_internal_bytes(
        &self,
        msg: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        correlation_id: u16,
    ) -> Result<()> {
        self.stream_large_message_zoned(msg, type_hash, actor_id, correlation_id)
            .await
    }

    async fn stream_large_message_internal(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
        correlation_id: u16,
    ) -> Result<()> {
        self.stream_large_message_internal_bytes(
            bytes::Bytes::copy_from_slice(msg),
            type_hash,
            actor_id,
            correlation_id,
        )
        .await
    }

    /// Stream a large message directly to the socket, bypassing the ring buffer
    /// This provides maximum performance for large messages like PreBacktest
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.stream_large_message_internal(msg, type_hash, actor_id, 0)
            .await
    }

    pub async fn stream_large_message_bytes(
        &self,
        msg: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.stream_large_message_internal_bytes(msg, type_hash, actor_id, 0)
            .await
    }

    /// Stream a large ask message directly to the socket with correlation ID.
    pub async fn stream_large_message_with_correlation(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
        correlation_id: u16,
    ) -> Result<()> {
        self.stream_large_message_internal(msg, type_hash, actor_id, correlation_id)
            .await
    }

    pub async fn stream_large_message_with_correlation_bytes(
        &self,
        msg: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        correlation_id: u16,
    ) -> Result<()> {
        self.stream_large_message_internal_bytes(msg, type_hash, actor_id, correlation_id)
            .await
    }

    /// Zero-copy vectored write for header + payload in single operation
    /// This eliminates copying payload data into frame buffer - optimal for streaming
    pub fn write_bytes_vectored(&self, header: bytes::Bytes, payload: bytes::Bytes) -> Result<()> {
        // Create vectored command that preserves both header and payload as separate Bytes
        let command = VectoredWriteCommand { header, payload };

        // Try to send via streaming channel first for vectored operations.
        // No fallback to ring-buffer: streaming is explicit and must succeed or error.
        self.streaming_tx
            .send(StreamingCommand::VectoredWrite(command))
            .map_err(|_| GossipError::Shutdown)
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        // Send chunks as a batch via streaming channel for optimal vectored I/O
        let command = StreamingCommand::OwnedChunks(chunks);
        self.streaming_tx
            .send(command)
            .map_err(|_| GossipError::Shutdown)?;

        Ok(())
    }
}

/// Guard to ensure streaming_active is released on drop
struct StreamingGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl Debug for LockFreeStreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockFreeStreamHandle")
            .field("addr", &self.addr)
            .field("channel_id", &self.channel_id)
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field("sequence", &self.sequence_counter.load(Ordering::Relaxed))
            .finish()
    }
}

/// Message types for seamless batching with tell()
#[cfg_attr(
    feature = "legacy_tell_bytes",
    deprecated(
        since = "0.1.0",
        note = "TellMessage is deprecated. Switch to tell_bytes()/tell_typed() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
    )
)]
pub enum TellMessage<'a> {
    Single(&'a [u8]),
    Batch(Vec<&'a [u8]>),
    BatchSlice(&'a [&'a [u8]]),
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> TellMessage<'a> {
    pub fn single(data: &'a [u8]) -> Self {
        Self::Single(data)
    }

    pub fn batch(messages: Vec<&'a [u8]>) -> Self {
        Self::Batch(messages)
    }

    pub fn from_slice(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::BatchSlice(messages)
        }
    }

    pub async fn send_via(self, handle: &ConnectionHandle) -> Result<()> {
        match self {
            TellMessage::Single(data) => handle.tell_raw(data).await,
            TellMessage::Batch(messages) => handle.tell_batch(&messages).await,
            TellMessage::BatchSlice(messages) => handle.tell_batch(messages).await,
        }
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a [u8]> for TellMessage<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self::Single(data)
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<Vec<&'a [u8]>> for TellMessage<'a> {
    fn from(messages: Vec<&'a [u8]>) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::Batch(messages)
        }
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a [&'a [u8]]> for TellMessage<'a> {
    fn from(messages: &'a [&'a [u8]]) -> Self {
        if messages.len() == 1 {
            Self::Single(messages[0])
        } else {
            Self::BatchSlice(messages)
        }
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a, const N: usize> From<&'a [u8; N]> for TellMessage<'a> {
    fn from(data: &'a [u8; N]) -> Self {
        Self::Single(data.as_slice())
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a Vec<u8>> for TellMessage<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self::Single(data.as_slice())
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> TellMessage<'a> {
    pub fn single(_: &'a [u8]) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }

    pub fn batch(_: Vec<&'a [u8]>) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }

    pub fn from_slice(_: &'a [&'a [u8]]) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }

    pub async fn send_via(self, _: &ConnectionHandle) -> Result<()> {
        Err(GossipError::LegacyTellApiDisabled)
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a [u8]> for TellMessage<'a> {
    fn from(_: &'a [u8]) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<Vec<&'a [u8]>> for TellMessage<'a> {
    fn from(_: Vec<&'a [u8]>) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a [&'a [u8]]> for TellMessage<'a> {
    fn from(_: &'a [&'a [u8]]) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a, const N: usize> From<&'a [u8; N]> for TellMessage<'a> {
    fn from(_: &'a [u8; N]) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[cfg_attr(not(feature = "legacy_tell_bytes"), allow(deprecated))]
impl<'a> From<&'a Vec<u8>> for TellMessage<'a> {
    fn from(_: &'a Vec<u8>) -> Self {
        panic!("TellMessage requires the 'legacy_tell_bytes' feature or migration to tell_bytes()")
    }
}

#[cfg(feature = "legacy_tell_bytes")]
#[deprecated(
    since = "0.1.0",
    note = "tell_msg! macro is deprecated. Use tell_bytes()/tell_typed() or enable \
'legacy_tell_bytes_unlocked' alongside 'legacy_tell_bytes' to temporarily opt back in."
)]
#[macro_export]
macro_rules! tell_msg {
    ($single:expr) => {
        TellMessage::single($single)
    };
    ($($msg:expr),+ $(,)?) => {
        TellMessage::batch(vec![$($msg),+])
    };
}

#[cfg(not(feature = "legacy_tell_bytes"))]
#[macro_export]
macro_rules! tell_msg {
    ($($msg:expr),+ $(,)?) => {
        compile_error!("tell_msg! macro requires the 'legacy_tell_bytes' feature or migration to tell_bytes()/tell_typed() APIs");
    };
}

/// Connection pool for maintaining persistent TCP connections to peers
/// All connections are persistent - there is no checkout/checkin
/// Lock-free connection pool using atomic operations and lock-free data structures
pub struct ConnectionPool {
    /// PRIMARY: Mapping Peer ID -> LockFreeConnection
    /// This is the main storage - we identify connections by peer ID, not address
    connections_by_peer: dashmap::DashMap<crate::PeerId, Arc<LockFreeConnection>>,
    /// SECONDARY: Mapping SocketAddr -> Peer ID (for incoming connection identification)
    addr_to_peer_id: dashmap::DashMap<SocketAddr, crate::PeerId>,
    /// Configuration: Peer ID -> Expected SocketAddr (where to connect)
    pub peer_id_to_addr: dashmap::DashMap<crate::PeerId, SocketAddr>,
    /// Address-based connection index for fast lookup by SocketAddr
    connections_by_addr: dashmap::DashMap<SocketAddr, Arc<LockFreeConnection>>,
    /// Shared correlation trackers by peer ID - ensures ask/response works across bidirectional connections
    correlation_trackers: dashmap::DashMap<crate::PeerId, Arc<CorrelationTracker>>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: Option<std::sync::Weak<GossipRegistry>>,
    /// Shared message buffer pool for zero-allocation processing
    message_buffer_pool: Arc<MessageBufferPool>,
    /// Connection counter for load balancing
    connection_counter: AtomicUsize,
    /// Shared view of negotiated peer capabilities (set by registry)
    peer_capabilities:
        Option<Arc<dashmap::DashMap<SocketAddr, crate::handshake::PeerCapabilities>>>,
}

unsafe impl Send for ConnectionPool {}
unsafe impl Sync for ConnectionPool {}

/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 1024;

/// Pending response slot
struct PendingResponseSlot {
    notify: tokio::sync::Notify,
    in_use: AtomicBool,
    ready: AtomicBool,
    response: UnsafeCell<MaybeUninit<bytes::Bytes>>,
}

unsafe impl Sync for PendingResponseSlot {}

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next correlation ID to use
    next_id: AtomicU16,
    /// Fixed-size array of pending responses
    pending: [PendingResponseSlot; PENDING_RESPONSES_SIZE],
}

impl std::fmt::Debug for CorrelationTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrelationTracker")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

impl CorrelationTracker {
    fn new() -> Arc<Self> {
        let pending = std::array::from_fn(|_| PendingResponseSlot {
            notify: tokio::sync::Notify::new(),
            in_use: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            response: UnsafeCell::new(MaybeUninit::uninit()),
        });
        Arc::new(Self {
            next_id: AtomicU16::new(1),
            pending,
        })
    }

    fn clear_response(slot_ref: &PendingResponseSlot) {
        if slot_ref.ready.swap(false, Ordering::AcqRel) {
            unsafe {
                (*slot_ref.response.get()).assume_init_drop();
            }
        }
    }

    fn take_response(slot_ref: &PendingResponseSlot) -> Option<bytes::Bytes> {
        if !slot_ref.ready.swap(false, Ordering::AcqRel) {
            return None;
        }
        // Safety: writer sets ready true only after initializing response.
        Some(unsafe { (*slot_ref.response.get()).assume_init_read() })
    }

    /// Allocate a correlation ID and reserve the response slot.
    fn allocate(&self) -> u16 {
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id == 0 {
                continue; // Skip 0 as it's reserved
            }

            let slot = (id as usize) % PENDING_RESPONSES_SIZE;
            let slot_ref = &self.pending[slot];
            if !slot_ref.in_use.swap(true, Ordering::AcqRel) {
                Self::clear_response(slot_ref);
                debug!(
                    "CorrelationTracker: Allocated correlation_id {} in slot {}",
                    id, slot
                );
                return id;
            }

            // Slot is occupied, try next ID
            debug!("CorrelationTracker: Slot {} occupied, trying next ID", slot);
        }
    }

    /// Check if a correlation ID has a pending request
    pub(crate) fn has_pending(&self, correlation_id: u16) -> bool {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        self.pending[slot].in_use.load(Ordering::Acquire)
    }

    /// Complete a pending request with a response
    pub(crate) fn complete(&self, correlation_id: u16, response: bytes::Bytes) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];
        if !slot_ref.in_use.load(Ordering::Acquire) {
            return;
        }
        Self::clear_response(slot_ref);
        unsafe {
            (*slot_ref.response.get()).write(response);
        }
        slot_ref.ready.store(true, Ordering::Release);
        slot_ref.notify.notify_waiters();
    }

    /// Cancel a pending request (used when send fails).
    pub(crate) fn cancel(&self, correlation_id: u16) {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];
        slot_ref.in_use.store(false, Ordering::Release);
        Self::clear_response(slot_ref);
        slot_ref.notify.notify_waiters();
    }

    async fn wait_for_response(
        &self,
        correlation_id: u16,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let slot = (correlation_id as usize) % PENDING_RESPONSES_SIZE;
        let slot_ref = &self.pending[slot];

        loop {
            if let Some(response) = Self::take_response(slot_ref) {
                slot_ref.in_use.store(false, Ordering::Release);
                return Ok(response);
            }

            let notified = tokio::time::timeout(timeout, slot_ref.notify.notified()).await;
            match notified {
                Ok(()) => continue,
                Err(_) => {
                    slot_ref.in_use.store(false, Ordering::Release);
                    Self::clear_response(slot_ref);
                    return Err(crate::GossipError::Timeout);
                }
            }
        }
    }
}

/// Handle to send messages through a persistent connection - LOCK-FREE
#[derive(Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    // Direct lock-free stream handle - NO MUTEX!
    stream_handle: Arc<LockFreeStreamHandle>,
    // Correlation tracker for ask/response
    correlation: Arc<CorrelationTracker>,
    peer_capabilities:
        Option<Arc<dashmap::DashMap<SocketAddr, crate::handshake::PeerCapabilities>>>,
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle")
            .field("addr", &self.addr)
            .field("stream_handle", &self.stream_handle)
            .field(
                "has_capabilities",
                &self
                    .peer_capabilities
                    .as_ref()
                    .map(|_| true)
                    .unwrap_or(false),
            )
            .finish()
    }
}

/// Delegated reply sender for asynchronous response handling
/// Allows passing around the ability to reply to a request without blocking the original caller
pub struct DelegatedReplySender {
    sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
    receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
    request_len: usize,
    timeout: Option<Duration>,
    created_at: std::time::Instant,
}

impl DelegatedReplySender {
    /// Create a new delegated reply sender
    pub fn new(
        sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
        receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
        request_len: usize,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: None,
            created_at: std::time::Instant::now(),
        }
    }

    /// Create a new delegated reply sender with timeout
    pub fn new_with_timeout(
        sender: tokio::sync::oneshot::Sender<bytes::Bytes>,
        receiver: tokio::sync::oneshot::Receiver<bytes::Bytes>,
        request_len: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            sender,
            receiver,
            request_len,
            timeout: Some(timeout),
            created_at: std::time::Instant::now(),
        }
    }

    /// Send a reply using this delegated sender
    /// This can be called from anywhere in the code to complete the request-response cycle
    pub fn reply(self, response: bytes::Bytes) -> Result<()> {
        match self.sender.send(response) {
            Ok(()) => Ok(()),
            Err(_) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply receiver was dropped",
            ))),
        }
    }

    /// Send a reply with error
    pub fn reply_error(self, error: &str) -> Result<()> {
        let error_response = bytes::Bytes::from(format!("ERROR:{}", error).into_bytes());
        self.reply(error_response)
    }

    /// Wait for the reply with optional timeout
    pub async fn wait_for_reply(self) -> Result<bytes::Bytes> {
        if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, self.receiver).await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
                Err(_) => Err(GossipError::Timeout),
            }
        } else {
            match self.receiver.await {
                Ok(response) => Ok(response),
                Err(_) => Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Reply sender was dropped",
                ))),
            }
        }
    }

    /// Wait for the reply with a custom timeout
    pub async fn wait_for_reply_with_timeout(self, timeout: Duration) -> Result<bytes::Bytes> {
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Reply sender was dropped",
            ))),
            Err(_) => Err(GossipError::Timeout),
        }
    }

    /// Get the original request length (useful for creating mock responses)
    pub fn request_len(&self) -> usize {
        self.request_len
    }

    /// Get the elapsed time since the request was made
    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Check if this reply sender has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.created_at.elapsed() > timeout
        } else {
            false
        }
    }

    /// Create a mock reply for testing (simulates the original ask() behavior)
    pub fn create_mock_reply(&self) -> bytes::Bytes {
        bytes::Bytes::from(format!("RESPONSE:{}", self.request_len).into_bytes())
    }
}

impl std::fmt::Debug for DelegatedReplySender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegatedReplySender")
            .field("request_len", &self.request_len)
            .field("timeout", &self.timeout)
            .field("elapsed", &self.elapsed())
            .field("is_timed_out", &self.is_timed_out())
            .finish()
    }
}

impl ConnectionHandle {
    fn peer_supports_streaming(&self) -> bool {
        self.peer_capabilities
            .as_ref()
            .and_then(|caps| caps.get(&self.addr))
            .map(|entry| entry.value().supports_streaming())
            .unwrap_or(true)
    }

    fn ensure_streaming(&self) -> Result<()> {
        if self.peer_supports_streaming() {
            Ok(())
        } else {
            Err(GossipError::InvalidStreamFrame(
                "Peer does not support streaming capability".into(),
            ))
        }
    }

    /// Send pre-serialized data through this connection - LOCK-FREE
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        self.stream_handle
            .write_bytes_control(bytes::Bytes::from(data))
            .await
    }

    /// Send raw bytes without any framing - used by ReplyTo
    #[cfg(feature = "legacy_tell_bytes")]
    pub async fn send_raw_bytes(&self, data: &[u8]) -> Result<()> {
        self.stream_handle
            .write_bytes_control(bytes::Bytes::copy_from_slice(data))
            .await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "send_raw_bytes() is disabled; use tell_bytes()/tell_typed() or enable 'legacy_tell_bytes'"
    )]
    pub async fn send_raw_bytes(&self, data: &[u8]) -> Result<()> {
        let _ = data;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Send a response payload with framing, without copying the payload.
    pub async fn send_response_bytes(
        &self,
        correlation_id: u16,
        payload: bytes::Bytes,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload.len(),
        );

        self.stream_handle
            .write_header_and_payload_control_inline(header, 16, payload)
            .await
    }

    /// Send a response payload using a Buf without copying.
    pub async fn send_response_buf<B>(
        &self,
        correlation_id: u16,
        payload: B,
        payload_len: usize,
    ) -> Result<()>
    where
        B: Buf + Send + 'static,
    {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        self.stream_handle
            .write_header_and_payload_control_inline_buf(header, 16, payload)
            .await
    }

    /// Send a response payload using a pooled payload without dynamic dispatch.
    pub async fn send_response_pooled(
        &self,
        correlation_id: u16,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        payload_len: usize,
    ) -> Result<()> {
        let header = framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            payload_len,
        );
        let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
        self.stream_handle
            .write_pooled_control_inline(header, 16, prefix, prefix_len, payload)
            .await
    }

    /// Send bytes without copying - TRUE ZERO-COPY
    pub async fn send_bytes_zero_copy(&self, data: bytes::Bytes) -> Result<()> {
        self.stream_handle.write_bytes_control(data).await
    }

    /// Stream a large message directly - MAXIMUM PERFORMANCE
    pub async fn stream_large_message(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
    ) -> Result<()> {
        self.ensure_streaming()?;
        self.stream_handle
            .stream_large_message(msg, type_hash, actor_id)
            .await
    }

    /// Stream a large ask message directly and wait for response (lock-free).
    pub async fn ask_streaming(
        &self,
        msg: &[u8],
        type_hash: u32,
        actor_id: u64,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        self.ensure_streaming()?;
        let correlation_id = self.correlation.allocate();
        if let Err(e) = self
            .stream_handle
            .stream_large_message_with_correlation(msg, type_hash, actor_id, correlation_id)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Stream a large ask message using owned Bytes (avoids payload copy).
    pub async fn ask_streaming_bytes(
        &self,
        msg: bytes::Bytes,
        type_hash: u32,
        actor_id: u64,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        self.ensure_streaming()?;
        let correlation_id = self.correlation.allocate();
        if let Err(e) = self
            .stream_handle
            .stream_large_message_with_correlation_bytes(msg, type_hash, actor_id, correlation_id)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Get the streaming threshold for this connection
    ///
    /// Messages larger than this threshold should be sent via streaming
    /// rather than through the ring buffer to prevent message loss.
    pub fn streaming_threshold(&self) -> usize {
        self.stream_handle.streaming_threshold()
    }

    /// Raw tell() - LOCK-FREE write (used internally)
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell_raw(&self, data: &[u8]) -> Result<()> {
        let mut message = bytes::BytesMut::with_capacity(4 + data.len());
        message.extend_from_slice(&(data.len() as u32).to_be_bytes());
        message.extend_from_slice(data);

        self.stream_handle
            .write_bytes_control(message.freeze())
            .await
    }

    /// Raw tell() - disabled without `legacy_tell_bytes`
    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "tell_raw() is disabled; enable the 'legacy_tell_bytes' feature or switch to tell_bytes()/tell_typed()"
    )]
    pub async fn tell_raw(&self, data: &[u8]) -> Result<()> {
        let _ = data;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Tell using owned bytes to avoid payload copies.
    pub async fn tell_bytes(&self, data: bytes::Bytes) -> Result<()> {
        let mut header = [0u8; 16]; // Changed from 8 to 16 for alignment
        header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());

        self.stream_handle
            .write_header_and_payload_control_inline(header, 4, data)
            .await
    }

    /// Tell multiple messages using owned bytes (zero copy per entry).
    pub async fn tell_bytes_batch(&self, messages: &[bytes::Bytes]) -> Result<()> {
        for msg in messages {
            self.tell_bytes(msg.clone()).await?;
        }
        Ok(())
    }

    async fn send_typed_payload<T>(&self, payload: crate::typed::PooledPayload) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<T>(payload);
        let (prefix_padded, prefix_len) = crate::typed::pad_type_hash_prefix(prefix);
        let mut header = [0u8; 16]; // Changed from 8 to 16 for alignment
        header[..4].copy_from_slice(&(payload_len as u32).to_be_bytes());

        #[cfg(any(test, feature = "test-helpers", debug_assertions))]
        {
            if typed_tell_capture_enabled() {
                let mut capture = bytes::BytesMut::with_capacity(prefix_len as usize + payload_len);
                if let Some(prefix) = prefix_padded {
                    capture.extend_from_slice(&prefix[..prefix_len as usize]);
                }
                capture.extend_from_slice(payload.as_bytes());
                crate::test_helpers::record_raw_payload(capture.freeze());
            }
        }

        self.stream_handle
            .write_pooled_control_inline(header, 4, prefix_padded, prefix_len, payload)
            .await
    }

    /// Tell with typed payload (rkyv) and debug-only type hash verification.
    pub async fn tell_typed<T>(&self, value: &T) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let payload = crate::typed::encode_typed_pooled(value)?;
        self.send_typed_payload::<T>(payload).await
    }

    /// Tell multiple typed payloads (each sent zero copy via pooled buffers).
    pub async fn tell_typed_batch<T>(&self, values: &[T]) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        let batch = crate::typed::TypedBatch::from_slice(values)?;
        self.tell_typed_pooled_batch(batch).await
    }

    /// Tell multiple pre-encoded typed payloads without extra copies.
    pub async fn tell_typed_pooled_batch<T>(&self, batch: crate::typed::TypedBatch<T>) -> Result<()>
    where
        T: crate::typed::WireEncode,
    {
        for payload in batch {
            self.send_typed_payload::<T>(payload).await?;
        }
        Ok(())
    }

    /// Send a pre-formatted binary message (already has length prefix)
    pub async fn send_binary_message(&self, message: &[u8]) -> Result<()> {
        // Message already has length prefix, send as-is
        self.stream_handle
            .write_bytes_control(bytes::Bytes::copy_from_slice(message))
            .await
    }

    /// Smart tell() - accepts TellMessage with automatic batch detection
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell<'a, T: Into<TellMessage<'a>>>(&self, message: T) -> Result<()> {
        let tell_message = message.into();
        tell_message.send_via(self).await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "tell() is disabled; use tell_bytes()/tell_typed() or enable 'legacy_tell_bytes'"
    )]
    pub async fn tell<'a, T: Into<TellMessage<'a>>>(&self, message: T) -> Result<()> {
        let _ = message.into();
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Smart tell() - automatically uses batching for Vec<T>
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell_smart<T: AsRef<[u8]>>(&self, payload: &[T]) -> Result<()> {
        if payload.len() == 1 {
            self.tell(payload[0].as_ref()).await
        } else {
            let batch: Vec<&[u8]> = payload.iter().map(|item| item.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(note = "tell_smart() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'")]
    pub async fn tell_smart<T: AsRef<[u8]>>(&self, payload: &[T]) -> Result<()> {
        let _ = payload;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Tell with automatic batching detection
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell_auto(&self, data: &[u8]) -> Result<()> {
        self.tell(data).await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(note = "tell_auto() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'")]
    pub async fn tell_auto(&self, data: &[u8]) -> Result<()> {
        let _ = data;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Tell multiple messages with a single call
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell_many<T: AsRef<[u8]>>(&self, messages: &[T]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() == 1 {
            self.tell(messages[0].as_ref()).await
        } else {
            let batch: Vec<&[u8]> = messages.iter().map(|msg| msg.as_ref()).collect();
            self.tell_batch(&batch).await
        }
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(note = "tell_many() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'")]
    pub async fn tell_many<T: AsRef<[u8]>>(&self, messages: &[T]) -> Result<()> {
        let _ = messages;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Send a TellMessage (single or batch) - same as tell() but explicit
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn send_tell_message(&self, message: TellMessage<'_>) -> Result<()> {
        message.send_via(self).await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "send_tell_message() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'"
    )]
    pub async fn send_tell_message(&self, _message: TellMessage<'_>) -> Result<()> {
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Universal send() - detects single vs multiple messages automatically
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn send_messages(&self, messages: &[&[u8]]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        if messages.len() == 1 {
            self.tell_raw(messages[0]).await
        } else {
            self.tell_batch(messages).await
        }
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "send_messages() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'"
    )]
    pub async fn send_messages(&self, messages: &[&[u8]]) -> Result<()> {
        let _ = messages;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Batch tell() for multiple messages - LOCK-FREE
    #[cfg(feature = "legacy_tell_bytes")]
    #[cfg_attr(
        feature = "legacy_tell_bytes",
        deprecated(
            since = "0.1.0",
            note = "Legacy tell() bytes path is deprecated. Use tell_typed()/tell_bytes() \
or enable both 'legacy_tell_bytes' and 'legacy_tell_bytes_unlocked' to temporarily opt back in."
        )
    )]
    pub async fn tell_batch(&self, messages: &[&[u8]]) -> Result<()> {
        let total_size: usize = messages.iter().map(|m| 4 + m.len()).sum();
        let mut batch_buffer = bytes::BytesMut::with_capacity(total_size);

        for msg in messages {
            batch_buffer.extend_from_slice(&(msg.len() as u32).to_be_bytes());
            batch_buffer.extend_from_slice(msg);
        }

        self.stream_handle
            .write_bytes_control(batch_buffer.freeze())
            .await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(note = "tell_batch() is disabled; use tell_bytes() or enable 'legacy_tell_bytes'")]
    pub async fn tell_batch(&self, messages: &[&[u8]]) -> Result<()> {
        let _ = messages;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Direct access to try_send for maximum performance testing
    pub fn try_send_direct(&self, _data: &[u8]) -> Result<()> {
        // Direct TCP doesn't support try_send - would need try_lock
        Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "use tell() for direct TCP writes",
        )))
    }

    /// Send raw bytes through existing connection (zero-copy where possible)
    #[cfg(feature = "legacy_tell_bytes")]
    pub async fn send_raw(&self, data: &[u8]) -> Result<()> {
        self.tell_raw(data).await
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "send_raw() is disabled; use tell_bytes()/tell_typed() or enable 'legacy_tell_bytes'"
    )]
    pub async fn send_raw(&self, data: &[u8]) -> Result<()> {
        let _ = data;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Ask using owned bytes to avoid payload copies. Returns response as Bytes.
    pub async fn ask(&self, request: bytes::Bytes) -> Result<bytes::Bytes> {
        self.ask_with_timeout(request, Duration::from_secs(30))
            .await
    }

    /// Ask with typed request and return a zero-copy archived response.
    pub async fn ask_typed_archived<Req, Resp>(
        &self,
        request: &Req,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let (prefix_padded, prefix_len) = crate::typed::pad_type_hash_prefix(prefix);
        let response = self
            .ask_with_timeout_pooled(
                payload,
                prefix_padded,
                prefix_len,
                payload_len,
                Duration::from_secs(30),
            )
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    /// Ask with typed request and custom timeout, returning a zero-copy archived response.
    pub async fn ask_typed_archived_with_timeout<Req, Resp>(
        &self,
        request: &Req,
        timeout: Duration,
    ) -> Result<crate::typed::ArchivedBytes<Resp>>
    where
        Req: crate::typed::WireEncode,
        Resp: crate::typed::WireType + rkyv::Archive,
        for<'a> Resp::Archived: rkyv::Portable
            + rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let payload = crate::typed::encode_typed_pooled(request)?;
        let (payload, prefix, payload_len) = crate::typed::typed_payload_parts::<Req>(payload);
        let (prefix_padded, prefix_len) = crate::typed::pad_type_hash_prefix(prefix);
        let response = self
            .ask_with_timeout_pooled(payload, prefix_padded, prefix_len, payload_len, timeout)
            .await?;
        crate::typed::decode_typed_archived::<Resp>(response)
    }

    async fn ask_with_timeout_pooled(
        &self,
        payload: crate::typed::PooledPayload,
        prefix: Option<[u8; 16]>,
        prefix_len: u8,
        payload_len: usize,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();
        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            payload_len,
        );
        if let Err(e) = self
            .stream_handle
            .write_pooled_ask_inline(header, 16, prefix, prefix_len, payload)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Ask using owned bytes and custom timeout. Returns response as Bytes.
    pub async fn ask_with_timeout(
        &self,
        request: bytes::Bytes,
        timeout: Duration,
    ) -> Result<bytes::Bytes> {
        let correlation_id = self.correlation.allocate();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

        if let Err(e) = self
            .stream_handle
            .write_header_and_payload_ask_inline(header, 16, request)
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        self.correlation
            .wait_for_response(correlation_id, timeout)
            .await
    }

    /// Ask method that returns a ReplyTo handle for delegated replies
    pub async fn ask_with_reply_to(&self, request: &[u8]) -> Result<crate::ReplyTo> {
        // Allocate correlation ID
        let correlation_id = self.correlation.allocate();

        let header = framing::write_ask_response_header(
            crate::MessageType::Ask,
            correlation_id,
            request.len(),
        );

        if let Err(e) = self
            .stream_handle
            .write_header_and_payload_ask_inline(header, 16, bytes::Bytes::copy_from_slice(request))
            .await
        {
            self.correlation.cancel(correlation_id);
            return Err(e);
        }

        // Return ReplyTo handle
        Ok(crate::ReplyTo {
            correlation_id,
            connection: Arc::new(self.clone()),
        })
    }

    /// Ask method with delegated reply sender for asynchronous response handling
    /// Returns a DelegatedReplySender that can be passed around to handle the response elsewhere
    #[cfg(feature = "legacy_tell_bytes")]
    pub async fn ask_with_reply_sender(&self, request: &[u8]) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel::<bytes::Bytes>();

        // Send the request (in a real implementation, this would include correlation ID)
        self.tell(request).await?;

        // Return the delegated reply sender
        Ok(DelegatedReplySender::new(
            reply_sender,
            reply_receiver,
            request.len(),
        ))
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "ask_with_reply_sender() is disabled; use ask()/ask_typed_* or enable 'legacy_tell_bytes'"
    )]
    pub async fn ask_with_reply_sender(&self, request: &[u8]) -> Result<DelegatedReplySender> {
        let _ = request;
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Ask method with timeout and delegated reply
    #[cfg(feature = "legacy_tell_bytes")]
    pub async fn ask_with_timeout_and_reply(
        &self,
        request: &[u8],
        timeout: Duration,
    ) -> Result<DelegatedReplySender> {
        // Create a oneshot channel for the response
        let (reply_sender, reply_receiver) = tokio::sync::oneshot::channel::<bytes::Bytes>();

        // Send the request
        self.tell(request).await?;

        // Return the delegated reply sender with timeout
        Ok(DelegatedReplySender::new_with_timeout(
            reply_sender,
            reply_receiver,
            request.len(),
            timeout,
        ))
    }

    #[cfg(not(feature = "legacy_tell_bytes"))]
    #[deprecated(
        note = "ask_with_timeout_and_reply() is disabled; use ask()/ask_typed_* or enable 'legacy_tell_bytes'"
    )]
    pub async fn ask_with_timeout_and_reply(
        &self,
        request: &[u8],
        timeout: Duration,
    ) -> Result<DelegatedReplySender> {
        let _ = (request, timeout);
        Err(GossipError::LegacyTellApiDisabled)
    }

    /// Batch ask method for multiple requests in a single network round-trip
    /// Returns a vector of response futures that can be awaited independently
    pub async fn ask_batch(
        &self,
        requests: &[&[u8]],
    ) -> Result<Vec<tokio::sync::oneshot::Receiver<bytes::Bytes>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut receivers = Vec::with_capacity(requests.len());
        let mut pending = Vec::with_capacity(requests.len());
        let mut correlation_ids = Vec::with_capacity(requests.len());
        let mut batch_message = Vec::new();

        // Process each request
        for request in requests {
            // Create oneshot channel for this response
            let (tx, rx) = tokio::sync::oneshot::channel::<bytes::Bytes>();
            receivers.push(rx);

            // Allocate correlation ID
            let correlation_id = self.correlation.allocate();
            correlation_ids.push(correlation_id);
            pending.push((correlation_id, tx));

            // Build ask message: [type:1][correlation_id:2][pad:1] + payload
            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                correlation_id,
                request.len(),
            );
            batch_message.extend_from_slice(&header);
            batch_message.extend_from_slice(request);
        }

        if let Err(e) = self
            .stream_handle
            .write_bytes_ask(bytes::Bytes::from(batch_message))
            .await
        {
            for correlation_id in correlation_ids {
                self.correlation.cancel(correlation_id);
            }
            return Err(e);
        }

        let correlation = self.correlation.clone();
        for (correlation_id, tx) in pending {
            let correlation = correlation.clone();
            tokio::spawn(async move {
                match correlation
                    .wait_for_response(correlation_id, Duration::from_secs(30))
                    .await
                {
                    Ok(response) => {
                        let _ = tx.send(response);
                    }
                    Err(_) => {
                        // Drop sender to signal failure to receiver.
                    }
                }
            });
        }

        Ok(receivers)
    }

    /// Batch ask with timeout - returns Vec<Result<Vec<u8>>> with individual timeout handling
    pub async fn ask_batch_with_timeout(
        &self,
        requests: &[&[u8]],
        timeout: Duration,
    ) -> Result<Vec<Result<bytes::Bytes>>> {
        let receivers = self.ask_batch(requests).await?;

        // Create futures for all responses with timeout
        let mut response_futures = Vec::with_capacity(receivers.len());

        for receiver in receivers {
            let timeout_future = async move {
                match tokio::time::timeout(timeout, receiver).await {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(_)) => Err(crate::GossipError::Network(std::io::Error::other(
                        "Response channel closed",
                    ))),
                    Err(_) => Err(crate::GossipError::Timeout),
                }
            };
            response_futures.push(timeout_future);
        }

        // Wait for all responses concurrently
        let results = futures::future::join_all(response_futures).await;
        Ok(results)
    }

    /// High-performance batch ask with pre-allocated buffers
    /// This version minimizes allocations for maximum throughput
    pub async fn ask_batch_optimized(
        &self,
        requests: &[&[u8]],
        response_buffer: &mut Vec<tokio::sync::oneshot::Receiver<bytes::Bytes>>,
    ) -> Result<()> {
        response_buffer.clear();
        response_buffer.reserve(requests.len());

        // Pre-calculate total message size
        let total_size: usize = requests
            .iter()
            .map(|req| framing::ASK_RESPONSE_FRAME_HEADER_LEN + req.len())
            .sum();

        let mut batch_message = bytes::BytesMut::with_capacity(total_size);
        let mut pending = Vec::with_capacity(requests.len());
        let mut correlation_ids = Vec::with_capacity(requests.len());

        // Build all messages
        for request in requests {
            // Create oneshot channel for this response
            let (tx, rx) = tokio::sync::oneshot::channel::<bytes::Bytes>();
            response_buffer.push(rx);

            // Allocate correlation ID
            let correlation_id = self.correlation.allocate();
            correlation_ids.push(correlation_id);
            pending.push((correlation_id, tx));

            // Build ask message
            let header = framing::write_ask_response_header(
                crate::MessageType::Ask,
                correlation_id,
                request.len(),
            );
            batch_message.extend_from_slice(&header);
            batch_message.extend_from_slice(request);
        }

        if let Err(e) = self
            .stream_handle
            .write_bytes_ask(batch_message.freeze())
            .await
        {
            for correlation_id in correlation_ids {
                self.correlation.cancel(correlation_id);
            }
            return Err(e);
        }

        let correlation = self.correlation.clone();
        for (correlation_id, tx) in pending {
            let correlation = correlation.clone();
            tokio::spawn(async move {
                match correlation
                    .wait_for_response(correlation_id, Duration::from_secs(30))
                    .await
                {
                    Ok(response) => {
                        let _ = tx.send(response);
                    }
                    Err(_) => {
                        // Drop sender to signal failure to receiver.
                    }
                }
            });
        }

        Ok(())
    }

    /// High-performance streaming API - send structured data with custom framing - LOCK-FREE
    pub async fn stream_send<T>(&self, data: &T) -> Result<()>
    where
        T: for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        // Serialize the data using rkyv for maximum performance
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(data)
            .map_err(crate::GossipError::Serialization)?;
        let payload = bytes::Bytes::from(payload.into_boxed_slice());

        // Create stream frame: [frame_type, channel_id, flags, seq_id[2], payload_len[4]]
        let frame_header = StreamFrameHeader {
            frame_type: StreamFrameType::Data as u8,
            channel_id: ChannelId::TellAsk as u8,
            flags: 0,
            sequence_id: self.stream_handle.next_frame_sequence_id(),
            payload_len: payload.len() as u32,
        };

        let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
            .map_err(crate::GossipError::Serialization)?;
        let header_bytes = bytes::Bytes::from(header_bytes.into_boxed_slice());

        // Use lock-free ring buffer without concatenating header+payload.
        self.stream_handle
            .write_header_and_payload_nonblocking(header_bytes, payload)
    }

    /// High-performance streaming API - send batch of structured data - LOCK-FREE
    pub async fn stream_send_batch<T>(&self, batch: &[T]) -> Result<()>
    where
        T: for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        if batch.is_empty() {
            return Ok(());
        }

        // Pre-allocate buffer for entire batch
        let mut total_payload = Vec::new();

        for item in batch {
            let payload = rkyv::to_bytes::<rkyv::rancor::Error>(item)
                .map_err(crate::GossipError::Serialization)?;

            let frame_header = StreamFrameHeader {
                frame_type: StreamFrameType::Data as u8,
                channel_id: ChannelId::TellAsk as u8,
                flags: if std::ptr::eq(item, batch.last().unwrap()) {
                    0
                } else {
                    StreamFrameFlags::More as u8
                },
                sequence_id: self.stream_handle.next_frame_sequence_id(),
                payload_len: payload.len() as u32,
            };

            let header_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&frame_header)
                .map_err(crate::GossipError::Serialization)?;
            total_payload.extend_from_slice(&header_bytes);
            total_payload.extend_from_slice(&payload);
        }

        // Use lock-free ring buffer for entire batch - NO MUTEX!
        self.stream_handle
            .write_bytes_nonblocking(bytes::Bytes::from(total_payload))
    }

    /// Get truly lock-free streaming handle - direct access to the internal handle
    pub fn get_lock_free_stream(&self) -> &Arc<LockFreeStreamHandle> {
        &self.stream_handle
    }

    /// Zero-copy vectored write for header + payload in single syscall
    /// This eliminates the need to copy payload data into frame buffer
    pub fn write_bytes_vectored(&self, header: bytes::Bytes, payload: bytes::Bytes) -> Result<()> {
        self.stream_handle.write_bytes_vectored(header, payload)
    }

    /// Send owned chunks without copying - optimal for streaming large messages
    pub fn write_owned_chunks(&self, chunks: Vec<bytes::Bytes>) -> Result<()> {
        self.stream_handle.write_owned_chunks(chunks)
    }
}

impl ConnectionPool {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        const POOL_SIZE: usize = 256;
        const BUFFER_SIZE: usize = TCP_BUFFER_SIZE / 128; // Smaller pool buffers (8KB default)

        let pool = Self {
            connections_by_peer: dashmap::DashMap::new(),
            addr_to_peer_id: dashmap::DashMap::new(),
            peer_id_to_addr: dashmap::DashMap::new(),
            connections_by_addr: dashmap::DashMap::new(),
            correlation_trackers: dashmap::DashMap::new(),
            max_connections,
            connection_timeout,
            registry: None,
            message_buffer_pool: Arc::new(MessageBufferPool::new(POOL_SIZE, BUFFER_SIZE)),
            connection_counter: AtomicUsize::new(0),
            peer_capabilities: None,
        };

        // Log the pool's address for debugging
        debug!(
            "CONNECTION POOL: Created new pool at {:p}",
            &pool as *const _
        );
        pool
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&mut self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry = Some(std::sync::Arc::downgrade(&registry));
        self.peer_capabilities = Some(registry.peer_capabilities.clone());
    }

    fn clear_capabilities_for_addr(&self, addr: &SocketAddr) {
        if let Some(registry) = self.registry.as_ref().and_then(|w| w.upgrade()) {
            registry.clear_peer_capabilities(addr);
        }
    }

    /// Store or update the address for a peer
    /// Only updates if no address is already configured for this peer
    pub fn update_node_address(&self, peer_id: &crate::PeerId, addr: SocketAddr) {
        // Check if we already have a configured address for this node
        if let Some(existing_addr_entry) = self.peer_id_to_addr.get(peer_id) {
            let existing_addr = *existing_addr_entry.value();
            debug!("CONNECTION POOL: Node {} already has configured address {}, not updating to ephemeral port {}",
                   peer_id, existing_addr, addr);
            return;
        }

        // Only update if no address is configured
        self.peer_id_to_addr.insert(peer_id.clone(), addr);
        self.addr_to_peer_id.insert(addr, peer_id.clone());
        debug!(
            "CONNECTION POOL: Set initial address for node {} to {}",
            peer_id, addr
        );
    }

    /// Reindex an existing connection under a new logical address for the peer.
    ///
    /// This is needed when a peer connects FROM an ephemeral TCP port but advertises
    /// a different bind address in gossip. We need to update `connections_by_addr` so
    /// that lookups by the advertised address find the connection.
    pub fn reindex_connection_addr(&self, peer_id: &crate::PeerId, new_addr: SocketAddr) {
        // First, check if this peer still has an active connection
        // This guards against race conditions where disconnect happens between checks
        let connection = match self.connections_by_peer.get(peer_id) {
            Some(entry) => entry.value().clone(),
            None => {
                // Peer was disconnected, nothing to reindex
                return;
            }
        };

        // Check if new_addr is already indexed
        if let Some(existing_peer_id) = self.addr_to_peer_id.get(&new_addr) {
            if existing_peer_id.value() == peer_id {
                // Already indexed under the advertised address for this peer.
                // But we still need to ensure the OLD (ephemeral) address is indexed too!
                // Without this, lookups by ephemeral address fail after gossip rounds.
                let old_addr = connection.addr;
                if old_addr != new_addr && !self.connections_by_addr.contains_key(&old_addr) {
                    self.connections_by_addr.insert(old_addr, connection);
                    self.addr_to_peer_id.insert(old_addr, peer_id.clone());
                    debug!(
                        old_addr = %old_addr,
                        new_addr = %new_addr,
                        peer_id = %peer_id,
                        "📍 Added missing ephemeral address mapping"
                    );
                }
                return;
            } else {
                // Stale entry from different peer - remove it before reindexing
                // This can happen if an old connection wasn't fully cleaned up
                warn!(
                    "CONNECTION POOL: Removing stale address mapping {} (was peer {}, now peer {})",
                    new_addr,
                    existing_peer_id.value(),
                    peer_id
                );
                self.connections_by_addr.remove(&new_addr);
                self.addr_to_peer_id.remove(&new_addr);
            }
        }

        let old_addr = connection.addr;

        // Double-check peer still exists (guard against concurrent disconnect)
        if !self.connections_by_peer.contains_key(peer_id) {
            return;
        }

        // Insert the connection under the new (advertised) address
        self.connections_by_addr
            .insert(new_addr, connection.clone());
        self.addr_to_peer_id.insert(new_addr, peer_id.clone());
        // Also update peer_id_to_addr so disconnect uses the correct address
        self.peer_id_to_addr.insert(peer_id.clone(), new_addr);

        // IMPORTANT: Keep the old (ephemeral) address entry as well!
        // Inbound messages still arrive with the TCP source address (old_addr),
        // so we need both addresses to point to the same connection.
        // The old entry is NOT removed - both addresses are valid for this peer.
        if old_addr != new_addr {
            // Re-insert connection under old addr to ensure both addresses work
            self.connections_by_addr.insert(old_addr, connection);
            // Keep addr_to_peer_id for old_addr so lookups work
            self.addr_to_peer_id.insert(old_addr, peer_id.clone());
        }

        info!(
            old_addr = %old_addr,
            new_addr = %new_addr,
            peer_id = %peer_id,
            "📍 Reindexed connection from ephemeral port to bind address"
        );
    }

    /// Get a connection by peer ID
    pub fn get_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        // PRIMARY: Look up connection directly by peer ID
        if let Some(conn_entry) = self.connections_by_peer.get(peer_id) {
            let conn = conn_entry.value().clone();
            if conn.is_connected() {
                debug!("CONNECTION POOL: Found connection for peer '{}'", peer_id);
                return Some(conn);
            }
            warn!(
                "CONNECTION POOL: Connection for peer '{}' is disconnected",
                peer_id
            );
        }

        // FALLBACK: Outbound connections may only be indexed by address.
        // Look up the address via peer_id_to_addr, then get the connection by address.
        if let Some(addr) = self.peer_id_to_addr.get(peer_id).map(|e| *e.value()) {
            if let Some(conn_entry) = self.connections_by_addr.get(&addr) {
                let conn = conn_entry.value().clone();
                if conn.is_connected() {
                    debug!(
                        "CONNECTION POOL: Found connection for peer '{}' via address fallback ({})",
                        peer_id, addr
                    );
                    // Index by peer_id for future lookups
                    self.connections_by_peer
                        .insert(peer_id.clone(), conn.clone());
                    return Some(conn);
                }
            }
        }

        warn!(
            "CONNECTION POOL: No connection found for peer '{}'",
            peer_id
        );
        // Debug: show what nodes we do have connections for
        let connected_nodes: Vec<String> = self
            .connections_by_peer
            .iter()
            .map(|entry| entry.key().to_hex())
            .collect();
        warn!(
            "CONNECTION POOL: Available node connections: {:?}",
            connected_nodes
        );
        None
    }

    /// Get a connection by socket address
    pub fn get_connection_by_addr(&self, addr: &SocketAddr) -> Option<Arc<LockFreeConnection>> {
        self.connections_by_addr.get(addr).and_then(|entry| {
            let conn = entry.value().clone();
            if conn.is_connected() {
                Some(conn)
            } else {
                None
            }
        })
    }

    /// Get the peer ID for a given socket address
    pub fn get_peer_id_by_addr(&self, addr: &SocketAddr) -> Option<crate::PeerId> {
        self.addr_to_peer_id.get(addr).map(|e| e.value().clone())
    }

    /// Add an additional address mapping for a peer ID.
    /// Used when a peer connects from an ephemeral port that differs from their bind address.
    pub fn add_addr_to_peer_id(&self, addr: SocketAddr, peer_id: crate::PeerId) {
        debug!(
            "CONNECTION POOL: Adding additional address {} -> peer_id {}",
            addr, peer_id
        );
        self.addr_to_peer_id.insert(addr, peer_id);
    }

    /// Get the shared correlation tracker for a peer ID
    pub(crate) fn get_shared_correlation_tracker(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<CorrelationTracker>> {
        self.correlation_trackers
            .get(peer_id)
            .map(|e| e.value().clone())
    }

    /// Get or create a correlation tracker for a peer
    pub(crate) fn get_or_create_correlation_tracker(
        &self,
        peer_id: &crate::PeerId,
    ) -> Arc<CorrelationTracker> {
        let tracker = self
            .correlation_trackers
            .entry(peer_id.clone())
            .or_insert_with(|| {
                debug!(
                    "CONNECTION POOL: Creating new correlation tracker for peer {}",
                    peer_id
                );
                CorrelationTracker::new()
            })
            .clone();
        debug!(
            "CONNECTION POOL: Got correlation tracker for peer {} (total trackers: {})",
            peer_id,
            self.correlation_trackers.len()
        );
        tracker
    }

    /// Add a connection indexed by peer ID
    pub fn add_connection_by_peer_id(
        &self,
        peer_id: crate::PeerId,
        addr: SocketAddr,
        mut connection: Arc<LockFreeConnection>,
    ) -> bool {
        // Only set correlation tracker if the connection doesn't already have one
        if connection.correlation.is_none() {
            // Get or create shared correlation tracker for this peer
            let correlation_tracker = self.get_or_create_correlation_tracker(&peer_id);

            // Set the correlation tracker on the connection
            // We need to make the connection mutable
            if let Some(conn_mut) = Arc::get_mut(&mut connection) {
                conn_mut.correlation = Some(correlation_tracker);
            } else {
                warn!(
                    "CONNECTION POOL: Cannot set correlation tracker - Arc has multiple references"
                );
            }
        } else {
            // Connection already has a correlation tracker - ensure it's registered
            if let Some(ref correlation) = connection.correlation {
                self.correlation_trackers
                    .insert(peer_id.clone(), correlation.clone());
                debug!(
                    "CONNECTION POOL: Registered existing correlation tracker for peer '{}'",
                    peer_id
                );
            }
        }

        // Update the address mappings
        self.addr_to_peer_id.insert(addr, peer_id.clone());

        debug!(
            "CONNECTION POOL: Added connection for peer '{}' (address: {})",
            peer_id, addr
        );

        // PRIMARY: Store the connection by peer ID
        self.connections_by_peer.insert(peer_id, connection.clone());

        // Also index by address for direct lookups
        self.connections_by_addr.insert(addr, connection);

        self.connection_counter.fetch_add(1, Ordering::AcqRel);
        true
    }

    /// Index an existing connection by an additional address.
    ///
    /// This is useful for incoming connections where the ephemeral TCP address
    /// differs from the peer's configured bind address. By indexing both addresses,
    /// response delivery can find the connection by the ephemeral address.
    pub fn index_connection_by_addr(&self, addr: SocketAddr, connection: Arc<LockFreeConnection>) {
        debug!(
            "CONNECTION POOL: Indexing connection by additional address {}",
            addr
        );
        self.connections_by_addr.insert(addr, connection);
    }

    /// Send data to a peer by ID
    pub fn send_to_peer_id(&self, peer_id: &crate::PeerId, data: &[u8]) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(data));
            } else {
                warn!(peer_id = %peer_id, "Connection found but no stream handle");
            }
        } else {
            warn!(peer_id = %peer_id, "No connection found for peer");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for peer {}", peer_id),
        )))
    }

    /// Send bytes to a peer by its ID (zero-copy version)
    pub fn send_bytes_to_peer_id(&self, peer_id: &crate::PeerId, data: bytes::Bytes) -> Result<()> {
        debug!(
            "CONNECTION POOL: send_bytes_to_peer_id called for peer '{}', pool has {} peer connections",
            peer_id,
            self.connections_by_peer.len()
        );
        if let Some(connection) = self.get_connection_by_peer_id(peer_id) {
            if let Some(ref stream_handle) = connection.stream_handle {
                debug!(
                    "CONNECTION POOL: Sending {} bytes to peer '{}'",
                    data.len(),
                    peer_id
                );
                return stream_handle.write_bytes_nonblocking(data);
            } else {
                warn!(peer_id = %peer_id, "Connection found but no stream handle");
            }
        } else {
            warn!(peer_id = %peer_id, "No connection found for peer");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Connection not found for peer {}", peer_id),
        )))
    }

    /// Get or create a lock-free connection - NO MUTEX NEEDED
    pub fn get_lock_free_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        self.connections_by_addr
            .get(&addr)
            .map(|entry| entry.value().clone())
    }

    /// Add a new lock-free connection - completely lock-free operation
    pub fn add_lock_free_connection(
        &self,
        addr: SocketAddr,
        tcp_stream: TcpStream,
    ) -> Result<Arc<LockFreeConnection>> {
        let connection_count = self.connection_counter.fetch_add(1, Ordering::AcqRel);

        if connection_count >= self.max_connections {
            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::GossipError::Network(std::io::Error::other(format!(
                "Max connections ({}) reached",
                self.max_connections
            ))));
        }

        // Split the stream for reading and writing
        let (reader, writer) = tcp_stream.into_split();

        // Create lock-free streaming handle with exclusive socket ownership
        let buffer_config = self
            .registry
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| {
                BufferConfig::default().with_ask_inflight_limit(registry.config.ask_inflight_limit)
            })
            .unwrap_or_default();

        let (stream_handle, writer_task_handle) = LockFreeStreamHandle::new(
            writer, // Pass writer half
            addr,
            ChannelId::Global,
            buffer_config,
        );
        let stream_handle = Arc::new(stream_handle);

        let mut connection = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        connection.stream_handle = Some(stream_handle);
        connection.set_state(ConnectionState::Connected);
        connection.update_last_used();

        // Track the writer task handle (H-004)
        connection
            .task_tracker
            .lock()
            .set_writer(writer_task_handle);

        let connection_arc = Arc::new(connection);

        // Spawn reader task for this connection
        // This reader needs to process incoming messages on outgoing connections
        let reader_connection = connection_arc.clone();
        let registry_weak = self.registry.clone();
        let reader_task_handle = tokio::spawn(async move {
            info!(peer = %addr, "Starting reader task for outgoing connection");
            Self::handle_persistent_connection_reader(
                reader,
                None,
                addr,
                registry_weak,
                Some(reader_connection.clone()),
            )
            .await;
            reader_connection.set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "Reader task for outgoing connection ended");
        });

        // Track the reader task handle (H-004)
        connection_arc
            .task_tracker
            .lock()
            .set_reader(reader_task_handle);

        // Insert into lock-free hash map
        self.connections_by_addr
            .insert(addr, connection_arc.clone());
        debug!(
            "CONNECTION POOL: Added lock-free connection to {} - pool now has {} connections",
            addr,
            self.connections_by_addr.len()
        );

        Ok(connection_arc)
    }

    /// Send data through lock-free connection - NO BLOCKING
    pub fn send_lock_free(&self, addr: SocketAddr, data: &[u8]) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                return stream_handle.write_bytes_nonblocking(bytes::Bytes::copy_from_slice(data));
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
            warn!(
                "Available connections: {:?}",
                self.connections_by_addr
                    .iter()
                    .map(|e| *e.key())
                    .collect::<Vec<_>>()
            );
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Send header + payload without copying the payload.
    pub fn send_lock_free_parts(
        &self,
        addr: SocketAddr,
        header: bytes::Bytes,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                stream_handle.write_header_and_payload_nonblocking_checked(header, payload)?;
                return Ok(());
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Send header + payload without copying the payload, using an inline header.
    pub fn send_lock_free_parts_inline(
        &self,
        addr: SocketAddr,
        header: [u8; 16],
        header_len: u8,
        payload: bytes::Bytes,
    ) -> Result<()> {
        if let Some(connection) = self.get_lock_free_connection(addr) {
            if let Some(ref stream_handle) = connection.stream_handle {
                stream_handle
                    .write_header_and_payload_nonblocking_inline(header, header_len, payload)?;
                return Ok(());
            } else {
                warn!(addr = %addr, "Connection found but no stream handle");
            }
        } else {
            warn!(addr = %addr, "No connection found for address");
        }
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Connection not found",
        )))
    }

    /// Try to send data through any available connection for a node
    /// This handles cases where we might have multiple connections (incoming/outgoing)
    pub fn send_to_node(
        &self,
        node_addr: SocketAddr,
        data: &[u8],
        _registry: &GossipRegistry,
    ) -> Result<()> {
        // First try direct lookup
        if let Ok(()) = self.send_lock_free(node_addr, data) {
            return Ok(());
        }

        // If that fails, look for any connection that could reach this node
        // This could be enhanced with a node ID -> connections mapping
        debug!(node_addr = %node_addr, "Direct send failed, looking for alternative connections");

        // For now, we'll rely on the caller to handle fallback strategies
        Err(crate::GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No connection found for node {}", node_addr),
        )))
    }

    /// Remove a connection from the pool - lock-free operation
    pub fn remove_connection(&self, addr: SocketAddr) -> Option<Arc<LockFreeConnection>> {
        // First remove from address-based map
        if let Some((_, connection)) = self.connections_by_addr.remove(&addr) {
            debug!(
                "CONNECTION POOL: Removed connection to {} - pool now has {} connections",
                addr,
                self.connections_by_addr.len()
            );

            // Also remove from node ID mapping
            if let Some(node_id_entry) = self.addr_to_peer_id.remove(&addr) {
                let (_, node_id) = node_id_entry;
                if let Some((_, _)) = self.connections_by_peer.remove(&node_id) {
                    debug!(
                        "CONNECTION POOL: Also removed connection by node ID '{}'",
                        node_id
                    );
                }
            }

            self.connection_counter.fetch_sub(1, Ordering::AcqRel);
            self.clear_capabilities_for_addr(&addr);

            // H-004: Gracefully shut down writer; abort reader to prevent resource leaks
            connection.shutdown_tasks_gracefully();

            Some(connection)
        } else {
            None
        }
    }

    /// Disconnect and remove a connection by peer ID
    pub fn disconnect_connection_by_peer_id(
        &self,
        peer_id: &crate::PeerId,
    ) -> Option<Arc<LockFreeConnection>> {
        if let Some((_, connection)) = self.connections_by_peer.remove(peer_id) {
            // Remove peer_id_to_addr entry
            self.peer_id_to_addr.remove(peer_id);

            // Remove correlation tracker to prevent memory leak (LEAK-001 fix)
            if self.correlation_trackers.remove(peer_id).is_some() {
                debug!(
                    peer_id = %peer_id,
                    "Cleaned up correlation tracker for disconnected peer"
                );
            }

            // Remove ALL addr_to_peer_id entries for this peer
            // (may have multiple due to reindexing keeping both ephemeral and bind addresses)
            let addrs_to_remove: Vec<SocketAddr> = self
                .addr_to_peer_id
                .iter()
                .filter(|entry| entry.value() == peer_id)
                .map(|entry| *entry.key())
                .collect();

            for addr in &addrs_to_remove {
                self.addr_to_peer_id.remove(addr);
                self.connections_by_addr.remove(addr);
                self.clear_capabilities_for_addr(addr);
            }

            self.connection_counter.fetch_sub(1, Ordering::AcqRel);

            // H-004: Gracefully shut down writer; abort reader to prevent resource leaks
            connection.shutdown_tasks_gracefully();

            Some(connection)
        } else {
            None
        }
    }

    /// Get connection count - lock-free operation
    pub fn connection_count(&self) -> usize {
        self.connections_by_peer
            .iter()
            .filter(|entry| entry.value().is_connected())
            .count()
    }

    /// Get all connected peers - lock-free operation
    pub fn get_connected_peers(&self) -> Vec<SocketAddr> {
        self.connections_by_addr
            .iter()
            .filter_map(|entry| {
                if entry.value().is_connected() {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get all connections (including disconnected) - for debugging
    pub fn get_all_connections(&self) -> Vec<SocketAddr> {
        self.connections_by_addr
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get a buffer from the pool or create a new one
    pub fn get_buffer(&mut self, min_capacity: usize) -> Vec<u8> {
        // Use the message buffer pool for lock-free buffer management
        if let Some(buffer) = self.message_buffer_pool.get_buffer() {
            if buffer.capacity() >= min_capacity {
                return buffer;
            }
            // Buffer too small, return it and create new one
            self.message_buffer_pool.return_buffer(buffer);
        }
        Vec::with_capacity(min_capacity.max(1024)) // Minimum 1KB buffers
    }

    /// Return a buffer to the pool for reuse
    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Return to the lock-free message buffer pool (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Get a message buffer from the pool for zero-copy processing
    pub fn get_message_buffer(&mut self) -> Vec<u8> {
        self.message_buffer_pool
            .get_buffer()
            .unwrap_or_else(|| Vec::with_capacity(TCP_BUFFER_SIZE / 256)) // Default small buffer
    }

    /// Return a message buffer to the pool
    pub fn return_message_buffer(&mut self, buffer: Vec<u8>) {
        if buffer.capacity() >= 1024 && buffer.capacity() <= TCP_BUFFER_SIZE {
            // Keep buffers with reasonable size (up to TCP_BUFFER_SIZE)
            self.message_buffer_pool.return_buffer(buffer);
        }
        // Otherwise let the buffer drop
    }

    /// Create a gossip message buffer with length header (optimized for reuse)
    pub fn create_message_buffer(&mut self, data: &[u8]) -> Vec<u8> {
        let header = framing::write_gossip_frame_prefix(data.len());
        let mut buffer = self.get_buffer(header.len() + data.len());
        buffer.extend_from_slice(&header);
        buffer.extend_from_slice(data);
        crate::telemetry::gossip_zero_copy::record_outbound_frame(
            "connection_pool::create_message_buffer",
            buffer.len(),
        );
        buffer
    }

    /// Get or create a persistent connection to a peer
    /// Fast path: Check for existing connection without creating new ones
    pub fn get_existing_connection(&mut self, addr: SocketAddr) -> Option<ConnectionHandle> {
        let _current_time = current_timestamp();

        if let Some(conn) = self.connections_by_addr.get_mut(&addr) {
            // The connection here is a mutable reference to Arc<LockFreeConnection>
            if conn.value().is_connected() {
                conn.value().update_last_used();
                debug!(addr = %addr, "using existing persistent connection (fast path)");
                if let Some(ref stream_handle) = conn.value().stream_handle {
                    return Some(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .value()
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                        peer_capabilities: self.peer_capabilities.clone(),
                    });
                }

                debug!(addr = %addr, "existing connection missing stream handle");
                return None;
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections_by_addr.remove(&addr);
            }
        }
        None
    }

    /// Get or create a connection to a peer by its ID
    pub async fn get_connection_to_peer(
        &mut self,
        peer_id: &crate::PeerId,
    ) -> Result<ConnectionHandle> {
        debug!(
            "CONNECTION POOL: get_connection_to_peer called for peer '{}'",
            peer_id
        );

        // First check if we already have a connection to this node
        if let Some(conn_entry) = self.connections_by_peer.get(peer_id) {
            let conn = conn_entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                debug!(
                    "CONNECTION POOL: Found existing connection to peer '{}'",
                    peer_id
                );

                if let Some(ref stream_handle) = conn.stream_handle {
                    // Need to get the address for ConnectionHandle
                    let addr = conn.addr;
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                        peer_capabilities: self.peer_capabilities.clone(),
                    });
                } else {
                    return Err(crate::GossipError::Network(std::io::Error::other(
                        "Connection exists but no stream handle",
                    )));
                }
            } else {
                // Remove disconnected connection
                debug!(
                    "CONNECTION POOL: Removing disconnected connection to peer '{}'",
                    peer_id
                );
                drop(conn_entry);
                self.connections_by_peer.remove(peer_id);
            }
        }

        // Look up the address for this node
        let addr = if let Some(addr_entry) = self.peer_id_to_addr.get(peer_id) {
            *addr_entry.value()
        } else {
            return Err(crate::GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No address configured for peer '{}'", peer_id),
            )));
        };

        // If this address is already mapped to a different peer_id, tear down the stale mapping.
        if let Some(existing_peer_id) = self.addr_to_peer_id.get(&addr).map(|e| e.value().clone()) {
            if existing_peer_id != *peer_id {
                warn!(
                    addr = %addr,
                    expected = %peer_id,
                    found = %existing_peer_id,
                    "address already mapped to a different peer_id - disconnecting stale mapping"
                );
                let _ = self.disconnect_connection_by_peer_id(&existing_peer_id);
            }
        }

        debug!(
            "CONNECTION POOL: Creating new connection to peer '{}' at {}",
            peer_id, addr
        );

        // Convert PeerId to NodeId for TLS
        let node_id_for_tls = Some(peer_id.to_node_id());

        // Create the connection and store it by node ID
        // Pass the NodeId so TLS can work even if gossip state doesn't have it yet
        let handle = self
            .get_connection_with_node_id(addr, node_id_for_tls)
            .await?;

        // After successful connection, ensure it's indexed by node ID
        if let Some(conn) = self.connections_by_addr.get(&addr) {
            self.connections_by_peer
                .insert(peer_id.clone(), conn.value().clone());
            self.addr_to_peer_id.insert(addr, peer_id.clone());
            debug!(
                "CONNECTION POOL: Indexed new connection under peer ID '{}'",
                peer_id
            );
        }

        Ok(handle)
    }

    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        self.get_connection_with_node_id(addr, None).await
    }

    pub async fn get_connection_with_node_id(
        &mut self,
        addr: SocketAddr,
        node_id: Option<crate::NodeId>,
    ) -> Result<ConnectionHandle> {
        let _current_time = current_timestamp();
        // Debug logging removed for performance - these logs were too verbose
        // debug!("CONNECTION POOL: get_connection called on pool at {:p} for {}", self as *const _, addr);
        // debug!("CONNECTION POOL: This pool instance has {} connections stored", self.connections_by_addr.len());

        // Use address index to reuse existing connections when available

        // Check if we already have a lock-free connection
        if let Some(entry) = self.connections_by_addr.get(&addr) {
            let conn = entry.value();
            if conn.is_connected() {
                conn.update_last_used();
                debug!(addr = %addr, "found existing lock-free connection, reusing handle");

                // Return the existing lock-free connection handle
                if let Some(ref stream_handle) = conn.stream_handle {
                    return Ok(ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: conn
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                        peer_capabilities: self.peer_capabilities.clone(),
                    });
                } else {
                    // Connection exists but no stream handle - this shouldn't happen
                    return Err(crate::GossipError::Network(std::io::Error::other(
                        "Connection exists but no stream handle",
                    )));
                }
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                drop(entry);
                self.connections_by_addr.remove(&addr);
            }
        }

        // Extract what we need before any await points to avoid Send issues
        let max_connections = self.max_connections;
        let connection_timeout = self.connection_timeout;
        let registry_weak = self.registry.clone();
        let resolved_node_id = match node_id {
            Some(node_id) => Some(node_id),
            None => {
                if let Some(registry_arc) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
                    registry_arc.lookup_node_id(&addr).await.or_else(|| {
                        registry_arc
                            .peer_capability_addr_to_node
                            .get(&addr)
                            .map(|entry| *entry.value())
                    })
                } else {
                    None
                }
            }
        };

        // Make room if necessary - operate on self.connections_by_addr directly!
        if self.connections_by_addr.len() >= max_connections {
            let oldest_addr = self
                .connections_by_addr
                .iter()
                .min_by_key(|entry| entry.value().last_used.load(Ordering::Acquire))
                .map(|entry| *entry.key());

            if let Some(oldest) = oldest_addr {
                self.connections_by_addr.remove(&oldest);
                warn!(addr = %oldest, "removed oldest connection to make room");
            }
        }

        // Duplicate connection tie-breaker: decide whether to reuse an existing link
        if let (Some(registry_arc), Some(node_id_value)) = (
            registry_weak.as_ref().and_then(|w| w.upgrade()),
            resolved_node_id.as_ref(),
        ) {
            let remote_peer_id = crate::PeerId::from(node_id_value);
            if let Some(existing_conn) = self.get_connection_by_peer_id(&remote_peer_id) {
                if !registry_arc.should_keep_connection(&remote_peer_id, true) {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: reusing existing connection instead of dialing outbound"
                    );
                    if let Some(ref stream_handle) = existing_conn.stream_handle {
                        return Ok(ConnectionHandle {
                            addr: existing_conn.addr,
                            stream_handle: stream_handle.clone(),
                            correlation: existing_conn
                                .correlation
                                .clone()
                                .unwrap_or_else(CorrelationTracker::new),
                            peer_capabilities: self.peer_capabilities.clone(),
                        });
                    } else {
                        return Err(GossipError::Network(std::io::Error::other(
                            "Existing connection missing stream handle",
                        )));
                    }
                } else {
                    debug!(
                        remote = %remote_peer_id,
                        "tie-breaker: replacing existing connection with outbound dial"
                    );
                    if let Some(removed) = self.disconnect_connection_by_peer_id(&remote_peer_id) {
                        if let Some(handle) = removed.stream_handle.as_ref() {
                            handle.shutdown();
                        }
                    }
                }
            }
        }

        // Connect with timeout
        debug!("CONNECTION POOL: Attempting to connect to {}", addr);
        let stream = tokio::time::timeout(connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                debug!("CONNECTION POOL: Connection to {} timed out after {:?}", addr, connection_timeout);
                GossipError::Timeout
            })?
            .map_err(|e| {
                debug!("CONNECTION POOL: Connection to {} failed: {} (will retry in {}s if this is a gossip peer)",
                      addr, e, 5); // 5s is the default retry interval
                GossipError::Network(e)
            })?;
        debug!("CONNECTION POOL: Successfully connected to {}", addr);

        // Configure socket
        stream.set_nodelay(true).map_err(GossipError::Network)?;

        // Check if TLS is enabled
        let (mut reader, writer) = if let Some(registry_arc) =
            registry_weak.as_ref().and_then(|w| w.upgrade())
        {
            if let Some(tls_config) = &registry_arc.tls_config {
                // TLS is enabled - perform handshake as client
                debug!("CONNECTION POOL: TLS enabled, checking for peer NodeId");

                // Use provided NodeId if available, otherwise look it up from gossip state
                let peer_node_id = resolved_node_id.or_else(|| {
                    registry_arc
                        .peer_capability_addr_to_node
                        .get(&addr)
                        .map(|entry| *entry.value())
                });

                let mut discovered_node_id = peer_node_id;
                let (server_name, server_name_label) = if let Some(node_id) = discovered_node_id {
                    debug!(
                        "CONNECTION POOL: Found NodeId for peer {}, performing TLS handshake",
                        addr
                    );
                    let dns_name = crate::tls::name::encode(&node_id);
                    let server_name = rustls::pki_types::ServerName::try_from(dns_name)
                        .map_err(|e| GossipError::TlsError(format!("Invalid DNS name: {}", e)))?;
                    (server_name, format!("NodeId {}", node_id.fmt_short()))
                } else {
                    // Use placeholder DNS name so TLS can still negotiate; verifier will extract NodeId from cert
                    let placeholder = format!("peer-{}.kameo.invalid", addr.port());
                    let server_name = rustls::pki_types::ServerName::try_from(placeholder.clone())
                        .map_err(|e| {
                            GossipError::TlsError(format!("Invalid fallback DNS name: {}", e))
                        })?;
                    (
                        server_name,
                        format!("placeholder SNI {} (NodeId unknown)", placeholder),
                    )
                };

                info!(
                    "🔐 TLS ENABLED: Initiating TLS connection to {} using {}",
                    addr, server_name_label
                );
                let connector = tokio_rustls::TlsConnector::from(tls_config.client_config.clone());

                match tokio::time::timeout(
                    Duration::from_secs(10),
                    connector.connect(server_name, stream),
                )
                .await
                {
                    Ok(Ok(mut tls_stream)) => {
                        let cert_node_id = tls_stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certs| certs.first())
                            .and_then(|cert| crate::tls::extract_node_id_from_cert(cert).ok());

                        if let (Some(expected), Some(actual)) = (discovered_node_id, cert_node_id) {
                            if expected != actual {
                                return Err(crate::GossipError::TlsHandshakeFailed(format!(
                                    "NodeId mismatch: expected {}, got {}",
                                    expected.fmt_short(),
                                    actual.fmt_short()
                                )));
                            }
                        }

                        if discovered_node_id.is_none() {
                            if let Some(node_id) = cert_node_id {
                                debug!(
                                    addr = %addr,
                                    "Extracted NodeId {} from peer certificate",
                                    node_id.fmt_short()
                                );
                                if registry_arc.lookup_node_id(&addr).await.is_none() {
                                    registry_arc
                                        .add_peer_with_node_id(addr, Some(node_id))
                                        .await;
                                }
                                discovered_node_id = Some(node_id);
                            } else {
                                warn!(
                                    addr = %addr,
                                    "Failed to extract NodeId from peer certificate"
                                );
                            }
                        }

                        if let Some(node_id) = discovered_node_id {
                            info!(
                                "✅ TLS handshake successful with {} (NodeId: {})",
                                addr,
                                node_id.fmt_short()
                            );
                        } else {
                            info!("✅ TLS handshake successful with {} (NodeId unknown)", addr);
                        }

                        let negotiated_alpn = tls_stream
                            .get_ref()
                            .1
                            .alpn_protocol()
                            .map(|proto| proto.to_vec());

                        let enable_peer_discovery = registry_arc.config.enable_peer_discovery;
                        let peer_caps = match crate::handshake::perform_hello_handshake(
                            &mut tls_stream,
                            negotiated_alpn.as_deref(),
                            enable_peer_discovery,
                        )
                        .await
                        {
                            Ok(caps) => caps,
                            Err(err) => {
                                warn!(
                                    addr = %addr,
                                    error = %err,
                                    "Hello handshake failed after TLS session establishment"
                                );
                                return Err(err);
                            }
                        };
                        registry_arc.set_peer_capabilities(addr, peer_caps.clone());

                        if let Some(node_id) = discovered_node_id
                            .or_else(|| registry_arc.lookup_node_id(&addr).now_or_never().flatten())
                        {
                            registry_arc
                                .associate_peer_capabilities_with_node(addr, node_id)
                                .await;
                        }

                        let (read_half, write_half) = tokio::io::split(tls_stream);
                        (
                            Box::pin(read_half) as Pin<Box<dyn AsyncRead + Send>>,
                            Box::pin(write_half) as Pin<Box<dyn AsyncWrite + Send>>,
                        )
                    }
                    Ok(Err(e)) => {
                        error!(addr = %addr, error = %e, "❌ TLS handshake failed");
                        return Err(GossipError::TlsError(format!(
                            "TLS handshake failed: {}",
                            e
                        )));
                    }
                    Err(_) => {
                        error!(
                            addr = %addr,
                            "⏱️ TLS handshake timed out (NodeId hint: {:?})",
                            discovered_node_id.as_ref().map(|id| id.fmt_short())
                        );
                        return Err(GossipError::Timeout);
                    }
                }
            } else {
                // No TLS configured - panic to enforce TLS-only
                panic!("🔥 TLS is NOT configured but is required! Cannot establish plain TCP connection to {}", addr);
            }
        } else {
            // No registry reference - panic to enforce TLS requirement
            panic!("🔥 No registry reference available - TLS cannot be verified! Cannot establish connection to {}", addr);
        };

        // Create lock-free connection for receiving
        let buffer_config = self
            .registry
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .map(|registry| {
                BufferConfig::default().with_ask_inflight_limit(registry.config.ask_inflight_limit)
            })
            .unwrap_or_default();
        let (stream_handle, writer_task_handle) =
            LockFreeStreamHandle::new(writer, addr, ChannelId::Global, buffer_config);
        let stream_handle = Arc::new(stream_handle);

        let mut conn = LockFreeConnection::new(addr, ConnectionDirection::Outbound);
        conn.stream_handle = Some(stream_handle.clone());
        conn.set_state(ConnectionState::Connected);
        conn.update_last_used();

        // Track the writer task handle (H-004)
        conn.task_tracker.lock().set_writer(writer_task_handle);

        // For outgoing connections, we might know the peer ID from configuration
        let peer_id_opt = self
            .addr_to_peer_id
            .get(&addr)
            .map(|entry| entry.clone())
            .or_else(|| {
                // Try reverse lookup: find peer ID that maps to this address
                self.peer_id_to_addr
                    .iter()
                    .find(|entry| entry.value() == &addr)
                    .map(|entry| entry.key().clone())
            });

        if let Some(peer_id) = peer_id_opt {
            // Use shared correlation tracker for this peer
            conn.correlation = Some(self.get_or_create_correlation_tracker(&peer_id));
            debug!(
                "CONNECTION POOL: Using shared correlation tracker for peer {:?} at {}",
                peer_id, addr
            );
        } else {
            // No peer ID yet, create a new correlation tracker
            // This will be replaced when we learn the peer ID from their FullSync message
            conn.correlation = Some(CorrelationTracker::new());
            debug!(
                "CONNECTION POOL: Created new correlation tracker for unknown peer at {}",
                addr
            );
        }

        let connection_arc = Arc::new(conn);

        // Insert into lock-free map before spawning
        self.connections_by_addr
            .insert(addr, connection_arc.clone());
        debug!("CONNECTION POOL: Added connection via get_connection to {} - pool now has {} connections",
              addr, self.connections_by_addr.len());
        // Double check it's really there
        assert!(
            self.connections_by_addr.contains_key(&addr),
            "Connection was not added to pool!"
        );
        debug!("CONNECTION POOL: Verified connection exists for {}", addr);

        // Send initial FullSync message to identify ourselves
        if let Some(registry_arc) = registry_weak.as_ref().and_then(|w| w.upgrade()) {
            let initial_msg = {
                let actor_state = registry_arc.actor_state.read().await;
                let gossip_state = registry_arc.gossip_state.lock().await;

                RegistryMessage::FullSync {
                    local_actors: actor_state.local_actors.clone().into_iter().collect(),
                    known_actors: actor_state.known_actors.clone().into_iter().collect(),
                    sender_peer_id: registry_arc.peer_id.clone(),
                    sender_bind_addr: Some(registry_arc.bind_addr.to_string()), // Use our listening address, not ephemeral port
                    sequence: gossip_state.gossip_sequence,
                    wall_clock_time: crate::current_timestamp(),
                }
            };

            // Serialize and send the initial message with Gossip type prefix
            match rkyv::to_bytes::<rkyv::rancor::Error>(&initial_msg) {
                Ok(data) => {
                    let header = framing::write_gossip_frame_prefix(data.len());
                    let mut msg_buffer = Vec::with_capacity(header.len() + data.len());
                    msg_buffer.extend_from_slice(&header);
                    msg_buffer.extend_from_slice(&data);
                    crate::telemetry::gossip_zero_copy::record_outbound_frame(
                        "connection_pool::initial_full_sync",
                        msg_buffer.len(),
                    );

                    // Create a connection handle to send the message
                    let conn_handle = ConnectionHandle {
                        addr,
                        stream_handle: stream_handle.clone(),
                        correlation: connection_arc
                            .correlation
                            .clone()
                            .unwrap_or_else(CorrelationTracker::new),
                        peer_capabilities: self.peer_capabilities.clone(),
                    };
                    if let Err(e) = conn_handle.send_data(msg_buffer).await {
                        warn!(peer = %addr, error = %e, "Failed to send initial FullSync message");
                    } else {
                        info!(peer = %addr, "Sent initial FullSync message to identify ourselves");
                    }
                }
                Err(e) => {
                    warn!(peer = %addr, error = %e, "Failed to serialize initial FullSync message");
                }
            }
        }

        // Note: actor_message_handler is fetched from registry on each message to handle
        // cases where the handler is registered after connection establishment

        // Spawn reader task for outgoing connection
        // This MUST process incoming messages to receive responses!
        let reader_connection = connection_arc.clone();
        let registry_weak_for_reader = registry_weak.clone();
        let reader_task_handle = tokio::spawn(async move {
            info!(peer = %addr, "Starting outgoing connection reader with message processing");

            let max_message_size = registry_weak_for_reader
                .as_ref()
                .and_then(|w| w.upgrade())
                .map(|registry| registry.config.max_message_size)
                .unwrap_or(10 * 1024 * 1024);
            let mut streaming_state = streaming::StreamAssembler::new(max_message_size);
            let gossip_buffer = crate::gossip_buffer::GossipFrameBuffer::new();

            loop {
                match crate::handle::read_message_from_tls_reader(
                    &mut reader,
                    max_message_size,
                    &gossip_buffer,
                )
                .await
                {
                    Ok(crate::handle::MessageReadResult::Gossip(frame, correlation_id)) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            if let Err(e) =
                                handle_incoming_message(registry, addr, frame, correlation_id).await
                            {
                                warn!(peer = %addr, error = %e, "Failed to handle gossip message");
                            }
                        }
                    }
                    Ok(crate::handle::MessageReadResult::AskRaw {
                        correlation_id,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            crate::handle::handle_raw_ask_request(
                                &registry,
                                addr,
                                correlation_id,
                                payload,
                            )
                            .await;
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Response {
                        correlation_id,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            crate::handle::handle_response_message(
                                &registry,
                                addr,
                                correlation_id,
                                payload,
                            )
                            .await;
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Actor {
                        msg_type,
                        correlation_id,
                        actor_id,
                        type_hash,
                        payload,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            if let Some(handler) = registry.load_actor_message_handler() {
                                let mut id_buf = itoa::Buffer::new();
                                let actor_id_str = id_buf.format(actor_id);
                                let correlation = if msg_type == crate::MessageType::ActorAsk as u8
                                {
                                    Some(correlation_id)
                                } else {
                                    None
                                };
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        type_hash,
                                        payload.as_slice(),
                                        correlation,
                                    )
                                    .await;
                            }
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Streaming {
                        frame,
                        correlation_id,
                    }) => {
                        if let Some(registry) =
                            registry_weak_for_reader.as_ref().and_then(|w| w.upgrade())
                        {
                            Self::process_reader_streaming_frame(
                                &registry,
                                &mut streaming_state,
                                &mut reader,
                                addr,
                                frame,
                                correlation_id,
                            )
                            .await;
                        }
                    }
                    Ok(crate::handle::MessageReadResult::Raw(_payload)) => {
                        #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                        {
                            if typed_tell_capture_enabled() {
                                crate::test_helpers::record_raw_payload(_payload.clone());
                            }
                        }
                        // Raw tell payloads are ignored on outgoing readers.
                    }
                    Err(e) => {
                        reader_connection.set_state(ConnectionState::Disconnected);
                        warn!(peer = %addr, error = %e, "Outgoing connection reader error");
                        break;
                    }
                }
            }

            info!(peer = %addr, "Outgoing connection reader exited");
        });

        // Track the reader task handle (H-004)
        connection_arc
            .task_tracker
            .lock()
            .set_reader(reader_task_handle);

        // Reset failure state for this peer since we successfully connected
        if let Some(ref registry_weak) = registry_weak {
            if let Some(registry) = registry_weak.upgrade() {
                let registry_clone = registry.clone();
                let peer_addr = addr;
                tokio::spawn(async move {
                    let mut gossip_state = registry_clone.gossip_state.lock().await;

                    // Check if we need to reset failures and clear pending
                    let need_to_clear_pending = if let Some(peer_info) =
                        gossip_state.peers.get_mut(&peer_addr)
                    {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %peer_addr,
                                  prev_failures = peer_info.failures,
                                  "✅ Successfully established outgoing connection - resetting failure state");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        had_failures
                    } else {
                        false
                    };

                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state.pending_peer_failures.remove(&peer_addr);
                    }
                });
            }
        }

        info!(peer = %addr, "successfully created new persistent connection");

        // Verify the connection is in the pool
        debug!(
            "CONNECTION POOL: After get_connection, pool has {} connections",
            self.connections_by_addr.len()
        );
        debug!(
            "CONNECTION POOL: Pool contains connection to {}? {}",
            addr,
            self.connections_by_addr.contains_key(&addr)
        );

        // Return a lock-free ConnectionHandle
        Ok(ConnectionHandle {
            addr,
            stream_handle,
            correlation: connection_arc
                .correlation
                .clone()
                .unwrap_or_else(CorrelationTracker::new),
            peer_capabilities: self.peer_capabilities.clone(),
        })
    }

    /// Mark a connection as disconnected
    pub fn mark_disconnected(&mut self, addr: SocketAddr) {
        if let Some(entry) = self.connections_by_addr.get(&addr) {
            entry.value().set_state(ConnectionState::Disconnected);
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Remove a connection from the pool by address
    pub fn remove_connection_mut(&mut self, addr: SocketAddr) {
        if let Some((_, conn)) = self.connections_by_addr.remove(&addr) {
            // H-004: Gracefully shut down writer; abort reader to prevent resource leaks
            conn.shutdown_tasks_gracefully();

            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            // No need to drop writer
            self.clear_capabilities_for_addr(&addr);
        }
    }

    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections_by_addr
            .get(addr)
            .map(|entry| entry.value().is_connected())
            .unwrap_or(false)
    }

    /// Check if we have a connection to a peer by peer ID
    pub fn has_connection_by_peer_id(&self, peer_id: &crate::PeerId) -> bool {
        self.connections_by_peer
            .get(peer_id)
            .map(|entry| entry.value().is_connected())
            .unwrap_or(false)
    }

    /// Check health of all connections
    pub async fn check_connection_health(&mut self) -> Vec<SocketAddr> {
        // Health checking is now done by the persistent connection handlers
        Vec::new()
    }

    /// Clean up stale connections
    pub fn cleanup_stale_connections(&mut self) {
        // Find disconnected peers and use peer-id-based removal to clean up all maps
        let stale_peer_ids: Vec<_> = self
            .connections_by_peer
            .iter()
            .filter(|entry| !entry.value().is_connected())
            .map(|entry| entry.key().clone())
            .collect();

        for peer_id in stale_peer_ids {
            if let Some(_conn) = self.disconnect_connection_by_peer_id(&peer_id) {
                debug!(peer_id = %peer_id, "cleaned up disconnected connection (all aliases)");
            }
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&mut self) {
        // Use peer-id-based removal to properly clean up all address aliases
        // This avoids double-decrement of connection_counter when a connection
        // has both ephemeral and bind addresses after reindexing
        let peer_ids: Vec<_> = self
            .connections_by_peer
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        let count = peer_ids.len();
        for peer_id in peer_ids {
            self.disconnect_connection_by_peer_id(&peer_id);
        }
        info!("closed all {} connections", count);
    }
    /// Handle persistent connection reader - only reads messages, no channels
    pub(crate) async fn handle_persistent_connection_reader(
        mut reader: tokio::net::tcp::OwnedReadHalf,
        writer: Option<tokio::net::tcp::OwnedWriteHalf>,
        peer_addr: SocketAddr,
        registry_weak: Option<std::sync::Weak<GossipRegistry>>,
        connection: Option<Arc<LockFreeConnection>>,
    ) {
        use tokio::io::AsyncReadExt;

        let mut partial_msg_buf = bytes::BytesMut::new();
        const READ_BUF_SIZE: usize = 64 * 1024;
        // For incoming connections with a writer, create a stream handle
        // For outgoing connections, we'll use the existing handle from the pool
        // Note: The writer_task_handle is not tracked here as this is for response handling
        // on incoming connections, not for the main connection lifecycle tracking.
        let response_handle = writer.map(|writer| {
            let buffer_config = registry_weak
                .as_ref()
                .and_then(|weak| weak.upgrade())
                .map(|registry| {
                    BufferConfig::default()
                        .with_ask_inflight_limit(registry.config.ask_inflight_limit)
                })
                .unwrap_or_default();
            let (handle, _writer_task_handle) =
                LockFreeStreamHandle::new(writer, peer_addr, ChannelId::Global, buffer_config);
            Arc::new(handle)
        });

        loop {
            partial_msg_buf.reserve(READ_BUF_SIZE);
            match reader.read_buf(&mut partial_msg_buf).await {
                Ok(0) => {
                    info!(peer = %peer_addr, "Connection closed by peer");
                    break;
                }
                Ok(_n) => {
                    // Process complete messages
                    while partial_msg_buf.len() >= 4 {
                        let len = u32::from_be_bytes([
                            partial_msg_buf[0],
                            partial_msg_buf[1],
                            partial_msg_buf[2],
                            partial_msg_buf[3],
                        ]) as usize;
                        debug!(
                            peer = %peer_addr,
                            frame_len = len,
                            "🔍 TLS reader got frame length prefix (server stream)"
                        );

                        if len > 100 * 1024 * 1024 {
                            warn!(peer = %peer_addr, len = len, "Message too large - possible buffer corruption");
                            // This usually indicates we're reading from the wrong position in the buffer
                            // Clear the buffer completely and restart message processing
                            partial_msg_buf.clear();
                            break;
                        }

                        // Debug log for large messages (>1MB)
                        if len > 1024 * 1024 {
                            // eprintln!("📦 SERVER: LARGE MESSAGE detected! len={}, from peer={}", len, peer_addr);
                            // info!(peer = %peer_addr, len = len, buf_len = partial_msg_buf.len(), needed = 4 + len,
                            //       "📦 LARGE MESSAGE: Receiving large message (have {} of {} bytes)",
                            //       partial_msg_buf.len(), 4 + len);

                            // Pre-allocate buffer capacity for large messages to avoid repeated allocations
                            let total_needed = 4 + len;
                            if partial_msg_buf.capacity() < total_needed {
                                partial_msg_buf.reserve(total_needed - partial_msg_buf.len());
                                info!(peer = %peer_addr, "📦 LARGE MESSAGE: Reserved {} bytes for message", total_needed);
                            }
                        }

                        let total_len = 4 + len;
                        if partial_msg_buf.len() < total_len {
                            // if len > 1024 * 1024 {
                            //     eprintln!("📦 SERVER: Accumulating large message... have {}/{} bytes", partial_msg_buf.len(), total_len);
                            // }
                            // Not enough data yet, break inner loop to read more
                            break;
                        }
                        if partial_msg_buf.len() >= total_len {
                            let msg_buf = partial_msg_buf.split_to(total_len).freeze();
                            let msg_data =
                                msg_buf.slice(crate::framing::LENGTH_PREFIX_LEN..total_len);

                            // Log when we have the complete large message
                            // if len > 1024 * 1024 {
                            //     eprintln!("📦 SERVER: LARGE MESSAGE COMPLETE! len={}, processing...", len);
                            //     // info!(peer = %peer_addr, len = len, "📦 LARGE MESSAGE: Complete message received, processing...");
                            // }

                            // Debug: Log first few bytes of every message
                            if msg_data.len() >= crate::framing::ASK_RESPONSE_HEADER_LEN {
                                info!(peer = %peer_addr,
                                  "🔍 SERVER RECV: msg_len={}, first_bytes=[{:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}]",
                                  msg_data.len(),
                                  msg_data[0], msg_data[1], msg_data[2], msg_data[3],
                                  msg_data[4], msg_data[5], msg_data[6], msg_data[7]);
                            }

                            // Check if this is an Ask/Response message by looking at first byte
                            if !msg_data.is_empty() {
                                let msg_type_byte = msg_data[0];
                                debug!(
                                    peer = %peer_addr,
                                    msg_type_byte,
                                    payload_bytes = msg_data.len(),
                                    "🔍 TLS reader inspecting message type (server stream)"
                                );
                                // Debug log for large messages
                                // if len > 1024 * 1024 {
                                //     info!(peer = %peer_addr, first_byte = msg_data[0], "📦 LARGE MESSAGE first byte: {} (0=Gossip, 3=ActorTell)", msg_data[0]);
                                // }
                                if let Some(msg_type) = crate::MessageType::from_byte(msg_type_byte)
                                {
                                    // This is an Ask/Response message
                                    if msg_data.len() < crate::framing::ASK_RESPONSE_HEADER_LEN {
                                        warn!(peer = %peer_addr, "Ask/Response message too small");
                                        continue;
                                    }

                                    let correlation_id =
                                        u16::from_be_bytes([msg_data[1], msg_data[2]]);
                                    let payload =
                                        msg_data.slice(crate::framing::ASK_RESPONSE_HEADER_LEN..);

                                    match msg_type {
                                        crate::MessageType::Ask => {
                                            info!(peer = %peer_addr, correlation_id = correlation_id, payload_len = payload.len(),
                                              "📨 ASK DEBUG: Received Ask message on bidirectional connection");

                                            // Verify alignment (panic in production, allow in tests)
                                            #[cfg(not(test))]
                                            {
                                                let required_alignment = std::mem::align_of::<<
                                                    crate::registry::RegistryMessage as rkyv::Archive
                                                >::Archived>();
                                                let is_aligned = (payload.as_ptr() as usize)
                                                    .is_multiple_of(required_alignment);
                                                assert!(
                                                    is_aligned,
                                                    "Ask payload must be {}-byte aligned! Pointer: {:p}, offset: {}",
                                                    required_alignment,
                                                    payload.as_ptr(),
                                                    payload.as_ptr() as usize
                                                );
                                            }

                                            // ZERO-COPY: Access archived RegistryMessage without allocation
                                            match rkyv::access::<
                                                <crate::registry::RegistryMessage as rkyv::Archive>::Archived,
                                                rkyv::rancor::Error,
                                            >(
                                                payload.as_ref()
                                            ) {
                                                Ok(archived_msg) => {
                                                    info!(peer = %peer_addr, correlation_id = correlation_id,
                                                      "✅ ZERO-COPY: Successfully accessed archived RegistryMessage");
                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Received Ask with RegistryMessage payload");

                                                    // Use zero-copy accessor trait to extract fields
                                                    use crate::registry::RegistryMessageArchivedAccess;

                                                    let actor_id = archived_msg.actor_id();
                                                    let type_hash = archived_msg.type_hash();
                                                    let payload_slice = archived_msg.payload();
                                                    let correlation_id_opt = archived_msg.correlation_id();

                                                    // Special handling for ActorMessage with correlation_id
                                                    if let (Some(actor_id_str), Some(type_hash_val), Some(payload_bytes), Some(corr_id_opt)) =
                                                       (actor_id, type_hash, payload_slice, correlation_id_opt)
                                                    {
                                                        // Use Ask envelope's correlation_id if not set in message
                                                        let corr_id = corr_id_opt.unwrap_or(correlation_id);

                                                        info!(peer = %peer_addr, actor_id = %actor_id_str, type_hash = %format!("{:08x}", type_hash_val),
                                                              payload_len = payload_bytes.len(), correlation_id = corr_id,
                                                              "🎯 ZERO-COPY: Processing ActorMessage ask");

                                                        if let Some(ref registry_weak) = registry_weak {
                                                            if let Some(registry) = registry_weak.upgrade() {
                                                                let registry_for_reply = registry.clone();
                                                                let send_peer_addr = peer_addr;
                                                                let send_reply = move |reply_payload: bytes::Bytes, corr_id: u16| {
                                                                    let registry = registry_for_reply.clone();
                                                                    async move {
                                                                        let reply_len = reply_payload.len();
                                                                        debug!(peer = %send_peer_addr, correlation_id = corr_id, reply_len = reply_len,
                                                                               "Got reply from actor, sending response back");

                                                                        let header = framing::write_ask_response_header(
                                                                            crate::MessageType::Response,
                                                                            corr_id,
                                                                            reply_len,
                                                                        );

                                                                        let pool = registry.connection_pool.lock().await;
                                                                        if let Some(conn) = pool.connections_by_addr.get(&send_peer_addr).map(|c| c.value().clone()) {
                                                                            if let Some(ref stream_handle) = conn.stream_handle {
                                                                                stream_handle
                                                                                    .write_header_and_payload_control_inline(
                                                                                        header,
                                                                                        16,
                                                                                        reply_payload,
                                                                                    )
                                                                                    .await
                                                                            } else {
                                                                                warn!(peer = %send_peer_addr, "Connection has no stream handle");
                                                                                Err(GossipError::Network(io::Error::new(
                                                                                    io::ErrorKind::NotConnected,
                                                                                    "missing stream handle for ask reply",
                                                                                )))
                                                                            }
                                                                        } else {
                                                                            warn!(peer = %send_peer_addr, "No connection found in pool to send ask reply");
                                                                            Err(GossipError::Network(io::Error::new(
                                                                                io::ErrorKind::NotFound,
                                                                                "connection not found for ask reply",
                                                                            )))
                                                                        }
                                                                    }
                                                                };

                                                                match dispatch_actor_message_zero_copy(
                                                                    &registry,
                                                                    actor_id_str,
                                                                    type_hash_val,
                                                                    payload_bytes,
                                                                    Some(corr_id),
                                                                    send_reply,
                                                                )
                                                                .await
                                                                {
                                                                    Ok(true) => {
                                                                        debug!(peer = %peer_addr, correlation_id = corr_id, "Sent ask reply through connection pool");
                                                                    }
                                                                    Ok(false) => {
                                                                        debug!(peer = %peer_addr, correlation_id = corr_id, "Actor handled ask with no reply");
                                                                    }
                                                                    Err(e) => {
                                                                        warn!(peer = %peer_addr, error = %e, correlation_id = corr_id, "Failed to handle actor message");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    } else {
                                                        // For other message types (DeltaGossip, FullSync, etc.), we need owned RegistryMessage
                                                        // This is acceptable for these less frequent message types
                                                        let payload_bytes = payload.clone();
                                                        match RegistryMessageFrame::new(
                                                            payload_bytes,
                                                        ) {
                                                            Ok(frame) => {
                                                                if let Some(ref registry_weak) =
                                                                    registry_weak
                                                                {
                                                                    if let Some(registry) =
                                                                        registry_weak.upgrade()
                                                                    {
                                                                        match handle_incoming_message(
                                                                            registry.clone(),
                                                                            peer_addr,
                                                                            frame,
                                                                            Some(correlation_id),
                                                                        )
                                                                        .await
                                                                        {
                                                                            Ok(()) => {
                                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "Ask message processed");
                                                                            }
                                                                            Err(e) => {
                                                                                warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle Ask message");
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            Err(e) => {
                                                                warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id,
                                                                      "Failed to deserialize non-ActorMessage RegistryMessage");
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(peer = %peer_addr, correlation_id = correlation_id, error = %e,
                                                       payload_len = payload.len(),
                                                       "HANDLE ASK: Failed to access archived RegistryMessage, falling back");

                                                    // Not a RegistryMessage, handle as before for test helpers
                                                    #[cfg(any(
                                                        test,
                                                        feature = "test-helpers",
                                                        debug_assertions
                                                    ))]
                                                    {
                                                        if let Some(ref registry_weak) =
                                                            registry_weak
                                                        {
                                                            if let Some(registry) =
                                                                registry_weak.upgrade()
                                                            {
                                                                let conn = {
                                                                    let pool = registry
                                                                        .connection_pool
                                                                        .lock()
                                                                        .await;
                                                                    pool.connections_by_addr
                                                                        .get(&peer_addr)
                                                                        .map(|conn_ref| {
                                                                            conn_ref.value().clone()
                                                                        })
                                                                };

                                                                if let Some(conn) = conn {
                                                                    // Process the request and generate response
                                                                    let response_data =
                                                                    process_mock_request_payload(
                                                                        payload.as_ref(),
                                                                    );

                                                                    // Build response message
                                                                    let mut msg =
                                                                    bytes::BytesMut::with_capacity(
                                                                        framing::ASK_RESPONSE_FRAME_HEADER_LEN
                                                                            + response_data.len(),
                                                                    );

                                                                    // Header: [type:1][corr_id:2][pad:1]
                                                                    let header =
                                                                    framing::write_ask_response_header(
                                                                    crate::MessageType::Response,
                                                                    correlation_id,
                                                                    response_data.len(),
                                                                );
                                                                    msg.extend_from_slice(&header);
                                                                    msg.extend_from_slice(
                                                                        &response_data,
                                                                    );

                                                                    // Send response back through stream handle
                                                                    if let Some(ref stream_handle) =
                                                                        conn.stream_handle
                                                                    {
                                                                        if let Err(e) = stream_handle
                                                                        .write_bytes_nonblocking(
                                                                            msg.freeze(),
                                                                        )
                                                                    {
                                                                        warn!("Failed to send mock response: {}", e);
                                                                    } else {
                                                                        debug!("Sent mock response for correlation_id {}", correlation_id);
                                                                    }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    #[cfg(not(any(
                                                        test,
                                                        feature = "test-helpers",
                                                        debug_assertions
                                                    )))]
                                                    {
                                                        // This might be a kameo AskWrapper - try to handle it
                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, payload_len = payload.len(),
                                                      "Received non-RegistryMessage Ask request, checking if it's from kameo");

                                                        // Try to parse binary format from kameo:
                                                        // Full: [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                                                        // After ASK_RESPONSE_HEADER_LEN (4) slice, payload starts with reserved(8)
                                                        // Actor fields start at offset 8 (skipping 8 bytes of reserved)
                                                        if payload.len() >= 24 {
                                                            let actor_id = u64::from_be_bytes([
                                                                payload[8], payload[9], payload[10],
                                                                payload[11], payload[12], payload[13],
                                                                payload[14], payload[15],
                                                            ]);
                                                            let type_hash = u32::from_be_bytes([
                                                                payload[16],
                                                                payload[17],
                                                                payload[18],
                                                                payload[19],
                                                            ]);
                                                            let payload_len = u32::from_be_bytes([
                                                                payload[20],
                                                                payload[21],
                                                                payload[22],
                                                                payload[23],
                                                            ])
                                                                as usize;

                                                            if payload.len() >= 24 + payload_len {
                                                                let inner_payload =
                                                                    &payload[24..24 + payload_len];

                                                                debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                               actor_id = actor_id, type_hash = type_hash,
                                                               "Successfully decoded kameo binary message format");

                                                                if let Some(ref registry_weak) =
                                                                    registry_weak
                                                                {
                                                                    if let Some(registry) =
                                                                        registry_weak.upgrade()
                                                                    {
                                                                        // Handle the actor message
                                                                        match registry
                                                                            .handle_actor_message_id(
                                                                                actor_id,
                                                                                type_hash,
                                                                                inner_payload,
                                                                                Some(correlation_id),
                                                                            )
                                                                            .await
                                                                        {
                                                                            Ok(Some(
                                                                                reply_payload,
                                                                            )) => {
                                                                                let reply_len =
                                                                                    reply_payload
                                                                                        .len();
                                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, reply_len = reply_len,
                                                                           "Got reply from kameo actor, sending response back");

                                                                                let header =
                                                                                framing::write_ask_response_header(
                                                                                    crate::MessageType::Response,
                                                                                    correlation_id,
                                                                                    reply_len,
                                                                                );
                                                                                let payload =
                                                                                    reply_payload;

                                                                                // Send response back through the response handle we saved
                                                                                if let Some(
                                                                                    ref handle,
                                                                                ) =
                                                                                    response_handle
                                                                                {
                                                                                    if let Err(e) = handle
                                                                                    .write_header_and_payload_control_inline(
                                                                                        header,
                                                                                        16,
                                                                                        payload.clone(),
                                                                                    )
                                                                                    .await
                                                                                {
                                                                                    warn!(peer = %peer_addr, error = %e, "Failed to send ask reply");
                                                                                } else {
                                                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ask reply directly through writer");
                                                                                }
                                                                                } else {
                                                                                    // Fall back to finding in pool
                                                                                    warn!(peer = %peer_addr, "No response_handle, falling back to pool lookup");
                                                                                    let pool = registry
                                                                                    .connection_pool
                                                                                    .lock()
                                                                                    .await;
                                                                                    if let Some(conn) = pool.connections_by_addr.get(&peer_addr).map(|c| c.value().clone()) {
                                                                                    if let Some(ref stream_handle) = conn.stream_handle {
                                                                                        if let Err(e) = stream_handle
                                                                                            .write_header_and_payload_control_inline(
                                                                                                header,
                                                                                                16,
                                                                                                payload.clone(),
                                                                                            )
                                                                                            .await
                                                                                        {
                                                                                            warn!(peer = %peer_addr, error = %e, "Failed to send ask reply");
                                                                                        } else {
                                                                                            debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ask reply through connection pool");
                                                                                        }
                                                                                    } else {
                                                                                        warn!(peer = %peer_addr, "Connection has no stream handle");
                                                                                    }
                                                                                } else {
                                                                                    warn!(peer = %peer_addr, "No connection found in pool for reply");
                                                                                }
                                                                                }
                                                                            }
                                                                            Ok(None) => {
                                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "No reply from kameo actor");
                                                                            }
                                                                            Err(e) => {
                                                                                warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle kameo actor message");
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            } else {
                                                                debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                               "Binary message payload too short: expected {} bytes but got {}",
                                                               24 + payload_len, payload.len());
                                                            }
                                                        } else {
                                                            debug!(peer = %peer_addr, correlation_id = correlation_id,
                                                           "Ask payload too short for binary format: {} bytes (need at least 24)",
                                                           payload.len());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        crate::MessageType::StreamStart
                                        | crate::MessageType::StreamData
                                        | crate::MessageType::StreamEnd => {
                                            // Streaming frames are handled elsewhere (streaming assembler).
                                            debug!(peer = %peer_addr, msg_type = ?msg_type,
                                                "Ignoring streaming frame in ask/response fast path");
                                            continue;
                                        }
                                        crate::MessageType::Response => {
                                            // Handle incoming response (fast path via connection correlation)
                                            let response_payload = payload;

                                            if let Some(conn) = connection.as_ref() {
                                                if let Some(ref correlation) = conn.correlation {
                                                    if correlation.has_pending(correlation_id) {
                                                        correlation.complete(
                                                            correlation_id,
                                                            response_payload,
                                                        );
                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, "Delivered response via connection correlation tracker");
                                                        continue;
                                                    }
                                                }
                                            }

                                            // Fallback: shared correlation tracker via peer_id
                                            if let Some(ref registry_weak) = registry_weak {
                                                if let Some(registry) = registry_weak.upgrade() {
                                                    let pool =
                                                        registry.connection_pool.lock().await;
                                                    let mut delivered = false;

                                                    if let Some(peer_id) = pool
                                                        .addr_to_peer_id
                                                        .get(&peer_addr)
                                                        .map(|e| e.clone())
                                                    {
                                                        if let Some(correlation) =
                                                            pool.correlation_trackers.get(&peer_id)
                                                        {
                                                            if correlation
                                                                .has_pending(correlation_id)
                                                            {
                                                                correlation.complete(
                                                                    correlation_id,
                                                                    response_payload,
                                                                );
                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "Delivered response to shared correlation tracker");
                                                                delivered = true;
                                                            }
                                                        }
                                                    }

                                                    if !delivered {
                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, "Could not find pending request for correlation_id");
                                                    }
                                                }
                                            }
                                        }
                                        crate::MessageType::Gossip => {
                                            // Gossip messages can arrive here, just ignore them
                                        }
                                        crate::MessageType::ActorTell => {
                                            // Direct actor tell message format (from kameo):
                                            // Full header: [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                                            // After ASK_RESPONSE_HEADER_LEN (4) slice, payload starts with reserved(8)
                                            // Actor fields start at offset 8 into payload (skipping 8 bytes of reserved)
                                            if payload.len() >= 24 {
                                                // Skip 8 reserved bytes, then read actor fields
                                                let actor_id = u64::from_be_bytes(
                                                    payload[8..16].try_into().unwrap(),
                                                );
                                                let type_hash = u32::from_be_bytes(
                                                    payload[16..20].try_into().unwrap(),
                                                );
                                                let payload_len = u32::from_be_bytes(
                                                    payload[20..24].try_into().unwrap(),
                                                )
                                                    as usize;

                                                if payload.len() >= 24 + payload_len {
                                                    let actor_payload =
                                                        &payload[24..24 + payload_len];

                                                    #[cfg(any(
                                                        test,
                                                        feature = "test-helpers",
                                                        debug_assertions
                                                    ))]
                                                    {
                                                        if std::env::var(
                                                            "KAMEO_REMOTE_TYPED_TELL_CAPTURE",
                                                        )
                                                        .is_ok()
                                                        {
                                                            crate::test_helpers::record_raw_payload(
                                                                bytes::Bytes::copy_from_slice(
                                                                    actor_payload,
                                                                ),
                                                            );
                                                        }
                                                    }

                                                    // Log large ActorTell messages
                                                    if payload_len > 1024 * 1024 {
                                                        info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                          payload_len = payload_len, "📨 LARGE ActorTell message - will call handler");
                                                    }

                                                    // Call actor message handler if available
                                                    if let Some(ref registry_weak) = registry_weak {
                                                        if let Some(registry) =
                                                            registry_weak.upgrade()
                                                        {
                                                            if let Some(handler) = registry
                                                                .load_actor_message_handler()
                                                            {
                                                                if payload_len > 1024 * 1024 {
                                                                    info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                       "🚀 Calling actor message handler for LARGE ActorTell");
                                                                } else {
                                                                    debug!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                       "Calling actor message handler for ActorTell");
                                                                }
                                                                match handler
                                                                    .handle_actor_message_id(
                                                                        actor_id,
                                                                        type_hash,
                                                                        actor_payload,
                                                                        None, // No correlation for tell
                                                                    )
                                                                    .await
                                                                {
                                                                    Ok(_) => {
                                                                        if payload_len > 1024 * 1024
                                                                        {
                                                                            info!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                               "✅ Successfully handled LARGE ActorTell");
                                                                        } else {
                                                                            debug!(peer = %peer_addr, actor_id = actor_id, type_hash = %format!("{:08x}", type_hash),
                                                                               "Successfully handled ActorTell");
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        error!(peer = %peer_addr, error = %e, "Failed to handle ActorTell on incoming connection")
                                                                    }
                                                                }
                                                            } else {
                                                                warn!(peer = %peer_addr, "No actor message handler registered in registry");
                                                            }
                                                        } else {
                                                            warn!(peer = %peer_addr, "Registry weak reference could not be upgraded");
                                                        }
                                                    } else {
                                                        warn!(peer = %peer_addr, "No registry weak reference available");
                                                    }
                                                } else {
                                                    warn!(peer = %peer_addr, expected = 24 + payload_len, actual = payload.len(),
                                                      "ActorTell payload too short");
                                                }
                                            } else {
                                                warn!(peer = %peer_addr, payload_len = payload.len(), "ActorTell header too short (need at least 24)");
                                            }
                                        }
                                        crate::MessageType::ActorAsk => {
                                            // Direct actor ask message format (from kameo):
                                            // Full header: [type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
                                            // After ASK_RESPONSE_HEADER_LEN (4) slice, payload starts with reserved(8)
                                            // Actor fields start at offset 8 into payload (skipping 8 bytes of reserved)
                                            if payload.len() >= 24 {
                                                // Skip 8 reserved bytes, then read actor fields
                                                let actor_id = u64::from_be_bytes(
                                                    payload[8..16].try_into().unwrap(),
                                                );
                                                let type_hash = u32::from_be_bytes(
                                                    payload[16..20].try_into().unwrap(),
                                                );
                                                let payload_len = u32::from_be_bytes(
                                                    payload[20..24].try_into().unwrap(),
                                                )
                                                    as usize;

                                                if payload.len() >= 24 + payload_len {
                                                    let actor_payload =
                                                        &payload[24..24 + payload_len];

                                                    if let Some(ref registry_weak) = registry_weak {
                                                        if let Some(registry) =
                                                            registry_weak.upgrade()
                                                        {
                                                            match registry
                                                                .handle_actor_message_id(
                                                                    actor_id,
                                                                    type_hash,
                                                                    actor_payload,
                                                                    Some(correlation_id),
                                                                )
                                                                .await
                                                            {
                                                                Ok(Some(reply_payload)) => {
                                                                    let reply_len =
                                                                        reply_payload.len();
                                                                    let header =
                                                                    framing::write_ask_response_header(
                                                                        crate::MessageType::Response,
                                                                        correlation_id,
                                                                        reply_len,
                                                                    );
                                                                    let payload = reply_payload;

                                                                    if let Some(ref handle) =
                                                                        response_handle
                                                                    {
                                                                        if let Err(e) = handle
                                                                        .write_header_and_payload_control_inline(
                                                                            header,
                                                                            16,
                                                                            payload.clone(),
                                                                        )
                                                                        .await
                                                                    {
                                                                        warn!(peer = %peer_addr, error = %e, "Failed to send ActorAsk response");
                                                                    } else {
                                                                        debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ActorAsk response");
                                                                    }
                                                                    } else {
                                                                        let pool = registry
                                                                            .connection_pool
                                                                            .lock()
                                                                            .await;
                                                                        if let Some(conn) = pool
                                                                            .connections_by_addr
                                                                            .get(&peer_addr)
                                                                            .map(|c| {
                                                                                c.value().clone()
                                                                            })
                                                                        {
                                                                            if let Some(
                                                                                ref stream_handle,
                                                                            ) =
                                                                                conn.stream_handle
                                                                            {
                                                                                if let Err(e) = stream_handle
                                                                                .write_header_and_payload_control_inline(
                                                                                    header,
                                                                                    16,
                                                                                    payload.clone(),
                                                                                )
                                                                                .await
                                                                            {
                                                                                warn!(peer = %peer_addr, error = %e, "Failed to send ActorAsk response");
                                                                            } else {
                                                                                debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent ActorAsk response through connection pool");
                                                                            }
                                                                            } else {
                                                                                warn!(peer = %peer_addr, "Connection has no stream handle");
                                                                            }
                                                                        } else {
                                                                            warn!(peer = %peer_addr, "No connection found in pool for ActorAsk reply");
                                                                        }
                                                                    }
                                                                }
                                                                Ok(None) => {
                                                                    debug!(peer = %peer_addr, correlation_id = correlation_id, "ActorAsk handled with no reply");
                                                                }
                                                                Err(e) => {
                                                                    warn!(peer = %peer_addr, error = %e, correlation_id = correlation_id, "Failed to handle ActorAsk");
                                                                }
                                                            }
                                                        } else {
                                                            warn!(peer = %peer_addr, "Registry weak reference could not be upgraded");
                                                        }
                                                    } else {
                                                        warn!(peer = %peer_addr, "No registry weak reference available");
                                                    }
                                                } else {
                                                    warn!(peer = %peer_addr, expected = 24 + payload_len, actual = payload.len(),
                                                      "ActorAsk payload too short");
                                                }
                                            } else {
                                                warn!(peer = %peer_addr, payload_len = payload.len(), "ActorAsk header too short (need at least 24)");
                                            }
                                        }
                                    }

                                    // if len > 1024 * 1024 {
                                    //     eprintln!("📦 SERVER: Drained {} bytes from buffer after processing large message", total_len);
                                    // }
                                    continue;
                                }
                            }

                            // This is a gossip protocol message

                            // Verify alignment (panic in production, allow in tests)
                            #[cfg(not(test))]
                            {
                                let required_alignment = std::mem::align_of::<
                                    <crate::registry::RegistryMessage as rkyv::Archive>::Archived,
                                >();
                                let is_aligned =
                                    (msg_data.as_ptr() as usize).is_multiple_of(required_alignment);
                                assert!(
                                    is_aligned,
                                    "Gossip payload must be {}-byte aligned! Pointer: {:p}, offset: {}",
                                    required_alignment,
                                    msg_data.as_ptr(),
                                    msg_data.as_ptr() as usize
                                );
                            }

                            // Process message
                            if let Some(ref registry_weak) = registry_weak {
                                if let Some(registry) = registry_weak.upgrade() {
                                    let frame_bytes = msg_data.clone();
                                    if let Ok(frame) =
                                        crate::registry::RegistryMessageFrame::new(frame_bytes)
                                    {
                                        if let Err(e) = handle_incoming_message(
                                            registry, peer_addr, frame, None,
                                        )
                                        .await
                                        {
                                            warn!(peer = %peer_addr, error = %e, "Failed to handle message");
                                        }
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "Read error");
                    break;
                }
            }
        }

        // Handle peer failure when connection is lost
        info!(peer = %peer_addr, "CONNECTION_POOL: Triggering peer failure handling");
        if let Some(ref registry_weak) = registry_weak {
            if let Some(registry) = registry_weak.upgrade() {
                if let Err(e) = registry.handle_peer_connection_failure(peer_addr).await {
                    warn!(error = %e, peer = %peer_addr, "CONNECTION_POOL: Failed to handle peer connection failure");
                }
            }
        }
    }

    async fn process_reader_streaming_frame<R>(
        registry: &Arc<GossipRegistry>,
        assembler: &mut streaming::StreamAssembler,
        reader: &mut R,
        peer_addr: SocketAddr,
        frame: crate::handle::StreamingFrame,
        correlation_id: u16,
    ) where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        let correlation = if correlation_id == 0 {
            None
        } else {
            Some(correlation_id)
        };

        match frame {
            crate::handle::StreamingFrame::Start { descriptor } => {
                if let Err(err) = assembler.start_stream(descriptor, correlation) {
                    warn!(peer = %peer_addr, error = %err, "Failed to start streaming session");
                }
            }
            crate::handle::StreamingFrame::Data { payload } => match payload {
                crate::handle::StreamingData::Direct { chunk_len } => {
                    match assembler.read_data_direct(reader, chunk_len).await {
                        Ok(Some(complete)) => {
                            Self::deliver_streaming_completion(registry, peer_addr, complete).await;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(peer = %peer_addr, error = %err, "Streaming direct read error");
                            if let Some(descriptor) = assembler.abort_if_stale(Duration::ZERO) {
                                warn!(
                                    peer = %peer_addr,
                                    stream_id = descriptor.stream_id,
                                    "Streaming state reset after direct read error"
                                );
                            }
                        }
                    }
                }
            },
            crate::handle::StreamingFrame::End => match assembler.finish_with_end() {
                Ok(Some(complete)) => {
                    Self::deliver_streaming_completion(registry, peer_addr, complete).await;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(peer = %peer_addr, error = %err, "Streaming end frame error");
                    if let Some(descriptor) = assembler.abort_if_stale(Duration::ZERO) {
                        warn!(
                            peer = %peer_addr,
                            stream_id = descriptor.stream_id,
                            "Streaming state reset after end error"
                        );
                    }
                }
            },
        }
    }

    async fn deliver_streaming_completion(
        registry: &Arc<GossipRegistry>,
        peer_addr: SocketAddr,
        completion: streaming::CompletedStream,
    ) {
        let actor_id_str = completion.descriptor.actor_id.to_string();
        match registry
            .handle_actor_message(
                &actor_id_str,
                completion.descriptor.type_hash,
                &completion.payload,
                completion.correlation_id,
            )
            .await
        {
            Ok(Some(reply_payload)) => {
                if let Some(corr_id) = completion.correlation_id {
                    let reply_len = reply_payload.len();
                    let header = framing::write_ask_response_header(
                        crate::MessageType::Response,
                        corr_id,
                        reply_len,
                    );
                    let pool = registry.connection_pool.lock().await;
                    if let Some(conn) = pool
                        .connections_by_addr
                        .get(&peer_addr)
                        .map(|c| c.value().clone())
                    {
                        if let Some(ref stream_handle) = conn.stream_handle {
                            if let Err(e) = stream_handle
                                .write_header_and_payload_control_inline(
                                    header,
                                    crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN as u8,
                                    reply_payload,
                                )
                                .await
                            {
                                warn!(peer = %peer_addr, error = %e, "Failed to send streaming response");
                            }
                        } else {
                            warn!(peer = %peer_addr, "Connection has no stream handle for streaming response");
                        }
                    } else {
                        warn!(peer = %peer_addr, "No connection found in pool for streaming response");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    peer = %peer_addr,
                    error = %e,
                    "Failed to handle streaming actor payload"
                );
            }
        }
    }
}

/// Resolve the peer state address for a sender.
async fn resolve_peer_state_addr(
    registry: &GossipRegistry,
    sender_peer_id: Option<&crate::PeerId>,
    socket_addr: SocketAddr,
) -> SocketAddr {
    if let Some(peer_id) = sender_peer_id {
        if let Some(addr) = {
            let pool = registry.connection_pool.lock().await;
            pool.peer_id_to_addr
                .get(peer_id)
                .map(|entry| *entry.value())
                .filter(|addr| addr.port() != 0)
        } {
            return addr;
        }

        if let Some(addr) = registry.lookup_advertised_addr(&peer_id.to_node_id()).await {
            return addr;
        }
    }

    if let Some(node_id) = registry
        .peer_capability_addr_to_node
        .get(&socket_addr)
        .map(|entry| *entry.value())
    {
        if let Some(addr) = registry.lookup_advertised_addr(&node_id).await {
            return addr;
        }
    }

    socket_addr
}

async fn dispatch_actor_message_zero_copy<F, Fut>(
    registry: &Arc<GossipRegistry>,
    actor_id: &str,
    type_hash: u32,
    payload: &[u8],
    correlation_id: Option<u16>,
    mut send_reply: F,
) -> Result<bool>
where
    F: FnMut(bytes::Bytes, u16) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    match registry
        .handle_actor_message(actor_id, type_hash, payload, correlation_id)
        .await
    {
        Ok(Some(reply_payload)) => {
            if let Some(corr_id) = correlation_id {
                send_reply(reply_payload, corr_id).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Ok(None) => Ok(false),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Copy, Clone)]
enum RegistryMessageKind {
    DeltaGossip,
    DeltaGossipResponse,
    FullSync,
    FullSyncResponse,
    FullSyncRequest,
    PeerHealthQuery,
    PeerHealthReport,
    ActorMessage,
    ImmediateAck,
    PeerListGossip,
}

type ArchivedRegistryMessage = <RegistryMessage as Archive>::Archived;
type ArchivedRegistryChange = <crate::registry::RegistryChange as Archive>::Archived;

fn registry_message_kind(frame: &RegistryMessageFrame) -> Result<RegistryMessageKind> {
    let archived = frame.archived()?;
    Ok(match archived {
        ArchivedRegistryMessage::DeltaGossip { .. } => RegistryMessageKind::DeltaGossip,
        ArchivedRegistryMessage::DeltaGossipResponse { .. } => {
            RegistryMessageKind::DeltaGossipResponse
        }
        ArchivedRegistryMessage::FullSync { .. } => RegistryMessageKind::FullSync,
        ArchivedRegistryMessage::FullSyncResponse { .. } => RegistryMessageKind::FullSyncResponse,
        ArchivedRegistryMessage::FullSyncRequest { .. } => RegistryMessageKind::FullSyncRequest,
        ArchivedRegistryMessage::PeerHealthQuery { .. } => RegistryMessageKind::PeerHealthQuery,
        ArchivedRegistryMessage::PeerHealthReport { .. } => RegistryMessageKind::PeerHealthReport,
        ArchivedRegistryMessage::ActorMessage { .. } => RegistryMessageKind::ActorMessage,
        ArchivedRegistryMessage::ImmediateAck { .. } => RegistryMessageKind::ImmediateAck,
        ArchivedRegistryMessage::PeerListGossip { .. } => RegistryMessageKind::PeerListGossip,
    })
}

fn archived_peer_id(archived: &<crate::PeerId as Archive>::Archived) -> Result<crate::PeerId> {
    Ok(crate::rkyv_utils::deserialize_archived::<crate::PeerId>(
        archived,
    )?)
}

fn archived_node_id(archived: &<crate::NodeId as Archive>::Archived) -> Result<crate::NodeId> {
    Ok(crate::rkyv_utils::deserialize_archived::<crate::NodeId>(
        archived,
    )?)
}

fn archived_priority_is_immediate(
    priority: &<crate::RegistrationPriority as Archive>::Archived,
) -> bool {
    crate::rkyv_utils::deserialize_archived::<crate::RegistrationPriority>(priority)
        .map(|p| p.should_trigger_immediate_gossip())
        .unwrap_or(false)
}

fn archived_option_str(
    value: &rkyv::option::ArchivedOption<rkyv::string::ArchivedString>,
) -> Option<&str> {
    match value {
        rkyv::option::ArchivedOption::Some(inner) => Some(inner.as_str()),
        rkyv::option::ArchivedOption::None => None,
    }
}

fn compare_archived_vector_clock(
    archived: &<crate::VectorClock as Archive>::Archived,
    existing: &crate::VectorClock,
) -> crate::ClockOrdering {
    use std::cmp::Ordering;

    let mut incoming_greater = false;
    let mut existing_greater = false;

    let incoming_clock_for = |node: &crate::NodeId| -> u64 {
        for entry in archived.clocks.iter() {
            let arch_node = &entry.0;
            let arch_clock = &entry.1;
            if let Ok(node_id) = crate::rkyv_utils::deserialize_archived::<crate::NodeId>(arch_node)
            {
                if &node_id == node {
                    return u64::from(*arch_clock);
                }
            }
        }
        0
    };

    for entry in archived.clocks.iter() {
        let arch_node = &entry.0;
        let arch_clock = &entry.1;
        let Ok(node_id) = crate::rkyv_utils::deserialize_archived::<crate::NodeId>(arch_node)
        else {
            continue;
        };
        let incoming_clock = u64::from(*arch_clock);
        let existing_clock = existing.get(&node_id);
        match incoming_clock.cmp(&existing_clock) {
            Ordering::Greater => incoming_greater = true,
            Ordering::Less => existing_greater = true,
            Ordering::Equal => {}
        }
    }

    for (node_id, existing_clock) in existing.iter_entries() {
        let incoming_clock = incoming_clock_for(&node_id);
        match existing_clock.cmp(&incoming_clock) {
            Ordering::Greater => existing_greater = true,
            Ordering::Less => incoming_greater = true,
            Ordering::Equal => {}
        }
    }

    match (incoming_greater, existing_greater) {
        (true, false) => crate::ClockOrdering::After,
        (false, true) => crate::ClockOrdering::Before,
        (false, false) => crate::ClockOrdering::Equal,
        (true, true) => crate::ClockOrdering::Concurrent,
    }
}

fn hash_archived_vector_clock<H: std::hash::Hasher>(
    archived: &<crate::VectorClock as Archive>::Archived,
    state: &mut H,
) {
    use std::hash::Hash;

    for entry in archived.clocks.iter() {
        let arch_node = &entry.0;
        let arch_clock = &entry.1;
        if let Ok(node_id) = crate::rkyv_utils::deserialize_archived::<crate::NodeId>(arch_node) {
            node_id.hash(state);
            let clock = u64::from(*arch_clock);
            clock.hash(state);
        }
    }
}

fn collect_actor_map_from_archived(
    entries: &<Vec<(String, crate::RemoteActorLocation)> as Archive>::Archived,
) -> Result<HashMap<String, crate::RemoteActorLocation>> {
    let mut map = HashMap::with_capacity(entries.len());
    for entry in entries.iter() {
        let name = &entry.0;
        let location = &entry.1;
        let name = name.as_str().to_string();
        let location =
            crate::rkyv_utils::deserialize_archived::<crate::RemoteActorLocation>(location)?;
        map.insert(name, location);
    }
    Ok(map)
}

fn peer_health_status_from_archived(
    status: &<crate::registry::PeerHealthStatus as Archive>::Archived,
) -> crate::registry::PeerHealthStatus {
    crate::registry::PeerHealthStatus {
        is_alive: status.is_alive,
        last_contact: u64::from(status.last_contact),
        failure_count: u32::from(status.failure_count),
    }
}

/// Handle an incoming message on a bidirectional connection
pub(crate) fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
    frame: RegistryMessageFrame,
    ask_correlation_id: Option<u16>,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        let kind = match registry_message_kind(&frame) {
            Ok(kind) => kind,
            Err(err) => {
                warn!(error = %err, "Failed to access archived RegistryMessage frame");
                return Err(err);
            }
        };

        match kind {
            RegistryMessageKind::DeltaGossip => {
                let sender_peer_id = {
                    let archived = frame.archived()?;
                    let delta = match archived {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    archived_peer_id(&delta.sender_peer_id)?
                };

                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&sender_peer_id), _peer_addr).await;

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let since_sequence = u64::from(delta.since_sequence);
                    let current_sequence = u64::from(delta.current_sequence);
                    let change_count = delta.changes.len();

                    debug!(
                        sender = %sender_peer_id,
                        since_sequence = since_sequence,
                        changes = change_count,
                        "received delta gossip message on bidirectional connection"
                    );

                    // Add the sender as a peer (inlined to avoid separate lock)
                    if sender_peer_id != registry.peer_id {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            let current_time = crate::current_timestamp();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: None,
                                node_id: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                            });
                        }
                    }

                    // Check if this is a previously failed peer
                    let was_failed = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures >= registry.config.max_peer_failures)
                        .unwrap_or(false);

                    if was_failed {
                        info!(
                            peer = %sender_peer_id,
                            "✅ Received delta from previously failed peer - connection restored!"
                        );

                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    // Update peer info and check if we need to clear pending failures
                    let need_to_clear_pending =
                        if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                            // Always reset failure state when we receive messages from the peer
                            // This proves the peer is alive and communicating
                            let had_failures = peer_info.failures > 0;
                            if had_failures {
                                info!(peer = %sender_peer_id,
                              prev_failures = peer_info.failures,
                              "🔄 Resetting failure state after receiving DeltaGossip");
                                peer_info.failures = 0;
                                peer_info.last_failure_time = None;
                            }
                            peer_info.last_success = crate::current_timestamp();

                            peer_info.last_sequence =
                                std::cmp::max(peer_info.last_sequence, current_sequence);
                            peer_info.consecutive_deltas += 1;

                            had_failures
                        } else {
                            false
                        };

                    // Clear pending failure record if needed
                    if need_to_clear_pending {
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }
                    gossip_state.delta_exchanges += 1;
                }

                // CRITICAL OPTIMIZATION: Inline apply_delta to eliminate function call overhead
                // Apply the delta directly here to minimize async scheduling delays
                let (total_changes, has_immediate) = {
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let total_changes = delta.changes.len();
                    let has_immediate = delta.changes.iter().any(|change| match change {
                        ArchivedRegistryChange::ActorAdded { priority, .. } => {
                            archived_priority_is_immediate(priority)
                        }
                        ArchivedRegistryChange::ActorRemoved { priority, .. } => {
                            archived_priority_is_immediate(priority)
                        }
                    });
                    (total_changes, has_immediate)
                };

                if has_immediate {
                    info!(
                        "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                        total_changes, sender_socket_addr
                    );
                }

                // Pre-capture timing info outside lock for better performance - use high resolution timing
                let _received_instant = std::time::Instant::now();
                let received_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();

                // Collect immediate actors for ACK before consuming changes
                let immediate_actors = {
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let mut immediate_actors = Vec::new();
                    for change in delta.changes.iter() {
                        if let ArchivedRegistryChange::ActorAdded { name, priority, .. } = change {
                            if archived_priority_is_immediate(priority) {
                                immediate_actors.push(name.as_str().to_string());
                            }
                        }
                    }
                    immediate_actors
                };

                // OPTIMIZATION: Fast-path state updates with try_lock to avoid blocking
                let applied_count = if let Ok(mut actor_state) = registry.actor_state.try_write() {
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let precise_timing_nanos = u64::from(delta.precise_timing_nanos);
                    let mut applied = 0;

                    for change in delta.changes.iter() {
                        match change {
                            ArchivedRegistryChange::ActorAdded {
                                name,
                                location,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Don't override local actors
                                if actor_state.local_actors.contains_key(name_str) {
                                    continue;
                                }

                                // Quick conflict check
                                let should_apply = match actor_state.known_actors.get(name_str) {
                                    Some(existing) => {
                                        let incoming_wall = u64::from(location.wall_clock_time);
                                        incoming_wall > existing.wall_clock_time
                                            || (incoming_wall == existing.wall_clock_time
                                                && location.address.as_str()
                                                    > existing.address.as_str())
                                    }
                                    None => true,
                                };

                                if should_apply {
                                    let owned_location =
                                        crate::rkyv_utils::deserialize_archived::<
                                            crate::RemoteActorLocation,
                                        >(location)?;
                                    let owned_name = name_str.to_string();
                                    actor_state
                                        .known_actors
                                        .insert(owned_name.clone(), owned_location.clone());
                                    applied += 1;

                                    // Inline timing calculation for immediate priority
                                    if owned_location.priority.should_trigger_immediate_gossip() {
                                        let network_time_nanos =
                                            received_timestamp - precise_timing_nanos as u128;
                                        let network_time_ms =
                                            network_time_nanos as f64 / 1_000_000.0;
                                        let propagation_time_ms = network_time_ms; // Same as network time for now
                                        let processing_only_time_ms = 0.0; // No additional processing time beyond network

                                        eprintln!("🔍 FAST_PATH: Processing immediate priority actor: {} propagation_time_ms={:.3}ms", owned_name, propagation_time_ms);

                                        info!(
                                            actor_name = %owned_name,
                                            priority = ?owned_location.priority,
                                            propagation_time_ms = propagation_time_ms,
                                            network_processing_time_ms = network_time_ms,
                                            processing_only_time_ms = processing_only_time_ms,
                                            "RECEIVED_ACTOR"
                                        );
                                    }
                                }
                            }
                            ArchivedRegistryChange::ActorRemoved {
                                name,
                                vector_clock,
                                removing_node_id,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Check vector clock ordering before applying removal
                                let should_remove = match actor_state.known_actors.get(name_str) {
                                    Some(existing_location) => match compare_archived_vector_clock(
                                        vector_clock,
                                        &existing_location.vector_clock,
                                    ) {
                                        crate::ClockOrdering::After => true,
                                        crate::ClockOrdering::Concurrent => {
                                            // For concurrent removals, use node_id as deterministic tiebreaker
                                            // This ensures all nodes make the same decision
                                            let removing_node_id =
                                                archived_node_id(removing_node_id)?;
                                            removing_node_id > existing_location.node_id
                                        }
                                        _ => false, // Ignore outdated removals (Before or Equal)
                                    },
                                    None => false, // Actor doesn't exist
                                };

                                if should_remove
                                    && actor_state.known_actors.remove(name_str).is_some()
                                {
                                    applied += 1;
                                }
                            }
                        }
                    }

                    applied
                } else {
                    // Fallback to write lock if contended
                    let mut actor_state = registry.actor_state.write().await;
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossip { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let precise_timing_nanos = u64::from(delta.precise_timing_nanos);
                    let mut applied = 0;

                    for change in delta.changes.iter() {
                        match change {
                            ArchivedRegistryChange::ActorAdded {
                                name,
                                location,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Don't override local actors - early exit
                                if actor_state.local_actors.contains_key(name_str) {
                                    debug!(
                                        actor_name = %name_str,
                                        "skipping remote actor update - actor is local"
                                    );
                                    continue;
                                }

                                // Check if we already know about this actor
                                let should_apply = match actor_state.known_actors.get(name_str) {
                                    Some(existing_location) => {
                                        // Use vector clock for causal ordering
                                        match compare_archived_vector_clock(
                                            &location.vector_clock,
                                            &existing_location.vector_clock,
                                        ) {
                                            crate::ClockOrdering::After => true,
                                            crate::ClockOrdering::Concurrent => {
                                                // For concurrent updates, use node_id as tiebreaker
                                                let incoming_node_id =
                                                    archived_node_id(&location.node_id)?;
                                                incoming_node_id > existing_location.node_id
                                            }
                                            _ => false, // Keep existing for Before or Equal
                                        }
                                    }
                                    None => {
                                        debug!(
                                            actor_name = %name_str,
                                            "applying new actor"
                                        );
                                        true // New actor
                                    }
                                };

                                if should_apply {
                                    let owned_location =
                                        crate::rkyv_utils::deserialize_archived::<
                                            crate::RemoteActorLocation,
                                        >(location)?;
                                    let owned_name = name_str.to_string();
                                    actor_state
                                        .known_actors
                                        .insert(owned_name.clone(), owned_location.clone());
                                    applied += 1;

                                    // Log the timing information for immediate priority changes
                                    if owned_location.priority.should_trigger_immediate_gossip() {
                                        // Calculate time from when delta was sent (network + processing)
                                        let network_processing_time_nanos =
                                            received_timestamp - precise_timing_nanos as u128;
                                        let network_processing_time_ms =
                                            network_processing_time_nanos as f64 / 1_000_000.0;
                                        let propagation_time_ms = network_processing_time_ms; // Same as network time for now
                                        let processing_only_time_ms = 0.0; // No additional processing time beyond network

                                        // Debug: Break down where the time is spent
                                        eprintln!("🔍 TIMING_BREAKDOWN: sent={}, received={}, delta={}ns ({}ms)",
                                     precise_timing_nanos, received_timestamp,
                                     network_processing_time_nanos, network_processing_time_ms);

                                        info!(
                                            actor_name = %owned_name,
                                            priority = ?owned_location.priority,
                                            propagation_time_ms = propagation_time_ms,
                                            network_processing_time_ms = network_processing_time_ms,
                                            processing_only_time_ms = processing_only_time_ms,
                                            "RECEIVED_ACTOR"
                                        );
                                    }
                                }
                            }
                            ArchivedRegistryChange::ActorRemoved {
                                name,
                                vector_clock,
                                removing_node_id,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Check vector clock ordering before applying removal
                                let should_remove = match actor_state.known_actors.get(name_str) {
                                    Some(existing_location) => {
                                        match compare_archived_vector_clock(
                                            vector_clock,
                                            &existing_location.vector_clock,
                                        ) {
                                            crate::ClockOrdering::After => true,
                                            crate::ClockOrdering::Concurrent => {
                                                // For concurrent removals, use node_id as deterministic tiebreaker
                                                // This ensures all nodes make the same decision
                                                let removing_node_id =
                                                    archived_node_id(removing_node_id)?;
                                                removing_node_id > existing_location.node_id
                                            }
                                            _ => false, // Ignore outdated removals (Before or Equal)
                                        }
                                    }
                                    None => false, // Actor doesn't exist
                                };

                                if should_remove
                                    && actor_state.known_actors.remove(name_str).is_some()
                                {
                                    applied += 1;
                                }
                            }
                        }
                    }

                    applied
                };

                if applied_count > 0 {
                    debug!(
                        applied_count = applied_count,
                        sender = %sender_socket_addr,
                        "applied delta changes in batched update"
                    );
                }

                // NEW: Send ACK back for immediate registrations
                if !immediate_actors.is_empty() {
                    // Send ACKs for immediate priority actor additions
                    // Use lock-free send since we're responding on the same connection
                    for actor_name in immediate_actors {
                        // Send lightweight ACK immediately
                        let ack = crate::registry::RegistryMessage::ImmediateAck {
                            actor_name: actor_name.clone(),
                            success: true,
                        };

                        // Serialize and send
                        if let Ok(serialized) = rkyv::to_bytes::<rkyv::rancor::Error>(&ack) {
                            let mut pool = registry.connection_pool.lock().await;
                            let buffer = pool.create_message_buffer(&serialized);
                            // Use send_lock_free to send directly without needing a connection handle
                            if let Err(e) = pool.send_lock_free(sender_socket_addr, &buffer) {
                                warn!("Failed to send ImmediateAck: {}", e);
                            } else {
                                info!("Sent ImmediateAck for actor '{}'", actor_name);
                            }
                        }
                    }
                }

                // Note: Response will be sent during regular gossip rounds
                Ok(())
            }
            RegistryMessageKind::FullSync => {
                let (
                    sender_peer_id,
                    sender_socket_addr,
                    local_len,
                    known_len,
                    sequence,
                    wall_clock_time,
                ) = {
                    let archived = frame.archived()?;
                    let (
                        local_actors,
                        known_actors,
                        sender_peer_id,
                        sender_bind_addr,
                        sequence,
                        wall_clock_time,
                    ) = match archived {
                        ArchivedRegistryMessage::FullSync {
                            local_actors,
                            known_actors,
                            sender_peer_id,
                            sender_bind_addr,
                            sequence,
                            wall_clock_time,
                        } => (
                            local_actors,
                            known_actors,
                            sender_peer_id,
                            sender_bind_addr,
                            sequence,
                            wall_clock_time,
                        ),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let sender_peer_id = archived_peer_id(sender_peer_id)?;
                    let sender_socket_addr =
                        resolve_peer_addr(archived_option_str(sender_bind_addr), _peer_addr);
                    let local_len = local_actors.len();
                    let known_len = known_actors.len();
                    let sequence = u64::from(sequence);
                    let wall_clock_time = u64::from(wall_clock_time);
                    (
                        sender_peer_id,
                        sender_socket_addr,
                        local_len,
                        known_len,
                        sequence,
                        wall_clock_time,
                    )
                };

                // Use resolve_peer_addr for safe address resolution with validation
                // This handles: None, invalid addresses, 0.0.0.0, and falls back to TCP source
                debug!(
                    "Received FullSync from node '{}' at bind_addr {} (tcp_source={})",
                    sender_peer_id, sender_socket_addr, _peer_addr
                );

                // OPTIMIZATION: Do all peer management in one lock acquisition
                {
                    let mut gossip_state = registry.gossip_state.lock().await;

                    // FIX: If the resolved bind address differs from the TCP source address,
                    // migrate the PeerInfo from the ephemeral port entry to the bind address.
                    // This preserves node_id, sequence, and failure state learned during TLS handshake.
                    if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                        if let Some(mut old_peer_info) = gossip_state.peers.remove(&_peer_addr) {
                            info!(
                                old_addr = %_peer_addr,
                                new_addr = %sender_socket_addr,
                                node_id = ?old_peer_info.node_id,
                                "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSync"
                            );
                            // Update the address field and preserve the connection address
                            old_peer_info.address = sender_socket_addr;
                            old_peer_info.peer_address = Some(_peer_addr);
                            // Insert with new key (bind address), preserving all state
                            gossip_state.peers.insert(sender_socket_addr, old_peer_info);
                            // Also clean up pending failures for the old address
                            gossip_state.pending_peer_failures.remove(&_peer_addr);
                        }
                    }

                    // Add the sender as a peer if not already present (inlined to avoid separate lock)
                    if sender_socket_addr != registry.bind_addr {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            gossip_state.peers.entry(sender_socket_addr)
                        {
                            info!(peer = %sender_socket_addr, "Adding new peer from FullSync");
                            let current_time = crate::current_timestamp();
                            e.insert(crate::registry::PeerInfo {
                                address: sender_socket_addr,
                                peer_address: Some(_peer_addr), // Remember the actual connection address
                                node_id: None,
                                failures: 0,
                                last_attempt: current_time,
                                last_success: current_time,
                                last_sequence: 0,
                                last_sent_sequence: 0,
                                consecutive_deltas: 0,
                                last_failure_time: None,
                            });
                        }
                    }

                    // Update peer info and reset failure state
                    let had_failures = gossip_state
                        .peers
                        .get(&sender_socket_addr)
                        .map(|info| info.failures > 0)
                        .unwrap_or(false);

                    if had_failures {
                        // Clear the pending failure record
                        gossip_state
                            .pending_peer_failures
                            .remove(&sender_socket_addr);
                    }

                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let prev_failures = peer_info.failures;
                        // Always reset failure state when we receive a FullSync from the peer
                        // This proves the peer is alive and communicating
                        if peer_info.failures > 0 {
                            info!(peer = %sender_socket_addr,
                              prev_failures = prev_failures,
                              "🔄 Resetting failure state after receiving FullSync");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        peer_info.consecutive_deltas = 0;
                    } else {
                        warn!(peer = %sender_socket_addr, "Peer not found in peer list when trying to reset failure state");
                    }
                    gossip_state.full_sync_exchanges += 1;
                }

                debug!(
                    sender = %sender_peer_id,
                    sequence = sequence,
                    local_actors = local_len,
                    known_actors = known_len,
                    "📨 INCOMING: Received full sync message on bidirectional connection"
                );

                // IMPORTANT: Register the incoming connection with the peer_id mapping
                // This allows bidirectional communication to work properly
                {
                    let pool = registry.connection_pool.lock().await;

                    // NOTE: Do NOT remove addr_to_peer_id for the ephemeral address here.
                    // The reindex_connection_addr function preserves both addresses,
                    // and disconnect_connection_by_peer_id needs both entries to clean up properly.

                    pool.peer_id_to_addr
                        .insert(sender_peer_id.clone(), sender_socket_addr);
                    pool.addr_to_peer_id
                        .insert(sender_socket_addr, sender_peer_id.clone());

                    // CRITICAL FIX: Reindex the connection from ephemeral TCP port to bind address
                    // Without this, get_connection(bind_addr) fails because the connection is
                    // still indexed under the ephemeral port the peer connected FROM.
                    // This allows messages to be sent back to the peer using their advertised address.
                    // Note: reindex_connection_addr already has early-return if already indexed,
                    // and logs internally when it actually does work.
                    if sender_socket_addr != _peer_addr {
                        pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                    }

                    debug!(
                        "BIDIRECTIONAL: Registered incoming connection - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // Only remaining async operation
                let (local_map, known_map) = {
                    let archived = frame.archived()?;
                    let (local_actors, known_actors) = match archived {
                        ArchivedRegistryMessage::FullSync {
                            local_actors,
                            known_actors,
                            ..
                        } => (local_actors, known_actors),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    (
                        collect_actor_map_from_archived(local_actors)?,
                        collect_actor_map_from_archived(known_actors)?,
                    )
                };

                registry
                    .merge_full_sync(
                        local_map,
                        known_map,
                        sender_socket_addr,
                        sequence,
                        wall_clock_time,
                    )
                    .await;

                // Send back our state as a response so the sender can receive our actors
                // This is critical for late-joining nodes (like Node C) to get existing state
                {
                    // Get our current state
                    let (our_local_actors, our_known_actors, our_sequence) = {
                        let actor_state = registry.actor_state.read().await;
                        let gossip_state = registry.gossip_state.lock().await;
                        (
                            actor_state.local_actors.clone(),
                            actor_state.known_actors.clone(),
                            gossip_state.gossip_sequence,
                        )
                    };

                    // Calculate sizes before moving
                    let local_actors_count = our_local_actors.len();
                    let known_actors_count = our_known_actors.len();

                    // Create a FullSyncResponse message
                    let response = RegistryMessage::FullSyncResponse {
                        local_actors: our_local_actors.into_iter().collect(),
                        known_actors: our_known_actors.into_iter().collect(),
                        sender_peer_id: registry.peer_id.clone(), // Use peer ID
                        sender_bind_addr: Some(registry.bind_addr.to_string()), // Our listening address
                        sequence: our_sequence,
                        wall_clock_time: crate::current_timestamp(),
                    };

                    // Send the response back through existing connection
                    // We'll use send_lock_free which doesn't create new connections
                    let response_data = match rkyv::to_bytes::<rkyv::rancor::Error>(&response) {
                        Ok(data) => data,
                        Err(e) => {
                            warn!(error = %e, "Failed to serialize FullSync response");
                            return Ok(());
                        }
                    };

                    // Try to send immediately on existing connection
                    {
                        debug!(
                            "FULLSYNC RESPONSE: Node {} is about to acquire connection pool lock",
                            registry.bind_addr
                        );
                        let mut pool = registry.connection_pool.lock().await;
                        debug!(
                            "FULLSYNC RESPONSE: Node {} got pool lock, pool has {} total entries",
                            registry.bind_addr,
                            pool.connection_count()
                        );
                        debug!("FULLSYNC RESPONSE: Pool instance address: {:p}", &*pool);

                        // Log details about each connection
                        for entry in pool.connections_by_addr.iter() {
                            let addr = entry.key();
                            let conn = entry.value();
                            debug!(
                                "FULLSYNC RESPONSE: Connection to {} - state={:?}",
                                addr,
                                conn.get_state()
                            );
                        }

                        // Create message with length + type prefix
                        let buffer = pool.create_message_buffer(&response_data);

                        // Debug: Log what connections we have
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Available connections by addr: {:?}",
                            pool.connections_by_addr
                                .iter()
                                .map(|entry| *entry.key())
                                .collect::<Vec<_>>()
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Available node mappings: {:?}",
                            pool.peer_id_to_addr
                                .iter()
                                .map(|entry| (entry.key().clone(), *entry.value()))
                                .collect::<Vec<_>>()
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: Looking for connection to sender_peer_id: {}",
                            sender_peer_id
                        );
                        debug!(
                            "FULLSYNC RESPONSE DEBUG: sender_socket_addr={}",
                            sender_socket_addr
                        );

                        // Try to send using peer ID
                        let frozen_buffer = bytes::Bytes::from(buffer);
                        let send_result = match pool
                            .send_bytes_to_peer_id(&sender_peer_id, frozen_buffer.clone())
                        {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                warn!("Failed to send via peer ID {}: {}", sender_peer_id, e);
                                // Fall back to socket address
                                pool.send_lock_free(sender_socket_addr, &frozen_buffer)
                            }
                        };

                        if send_result.is_err() {
                            warn!(
                                "Primary send failed for peer {}, no fallback available",
                                sender_peer_id
                            );
                        }

                        match send_result {
                            Ok(()) => {
                                debug!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  local_actors = local_actors_count,
                                  known_actors = known_actors_count,
                                  bind_addr = %registry.bind_addr,
                                  "📤 RESPONSE: Successfully sent FullSync response with our state");
                            }
                            Err(e) => {
                                // If we can't send immediately, queue it for the next gossip round
                                warn!(peer = %sender_socket_addr,
                                  peer_id = %sender_peer_id,
                                  error = %e,
                                  "Could not send FullSync response immediately - will be sent in next gossip round");

                                // Store in gossip state to be sent during next gossip round
                                drop(pool); // Release the pool lock first
                                let mut gossip_state = registry.gossip_state.lock().await;

                                // Mark that we need to send a full sync to this peer
                                if let Some(peer_info) =
                                    gossip_state.peers.get_mut(&sender_socket_addr)
                                {
                                    // Force a full sync on the next gossip round
                                    peer_info.consecutive_deltas =
                                        registry.config.max_delta_history as u64;
                                    info!(peer = %sender_socket_addr,
                                      "Marked peer for full sync in next gossip round");
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
            RegistryMessageKind::FullSyncRequest => {
                let sender_peer_id = {
                    let archived = frame.archived()?;
                    let sender_peer_id = match archived {
                        ArchivedRegistryMessage::FullSyncRequest { sender_peer_id, .. } => {
                            sender_peer_id
                        }
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    archived_peer_id(sender_peer_id)?
                };

                debug!(
                    sender = %sender_peer_id,
                    "received full sync request on bidirectional connection"
                );

                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    gossip_state.full_sync_exchanges += 1;
                }

                // Note: Response will be sent during regular gossip rounds
                Ok(())
            }
            RegistryMessageKind::DeltaGossipResponse => {
                let sender_peer_id = {
                    let archived = frame.archived()?;
                    let delta = match archived {
                        ArchivedRegistryMessage::DeltaGossipResponse { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    archived_peer_id(&delta.sender_peer_id)?
                };

                let total_changes = {
                    let archived = frame.archived()?;
                    let delta = match archived {
                        ArchivedRegistryMessage::DeltaGossipResponse { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    delta.changes.len()
                };

                debug!(
                    sender = %sender_peer_id,
                    changes = total_changes,
                    "received delta gossip response on bidirectional connection"
                );

                let has_immediate = {
                    let archived = frame.archived()?;
                    let delta = match archived {
                        ArchivedRegistryMessage::DeltaGossipResponse { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    delta.changes.iter().any(|change| match change {
                        ArchivedRegistryChange::ActorAdded { priority, .. } => {
                            archived_priority_is_immediate(priority)
                        }
                        ArchivedRegistryChange::ActorRemoved { priority, .. } => {
                            archived_priority_is_immediate(priority)
                        }
                    })
                };

                if has_immediate {
                    error!(
                        "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                        total_changes, sender_peer_id
                    );
                }

                let mut peer_actors_added = std::collections::HashSet::new();
                let mut peer_actors_removed = std::collections::HashSet::new();

                let received_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();

                let applied_count = {
                    let mut actor_state = registry.actor_state.write().await;
                    let delta = match frame.archived()? {
                        ArchivedRegistryMessage::DeltaGossipResponse { delta } => delta,
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let precise_timing_nanos = u64::from(delta.precise_timing_nanos);
                    let mut applied = 0;

                    for change in delta.changes.iter() {
                        match change {
                            ArchivedRegistryChange::ActorAdded {
                                name,
                                location,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Don't override local actors - early exit
                                if actor_state.local_actors.contains_key(name_str) {
                                    debug!(
                                        actor_name = %name_str,
                                        "skipping remote actor update - actor is local"
                                    );
                                    continue;
                                }

                                // Check if we already know about this actor
                                let should_apply = match actor_state.known_actors.get(name_str) {
                                    Some(existing_location) => {
                                        // Use vector clock for causal ordering
                                        match compare_archived_vector_clock(
                                            &location.vector_clock,
                                            &existing_location.vector_clock,
                                        ) {
                                            crate::ClockOrdering::After => true,
                                            crate::ClockOrdering::Concurrent => {
                                                // Concurrent updates - use deterministic hash-based tiebreaker
                                                use std::hash::{Hash, Hasher};
                                                let mut hasher1 =
                                                    std::collections::hash_map::DefaultHasher::new(
                                                    );
                                                name_str.hash(&mut hasher1);
                                                location.address.as_str().hash(&mut hasher1);
                                                let incoming_wall =
                                                    u64::from(location.wall_clock_time);
                                                incoming_wall.hash(&mut hasher1);
                                                let hash1 = hasher1.finish();

                                                let mut hasher2 =
                                                    std::collections::hash_map::DefaultHasher::new(
                                                    );
                                                name_str.hash(&mut hasher2);
                                                existing_location.address.hash(&mut hasher2);
                                                existing_location
                                                    .wall_clock_time
                                                    .hash(&mut hasher2);
                                                let hash2 = hasher2.finish();

                                                if hash1 != hash2 {
                                                    hash1 > hash2
                                                } else {
                                                    incoming_wall
                                                        > existing_location.wall_clock_time
                                                }
                                            }
                                            _ => false, // Keep existing for Before or Equal
                                        }
                                    }
                                    None => {
                                        debug!(
                                            actor_name = %name_str,
                                            "applying new actor"
                                        );
                                        true // New actor
                                    }
                                };

                                if should_apply {
                                    // Only clone when actually inserting
                                    let owned_location =
                                        crate::rkyv_utils::deserialize_archived::<
                                            crate::RemoteActorLocation,
                                        >(location)?;
                                    let actor_name = name_str.to_string();
                                    actor_state
                                        .known_actors
                                        .insert(actor_name.clone(), owned_location.clone());
                                    applied += 1;

                                    // Track this actor as belonging to the sender
                                    peer_actors_added.insert(actor_name.clone());

                                    // Move timing calculations outside critical section for logging
                                    if tracing::enabled!(tracing::Level::INFO) {
                                        let propagation_time_nanos = received_timestamp
                                            - owned_location.local_registration_time;
                                        let propagation_time_ms =
                                            propagation_time_nanos as f64 / 1_000_000.0;

                                        // Calculate time from when delta was sent (network + processing)
                                        let network_processing_time_nanos =
                                            received_timestamp - precise_timing_nanos as u128;
                                        let network_processing_time_ms =
                                            network_processing_time_nanos as f64 / 1_000_000.0;

                                        // Calculate pure serialization + processing time (excluding network)
                                        let processing_only_time_ms =
                                            propagation_time_ms - network_processing_time_ms;

                                        info!(
                                            actor_name = %name_str,
                                            priority = ?owned_location.priority,
                                            propagation_time_ms = propagation_time_ms,
                                            network_processing_time_ms = network_processing_time_ms,
                                            processing_only_time_ms = processing_only_time_ms,
                                            "RECEIVED_ACTOR"
                                        );
                                    }
                                }
                            }
                            ArchivedRegistryChange::ActorRemoved {
                                name,
                                vector_clock,
                                removing_node_id,
                                priority: _,
                            } => {
                                let name_str = name.as_str();
                                // Don't remove local actors - early exit
                                if actor_state.local_actors.contains_key(name_str) {
                                    debug!(
                                        actor_name = %name_str,
                                        "skipping actor removal - actor is local"
                                    );
                                    continue;
                                }

                                // Check vector clock ordering before applying removal
                                let should_remove = match actor_state.known_actors.get(name_str) {
                                    Some(existing_location) => {
                                        // Use vector clock for causal ordering
                                        match compare_archived_vector_clock(
                                            vector_clock,
                                            &existing_location.vector_clock,
                                        ) {
                                            crate::ClockOrdering::After => {
                                                // Removal is causally after current state
                                                debug!(
                                                    actor_name = %name_str,
                                                    "removal is causally after current state - applying"
                                                );
                                                true
                                            }
                                            crate::ClockOrdering::Concurrent => {
                                                // For concurrent removals, use deterministic tiebreaker
                                                // Removal should win if it's "newer" based on hash
                                                use std::hash::{Hash, Hasher};
                                                let mut hasher1 =
                                                    std::collections::hash_map::DefaultHasher::new(
                                                    );
                                                name_str.hash(&mut hasher1);
                                                let removing_node_id =
                                                    archived_node_id(removing_node_id)?;
                                                removing_node_id.hash(&mut hasher1);
                                                hash_archived_vector_clock(
                                                    vector_clock,
                                                    &mut hasher1,
                                                );
                                                let hash1 = hasher1.finish();

                                                let mut hasher2 =
                                                    std::collections::hash_map::DefaultHasher::new(
                                                    );
                                                name_str.hash(&mut hasher2);
                                                existing_location.node_id.hash(&mut hasher2);
                                                existing_location.vector_clock.hash(&mut hasher2);
                                                let hash2 = hasher2.finish();

                                                // Removal wins if its hash is greater (deterministic across all nodes)
                                                let should_apply = hash1 > hash2;
                                                debug!(
                                                    actor_name = %name_str,
                                                    removing_node = %removing_node_id.fmt_short(),
                                                    existing_node = %existing_location.node_id.fmt_short(),
                                                    should_apply = should_apply,
                                                    "removal is concurrent with current state - using node_id tiebreaker"
                                                );
                                                should_apply
                                            }
                                            _ => {
                                                // Removal is outdated (Before or Equal) - ignore it
                                                debug!(
                                                    actor_name = %name_str,
                                                    "removal is outdated - ignoring"
                                                );
                                                false
                                            }
                                        }
                                    }
                                    None => {
                                        // Actor doesn't exist, nothing to remove
                                        debug!(actor_name = %name_str, "actor not found - ignoring removal");
                                        false
                                    }
                                };

                                if should_remove
                                    && actor_state.known_actors.remove(name_str).is_some()
                                {
                                    applied += 1;
                                    peer_actors_removed.insert(name_str.to_string());
                                    debug!(actor_name = %name_str, "applied actor removal");
                                }
                            }
                        }
                    }

                    applied
                };

                let peer_actor_changes = peer_actors_added.len() + peer_actors_removed.len();

                if let Some(sender_addr) = {
                    let pool = registry.connection_pool.lock().await;
                    pool.peer_id_to_addr
                        .get(&sender_peer_id)
                        .map(|entry| *entry)
                } {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    let entry = gossip_state
                        .peer_to_actors
                        .entry(sender_addr)
                        .or_insert_with(std::collections::HashSet::new);

                    for name in peer_actors_removed {
                        entry.remove(&name);
                    }
                    for name in peer_actors_added {
                        entry.insert(name);
                    }
                } else {
                    debug!(
                        sender = %sender_peer_id,
                        "no address mapping for sender; skipping peer_to_actors update"
                    );
                }

                debug!(
                    sender = %sender_peer_id,
                    total_changes,
                    applied_changes = applied_count,
                    peer_actor_changes = peer_actor_changes,
                    "completed delta application with vector clock conflict resolution"
                );

                let mut gossip_state = registry.gossip_state.lock().await;
                gossip_state.delta_exchanges += 1;

                Ok(())
            }
            RegistryMessageKind::FullSyncResponse => {
                let (
                    sender_peer_id,
                    sender_socket_addr,
                    local_len,
                    known_len,
                    sequence,
                    wall_clock_time,
                ) = {
                    let archived = frame.archived()?;
                    let (
                        local_actors,
                        known_actors,
                        sender_peer_id,
                        sender_bind_addr,
                        sequence,
                        wall_clock_time,
                    ) = match archived {
                        ArchivedRegistryMessage::FullSyncResponse {
                            local_actors,
                            known_actors,
                            sender_peer_id,
                            sender_bind_addr,
                            sequence,
                            wall_clock_time,
                        } => (
                            local_actors,
                            known_actors,
                            sender_peer_id,
                            sender_bind_addr,
                            sequence,
                            wall_clock_time,
                        ),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    let sender_peer_id = archived_peer_id(sender_peer_id)?;
                    let sender_socket_addr =
                        resolve_peer_addr(archived_option_str(sender_bind_addr), _peer_addr);
                    let local_len = local_actors.len();
                    let known_len = known_actors.len();
                    let sequence = u64::from(sequence);
                    let wall_clock_time = u64::from(wall_clock_time);
                    (
                        sender_peer_id,
                        sender_socket_addr,
                        local_len,
                        known_len,
                        sequence,
                        wall_clock_time,
                    )
                };

                // Use resolve_peer_addr for safe address resolution with validation
                debug!(
                    sender = %sender_peer_id,
                    bind_addr = %sender_socket_addr,
                    tcp_source = %_peer_addr,
                    local_actors = local_len,
                    known_actors = known_len,
                    "RECEIVED: FullSyncResponse from peer (using bind_addr)"
                );

                let (local_map, known_map) = {
                    let archived = frame.archived()?;
                    let (local_actors, known_actors) = match archived {
                        ArchivedRegistryMessage::FullSyncResponse {
                            local_actors,
                            known_actors,
                            ..
                        } => (local_actors, known_actors),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    (
                        collect_actor_map_from_archived(local_actors)?,
                        collect_actor_map_from_archived(known_actors)?,
                    )
                };

                registry
                    .merge_full_sync(
                        local_map,
                        known_map,
                        sender_socket_addr,
                        sequence,
                        wall_clock_time,
                    )
                    .await;

                // FIX: Update peer_id mappings (mirror the FullSync handler logic)
                // This prevents stale ephemeral addresses from being reintroduced via resolve_peer_state_addr
                {
                    let pool = registry.connection_pool.lock().await;

                    // NOTE: Do NOT remove addr_to_peer_id for the ephemeral address here.
                    // The reindex_connection_addr function preserves both addresses,
                    // and disconnect_connection_by_peer_id needs both entries to clean up properly.

                    pool.peer_id_to_addr
                        .insert(sender_peer_id.clone(), sender_socket_addr);
                    pool.addr_to_peer_id
                        .insert(sender_socket_addr, sender_peer_id.clone());

                    // CRITICAL FIX: Reindex the connection from ephemeral TCP port to bind address
                    // Mirror the FullSync handler fix - allows sending to advertised address
                    // Note: reindex_connection_addr already has early-return if already indexed,
                    // and logs internally when it actually does work.
                    if sender_socket_addr != _peer_addr {
                        pool.reindex_connection_addr(&sender_peer_id, sender_socket_addr);
                    }

                    debug!(
                        "BIDIRECTIONAL: Updated connection mapping from FullSyncResponse - peer_id={} addr={}",
                        sender_peer_id, sender_socket_addr
                    );
                }

                // Reset failure state when receiving response
                let mut gossip_state = registry.gossip_state.lock().await;

                // FIX: If the resolved bind address differs from the TCP source address,
                // migrate the PeerInfo from the ephemeral port entry to the bind address.
                // This preserves node_id, sequence, and failure state learned during TLS handshake.
                if sender_socket_addr != _peer_addr && _peer_addr != registry.bind_addr {
                    if let Some(mut old_peer_info) = gossip_state.peers.remove(&_peer_addr) {
                        info!(
                            old_addr = %_peer_addr,
                            new_addr = %sender_socket_addr,
                            node_id = ?old_peer_info.node_id,
                            "🔄 Migrating peer info from ephemeral TCP source to bind address from FullSyncResponse"
                        );
                        // Update the address field and preserve the connection address
                        old_peer_info.address = sender_socket_addr;
                        old_peer_info.peer_address = Some(_peer_addr);
                        // Insert with new key (bind address), preserving all state
                        gossip_state.peers.insert(sender_socket_addr, old_peer_info);
                        // Also clean up pending failures for the old address
                        gossip_state.pending_peer_failures.remove(&_peer_addr);
                    }
                }

                // Reset failure state for responding peer
                let need_to_clear_pending =
                    if let Some(peer_info) = gossip_state.peers.get_mut(&sender_socket_addr) {
                        let had_failures = peer_info.failures > 0;
                        if had_failures {
                            info!(peer = %sender_socket_addr,
                          prev_failures = peer_info.failures,
                          "🔄 Resetting failure state after receiving FullSyncResponse");
                            peer_info.failures = 0;
                            peer_info.last_failure_time = None;
                        }
                        peer_info.last_success = crate::current_timestamp();
                        had_failures
                    } else {
                        false
                    };

                // Clear pending failure record if needed
                if need_to_clear_pending {
                    gossip_state
                        .pending_peer_failures
                        .remove(&sender_socket_addr);
                }

                gossip_state.full_sync_exchanges += 1;
                Ok(())
            }
            RegistryMessageKind::PeerHealthQuery => {
                let (sender_peer_id, target_peer) = {
                    let archived = frame.archived()?;
                    let (sender, target_peer) = match archived {
                        ArchivedRegistryMessage::PeerHealthQuery {
                            sender,
                            target_peer,
                            ..
                        } => (sender, target_peer),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    (archived_peer_id(sender)?, target_peer.as_str().to_string())
                };

                let sender_socket_addr =
                    resolve_peer_state_addr(&registry, Some(&sender_peer_id), _peer_addr).await;
                debug!(
                    sender = %sender_peer_id,
                    target = %target_peer,
                    "received peer health query"
                );

                // Check our connection status to the target peer
                let target_addr = match target_peer.parse::<SocketAddr>() {
                    Ok(addr) => addr,
                    Err(_) => {
                        warn!(
                            "Invalid target peer address in health query: {}",
                            target_peer
                        );
                        return Ok(());
                    }
                };

                let is_alive = {
                    let pool = registry.connection_pool.lock().await;
                    pool.has_connection(&target_addr)
                };

                let last_contact = if is_alive {
                    crate::current_timestamp()
                } else {
                    // Check when we last had successful contact
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.last_success)
                        .unwrap_or(0)
                };

                // Send our health report back
                let mut peer_statuses = HashMap::new();

                // Get actual failure count from gossip state
                let failure_count = {
                    let gossip_state = registry.gossip_state.lock().await;
                    gossip_state
                        .peers
                        .get(&target_addr)
                        .map(|info| info.failures as u32)
                        .unwrap_or(0)
                };

                peer_statuses.insert(
                    target_peer,
                    crate::registry::PeerHealthStatus {
                        is_alive,
                        last_contact,
                        failure_count,
                    },
                );

                let report = RegistryMessage::PeerHealthReport {
                    reporter: registry.peer_id.clone(),
                    peer_statuses: peer_statuses.into_iter().collect(),
                    timestamp: crate::current_timestamp(),
                };

                // Send report back to the querying peer
                if let Ok(data) = rkyv::to_bytes::<rkyv::rancor::Error>(&report) {
                    // Use the actual peer address we received from
                    let sender_addr = sender_socket_addr;

                    // Create message with length + type prefix
                    let mut pool = registry.connection_pool.lock().await;
                    let buffer = pool.create_message_buffer(&data);

                    // Use send_lock_free which doesn't create new connections
                    if let Err(e) = pool.send_lock_free(sender_addr, &buffer) {
                        warn!(peer = %sender_addr, error = %e, "Failed to send peer health report");
                    }
                }

                Ok(())
            }
            RegistryMessageKind::PeerHealthReport => {
                let (reporter, peer_count) = {
                    let archived = frame.archived()?;
                    let (reporter, peer_statuses) = match archived {
                        ArchivedRegistryMessage::PeerHealthReport {
                            reporter,
                            peer_statuses,
                            ..
                        } => (reporter, peer_statuses),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    (archived_peer_id(reporter)?, peer_statuses.len())
                };

                let reporter_addr =
                    resolve_peer_state_addr(&registry, Some(&reporter), _peer_addr).await;
                debug!(
                    reporter = %reporter,
                    peers = peer_count,
                    "received peer health report"
                );

                // Store the health reports
                {
                    let mut gossip_state = registry.gossip_state.lock().await;
                    let archived = frame.archived()?;
                    let peer_statuses = match archived {
                        ArchivedRegistryMessage::PeerHealthReport { peer_statuses, .. } => {
                            peer_statuses
                        }
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    for entry in peer_statuses.iter() {
                        let peer = &entry.0;
                        let status = &entry.1;
                        if let Ok(peer_addr) = peer.as_str().parse::<SocketAddr>() {
                            // For now, use the reporter's peer address from the connection
                            gossip_state
                                .peer_health_reports
                                .entry(peer_addr)
                                .or_insert_with(HashMap::new)
                                .insert(reporter_addr, peer_health_status_from_archived(status));
                        }
                    }
                }

                // Check if we have enough reports to make a decision
                registry.check_peer_consensus().await;

                Ok(())
            }
            RegistryMessageKind::ActorMessage => {
                let peer_state_addr = resolve_peer_state_addr(&registry, None, _peer_addr).await;
                let (actor_id, type_hash, payload, correlation_id) = {
                    let archived = frame.archived()?;
                    match archived {
                        ArchivedRegistryMessage::ActorMessage {
                            actor_id,
                            type_hash,
                            payload,
                            correlation_id,
                        } => {
                            let type_hash = u32::from(*type_hash);
                            let correlation_id = match correlation_id {
                                rkyv::option::ArchivedOption::Some(v) => Some((*v).into()),
                                rkyv::option::ArchivedOption::None => None,
                            };
                            (
                                actor_id.as_str(),
                                type_hash,
                                payload.as_slice(),
                                correlation_id,
                            )
                        }
                        _ => unreachable!("registry message kind mismatch"),
                    }
                };
                let correlation_id = correlation_id.or(ask_correlation_id);
                debug!(
                    actor_id = %actor_id,
                    type_hash = %format!("{:08x}", type_hash),
                    payload_len = payload.len(),
                    correlation_id = ?correlation_id,
                    "received actor message"
                );

                let registry_for_reply = registry.clone();
                let send_reply = move |reply_payload: bytes::Bytes, corr_id: u16| {
                    let registry = registry_for_reply.clone();
                    async move {
                        debug!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            correlation_id = corr_id,
                            reply_len = reply_payload.len(),
                            "sending ask reply back to sender"
                        );

                        let reply_len = reply_payload.len();
                        let header = framing::write_ask_response_header(
                            crate::MessageType::Response,
                            corr_id,
                            reply_len,
                        );

                        let pool = registry.connection_pool.lock().await;
                        if let Err(first_err) = pool.send_lock_free_parts_inline(
                            _peer_addr,
                            header,
                            16,
                            reply_payload.clone(),
                        ) {
                            if _peer_addr != peer_state_addr {
                                match pool.send_lock_free_parts_inline(
                                    peer_state_addr,
                                    header,
                                    16,
                                    reply_payload,
                                ) {
                                    Ok(()) => {
                                        debug!(
                                            peer = %_peer_addr,
                                            peer_state_addr = %peer_state_addr,
                                            "Ask reply sent via bind address (ephemeral port was reindexed)"
                                        );
                                        Ok(())
                                    }
                                    Err(second_err) => {
                                        warn!(
                                            peer = %_peer_addr,
                                            peer_state_addr = %peer_state_addr,
                                            error = %second_err,
                                            "Failed to send ask reply (tried both ephemeral and bind addresses)"
                                        );
                                        Err(second_err)
                                    }
                                }
                            } else {
                                warn!(peer = %_peer_addr, error = %first_err, "Failed to send ask reply");
                                Err(first_err)
                            }
                        } else {
                            Ok(())
                        }
                    }
                };

                match dispatch_actor_message_zero_copy(
                    &registry,
                    actor_id,
                    type_hash,
                    payload,
                    correlation_id,
                    send_reply,
                )
                .await
                {
                    Ok(true) => {
                        debug!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            correlation_id = ?correlation_id,
                            "actor message processed successfully with reply"
                        );
                    }
                    Ok(false) => {
                        debug!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            "actor message processed successfully"
                        );
                    }
                    Err(e) => {
                        warn!(
                            actor_id = %actor_id,
                            type_hash = %format!("{:08x}", type_hash),
                            error = %e,
                            "failed to process actor message"
                        );
                    }
                }

                Ok(())
            }
            RegistryMessageKind::ImmediateAck => {
                let mut pending_acks = registry.pending_acks.lock().await;
                let (actor_name, success) = {
                    let archived = frame.archived()?;
                    match archived {
                        ArchivedRegistryMessage::ImmediateAck {
                            actor_name,
                            success,
                        } => (actor_name.as_str(), *success),
                        _ => unreachable!("registry message kind mismatch"),
                    }
                };

                debug!(
                    actor_name = %actor_name,
                    success = success,
                    "received immediate ACK for synchronous registration"
                );

                // Look up the pending ACK sender for this actor
                if let Some(sender) = pending_acks.remove(actor_name) {
                    // Send the success status through the oneshot channel
                    if sender.send(success).is_err() {
                        warn!(
                            actor_name = %actor_name,
                            "Failed to send ACK to waiting registration - receiver dropped"
                        );
                    } else {
                        info!(
                            actor_name = %actor_name,
                            success = success,
                            "✅ Sent ACK to waiting synchronous registration"
                        );
                    }
                } else {
                    debug!(
                        actor_name = %actor_name,
                        "Received ACK but no pending registration found (may have timed out)"
                    );
                }

                Ok(())
            }
            RegistryMessageKind::PeerListGossip => {
                let peer_state_addr = resolve_peer_state_addr(&registry, None, _peer_addr).await;
                let (peer_count, timestamp, sender_addr) = {
                    let archived = frame.archived()?;
                    let (peers, timestamp, sender_addr) = match archived {
                        ArchivedRegistryMessage::PeerListGossip {
                            peers,
                            timestamp,
                            sender_addr,
                        } => (peers, timestamp, sender_addr),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    (peers.len(), u64::from(timestamp), sender_addr.as_str())
                };

                debug!(
                    peer_count = peer_count,
                    timestamp = timestamp,
                    sender = %sender_addr,
                    "received peer list gossip message"
                );

                // Accept peer list only from connected peers
                if !registry.has_active_connection(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from non-connected peer"
                    );
                    return Ok(());
                }

                if !registry.peer_supports_peer_list(&peer_state_addr).await {
                    debug!(
                        peer = %peer_state_addr,
                        "ignoring peer list gossip from peer without capability"
                    );
                    return Ok(());
                }

                let candidates = {
                    let archived = frame.archived()?;
                    let (peers, timestamp, sender_addr) = match archived {
                        ArchivedRegistryMessage::PeerListGossip {
                            peers,
                            timestamp,
                            sender_addr,
                        } => (peers, timestamp, sender_addr),
                        _ => unreachable!("registry message kind mismatch"),
                    };
                    registry
                        .on_peer_list_gossip_archived(
                            peers,
                            sender_addr.as_str(),
                            u64::from(timestamp),
                        )
                        .await
                };

                if candidates.is_empty() {
                    return Ok(());
                }

                let registry_clone = registry.clone();
                let discovery_handle = tokio::spawn(async move {
                    for addr in candidates {
                        let node_id = registry_clone.lookup_node_id(&addr).await;
                        registry_clone.add_peer_with_node_id(addr, node_id).await;

                        match registry_clone.get_connection(addr).await {
                            Ok(_) => {
                                registry_clone.mark_peer_connected(addr).await;
                                debug!(peer = %addr, "connected to discovered peer");
                            }
                            Err(e) => {
                                registry_clone.mark_peer_failed(addr).await;
                                warn!(peer = %addr, error = %e, "failed to connect to discovered peer");
                            }
                        }
                    }
                });

                // Track the discovery task handle (H-004)
                if let Ok(mut handles) = registry.discovery_task_handles.lock() {
                    // Clean up finished handles before adding new one
                    handles.retain(|h| !h.is_finished());
                    handles.push(discovery_handle);
                }

                Ok(())
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::sleep;

    async fn create_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connections in background
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    // Simple echo server - just keep connection open
                    let mut buf = vec![0; 1024];
                    loop {
                        use tokio::io::AsyncReadExt;
                        match stream.read(&mut buf).await {
                            Ok(0) => break, // Connection closed
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        addr
    }

    #[test]
    fn test_connection_handle_debug() {
        // Compile-time test to ensure Debug is implemented
        use std::fmt::Debug;
        fn assert_debug<T: Debug>() {}
        assert_debug::<ConnectionHandle>();
    }

    #[test]
    fn test_buffer_config_validation() {
        // Should reject buffers < 256KB
        let result = BufferConfig::new(100 * 1024);
        assert!(result.is_err());

        // Should accept valid sizes
        let config = BufferConfig::new(512 * 1024).unwrap();
        assert_eq!(config.tcp_buffer_size(), 512 * 1024);
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS);

        // Streaming threshold should be buffer_size - 1KB
        assert_eq!(config.streaming_threshold(), 511 * 1024);
    }

    #[test]
    fn test_streaming_threshold_calculation() {
        let config = BufferConfig::new(1024 * 1024).unwrap();

        // 1MB buffer should have ~1MB-1KB threshold
        let threshold = config.streaming_threshold();
        assert!(threshold < config.tcp_buffer_size());
        assert!(threshold > 1020 * 1024); // At least 1020KB
        assert_eq!(threshold, 1023 * 1024); // Exactly 1023KB
    }

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.tcp_buffer_size(), 1024 * 1024); // 1MB
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS); // 1024 slots
        assert_eq!(config.streaming_threshold(), 1023 * 1024); // 1MB - 1KB
        assert_eq!(
            config.ask_inflight_limit(),
            crate::config::DEFAULT_ASK_INFLIGHT_LIMIT
        );
    }

    #[test]
    fn test_buffer_config_minimum_size() {
        // Test exactly at minimum boundary
        let config = BufferConfig::new(256 * 1024).unwrap();
        assert_eq!(config.tcp_buffer_size(), 256 * 1024);
        assert_eq!(config.ring_buffer_slots(), RING_BUFFER_SLOTS);
        assert_eq!(config.streaming_threshold(), 255 * 1024);

        // Test just below minimum (should fail)
        let result = BufferConfig::new(256 * 1024 - 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_streaming_threshold_saturation() {
        // Test that streaming_threshold handles edge cases properly
        let config = BufferConfig::new(256 * 1024).unwrap(); // Minimum buffer (256KB)
                                                             // Should be 255KB (256KB - 1KB)
        assert_eq!(config.streaming_threshold(), 255 * 1024);

        // Test with exactly 1KB buffer would be rejected by validation,
        // but we can verify saturating_sub behavior directly
        let large_config = BufferConfig::new(2 * 1024 * 1024).unwrap(); // 2MB
        assert_eq!(large_config.streaming_threshold(), 2 * 1024 * 1024 - 1024);
    }

    #[test]
    fn test_control_reserved_slots_bounds() {
        assert_eq!(control_reserved_slots(1), 1);
        assert_eq!(control_reserved_slots(2), 1);
        assert_eq!(
            control_reserved_slots(CONTROL_RESERVED_SLOTS),
            CONTROL_RESERVED_SLOTS
        );
        assert_eq!(
            control_reserved_slots(CONTROL_RESERVED_SLOTS * 2),
            CONTROL_RESERVED_SLOTS
        );
    }

    #[test]
    fn test_should_flush_rules() {
        assert!(!should_flush(
            0,
            true,
            Duration::from_millis(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
        assert!(should_flush(
            4096,
            false,
            Duration::from_millis(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
        assert!(should_flush(
            1,
            true,
            Duration::from_millis(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
        assert!(should_flush(
            1,
            false,
            WRITER_MAX_LATENCY + Duration::from_micros(1),
            4 * 1024,
            WRITER_MAX_LATENCY
        ));
    }

    #[tokio::test]
    async fn test_permit_lanes_allow_control_when_ask_exhausted() {
        let (writer, _reader) = tokio::io::duplex(1024);
        let (handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:0".parse().unwrap(),
            ChannelId::TellAsk,
            BufferConfig::default(),
        );

        let (ask_available, control_available) = handle.permit_counts();
        assert!(ask_available > 0);
        assert!(control_available > 0);

        let mut ask_permits = Vec::with_capacity(ask_available);
        for _ in 0..ask_available {
            ask_permits.push(handle.acquire_ask_permit_for_test().await);
        }

        let control = tokio::time::timeout(
            Duration::from_millis(50),
            handle.acquire_control_permit_for_test(),
        )
        .await;
        assert!(control.is_ok());

        let blocked = tokio::time::timeout(
            Duration::from_millis(10),
            handle.acquire_ask_permit_for_test(),
        )
        .await;
        assert!(blocked.is_err());

        drop(control.unwrap());
        drop(ask_permits.pop());

        let recovered = tokio::time::timeout(
            Duration::from_millis(50),
            handle.acquire_ask_permit_for_test(),
        )
        .await;
        assert!(recovered.is_ok());
    }

    #[tokio::test]
    async fn test_connection_pool_new() {
        let pool = ConnectionPool::new(10, Duration::from_secs(5));
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.max_connections, 10);
        assert_eq!(pool.connection_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_set_registry() {
        use crate::{registry::GossipRegistry, GossipConfig, KeyPair};
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let registry = Arc::new(GossipRegistry::new(
            "127.0.0.1:8080".parse().unwrap(),
            GossipConfig {
                key_pair: Some(KeyPair::new_for_testing("conn_pool_registry")),
                ..Default::default()
            },
        ));

        pool.set_registry(registry.clone());
        assert!(pool.registry.is_some());
    }

    #[tokio::test]
    async fn test_connection_handle_send_data() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, writer) = stream.into_split();

        // Create a LockFreeStreamHandle
        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
            peer_capabilities: None,
        };

        let data = vec![1, 2, 3, 4];
        // Just test that send_data doesn't error - the data goes to the mock stream
        handle.send_data(data.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_handle_send_data_closed() {
        // Create a mock stream using a TCP server for testing
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;
        let stream = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let (_, mut writer) = stream.into_split();
        use tokio::io::AsyncWriteExt;
        let _ = writer.shutdown().await; // Close the writer

        // Create a LockFreeStreamHandle
        let (stream_handle, _writer_task) = LockFreeStreamHandle::new(
            writer,
            "127.0.0.1:8080".parse().unwrap(),
            ChannelId::Global,
            BufferConfig::default(),
        );
        let stream_handle = Arc::new(stream_handle);

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            stream_handle,
            correlation: CorrelationTracker::new(),
            peer_capabilities: None,
        };

        // The lock-free design uses fire-and-forget semantics, so send_data
        // will succeed even if the writer is closed. The error is detected
        // asynchronously in the background writer task.
        //
        // For this test, we'll just verify that send_data doesn't panic
        // and returns some result (which should be Ok due to fire-and-forget).
        let result = handle.send_data(vec![1, 2, 3]).await;
        // With fire-and-forget semantics, this should return Ok even with a closed writer
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_task_tracker_aborts_on_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let task_started = Arc::new(AtomicBool::new(false));
        let task_completed = Arc::new(AtomicBool::new(false));
        let started_clone = task_started.clone();
        let completed_clone = task_completed.clone();

        let handle = tokio::spawn(async move {
            started_clone.store(true, Ordering::SeqCst);
            // Long sleep that should be aborted
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            completed_clone.store(true, Ordering::SeqCst);
        });

        // Give task time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            task_started.load(Ordering::SeqCst),
            "Task should have started"
        );

        // Create tracker and set the handle
        let mut tracker = TaskTracker::new();
        tracker.set_writer(handle);

        // Drop the tracker - this should abort the task
        drop(tracker);

        // Give task time to be aborted
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Task should NOT have completed (it was aborted)
        assert!(
            !task_completed.load(Ordering::SeqCst),
            "Task should have been aborted, not completed"
        );
    }

    #[tokio::test]
    async fn test_task_tracker_replaces_old_handle() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let task2_started = Arc::new(AtomicBool::new(false));

        let handle1 = tokio::spawn(async move {
            // Long sleep that should be aborted when handle2 replaces it
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });

        let started_clone = task2_started.clone();
        let handle2 = tokio::spawn(async move {
            started_clone.store(true, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });

        let mut tracker = TaskTracker::new();

        // Set first handle
        tracker.set_writer(handle1);

        // Set second handle - first should be aborted
        tracker.set_writer(handle2);

        // Give task2 time to start
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            task2_started.load(Ordering::SeqCst),
            "Second task should have started"
        );

        // Clean up
        drop(tracker);
    }
}
