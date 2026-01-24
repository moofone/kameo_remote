use crate::framing::REGISTRY_ALIGNMENT;
use parking_lot::Mutex;
use rkyv::util::AlignedVec;
use std::cell::UnsafeCell;
use std::ops::RangeBounds;
use std::sync::{Arc, Weak};

type RegistryAlignedVec = AlignedVec<{ REGISTRY_ALIGNMENT }>;

#[derive(Debug, Clone)]
pub struct GossipFrameBuffer {
    pool: Arc<BufferPool>,
}

impl Default for GossipFrameBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl GossipFrameBuffer {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(BufferPool::new()),
        }
    }

    pub fn lease(&self, min_capacity: usize) -> GossipFrameLease {
        let buffer = self.pool.checkout(min_capacity);
        GossipFrameLease {
            pool: self.pool.clone(),
            buffer: Some(buffer),
        }
    }
}

#[derive(Debug)]
struct BufferPool {
    buffers: Mutex<Vec<RegistryAlignedVec>>,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            buffers: Mutex::new(Vec::new()),
        }
    }

    fn checkout(&self, min_capacity: usize) -> RegistryAlignedVec {
        let mut guard = self.buffers.lock();
        if let Some(mut buffer) = guard.pop() {
            ensure_capacity(&mut buffer, min_capacity);
            buffer.clear();
            buffer
        } else {
            RegistryAlignedVec::with_capacity(min_capacity)
        }
    }

    fn release(&self, mut buffer: RegistryAlignedVec) {
        buffer.clear();
        self.buffers.lock().push(buffer);
    }
}

pub struct GossipFrameLease {
    pool: Arc<BufferPool>,
    buffer: Option<RegistryAlignedVec>,
}

impl GossipFrameLease {
    pub fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        let buffer = self
            .buffer
            .as_mut()
            .expect("buffer already frozen or dropped");
        ensure_capacity(buffer, len);
        unsafe {
            buffer.set_len(len);
        }
        buffer.as_mut_slice()
    }

    pub fn freeze(mut self, len: usize) -> PooledFrameBytes {
        let mut buffer = self
            .buffer
            .take()
            .expect("buffer already frozen or dropped");
        ensure_capacity(&mut buffer, len);
        unsafe { buffer.set_len(len) };
        PooledFrameBytes::new(self.pool.clone(), buffer, 0, len)
    }

    pub fn freeze_range(mut self, offset: usize, len: usize) -> PooledFrameBytes {
        let mut buffer = self
            .buffer
            .take()
            .expect("buffer already frozen or dropped");
        let total = offset + len;
        ensure_capacity(&mut buffer, total);
        unsafe { buffer.set_len(total) };
        PooledFrameBytes::new(self.pool.clone(), buffer, offset, len)
    }
}

impl Drop for GossipFrameLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

#[derive(Debug)]
struct FrameStorage {
    pool: Weak<BufferPool>,
    buffer: UnsafeCell<Option<RegistryAlignedVec>>,
}

unsafe impl Send for FrameStorage {}
unsafe impl Sync for FrameStorage {}

impl FrameStorage {
    fn new(pool: Arc<BufferPool>, buffer: RegistryAlignedVec) -> Self {
        Self {
            pool: Arc::downgrade(&pool),
            buffer: UnsafeCell::new(Some(buffer)),
        }
    }

    fn orphan(buffer: RegistryAlignedVec) -> Self {
        Self {
            pool: Weak::new(),
            buffer: UnsafeCell::new(Some(buffer)),
        }
    }

    fn slice(&self, offset: usize, len: usize) -> &[u8] {
        unsafe {
            let opt = &*self.buffer.get();
            let buf = opt.as_ref().expect("frame storage released before access");
            &buf[offset..offset + len]
        }
    }

    fn take_buffer(&self) -> Option<RegistryAlignedVec> {
        unsafe { (*self.buffer.get()).take() }
    }
}

impl Drop for FrameStorage {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            if let Some(mut buffer) = unsafe { (*self.buffer.get()).take() } {
                buffer.clear();
                pool.release(buffer);
            }
        } else if let Some(buffer) = unsafe { (*self.buffer.get()).take() } {
            drop(buffer);
        }
    }
}

#[derive(Clone, Debug)]
pub struct PooledFrameBytes {
    storage: Arc<FrameStorage>,
    offset: usize,
    len: usize,
}

impl PooledFrameBytes {
    fn new(pool: Arc<BufferPool>, buffer: RegistryAlignedVec, offset: usize, len: usize) -> Self {
        let storage = Arc::new(FrameStorage::new(pool, buffer));
        Self {
            storage,
            offset,
            len,
        }
    }

    pub fn orphaned(bytes: &[u8]) -> Self {
        let mut buffer = RegistryAlignedVec::with_capacity(bytes.len());
        buffer.extend_from_slice(bytes);
        let storage = Arc::new(FrameStorage::orphan(buffer));
        Self {
            storage,
            offset: 0,
            len: bytes.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        self.storage.slice(self.offset, self.len)
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let (start, end) = normalize_range(range, self.len);
        Self {
            storage: self.storage.clone(),
            offset: self.offset + start,
            len: end - start,
        }
    }

    pub fn into_slice(self, range: impl RangeBounds<usize>) -> Self {
        let (start, end) = normalize_range(range, self.len);
        Self {
            storage: self.storage,
            offset: self.offset + start,
            len: end - start,
        }
    }

    pub fn to_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(self.as_slice())
    }

    pub fn into_bytes(self) -> bytes::Bytes {
        let offset = self.offset;
        let len = self.len;

        if Arc::strong_count(&self.storage) == 1 {
            match Arc::try_unwrap(self.storage) {
                Ok(storage) => {
                    if let Some(buffer) = storage.take_buffer() {
                        let boxed = buffer.into_boxed_slice();
                        return bytes::Bytes::from(boxed).slice(offset..offset + len);
                    }
                    bytes::Bytes::copy_from_slice(storage.slice(offset, len))
                }
                Err(storage) => bytes::Bytes::copy_from_slice(storage.slice(offset, len)),
            }
        } else {
            bytes::Bytes::copy_from_slice(self.as_slice())
        }
    }
}

fn normalize_range(range: impl RangeBounds<usize>, len: usize) -> (usize, usize) {
    use std::ops::Bound;
    let start = match range.start_bound() {
        Bound::Included(&value) => value,
        Bound::Excluded(&value) => value + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&value) => value + 1,
        Bound::Excluded(&value) => value,
        Bound::Unbounded => len,
    };
    assert!(start <= end && end <= len, "slice out of bounds");
    (start, end)
}

fn ensure_capacity(buffer: &mut RegistryAlignedVec, required: usize) {
    if buffer.capacity() >= required {
        return;
    }
    let mut new_cap = buffer.capacity().max(64);
    while new_cap < required {
        new_cap = new_cap.saturating_mul(2);
        if new_cap >= RegistryAlignedVec::MAX_CAPACITY {
            new_cap = RegistryAlignedVec::MAX_CAPACITY;
            break;
        }
    }
    unsafe {
        buffer.grow_capacity_to(new_cap);
    }
}
