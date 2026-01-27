use crate::{GossipError, Result};
use bytes::{Buf, Bytes};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

#[cfg(debug_assertions)]
use bytes::BufMut;

const SERIALIZER_POOL_SIZE: usize = 64;
const MAX_POOLED_BUFFER_CAPACITY: usize = 1024 * 1024; // 1MB
const MAX_POOLED_ARENA_CAPACITY: usize = 1024 * 1024; // 1MB

struct SerializerCtx {
    writer: rkyv::util::AlignedVec,
    arena: rkyv::ser::allocator::Arena,
}

impl SerializerCtx {
    fn new() -> Self {
        Self {
            writer: rkyv::util::AlignedVec::new(),
            arena: rkyv::ser::allocator::Arena::new(),
        }
    }
}

struct SerializerPool {
    // Boxing is intentional: pool returns Box<SerializerCtx> to callers
    // who may hold them across await points with stable addresses
    #[allow(clippy::vec_box)]
    inner: Mutex<Vec<Box<SerializerCtx>>>,
}

impl SerializerPool {
    fn new() -> Self {
        let mut pool = Vec::with_capacity(SERIALIZER_POOL_SIZE);
        for _ in 0..SERIALIZER_POOL_SIZE {
            pool.push(Box::new(SerializerCtx::new()));
        }
        Self {
            inner: Mutex::new(pool),
        }
    }

    fn acquire(&self) -> Box<SerializerCtx> {
        self.inner
            .lock()
            .expect("serializer pool poisoned")
            .pop()
            .unwrap_or_else(|| Box::new(SerializerCtx::new()))
    }

    fn release(&self, mut ctx: Box<SerializerCtx>) {
        ctx.writer.clear();
        if ctx.writer.capacity() > MAX_POOLED_BUFFER_CAPACITY {
            return;
        }
        if ctx.arena.capacity() > MAX_POOLED_ARENA_CAPACITY {
            ctx.arena = rkyv::ser::allocator::Arena::new();
        } else {
            ctx.arena.shrink();
        }

        let mut guard = self.inner.lock().expect("serializer pool poisoned");
        if guard.len() < SERIALIZER_POOL_SIZE {
            guard.push(ctx);
        }
    }

    fn acquire_many(&self, count: usize) -> Vec<Box<SerializerCtx>> {
        let mut guard = self.inner.lock().expect("serializer pool poisoned");
        let mut ctxs = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(ctx) = guard.pop() {
                ctxs.push(ctx);
            } else {
                ctxs.push(Box::new(SerializerCtx::new()));
            }
        }
        ctxs
    }
}

fn serializer_pool() -> &'static Arc<SerializerPool> {
    static POOL: OnceLock<Arc<SerializerPool>> = OnceLock::new();
    POOL.get_or_init(|| Arc::new(SerializerPool::new()))
}

#[inline]
fn encode_typed_in<T>(value: &T, ctx: &mut SerializerCtx) -> Result<usize>
where
    T: WireEncode,
{
    let writer = std::mem::take(&mut ctx.writer);
    let writer = rkyv::api::high::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
        value,
        writer,
        ctx.arena.acquire(),
    )
    .map_err(GossipError::Serialization)?;
    let len = writer.len();
    ctx.writer = writer;
    Ok(len)
}

/// Pooled payload that implements bytes::Buf without copying.
pub struct PooledPayload {
    ctx: Option<Box<SerializerCtx>>,
    pool: Arc<SerializerPool>,
    len: usize,
    pos: usize,
}

impl PooledPayload {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        if let Some(ctx) = self.ctx.as_ref() {
            &ctx.writer[..self.len]
        } else {
            &[]
        }
    }
}

impl Buf for PooledPayload {
    fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    fn chunk(&self) -> &[u8] {
        if let Some(ctx) = self.ctx.as_ref() {
            &ctx.writer[self.pos..self.len]
        } else {
            &[]
        }
    }

    fn advance(&mut self, cnt: usize) {
        let remaining = self.remaining();
        let to_advance = cnt.min(remaining);
        self.pos += to_advance;
    }
}

impl Drop for PooledPayload {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            self.pool.release(ctx);
        }
    }
}

/// Compile-time wire type marker with a stable hash identifier.
///
/// The hash should be derived from a stable, shared identifier (e.g. a protocol name)
/// so different binaries can agree on the same type mapping.
pub trait WireType {
    const TYPE_HASH: u64;
    const TYPE_NAME: &'static str;
}

/// Helper trait for rkyv-serializable wire types.
pub trait WireEncode:
    WireType
    + for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >
{
}

impl<T> WireEncode for T where
    T: WireType
        + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >
{
}

/// Helper trait for rkyv-deserializable wire types.
pub trait WireDecode: WireType + rkyv::Archive + Sized
where
    for<'a> <Self as rkyv::Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<Self, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

impl<T> WireDecode for T
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

/// FNV-1a 64-bit hash for stable compile-time hashing of string literals.
pub const fn fnv1a_hash(input: &str) -> u64 {
    let bytes = input.as_bytes();
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

/// Encode a typed message for the wire.
///
/// In debug builds, prefixes the payload with the type hash for validation.
pub fn encode_typed<T>(value: &T) -> Result<Bytes>
where
    T: WireEncode,
{
    let payload =
        rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(GossipError::Serialization)?;

    #[cfg(debug_assertions)]
    {
        let mut buf = bytes::BytesMut::with_capacity(8 + payload.len());
        buf.put_u64(T::TYPE_HASH);
        buf.extend_from_slice(payload.as_ref());
        Ok(buf.freeze())
    }

    #[cfg(not(debug_assertions))]
    {
        Ok(Bytes::copy_from_slice(payload.as_ref()))
    }
}

/// Encode a typed payload using the pooled serializer context.
pub fn encode_typed_pooled<T>(value: &T) -> Result<PooledPayload>
where
    T: WireEncode,
{
    let pool = serializer_pool().clone();
    let mut ctx = pool.acquire();
    let len = encode_typed_in(value, &mut ctx)?;

    Ok(PooledPayload {
        ctx: Some(ctx),
        pool,
        len,
        pos: 0,
    })
}

/// Wrap a pooled payload with the debug type hash prefix when enabled.
pub fn typed_payload_parts<T: WireType>(
    payload: PooledPayload,
) -> (PooledPayload, Option<[u8; 8]>, usize) {
    #[cfg(debug_assertions)]
    {
        let prefix = T::TYPE_HASH.to_be_bytes();
        let total_len = prefix.len() + payload.len();
        (payload, Some(prefix), total_len)
    }

    #[cfg(not(debug_assertions))]
    {
        let total_len = payload.len();
        return (payload, None, total_len);
    }
}

/// Pad an optional 8-byte type-hash prefix to 16 bytes for alignment-sensitive paths.
pub fn pad_type_hash_prefix(prefix: Option<[u8; 8]>) -> (Option<[u8; 16]>, u8) {
    let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0) as u8;
    let padded = prefix.map(|p| {
        let mut out = [0u8; 16];
        out[..p.len()].copy_from_slice(&p);
        out
    });
    (padded, prefix_len)
}

/// Pre-encoded batch of typed messages backed by pooled buffers.
pub struct TypedBatch<T: WireType> {
    payloads: Vec<PooledPayload>,
    _marker: PhantomData<T>,
}

impl<T: WireType> TypedBatch<T> {
    /// Number of payloads encoded in the batch.
    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}

impl<T: WireEncode> TypedBatch<T> {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self {
            payloads: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Create an empty batch with the provided capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            payloads: Vec::with_capacity(capacity),
            _marker: PhantomData,
        }
    }

    /// Encode and append a value using the pooled serializer.
    pub fn push(&mut self, value: &T) -> Result<()> {
        let payload = encode_typed_pooled(value)?;
        self.payloads.push(payload);
        Ok(())
    }

    /// Extend the batch from a slice of values.
    pub fn extend_from_slice(&mut self, values: &[T]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let pool = serializer_pool().clone();
        let mut ctxs = pool.acquire_many(values.len());
        for value in values {
            let mut ctx = ctxs
                .pop()
                .expect("serializer pool returned fewer contexts than requested");
            let len = match encode_typed_in(value, &mut ctx) {
                Ok(len) => len,
                Err(err) => {
                    pool.release(ctx);
                    for remaining in ctxs.drain(..) {
                        pool.release(remaining);
                    }
                    return Err(err);
                }
            };

            self.payloads.push(PooledPayload {
                ctx: Some(ctx),
                pool: pool.clone(),
                len,
                pos: 0,
            });
        }
        Ok(())
    }

    /// Encode a batch from a slice of values.
    pub fn from_slice(values: &[T]) -> Result<Self> {
        let mut batch = Self::with_capacity(values.len());
        batch.extend_from_slice(values)?;
        Ok(batch)
    }
}

impl<T: WireEncode> Default for TypedBatch<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: WireType> IntoIterator for TypedBatch<T> {
    type Item = PooledPayload;
    type IntoIter = std::vec::IntoIter<PooledPayload>;

    fn into_iter(self) -> Self::IntoIter {
        self.payloads.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_type;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct TestMsg {
        value: u64,
    }

    wire_type!(TestMsg, "typed::TestMsg");

    #[test]
    fn pooled_payload_buf_semantics() {
        let msg = TestMsg { value: 42 };
        let mut payload = encode_typed_pooled(&msg).unwrap();
        let remaining = payload.remaining();
        assert!(remaining > 0);
        assert_eq!(payload.chunk().len(), remaining);

        let advance_by = 1.min(remaining);
        payload.advance(advance_by);
        assert_eq!(payload.remaining(), remaining - advance_by);
    }

    #[test]
    fn pool_reuse_and_cap_behavior() {
        let pool = serializer_pool().clone();
        let initial = pool.inner.lock().unwrap().len();

        let msg = TestMsg { value: 7 };
        let payload = encode_typed_pooled(&msg).unwrap();
        drop(payload);

        let after = pool.inner.lock().unwrap().len();
        assert!(after >= initial.saturating_sub(1));

        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct BigMsg {
            data: Vec<u8>,
        }
        wire_type!(BigMsg, "typed::BigMsg");

        let big = BigMsg {
            data: vec![0u8; MAX_POOLED_BUFFER_CAPACITY + 1024],
        };
        let big_payload = encode_typed_pooled(&big).unwrap();
        drop(big_payload);

        let final_len = pool.inner.lock().unwrap().len();
        assert!(final_len <= after);
    }

    #[test]
    fn typed_payload_parts_includes_hash_in_debug() {
        let msg = TestMsg { value: 1 };
        let payload = encode_typed_pooled(&msg).unwrap();
        let (_payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);

        #[cfg(debug_assertions)]
        {
            assert!(total_len >= 8);
            assert!(prefix.is_some());
        }
    }

    #[test]
    fn typed_batch_from_slice_encodes_all_messages() {
        let msgs = vec![TestMsg { value: 1 }, TestMsg { value: 2 }];
        let batch = TypedBatch::from_slice(&msgs).unwrap();
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn typed_batch_into_iter_consumes_payloads() {
        let msgs = vec![TestMsg { value: 7 }];
        let batch = TypedBatch::from_slice(&msgs).unwrap();
        let mut iter = batch.into_iter();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    #[cfg(debug_assertions)]
    #[test]
    fn decode_typed_archived_rejects_hash_mismatch() {
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct OtherMsg {
            data: u32,
        }

        wire_type!(OtherMsg, "typed::OtherMsg");

        let payload = encode_typed(&TestMsg { value: 9 }).unwrap();
        match decode_typed_archived::<OtherMsg>(payload) {
            Err(GossipError::InvalidConfig(msg)) => {
                assert!(msg.contains("typed payload hash mismatch"));
            }
            Ok(_) => panic!("expected hash mismatch error"),
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn decode_typed_archived_preserves_backing_slice() {
        let payload = encode_typed(&TestMsg { value: 77 }).unwrap();
        let offset = if cfg!(debug_assertions) { 8 } else { 0 };
        let expected_ptr = unsafe { payload.as_ptr().add(offset) };

        let archived = decode_typed_archived::<TestMsg>(payload.clone()).unwrap();
        assert_eq!(archived.as_bytes().as_ptr(), expected_ptr);

        let archived_view = archived.archived().unwrap();
        assert_eq!(archived_view.value, 77);
    }
}

/// Zero-copy wrapper for archived payloads that keeps the underlying bytes alive.
pub struct ArchivedBytes<T> {
    bytes: Bytes,
    offset: usize,
    _marker: PhantomData<T>,
}

impl<T> ArchivedBytes<T> {
    /// Access the raw payload bytes (without the debug type hash prefix).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }

    /// Return the underlying buffer (includes debug prefix if present).
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl<T> ArchivedBytes<T>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
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
    /// Access the archived payload with validation.
    pub fn archived(&self) -> Result<&<T as rkyv::Archive>::Archived> {
        Ok(crate::rkyv_utils::access_archived::<T>(self.as_bytes())?)
    }
}

// CRITICAL_PATH: zero-copy typed decode guard (must stay allocation-free).
/// Decode a typed message into an archived view (zero-copy).
///
/// In debug builds, verifies and strips the type hash prefix without copying.
pub fn decode_typed_archived<T>(payload: Bytes) -> Result<ArchivedBytes<T>>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
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
    #[cfg(debug_assertions)]
    {
        if payload.len() < 8 {
            return Err(GossipError::InvalidConfig(format!(
                "typed payload too short for type hash ({})",
                T::TYPE_NAME
            )));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&payload[..8]);
        let hash = u64::from_be_bytes(hash_bytes);
        if hash != T::TYPE_HASH {
            return Err(GossipError::InvalidConfig(format!(
                "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
                T::TYPE_NAME,
                T::TYPE_HASH,
                hash
            )));
        }
        Ok(ArchivedBytes {
            bytes: payload,
            offset: 8,
            _marker: PhantomData,
        })
    }

    #[cfg(not(debug_assertions))]
    {
        Ok(ArchivedBytes {
            bytes: payload,
            offset: 0,
            _marker: PhantomData,
        })
    }
}

// CRITICAL_PATH: zero-copy typed decode guard (borrowed view).
/// Decode a typed message using zero-copy deserialization - MOST EFFICIENT
///
/// This is the RECOMMENDED function for hot paths. It returns a direct reference
/// to the archived data without any allocation. The returned reference is tied
/// to the lifetime of the input bytes.
///
/// **Performance**: 0 bytes allocated
/// **Safety**: Caller must ensure the payload has been validated before use.
/// **Use case**: Hot paths, high-frequency message processing
///
/// # Example
/// ```ignore
/// let archived = decode_typed_zero_copy::<MyMessage>(&bytes)?;
/// let field = archived.my_field(); // Access archived fields
/// ```
pub fn decode_typed_zero_copy<T>(payload: &[u8]) -> Result<&<T as rkyv::Archive>::Archived>
where
    T: WireType + rkyv::Archive,
    for<'b> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'b>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    #[cfg(debug_assertions)]
    {
        if payload.len() < 8 {
            return Err(GossipError::InvalidConfig(format!(
                "typed payload too short for type hash ({})",
                T::TYPE_NAME
            )));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&payload[..8]);
        let hash = u64::from_be_bytes(hash_bytes);
        if hash != T::TYPE_HASH {
            return Err(GossipError::InvalidConfig(format!(
                "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
                T::TYPE_NAME,
                T::TYPE_HASH,
                hash
            )));
        }
        let body = &payload[8..];
        Ok(crate::rkyv_utils::access_archived_unchecked::<T>(body))
    }

    #[cfg(not(debug_assertions))]
    {
        Ok(crate::rkyv_utils::access_archived_unchecked::<T>(payload))
    }
}

/// Implement WireType with a stable, shared string identifier.
#[macro_export]
macro_rules! wire_type {
    ($ty:ty, $name:expr) => {
        impl $crate::typed::WireType for $ty {
            const TYPE_HASH: u64 = $crate::typed::fnv1a_hash($name);
            const TYPE_NAME: &'static str = $name;
        }
    };
}
