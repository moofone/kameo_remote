//! Streaming framing helpers and descriptor definitions.
//!
//! Sprint 1 introduces the negotiated wire contract for Streaming. This module
//! centralizes the encode/decode helpers so both the send and receive paths share
//! a single definition of the descriptor layout and frame math.

use bytes::Bytes;
use std::alloc::{alloc, dealloc, Layout};
use std::convert::TryFrom;
use std::ops::Range;
use std::ptr::NonNull;
use std::slice;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

use crate::{framing, GossipError, MessageType, Result};

const STREAM_ALIGNMENT: usize = framing::REGISTRY_ALIGNMENT;

/// Size of the serialized StreamDescriptor structure (bytes).
pub const STREAM_DESCRIPTOR_SIZE: usize = 40;
/// Total bytes for a StreamStart frame (length + header prefix + descriptor).
pub const STREAM_START_FRAME_LEN: usize =
    framing::LENGTH_PREFIX_LEN + framing::STREAM_HEADER_PREFIX_LEN + STREAM_DESCRIPTOR_SIZE;
/// Bytes for the StreamData header (length + prefix).
pub const STREAM_DATA_HEADER_LEN: usize =
    framing::LENGTH_PREFIX_LEN + framing::STREAM_HEADER_PREFIX_LEN;

/// Metadata announced in StreamStart so the receiver can preallocate the final buffer.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDescriptor {
    pub stream_id: u64,
    pub payload_len: u64,
    pub chunk_len: u32,
    pub type_hash: u32,
    pub actor_id: u64,
    pub flags: u32,
    pub reserved: u32,
}

impl StreamDescriptor {
    /// Serialize to big-endian bytes for the wire format.
    pub fn to_be_bytes(self) -> [u8; STREAM_DESCRIPTOR_SIZE] {
        let mut buf = [0u8; STREAM_DESCRIPTOR_SIZE];
        buf[0..8].copy_from_slice(&self.stream_id.to_be_bytes());
        buf[8..16].copy_from_slice(&self.payload_len.to_be_bytes());
        buf[16..20].copy_from_slice(&self.chunk_len.to_be_bytes());
        buf[20..24].copy_from_slice(&self.type_hash.to_be_bytes());
        buf[24..32].copy_from_slice(&self.actor_id.to_be_bytes());
        buf[32..36].copy_from_slice(&self.flags.to_be_bytes());
        buf[36..40].copy_from_slice(&self.reserved.to_be_bytes());
        buf
    }

    /// Deserialize a descriptor from a big-endian slice.
    pub fn from_be_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < STREAM_DESCRIPTOR_SIZE {
            return Err(GossipError::InvalidStreamFrame(format!(
                "descriptor too short: {} < {}",
                bytes.len(),
                STREAM_DESCRIPTOR_SIZE
            )));
        }

        Ok(Self {
            stream_id: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            payload_len: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            chunk_len: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
            type_hash: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
            actor_id: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            flags: u32::from_be_bytes(bytes[32..36].try_into().unwrap()),
            reserved: u32::from_be_bytes(bytes[36..40].try_into().unwrap()),
        })
    }
}

/// Encode a StreamStart frame.
pub fn encode_stream_start_frame(
    descriptor: &StreamDescriptor,
    correlation_id: u16,
) -> [u8; STREAM_START_FRAME_LEN] {
    let mut frame = [0u8; STREAM_START_FRAME_LEN];
    frame[..4].copy_from_slice(
        &((framing::STREAM_HEADER_PREFIX_LEN + STREAM_DESCRIPTOR_SIZE) as u32).to_be_bytes(),
    );
    frame[4] = MessageType::StreamStart as u8;
    frame[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    let descriptor_offset = framing::LENGTH_PREFIX_LEN + framing::STREAM_HEADER_PREFIX_LEN;
    frame[descriptor_offset..descriptor_offset + STREAM_DESCRIPTOR_SIZE]
        .copy_from_slice(&descriptor.to_be_bytes());
    frame
}

/// Encode the StreamData header (payload is sent separately for vectored writes).
pub fn encode_stream_data_header(
    payload_len: usize,
    correlation_id: u16,
) -> Result<[u8; STREAM_DATA_HEADER_LEN]> {
    if payload_len > u32::MAX as usize {
        return Err(GossipError::InvalidStreamFrame(format!(
            "payload too large for StreamData: {} bytes",
            payload_len
        )));
    }

    let mut header = stream_data_header_template(correlation_id);
    header[..4]
        .copy_from_slice(&((framing::STREAM_HEADER_PREFIX_LEN + payload_len) as u32).to_be_bytes());
    Ok(header)
}

/// Pre-populated StreamData header template (length field must be patched before send).
pub fn stream_data_header_template(correlation_id: u16) -> [u8; STREAM_DATA_HEADER_LEN] {
    let mut header = [0u8; STREAM_DATA_HEADER_LEN];
    header[4] = MessageType::StreamData as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header
}

/// StreamEnd header with fixed payload length (no body).
pub fn encode_stream_end_header(correlation_id: u16) -> [u8; STREAM_DATA_HEADER_LEN] {
    let mut header = [0u8; STREAM_DATA_HEADER_LEN];
    header[..4].copy_from_slice(&(framing::STREAM_HEADER_PREFIX_LEN as u32).to_be_bytes());
    header[4] = MessageType::StreamEnd as u8;
    header[5..7].copy_from_slice(&correlation_id.to_be_bytes());
    header
}

/// Decode the descriptor portion of a StreamStart frame.
pub fn decode_stream_start(payload: &[u8]) -> Result<StreamDescriptor> {
    StreamDescriptor::from_be_slice(payload)
}

/// Completed zero-copy stream payload ready for actor dispatch.
#[derive(Debug)]
pub struct CompletedStream {
    pub descriptor: StreamDescriptor,
    pub payload: Bytes,
    pub correlation_id: Option<u16>,
}

#[derive(Debug)]
struct InProgressStream {
    descriptor: StreamDescriptor,
    buffer: StreamBuffer,
    write_offset: usize,
    correlation_id: Option<u16>,
    started_at: Instant,
}

impl InProgressStream {
    fn new(
        descriptor: StreamDescriptor,
        buffer: StreamBuffer,
        correlation_id: Option<u16>,
    ) -> Self {
        Self {
            descriptor,
            buffer,
            write_offset: 0,
            correlation_id,
            started_at: Instant::now(),
        }
    }

    fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.write_offset)
    }

    async fn read_chunk_direct<R>(&mut self, reader: &mut R, len: usize) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
    {
        if len == 0 {
            return Ok(());
        }

        let end = self.write_offset + len;
        {
            let buf = self.buffer.slice_mut(self.write_offset..end);
            reader.read_exact(buf).await.map_err(GossipError::Network)?;
        }
        self.write_offset = end;
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.write_offset == self.buffer.len()
    }

    fn fail(self) -> StreamDescriptor {
        self.descriptor
    }

    fn finalize(self) -> CompletedStream {
        let payload = self.buffer.into_bytes();
        CompletedStream {
            descriptor: self.descriptor,
            payload,
            correlation_id: self.correlation_id,
        }
    }
}

/// Stateful assembler for Streaming receive side.
#[derive(Debug)]
pub struct StreamAssembler {
    max_payload_len: usize,
    current: Option<InProgressStream>,
}

impl StreamAssembler {
    /// Create a new assembler enforcing the provided payload limit.
    pub fn new(max_payload_len: usize) -> Self {
        Self {
            max_payload_len,
            current: None,
        }
    }

    /// Start a new stream using the provided descriptor.
    pub fn start_stream(
        &mut self,
        descriptor: StreamDescriptor,
        correlation_id: Option<u16>,
    ) -> Result<Option<CompletedStream>> {
        if self.current.is_some() {
            return Err(GossipError::InvalidStreamFrame(
                "StreamStart received while another stream is active".into(),
            ));
        }

        let payload_len = self.validate_payload_len(descriptor.payload_len)?;
        if payload_len == 0 {
            // Immediate completion (no data frames expected)
            return Ok(Some(CompletedStream {
                descriptor,
                payload: Bytes::new(),
                correlation_id,
            }));
        }

        let buffer = StreamBuffer::new(payload_len);
        self.current = Some(InProgressStream::new(descriptor, buffer, correlation_id));
        Ok(None)
    }

    async fn drain_unclaimed_bytes<R>(reader: &mut R, mut len: usize) -> Result<()>
    where
        R: AsyncReadExt + Unpin,
    {
        if len == 0 {
            return Ok(());
        }

        let mut scratch = [0u8; 4096];
        while len > 0 {
            let take = len.min(scratch.len());
            reader
                .read_exact(&mut scratch[..take])
                .await
                .map_err(GossipError::Network)?;
            len -= take;
        }
        Ok(())
    }

    /// Read a StreamData payload directly into the active buffer without an intermediate allocation.
    pub async fn read_data_direct<R>(
        &mut self,
        reader: &mut R,
        chunk_len: usize,
    ) -> Result<Option<CompletedStream>>
    where
        R: AsyncReadExt + Unpin,
    {
        if chunk_len == 0 {
            return Ok(None);
        }

        let Some(state) = self.current.as_mut() else {
            Self::drain_unclaimed_bytes(reader, chunk_len).await?;
            return Err(GossipError::InvalidStreamFrame(
                "StreamData received before StreamStart".into(),
            ));
        };

        let remaining = state.remaining();
        if chunk_len > remaining {
            Self::drain_unclaimed_bytes(reader, chunk_len).await?;
            return Err(GossipError::InvalidStreamFrame(format!(
                "StreamData payload ({chunk_len}) exceeds remaining bytes ({remaining})"
            )));
        }

        state.read_chunk_direct(reader, chunk_len).await?;
        if state.is_complete() {
            return self.finish_current();
        }

        Ok(None)
    }

    /// Handle an optional StreamEnd marker (must arrive after completion).
    pub fn finish_with_end(&mut self) -> Result<Option<CompletedStream>> {
        match self.current.as_ref() {
            None => Ok(None),
            Some(state) => {
                if !state.is_complete() {
                    return Err(GossipError::InvalidStreamFrame(
                        "StreamEnd received before payload complete".into(),
                    ));
                }
                self.finish_current()
            }
        }
    }

    /// Abort the current stream if it has exceeded the provided age.
    pub fn abort_if_stale(&mut self, max_age: Duration) -> Option<StreamDescriptor> {
        if let Some(state) = self.current.as_ref() {
            if state.started_at.elapsed() > max_age {
                return Some(self.current.take().unwrap().fail());
            }
        }
        None
    }

    fn finish_current(&mut self) -> Result<Option<CompletedStream>> {
        let state = self.current.take().ok_or_else(|| {
            GossipError::InvalidStreamFrame("No active stream to finalize".into())
        })?;
        Ok(Some(state.finalize()))
    }

    fn validate_payload_len(&self, payload_len: u64) -> Result<usize> {
        let len = usize::try_from(payload_len).map_err(|_| {
            GossipError::InvalidStreamFrame(format!(
                "payload_len {} does not fit in usize",
                payload_len
            ))
        })?;
        if len > self.max_payload_len {
            return Err(GossipError::InvalidStreamFrame(format!(
                "payload_len {} exceeds max {}",
                payload_len, self.max_payload_len
            )));
        }
        Ok(len)
    }
}

impl StreamAssembler {
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    pub fn active_buffer_identity(&self) -> Option<(*const u8, usize)> {
        self.current
            .as_ref()
            .map(|state| (state.buffer.as_ptr(), state.buffer.len()))
    }

    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    #[allow(dead_code)]
    pub fn active_buffer_identity(&self) -> Option<(*const u8, usize)> {
        None
    }
}

#[derive(Debug)]
struct StreamBuffer {
    ptr: NonNull<u8>,
    len: usize,
}

impl StreamBuffer {
    fn new(len: usize) -> Self {
        if len == 0 {
            return Self {
                ptr: NonNull::dangling(),
                len,
            };
        }

        unsafe {
            let layout = Layout::from_size_align_unchecked(len, STREAM_ALIGNMENT);
            let raw = alloc(layout);
            if raw.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Self::zero_bytes(raw, len);
            Self {
                ptr: NonNull::new_unchecked(raw),
                len,
            }
        }
    }

    fn zero_bytes(ptr: *mut u8, len: usize) {
        unsafe {
            std::ptr::write_bytes(ptr, 0, len);
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    fn as_ptr(&self) -> *const u8 {
        if self.len == 0 {
            std::ptr::null()
        } else {
            self.ptr.as_ptr()
        }
    }

    fn slice_mut(&mut self, range: Range<usize>) -> &mut [u8] {
        assert!(range.end <= self.len);
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr().add(range.start), range.len()) }
    }

    fn into_bytes(self) -> Bytes {
        if self.len == 0 {
            return Bytes::new();
        }

        let len = self.len;
        let ptr = self.ptr;
        std::mem::forget(self);
        unsafe {
            let vec = Vec::from_raw_parts(ptr.as_ptr(), len, len);
            Bytes::from(vec)
        }
    }
}

impl Drop for StreamBuffer {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.len, STREAM_ALIGNMENT);
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

unsafe impl Send for StreamBuffer {}
unsafe impl Sync for StreamBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn sample_descriptor() -> StreamDescriptor {
        StreamDescriptor {
            stream_id: 0xAA55AA55AA55AA55,
            payload_len: 1024,
            chunk_len: 1024,
            type_hash: 0xDEADBEEF,
            actor_id: 0x1122334455667788,
            flags: 0x5A5A5A5A,
            reserved: 0,
        }
    }

    #[test]
    fn descriptor_layout_is_stable() {
        use std::mem::{align_of, size_of};
        assert_eq!(size_of::<StreamDescriptor>(), STREAM_DESCRIPTOR_SIZE);
        assert_eq!(align_of::<StreamDescriptor>(), align_of::<u64>());
        assert_eq!(std::mem::offset_of!(StreamDescriptor, stream_id), 0);
        assert_eq!(std::mem::offset_of!(StreamDescriptor, payload_len), 8);
        assert_eq!(std::mem::offset_of!(StreamDescriptor, chunk_len), 16);
        assert_eq!(std::mem::offset_of!(StreamDescriptor, type_hash), 20);
        assert_eq!(std::mem::offset_of!(StreamDescriptor, actor_id), 24);
        assert_eq!(std::mem::offset_of!(StreamDescriptor, flags), 32);
    }

    #[test]
    fn descriptor_roundtrip_big_endian() {
        let descriptor = sample_descriptor();
        let encoded = descriptor.to_be_bytes();
        let decoded = StreamDescriptor::from_be_slice(&encoded).unwrap();
        assert_eq!(descriptor, decoded);
    }

    #[test]
    fn stream_start_frame_alignment_is_correct() {
        let descriptor = sample_descriptor();
        let frame = encode_stream_start_frame(&descriptor, 0x2222);
        assert_eq!(frame.len(), STREAM_START_FRAME_LEN);
        assert_eq!(frame[4], MessageType::StreamStart as u8);
        assert_eq!(u16::from_be_bytes([frame[5], frame[6]]), 0x2222);
        let offset = framing::LENGTH_PREFIX_LEN + framing::STREAM_HEADER_PREFIX_LEN;
        assert_eq!(offset, 16);
        assert_eq!(
            &frame[offset..offset + STREAM_DESCRIPTOR_SIZE],
            &descriptor.to_be_bytes()
        );
    }

    #[test]
    fn stream_data_header_reflects_payload_length() {
        let header = encode_stream_data_header(512, 0xABCD).unwrap();
        assert_eq!(header.len(), STREAM_DATA_HEADER_LEN);
        assert_eq!(header[4], MessageType::StreamData as u8);
        assert_eq!(u16::from_be_bytes([header[5], header[6]]), 0xABCD);
        let expected = framing::STREAM_HEADER_PREFIX_LEN + 512;
        assert_eq!(
            u32::from_be_bytes(header[0..4].try_into().unwrap()),
            expected as u32
        );
    }

    #[test]
    fn decode_rejects_short_descriptor() {
        let short = [0u8; 10];
        let err = StreamDescriptor::from_be_slice(&short).unwrap_err();
        match err {
            GossipError::InvalidStreamFrame(msg) => {
                assert!(msg.contains("descriptor too short"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn data_header_rejects_large_payload() {
        let err = encode_stream_data_header(u32::MAX as usize + 1, 0).unwrap_err();
        match err {
            GossipError::InvalidStreamFrame(msg) => {
                assert!(msg.contains("payload too large"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn assembler_roundtrip_happy_path() {
        let mut assembler = StreamAssembler::new(1024);
        let descriptor = sample_descriptor();
        let correlation_id = Some(7);
        assert!(assembler
            .start_stream(descriptor, correlation_id)
            .unwrap()
            .is_none());

        let payload = vec![1u8; 1024];
        let (mut writer, mut reader) = tokio::io::duplex(2048);
        let writer_task = tokio::spawn({
            let chunk = payload.clone();
            async move {
                writer.write_all(&chunk).await.unwrap();
            }
        });

        let completed = assembler
            .read_data_direct(&mut reader, payload.len())
            .await
            .unwrap()
            .unwrap();
        writer_task.await.unwrap();

        assert_eq!(completed.descriptor.stream_id, descriptor.stream_id);
        assert_eq!(completed.correlation_id, correlation_id);
        assert_eq!(completed.payload.len(), descriptor.payload_len as usize);
        assert_eq!(completed.payload.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn assembler_rejects_data_before_start() {
        let mut assembler = StreamAssembler::new(512);
        let payload = vec![1u8, 2, 3];
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let writer_task = tokio::spawn({
            let bytes = payload.clone();
            async move {
                writer.write_all(&bytes).await.unwrap();
            }
        });
        let err = assembler
            .read_data_direct(&mut reader, payload.len())
            .await
            .unwrap_err();
        writer_task.await.unwrap();
        assert!(matches!(err, GossipError::InvalidStreamFrame(_)));
    }

    #[tokio::test]
    async fn assembler_detects_overflow() {
        let mut assembler = StreamAssembler::new(4);
        let mut descriptor = sample_descriptor();
        descriptor.payload_len = 4;
        assembler.start_stream(descriptor, None).unwrap();

        let payload = vec![1u8, 2, 3, 4, 5];
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let writer_task = tokio::spawn({
            let bytes = payload.clone();
            async move {
                writer.write_all(&bytes).await.unwrap();
            }
        });

        let err = assembler
            .read_data_direct(&mut reader, payload.len())
            .await
            .unwrap_err();
        writer_task.await.unwrap();
        assert!(matches!(err, GossipError::InvalidStreamFrame(_)));
    }

    #[test]
    fn assembler_rejects_concurrent_start() {
        let mut assembler = StreamAssembler::new(2048);
        let descriptor = sample_descriptor();
        assembler.start_stream(descriptor, None).unwrap();
        let err = assembler.start_stream(descriptor, None).unwrap_err();
        assert!(matches!(err, GossipError::InvalidStreamFrame(_)));
    }

    #[tokio::test]
    async fn assembler_finish_requires_complete_payload() {
        let mut assembler = StreamAssembler::new(16);
        let mut descriptor = sample_descriptor();
        descriptor.payload_len = 8;
        assembler.start_stream(descriptor, None).unwrap();

        let payload = vec![0u8; 4];
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let writer_task = tokio::spawn({
            let bytes = payload.clone();
            async move {
                writer.write_all(&bytes).await.unwrap();
            }
        });

        let partial = assembler
            .read_data_direct(&mut reader, payload.len())
            .await
            .unwrap();
        writer_task.await.unwrap();
        assert!(partial.is_none());

        let err = assembler.finish_with_end().unwrap_err();
        assert!(matches!(err, GossipError::InvalidStreamFrame(_)));
    }

    #[tokio::test]
    async fn assembler_end_after_completion_is_noop() {
        let mut assembler = StreamAssembler::new(16);
        let mut descriptor = sample_descriptor();
        descriptor.payload_len = 4;
        assembler.start_stream(descriptor, None).unwrap();

        let payload = vec![1u8; 4];
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let writer_task = tokio::spawn({
            let bytes = payload.clone();
            async move {
                writer.write_all(&bytes).await.unwrap();
            }
        });

        let completion = assembler
            .read_data_direct(&mut reader, payload.len())
            .await
            .unwrap();
        writer_task.await.unwrap();
        assert!(completion.is_some());

        let end_result = assembler.finish_with_end().unwrap();
        assert!(end_result.is_none());
    }
}
