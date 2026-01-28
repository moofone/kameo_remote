use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
};

use bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, info, warn};

use crate::{
    handle::{
        handle_raw_ask_request, handle_response_message, send_streaming_response, MessageReadResult,
    },
    registry::{GossipRegistry, RegistryMessage},
    GossipError, Result,
};

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
pub struct StreamingState {
    active_streams: HashMap<u64, InProgressStream>,
    max_concurrent_streams: usize,
}

/// A stream that is currently being assembled
#[derive(Debug)]
struct InProgressStream {
    stream_id: u64,
    total_size: u64,
    type_hash: u32,
    actor_id: u64,
    correlation_id: u16,
    received_size: usize,
    /// Accumulator for chunk data.
    /// Since we are on TCP, chunks are expected to arrive in order.
    /// This avoids the overhead of a BTreeMap and intermediate allocations.
    data_accumulator: BytesMut,
    /// Timestamp when stream started (for stale cleanup)
    started_at: std::time::Instant,
}

impl StreamingState {
    pub fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            max_concurrent_streams: 16, // Reasonable limit
        }
    }

    pub fn start_stream_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        correlation_id: u16,
    ) -> Result<()> {
        if self.active_streams.len() >= self.max_concurrent_streams {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "Too many concurrent streams",
            )));
        }

        // Only insert if not already exists to avoid resetting progress on duplicate start frames
        if !self.active_streams.contains_key(&header.stream_id) {
            let stream = InProgressStream {
                stream_id: header.stream_id,
                total_size: header.total_size,
                type_hash: header.type_hash,
                actor_id: header.actor_id,
                correlation_id,
                received_size: 0,
                data_accumulator: BytesMut::with_capacity(header.total_size as usize),
                started_at: std::time::Instant::now(),
            };
            self.active_streams.insert(header.stream_id, stream);
        }
        Ok(())
    }

    pub fn add_chunk_with_correlation(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Bytes,
    ) -> Result<Option<(Bytes, u16)>> {
        // If stream doesn't exist, we might have missed the start frame or it was cleaned up
        if !self.active_streams.contains_key(&header.stream_id) {
             return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                )));
        }

        let stream = self
            .active_streams
            .get_mut(&header.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                ))
            })?;

        // NOTE: In the previous BTreeMap implementation, deduplication was automatic by key.
        // With BytesMut, we assume strict TCP ordering and no duplicates.
        // For robustness, we could check if appending would exceed total_size.
        if stream.received_size + chunk_data.len() > stream.total_size as usize {
             return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk overflow for stream_id={}", header.stream_id),
                )));
        }

        // Validate chunk index? 
        // We are assuming ordered delivery. The chunk_index is mostly for manual assembly logic,
        // but with TCP and BytesMut we just append.
        
        // Append chunk
        stream.received_size += chunk_data.len();
        stream.data_accumulator.put_slice(&chunk_data);

        // Check if we have all chunks (when total matches expected size)
        if stream.received_size >= stream.total_size as usize {
            self.assemble_complete_message_with_correlation(header.stream_id)
        } else {
            Ok(None)
        }
    }

    pub fn finalize_stream_with_correlation(
        &mut self,
        stream_id: u64,
    ) -> Result<Option<(Bytes, u16)>> {
        // StreamEnd received - assemble the message
        self.assemble_complete_message_with_correlation(stream_id)
    }

    fn assemble_complete_message_with_correlation(
        &mut self,
        stream_id: u64,
    ) -> Result<Option<(Bytes, u16)>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

        let correlation_id = stream.correlation_id;
        let complete_data = stream.data_accumulator.freeze();

        info!(
            "✅ STREAMING: Assembled complete message for stream_id={} ({} bytes for actor={}, type_hash=0x{:x}, correlation_id={})",
            stream.stream_id,
            complete_data.len(),
            stream.actor_id,
            stream.type_hash,
            correlation_id
        );

        Ok(Some((complete_data, correlation_id)))
    }

    /// Clean up stale streams that have been incomplete for too long.
    pub fn cleanup_stale(&mut self) {
        use std::time::Duration;
        const STREAM_TIMEOUT: Duration = Duration::from_secs(60);

        let before_count = self.active_streams.len();

        self.active_streams.retain(|stream_id, stream| {
            let age = stream.started_at.elapsed();
            if age > STREAM_TIMEOUT {
                warn!(
                    stream_id = stream_id,
                    age_secs = age.as_secs(),
                    received_size = stream.received_size,
                    expected_size = stream.total_size,
                    "Cleaning up stale stream - StreamEnd never arrived"
                );
                false // Remove this entry
            } else {
                true // Keep this entry
            }
        });

        let removed = before_count - self.active_streams.len();
        if removed > 0 {
            info!(
                removed_count = removed,
                remaining = self.active_streams.len(),
                "Cleaned up stale in-progress streams"
            );
        }
    }
}

impl Default for StreamingState {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a single read result result using the shared protocol logic.
///
/// This handles:
/// - Gossip messages -> registry.handle_incoming_message
/// - Raw Asks -> handle_raw_ask_request
/// - Responses -> handle_response_message
/// - Actor messages -> registry.actor_message_handler
/// - Streaming messages -> state.streaming (assembly) -> handler
pub(crate) async fn process_read_result(
    result: MessageReadResult,
    streaming_state: &mut StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
) -> Result<()> {
    match result {
        MessageReadResult::Gossip(msg, correlation_id) => {
            // For ActorMessage with correlation_id from Ask envelope, ensure it's set
            let msg_to_handle = if let RegistryMessage::ActorMessage {
                actor_id,
                type_hash,
                payload,
                correlation_id: _,
            } = msg
            {
                // Create a new ActorMessage with the correlation_id from the Ask envelope
                RegistryMessage::ActorMessage {
                    actor_id,
                    type_hash,
                    payload,
                    correlation_id,
                }
            } else {
                msg
            };

            if let Err(e) =
                crate::connection_pool::handle_incoming_message(registry.clone(), peer_addr, msg_to_handle).await
            {
                warn!(error = %e, "Failed to process gossip message");
            }
        }
        MessageReadResult::AskRaw {
            correlation_id,
            payload,
        } => {
            handle_raw_ask_request(registry, peer_addr, correlation_id, &payload).await;
        }
        MessageReadResult::Response {
            correlation_id,
            payload,
        } => {
            handle_response_message(registry, peer_addr, correlation_id, payload).await;
        }
        MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            payload,
        } => {
            // Handle actor message directly
            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                let actor_id_str = actor_id.to_string();
                let correlation = if msg_type == crate::MessageType::ActorAsk as u8 {
                    Some(correlation_id)
                } else {
                    None
                };
                let _ = handler
                    .handle_actor_message(&actor_id_str, type_hash, &payload, correlation)
                    .await;
            }
        }
        MessageReadResult::Streaming {
            msg_type,
            correlation_id,
            stream_header,
            chunk_data,
        } => {
            // Handle streaming messages
            match msg_type {
                msg_type
                    if msg_type == crate::MessageType::StreamStart as u8
                        || msg_type == crate::MessageType::StreamResponseStart as u8 =>
                {
                    if let Err(e) = streaming_state
                        .start_stream_with_correlation(stream_header, correlation_id)
                    {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                    }
                }
                msg_type
                    if msg_type == crate::MessageType::StreamData as u8
                        || msg_type == crate::MessageType::StreamResponseData as u8 =>
                {
                     // Ensure stream is started (auto-start)
                    if let Err(e) = streaming_state
                        .start_stream_with_correlation(stream_header, correlation_id)
                    {
                        debug!(error = %e, "Auto-starting stream for data chunk: stream_id={}", stream_header.stream_id);
                    }
                    
                    if let Ok(Some((complete_data, corr_id))) =
                        streaming_state.add_chunk_with_correlation(stream_header, chunk_data)
                    {
                        if msg_type == crate::MessageType::StreamResponseData as u8 {
                            debug!(stream_id = stream_header.stream_id, correlation_id = corr_id, "✅ STREAMING RESPONSE: Assembled complete response via data chunk");
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                complete_data,
                            )
                            .await;
                        } else {
                            handle_assembled_message(
                                registry,
                                peer_addr,
                                stream_header.actor_id,
                                stream_header.type_hash,
                                complete_data,
                                corr_id,
                            )
                            .await;
                        }
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamEnd as u8 => {
                    // StreamEnd indicates the end of an incoming REQUEST (streaming tell/ask)
                    if let Ok(Some((complete_data, corr_id))) =
                        streaming_state.finalize_stream_with_correlation(stream_header.stream_id)
                    {
                         handle_assembled_message(
                            registry,
                            peer_addr,
                            stream_header.actor_id,
                            stream_header.type_hash,
                            complete_data,
                            corr_id,
                        )
                        .await;
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamResponseEnd as u8 => {
                    // StreamResponseEnd indicates the end of an incoming RESPONSE (from a remote ask)
                    if let Ok(Some((complete_data, corr_id))) =
                        streaming_state.finalize_stream_with_correlation(stream_header.stream_id)
                    {
                        if corr_id != 0 {
                            // This is a response to an ask - deliver it to the correlation tracker
                            debug!(stream_id = stream_header.stream_id, correlation_id = corr_id, response_len = complete_data.len(),
                                  "📨 STREAMING RESPONSE: Delivering complete streaming response to correlation tracker");
                            handle_response_message(
                                registry,
                                peer_addr,
                                corr_id,
                                complete_data,
                            )
                            .await;
                        } else {
                            debug!(stream_id = stream_header.stream_id,
                                  "Ignoring streaming response with correlation_id=0 (tell responses should not exist)");
                        }
                    }
                }
                _ => {
                    warn!("Unknown streaming message type: 0x{:02x}", msg_type);
                }
            }
        }
        MessageReadResult::Raw(_payload) => {
             #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok() {
                    crate::test_helpers::record_raw_payload(_payload.clone());
                }
            }
            debug!(peer_addr = %peer_addr, "Ignoring raw message payload");
        }
    }
    
    Ok(())
}

async fn handle_assembled_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    actor_id: u64,
    type_hash: u32,
    complete_data: Bytes,
    corr_id: u16,
) {
    // Complete message assembled - route to actor
    // corr_id == 0 means tell (fire-and-forget), non-zero means ask (expects response)
    let correlation_opt = if corr_id == 0 { None } else { Some(corr_id) };
    if let Some(handler) = &*registry.actor_message_handler.lock().await {
        let actor_id_str = actor_id.to_string();
        if let Ok(Some(response)) = handler
            .handle_actor_message(
                &actor_id_str,
                type_hash,
                &complete_data,
                correlation_opt,
            )
            .await
        {
            // Only send response for asks (non-zero correlation_id)
            if corr_id != 0 {
                send_streaming_response(registry, peer_addr, corr_id, response).await;
            }
        }
    }
}
