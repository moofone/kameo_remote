use crate::{GossipError, MessageType, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Handle for replying to a remote ask() request
/// This can be passed between local actors to delegate the reply
#[derive(Clone)]
pub struct ReplyTo {
    /// Correlation ID to match request with response
    pub(crate) correlation_id: u16,
    /// Connection back to the requesting node
    pub(crate) connection: Arc<crate::connection_pool::ConnectionHandle>,
}

impl ReplyTo {
    /// Send reply back to the original requester
    pub async fn reply(self, response: &[u8]) -> Result<()> {
        // Build response message with length prefix + 8-byte header + payload
        let total_size = 8 + response.len();
        let mut msg = Vec::with_capacity(4 + total_size);

        // Length prefix (4 bytes)
        msg.extend_from_slice(&(total_size as u32).to_be_bytes());

        // Header: [type:1][corr_id:2][reserved:5]
        msg.push(MessageType::Response as u8);
        msg.extend_from_slice(&self.correlation_id.to_be_bytes());
        msg.extend_from_slice(&[0u8; 5]); // reserved bytes
        msg.extend_from_slice(response);

        tracing::info!(
            "ReplyTo::reply: sending response with correlation_id={} on connection to {}",
            self.correlation_id,
            self.connection.addr
        );

        // Send directly using the connection's raw bytes method
        let result = self.connection.send_raw_bytes(&msg);

        match &result {
            Ok(_) => tracing::info!("ReplyTo::reply: successfully sent response"),
            Err(e) => tracing::error!("ReplyTo::reply: failed to send response: {}", e),
        }

        result
    }

    /// Reply with a serializable type using rkyv
    pub async fn reply_with<T>(self, value: &T) -> Result<()>
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
        let response =
            rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(GossipError::Serialization)?;
        self.reply(&response).await
    }

    /// Create a reply handle with timeout
    pub fn with_timeout(self, timeout: Duration) -> TimeoutReplyTo {
        TimeoutReplyTo {
            inner: self,
            deadline: Instant::now() + timeout,
        }
    }
}

/// ReplyTo handle with timeout enforcement
pub struct TimeoutReplyTo {
    inner: ReplyTo,
    deadline: Instant,
}

impl TimeoutReplyTo {
    /// Reply if not timed out
    pub async fn reply(self, response: &[u8]) -> Result<()> {
        if Instant::now() > self.deadline {
            return Err(GossipError::Timeout);
        }
        self.inner.reply(response).await
    }

    /// Reply with a serializable type if not timed out
    pub async fn reply_with<T>(self, value: &T) -> Result<()>
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
        if Instant::now() > self.deadline {
            return Err(GossipError::Timeout);
        }
        self.inner.reply_with(value).await
    }

    /// Check if timed out
    pub fn is_timed_out(&self) -> bool {
        Instant::now() > self.deadline
    }

    /// Get remaining time before timeout
    pub fn time_remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }
}

impl std::fmt::Debug for ReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyTo")
            .field("correlation_id", &self.correlation_id)
            .field("connection", &self.connection.addr)
            .finish()
    }
}

impl std::fmt::Debug for TimeoutReplyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutReplyTo")
            .field("correlation_id", &self.inner.correlation_id)
            .field("deadline", &self.deadline)
            .field("time_remaining", &self.time_remaining())
            .finish()
    }
}
