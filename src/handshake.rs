//! Hello handshake protocol for peer capability negotiation
//!
//! This module implements the Hello handshake that establishes peer capabilities
//! at connection time for TLS-only v2 peers.

use crate::{rkyv_utils, tls, GossipError, Result};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::{collections::HashSet, io};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};
use tracing::debug;

const HELLO_MAX_SIZE: usize = 1024;
const HELLO_TIMEOUT_MS: u64 = 3_000;

/// Protocol version constants
pub const PROTOCOL_VERSION_V2: u16 = 2;

/// Current protocol version
pub const CURRENT_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION_V2;

/// Feature flags for capability negotiation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum Feature {
    /// Peer list gossip for automatic peer discovery
    PeerListGossip,
    /// Negotiated Streaming wire contract
    Streaming,
}

/// Hello message sent during connection establishment
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct Hello {
    /// Protocol version this node supports
    pub protocol_version: u16,
    /// Features this node supports
    pub features: Vec<Feature>,
}

const HELLO_ALIGNMENT: usize = std::mem::align_of::<rkyv::Archived<Hello>>();
type HelloAlignedVec = rkyv::util::AlignedVec<{ HELLO_ALIGNMENT }>;

struct HelloFrame {
    bytes: HelloAlignedVec,
}

impl HelloFrame {
    fn archived(&self) -> Result<&<Hello as Archive>::Archived> {
        Ok(rkyv_utils::access_archived::<Hello>(self.bytes.as_ref())?)
    }
}

impl Hello {
    /// Create a new Hello message with current capabilities
    pub fn new() -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            features: vec![Feature::PeerListGossip, Feature::Streaming],
        }
    }

    /// Create Hello with specific features
    pub fn with_features(features: Vec<Feature>) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            features,
        }
    }
}

impl Default for Hello {
    fn default() -> Self {
        Self::new()
    }
}

/// Negotiated peer capabilities after Hello exchange
#[derive(Debug, Clone)]
pub struct PeerCapabilities {
    /// Negotiated protocol version (min of both peers)
    pub version: u16,
    /// Features both peers support (intersection)
    pub features: HashSet<Feature>,
}

impl PeerCapabilities {
    /// Create capabilities from a Hello exchange
    /// Takes the intersection of features and minimum version
    pub fn from_hello_exchange(local: &Hello, remote: &Hello) -> Self {
        let version = local.protocol_version.min(remote.protocol_version);

        // Compute feature intersection
        let local_features: HashSet<_> = local.features.iter().copied().collect();
        let remote_features: HashSet<_> = remote.features.iter().copied().collect();
        let features: HashSet<_> = local_features
            .intersection(&remote_features)
            .copied()
            .collect();

        Self { version, features }
    }

    /// Create capabilities from a Hello exchange using archived (zero-copy) data.
    pub fn from_hello_exchange_archived(
        local: &Hello,
        remote: &<Hello as Archive>::Archived,
    ) -> Self {
        let remote_version = remote.protocol_version.to_native();
        let version = local.protocol_version.min(remote_version);

        let local_features: HashSet<_> = local.features.iter().copied().collect();
        let remote_features: HashSet<Feature> = remote
            .features
            .iter()
            .map(|feature| match feature {
                <Feature as Archive>::Archived::PeerListGossip => Feature::PeerListGossip,
                <Feature as Archive>::Archived::Streaming => Feature::Streaming,
            })
            .collect();
        let features: HashSet<_> = local_features
            .intersection(&remote_features)
            .copied()
            .collect();

        Self { version, features }
    }

    /// Check if we can send peer list gossip to this peer
    pub fn can_send_peer_list(&self) -> bool {
        self.version >= PROTOCOL_VERSION_V2 && self.features.contains(&Feature::PeerListGossip)
    }

    /// Determine if streaming can be used with this peer.
    pub fn supports_streaming(&self) -> bool {
        self.version >= PROTOCOL_VERSION_V2 && self.features.contains(&Feature::Streaming)
    }

    /// Check if a specific feature is supported
    pub fn supports_feature(&self, feature: Feature) -> bool {
        self.features.contains(&feature)
    }
}

async fn read_exact_with_timeout<R>(reader: &mut R, buf: &mut [u8]) -> Result<()>
where
    R: AsyncRead + Unpin + Send,
{
    let mut offset = 0;
    while offset < buf.len() {
        let slice = &mut buf[offset..];
        let read_future = async { reader.read(slice).await };
        let bytes_read = match timeout(Duration::from_millis(HELLO_TIMEOUT_MS), read_future).await {
            Ok(Ok(0)) => {
                return Err(GossipError::Network(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF during Hello handshake",
                )))
            }
            Ok(Ok(n)) => n,
            Ok(Err(err)) => return Err(GossipError::Network(err)),
            Err(_) => return Err(GossipError::Timeout),
        };
        offset += bytes_read;
    }
    Ok(())
}

async fn send_hello_message<W>(stream: &mut W, hello: &Hello) -> Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(hello)?;
    let len = serialized.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&serialized).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_hello_message<R>(reader: &mut R) -> Result<HelloFrame>
where
    R: AsyncRead + Unpin + Send,
{
    let mut len_buf = [0u8; 4];
    read_exact_with_timeout(reader, &mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len == 0 || len > HELLO_MAX_SIZE {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "invalid Hello size: {} bytes",
            len
        )));
    }

    let mut buf = HelloAlignedVec::with_capacity(len);
    buf.resize(len, 0);
    read_exact_with_timeout(reader, &mut buf[..]).await?;
    Ok(HelloFrame { bytes: buf })
}

/// Perform Hello handshake if both peers negotiated discovery via ALPN
pub async fn perform_hello_handshake<S>(
    stream: &mut S,
    negotiated_alpn: Option<&[u8]>,
    enable_peer_discovery: bool,
) -> Result<PeerCapabilities>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let alpn = negotiated_alpn.ok_or_else(|| {
        GossipError::TlsHandshakeFailed("missing ALPN negotiation result".to_string())
    })?;

    if alpn != tls::ALPN_KAMEO_V2 {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "unsupported ALPN: {}",
            String::from_utf8_lossy(alpn)
        )));
    }

    let local_hello = if enable_peer_discovery {
        Hello::new()
    } else {
        Hello::with_features(Vec::new())
    };
    send_hello_message(stream, &local_hello).await?;
    let remote_frame = read_hello_message(stream).await?;
    let remote_hello = remote_frame.archived()?;
    let remote_version = remote_hello.protocol_version.to_native();
    if remote_version != CURRENT_PROTOCOL_VERSION {
        return Err(GossipError::TlsHandshakeFailed(format!(
            "unsupported protocol version: {}",
            remote_version
        )));
    }
    let caps = PeerCapabilities::from_hello_exchange_archived(&local_hello, remote_hello);
    debug!(
        negotiated_version = caps.version,
        peer_list = caps.can_send_peer_list(),
        "Hello handshake negotiated capabilities"
    );
    Ok(caps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_new() {
        let hello = Hello::new();
        assert_eq!(hello.protocol_version, CURRENT_PROTOCOL_VERSION);
        assert!(hello.features.contains(&Feature::PeerListGossip));
    }

    #[test]
    fn test_hello_serialization() {
        let hello = Hello::new();

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&hello).unwrap();

        let archived = crate::rkyv_utils::access_archived::<Hello>(&serialized).unwrap();

        assert_eq!(
            archived.protocol_version.to_native(),
            hello.protocol_version
        );
        assert_eq!(archived.features.len(), hello.features.len());
        assert!(archived
            .features
            .iter()
            .any(|feature| matches!(feature, <Feature as Archive>::Archived::PeerListGossip)));
    }

    #[test]
    fn test_peer_capabilities_from_hello_exchange_both_v2() {
        let local = Hello::new();
        let remote = Hello::new();

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert_eq!(caps.version, PROTOCOL_VERSION_V2);
        assert!(caps.features.contains(&Feature::PeerListGossip));
        assert!(caps.can_send_peer_list());
    }

    #[test]
    fn test_peer_capabilities_from_hello_exchange_partial_features() {
        let local = Hello::with_features(vec![Feature::PeerListGossip]);
        let remote = Hello {
            protocol_version: PROTOCOL_VERSION_V2,
            features: vec![], // Remote supports v2 but no features
        };

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert_eq!(caps.version, PROTOCOL_VERSION_V2);
        assert!(caps.features.is_empty()); // No common features
        assert!(!caps.can_send_peer_list()); // Needs both version and feature
    }

    #[test]
    fn test_peer_capabilities_supports_features() {
        let caps = PeerCapabilities::from_hello_exchange(&Hello::new(), &Hello::new());

        assert!(caps.supports_feature(Feature::PeerListGossip));
        assert!(caps.supports_streaming());
    }

    #[test]
    fn test_feature_serialization() {
        let feature = Feature::PeerListGossip;

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&feature).unwrap();
        let archived = crate::rkyv_utils::access_archived::<Feature>(&serialized).unwrap();
        assert!(matches!(
            archived,
            <Feature as Archive>::Archived::PeerListGossip
        ));
    }

    #[test]
    fn test_hello_handshake_negotiation() {
        // Scenario: Two v2 nodes negotiate capabilities
        let node_a_hello = Hello::new();
        let node_b_hello = Hello::new();

        // Both nodes perform handshake
        let a_caps = PeerCapabilities::from_hello_exchange(&node_a_hello, &node_b_hello);
        let b_caps = PeerCapabilities::from_hello_exchange(&node_b_hello, &node_a_hello);

        // Both should arrive at same capabilities
        assert_eq!(a_caps.version, b_caps.version);
        assert_eq!(a_caps.features, b_caps.features);
        assert!(a_caps.can_send_peer_list());
        assert!(b_caps.can_send_peer_list());
    }
}
