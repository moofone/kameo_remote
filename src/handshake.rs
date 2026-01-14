//! Hello handshake protocol for peer capability negotiation
//!
//! This module implements the Hello handshake that establishes peer capabilities
//! at connection time. This enables gradual feature rollout and backward compatibility.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::collections::HashSet;

/// Protocol version constants
pub const PROTOCOL_VERSION_V1: u16 = 1;
pub const PROTOCOL_VERSION_V2: u16 = 2;

/// Current protocol version
pub const CURRENT_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION_V2;

/// Feature flags for capability negotiation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum Feature {
    /// Peer list gossip for automatic peer discovery
    PeerListGossip,
}

/// Hello message sent during connection establishment
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct Hello {
    /// Protocol version this node supports
    pub protocol_version: u16,
    /// Features this node supports
    pub features: Vec<Feature>,
}

impl Hello {
    /// Create a new Hello message with current capabilities
    pub fn new() -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            features: vec![Feature::PeerListGossip],
        }
    }

    /// Create a legacy Hello message (v1, no peer discovery)
    pub fn legacy() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION_V1,
            features: vec![],
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
    /// Create capabilities for a legacy (v1) peer
    /// Legacy peers don't support any new features
    pub fn legacy() -> Self {
        Self {
            version: PROTOCOL_VERSION_V1,
            features: HashSet::new(),
        }
    }

    /// Create capabilities from a Hello exchange
    /// Takes the intersection of features and minimum version
    pub fn from_hello_exchange(local: &Hello, remote: &Hello) -> Self {
        let version = local.protocol_version.min(remote.protocol_version);

        // Compute feature intersection
        let local_features: HashSet<_> = local.features.iter().copied().collect();
        let remote_features: HashSet<_> = remote.features.iter().copied().collect();
        let features: HashSet<_> = local_features.intersection(&remote_features).copied().collect();

        Self { version, features }
    }

    /// Check if we can send peer list gossip to this peer
    pub fn can_send_peer_list(&self) -> bool {
        self.version >= PROTOCOL_VERSION_V2 && self.features.contains(&Feature::PeerListGossip)
    }

    /// Check if a specific feature is supported
    pub fn supports_feature(&self, feature: Feature) -> bool {
        self.features.contains(&feature)
    }
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
    fn test_hello_legacy() {
        let hello = Hello::legacy();
        assert_eq!(hello.protocol_version, PROTOCOL_VERSION_V1);
        assert!(hello.features.is_empty());
    }

    #[test]
    fn test_hello_serialization() {
        let hello = Hello::new();

        // Serialize
        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&hello).unwrap();

        // Deserialize
        let deserialized: Hello =
            rkyv::from_bytes::<Hello, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(deserialized.protocol_version, hello.protocol_version);
        assert_eq!(deserialized.features.len(), hello.features.len());
        assert!(deserialized.features.contains(&Feature::PeerListGossip));
    }

    #[test]
    fn test_peer_capabilities_legacy() {
        let caps = PeerCapabilities::legacy();
        assert_eq!(caps.version, PROTOCOL_VERSION_V1);
        assert!(caps.features.is_empty());
        assert!(!caps.can_send_peer_list());
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
    fn test_peer_capabilities_from_hello_exchange_legacy_remote() {
        let local = Hello::new();
        let remote = Hello::legacy();

        let caps = PeerCapabilities::from_hello_exchange(&local, &remote);

        assert_eq!(caps.version, PROTOCOL_VERSION_V1);
        assert!(caps.features.is_empty()); // No common features
        assert!(!caps.can_send_peer_list());
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
    fn test_peer_capabilities_supports_feature() {
        let caps = PeerCapabilities::from_hello_exchange(&Hello::new(), &Hello::new());

        assert!(caps.supports_feature(Feature::PeerListGossip));
    }

    #[test]
    fn test_feature_serialization() {
        let feature = Feature::PeerListGossip;

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&feature).unwrap();
        let deserialized: Feature =
            rkyv::from_bytes::<Feature, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(deserialized, feature);
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

    #[test]
    fn test_hello_handshake_with_legacy_node() {
        // Scenario: V2 node connects to V1 node
        let v2_hello = Hello::new();
        let v1_hello = Hello::legacy();

        let caps = PeerCapabilities::from_hello_exchange(&v2_hello, &v1_hello);

        // Should fall back to V1 behavior
        assert_eq!(caps.version, PROTOCOL_VERSION_V1);
        assert!(!caps.can_send_peer_list());
    }
}
