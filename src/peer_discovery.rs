//! Peer Discovery Manager
//!
//! Handles automatic peer discovery through gossip, including:
//! - Filtering discovered peers (self, already connected, SSRF/bogon)
//! - Exponential backoff for failed connection attempts
//! - Soft cap enforcement for connection limits

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use tracing::{debug, warn};

use crate::current_timestamp;
use crate::registry::PeerInfoGossip;

/// Maximum consecutive failures before removing a peer from discovery
pub const MAX_PEER_FAILURES: u8 = 10;

/// Maximum backoff time in seconds (1 hour)
pub const MAX_BACKOFF_SECONDS: u64 = 3600;

/// Unified peer state (replaces separate pending/failed/connected HashMaps)
///
/// Enables atomic state transitions - a peer is always in exactly ONE state.
/// Prevents race conditions where concurrent operations could leave a peer
/// in inconsistent states (e.g., both pending and connected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Peer discovered, connection attempt pending
    Pending {
        /// Timestamp (seconds since epoch) when peer was added to pending
        since: u64,
    },
    /// Connection attempt failed, in backoff
    Failed {
        /// Timestamp of last failure
        since: u64,
        /// Number of consecutive failures
        attempts: u8,
    },
    /// Successfully connected
    Connected,
}

impl PeerState {
    /// Check if peer is in Pending state
    pub fn is_pending(&self) -> bool {
        matches!(self, PeerState::Pending { .. })
    }

    /// Check if peer is in Failed state
    pub fn is_failed(&self) -> bool {
        matches!(self, PeerState::Failed { .. })
    }

    /// Check if peer is in Connected state
    pub fn is_connected(&self) -> bool {
        matches!(self, PeerState::Connected)
    }

    /// Get the pending timestamp if in Pending state
    pub fn pending_since(&self) -> Option<u64> {
        match self {
            PeerState::Pending { since } => Some(*since),
            _ => None,
        }
    }

    /// Get failure info if in Failed state
    pub fn failure_info(&self) -> Option<(u64, u8)> {
        match self {
            PeerState::Failed { since, attempts } => Some((*since, *attempts)),
            _ => None,
        }
    }

    /// Calculate backoff time for Failed state (same formula used during migration)
    pub fn backoff_seconds(&self) -> u64 {
        match self {
            PeerState::Failed { attempts, .. } => {
                let backoff = 2u64.saturating_pow(*attempts as u32);
                backoff.min(MAX_BACKOFF_SECONDS)
            }
            _ => 0,
        }
    }

    /// Check if we should retry (for Failed state)
    pub fn should_retry(&self, now: u64) -> bool {
        match self {
            PeerState::Failed { since, attempts } => {
                let backoff = 2u64
                    .saturating_pow(*attempts as u32)
                    .min(MAX_BACKOFF_SECONDS);
                now >= since.saturating_add(backoff)
            }
            _ => true, // Non-failed states can always "retry"
        }
    }
}

/// Configuration for peer discovery
#[derive(Debug, Clone)]
pub struct PeerDiscoveryConfig {
    /// Maximum number of peers to maintain (soft cap)
    pub max_peers: usize,
    /// Allow discovery of private IP addresses (10.x, 172.16-31.x, 192.168.x)
    pub allow_private_discovery: bool,
    /// Allow discovery of loopback addresses (127.x.x.x)
    pub allow_loopback_discovery: bool,
    /// Allow discovery of link-local addresses (169.254.x.x)
    pub allow_link_local_discovery: bool,
    /// Time-to-live for failed peers before eviction
    pub fail_ttl: Duration,
    /// Time-to-live for pending peers before eviction
    pub pending_ttl: Duration,
}

impl Default for PeerDiscoveryConfig {
    fn default() -> Self {
        Self {
            max_peers: 100,
            allow_private_discovery: true, // Allow private IPs by default
            allow_loopback_discovery: false, // Block loopback by default
            allow_link_local_discovery: false, // Block link-local by default
            fail_ttl: Duration::from_secs(6 * 60 * 60), // 6h matches GossipConfig default
            pending_ttl: Duration::from_secs(60 * 60), // 1h matches GossipConfig default
        }
    }
}

/// Summary of expired entries removed during cleanup
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PeerDiscoveryCleanupStats {
    pub pending_removed: usize,
    pub failed_removed: usize,
}

/// Peer Discovery Manager that stores all peer lifecycle data in a single map.
///
/// Every peer lives in exactly one `PeerState`, preventing drift between
/// separate HashMaps used during migration. All metrics, counters, and helpers
/// derive from this unified state.
#[derive(Debug)]
pub struct PeerDiscovery {
    /// Configuration
    config: PeerDiscoveryConfig,
    /// Local address (for self-filtering)
    local_addr: SocketAddr,
    /// Unified peer state map (atomic state transitions)
    peer_states: HashMap<SocketAddr, PeerState>,
}

impl PeerDiscovery {
    /// Create a new peer discovery manager
    pub fn new(local_addr: SocketAddr, config: PeerDiscoveryConfig) -> Self {
        Self {
            config,
            local_addr,
            peer_states: HashMap::new(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(local_addr: SocketAddr) -> Self {
        Self::new(local_addr, PeerDiscoveryConfig::default())
    }

    /// Process incoming peer list gossip and return candidates to connect to
    ///
    /// Filters out:
    /// - Self
    /// - Already connected peers
    /// - Pending connection peers
    /// - Peers in backoff
    /// - Unsafe addresses (bogon/SSRF)
    ///
    /// Returns candidates limited by soft cap.
    ///
    /// Uses unified `peer_states` for atomic state checks and transitions.
    pub fn on_peer_list_gossip(&mut self, peers: &[PeerInfoGossip]) -> Vec<SocketAddr> {
        let now = current_timestamp();
        let mut candidates = Vec::new();

        // Calculate how many more peers we can connect to using unified state
        // IMPORTANT: Include pending to prevent concurrent gossip overcommit
        let connected_count = self.connected_count_unified();
        let pending_count = self.pending_count_unified();
        let current_count = connected_count + pending_count;
        let remaining_slots = self.config.max_peers.saturating_sub(current_count);

        if remaining_slots == 0 {
            debug!(
                connected = connected_count,
                pending = pending_count,
                max = self.config.max_peers,
                "at soft cap, not accepting new peer candidates"
            );
            return candidates;
        }

        for peer_gossip in peers {
            // Parse the address
            let addr: SocketAddr = match peer_gossip.address.parse() {
                Ok(a) => a,
                Err(e) => {
                    debug!(addr = %peer_gossip.address, error = %e, "failed to parse peer address");
                    continue;
                }
            };

            // Filter self
            if addr == self.local_addr {
                continue;
            }

            // Check state using unified peer_states
            if let Some(state) = self.peer_states.get(&addr) {
                match state {
                    PeerState::Connected => {
                        // Already connected, skip
                        continue;
                    }
                    PeerState::Pending { .. } => {
                        // Already pending, skip
                        continue;
                    }
                    PeerState::Failed { .. } => {
                        // Check if backoff expired
                        if !state.should_retry(now) {
                            debug!(
                                addr = %addr,
                                backoff_secs = state.backoff_seconds(),
                                "peer in backoff, skipping"
                            );
                            continue;
                        }
                        // Backoff expired, can retry
                    }
                }
            }

            // Filter unsafe addresses
            if !self.is_safe_to_dial(&addr) {
                debug!(addr = %addr, "address blocked by security filter");
                continue;
            }

            candidates.push(addr);

            // Stop at soft cap
            if candidates.len() >= remaining_slots {
                break;
            }
        }

        // Atomically mark candidates as pending using peer_states
        for addr in &candidates {
            self.peer_states
                .insert(*addr, PeerState::Pending { since: now });
        }

        candidates
    }

    /// Process incoming peer list gossip using parsed socket addresses.
    /// Avoids allocating PeerInfoGossip for archived paths.
    pub fn on_peer_list_gossip_addrs<I>(&mut self, peers: I) -> Vec<SocketAddr>
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let now = current_timestamp();
        let mut candidates = Vec::new();

        // Calculate how many more peers we can connect to using unified state
        // IMPORTANT: Include pending to prevent concurrent gossip overcommit
        let connected_count = self.connected_count_unified();
        let pending_count = self.pending_count_unified();
        let current_count = connected_count + pending_count;
        let remaining_slots = self.config.max_peers.saturating_sub(current_count);

        if remaining_slots == 0 {
            debug!(
                connected = connected_count,
                pending = pending_count,
                max = self.config.max_peers,
                "at soft cap, not accepting new peer candidates"
            );
            return candidates;
        }

        for addr in peers {
            // Filter self
            if addr == self.local_addr {
                continue;
            }

            // Check state using unified peer_states
            if let Some(state) = self.peer_states.get(&addr) {
                match state {
                    PeerState::Connected => {
                        // Already connected, skip
                        continue;
                    }
                    PeerState::Pending { .. } => {
                        // Already pending, skip
                        continue;
                    }
                    PeerState::Failed { .. } => {
                        // Check if backoff expired
                        if !state.should_retry(now) {
                            debug!(
                                addr = %addr,
                                backoff_secs = state.backoff_seconds(),
                                "peer in backoff, skipping"
                            );
                            continue;
                        }
                        // Backoff expired, can retry
                    }
                }
            }

            // Filter unsafe addresses
            if !self.is_safe_to_dial(&addr) {
                debug!(addr = %addr, "address blocked by security filter");
                continue;
            }

            candidates.push(addr);

            // Stop at soft cap
            if candidates.len() >= remaining_slots {
                break;
            }
        }

        // Atomically mark candidates as pending using peer_states
        for addr in &candidates {
            self.peer_states
                .insert(*addr, PeerState::Pending { since: now });
        }

        candidates
    }

    /// Check if an address is safe to dial (SSRF/bogon filtering)
    pub fn is_safe_to_dial(&self, addr: &SocketAddr) -> bool {
        match addr.ip() {
            IpAddr::V4(ipv4) => {
                // Check loopback (127.x.x.x)
                if ipv4.is_loopback() && !self.config.allow_loopback_discovery {
                    return false;
                }

                // Check link-local (169.254.x.x)
                if ipv4.is_link_local() && !self.config.allow_link_local_discovery {
                    return false;
                }

                // Check private (10.x, 172.16-31.x, 192.168.x)
                if ipv4.is_private() && !self.config.allow_private_discovery {
                    return false;
                }

                // Check unspecified (0.0.0.0)
                if ipv4.is_unspecified() {
                    return false;
                }

                // Check broadcast (255.255.255.255)
                if ipv4.is_broadcast() {
                    return false;
                }

                // Check documentation ranges (192.0.2.x, 198.51.100.x, 203.0.113.x)
                if ipv4.is_documentation() {
                    return false;
                }

                true
            }
            IpAddr::V6(ipv6) => self.is_safe_ipv6(&ipv6),
        }
    }

    fn is_safe_ipv6(&self, ipv6: &Ipv6Addr) -> bool {
        if ipv6.is_loopback() && !self.config.allow_loopback_discovery {
            return false;
        }

        if ipv6.is_unspecified() {
            return false;
        }

        if ipv6.is_unicast_link_local() && !self.config.allow_link_local_discovery {
            return false;
        }

        if ipv6.is_unique_local() && !self.config.allow_private_discovery {
            return false;
        }

        true
    }

    /// Calculate if we should retry connecting to a peer based on backoff
    ///
    /// Uses unified peer_states for state lookup.
    pub fn should_retry(&self, addr: &SocketAddr) -> bool {
        match self.peer_states.get(addr) {
            Some(state) => state.should_retry(current_timestamp()),
            None => true, // No state record, can try
        }
    }

    /// Record a connection failure for a peer
    ///
    /// Atomically transitions peer to Failed state.
    /// Returns true if the peer should be removed (exceeded max failures).
    pub fn on_peer_failure(&mut self, addr: SocketAddr) -> bool {
        let now = current_timestamp();

        // Get current attempts from unified state
        let current_attempts = match self.peer_states.get(&addr) {
            Some(PeerState::Failed { attempts, .. }) => *attempts,
            _ => 0, // Any other state or no state means first failure
        };

        let new_attempts = current_attempts.saturating_add(1);
        let should_remove = new_attempts >= MAX_PEER_FAILURES;

        if should_remove {
            warn!(
                addr = %addr,
                failures = new_attempts,
                "peer exceeded max failures, removing from discovery"
            );
            // Remove from unified state
            self.peer_states.remove(&addr);
        } else {
            // Atomically transition to Failed state
            let new_state = PeerState::Failed {
                since: now,
                attempts: new_attempts,
            };
            self.peer_states.insert(addr, new_state);
        }

        should_remove
    }

    /// Record a successful connection to a peer
    ///
    /// Atomically transitions peer to Connected state.
    /// This is a single operation that replaces any previous state.
    pub fn on_peer_connected(&mut self, addr: SocketAddr) {
        // Atomically transition to Connected state (single operation)
        self.peer_states.insert(addr, PeerState::Connected);
    }

    /// Record a peer disconnection
    ///
    /// Atomically removes peer from tracking.
    pub fn on_peer_disconnected(&mut self, addr: SocketAddr) {
        // Atomically remove from unified state
        self.peer_states.remove(&addr);
    }

    /// Get the current number of connected peers
    pub fn peer_count(&self) -> usize {
        self.connected_count_unified()
    }

    /// Check if we're at the soft cap
    pub fn at_soft_cap(&self) -> bool {
        self.connected_count_unified() >= self.config.max_peers
    }

    /// Get remaining slots available for new connections
    pub fn remaining_slots(&self) -> usize {
        let connected = self.connected_count_unified();
        let pending = self.pending_count_unified();
        self.config.max_peers.saturating_sub(connected + pending)
    }

    /// Clear the pending state for an address (e.g., after timeout)
    ///
    /// Atomically removes peer if it's in Pending state.
    pub fn clear_pending(&mut self, addr: &SocketAddr) {
        // Only remove if currently in Pending state
        if matches!(self.peer_states.get(addr), Some(PeerState::Pending { .. })) {
            self.peer_states.remove(addr);
        }
    }

    /// Get configuration
    pub fn config(&self) -> &PeerDiscoveryConfig {
        &self.config
    }

    /// Get the count of failed peers (for monitoring metrics)
    pub fn failed_peer_count(&self) -> usize {
        self.failed_count_unified()
    }

    /// Get the count of connected peers
    pub fn connected_peer_count(&self) -> usize {
        self.connected_count_unified()
    }

    /// Get the count of pending peers
    pub fn pending_peer_count(&self) -> usize {
        self.pending_count_unified()
    }

    /// Count peers in Connected state (from unified peer_states)
    pub fn connected_count_unified(&self) -> usize {
        self.peer_states
            .values()
            .filter(|s| s.is_connected())
            .count()
    }

    /// Count peers in Pending state (from unified peer_states)
    pub fn pending_count_unified(&self) -> usize {
        self.peer_states.values().filter(|s| s.is_pending()).count()
    }

    /// Count peers in Failed state (from unified peer_states)
    pub fn failed_count_unified(&self) -> usize {
        self.peer_states.values().filter(|s| s.is_failed()).count()
    }

    /// Get the unified peer state for an address
    pub fn get_peer_state(&self, addr: &SocketAddr) -> Option<&PeerState> {
        self.peer_states.get(addr)
    }

    /// Remove expired pending/failed peers based on configured TTLs
    ///
    /// Also cleans up unified peer_states to stay in sync.
    pub fn cleanup_expired(&mut self, now: u64) -> PeerDiscoveryCleanupStats {
        let mut stats = PeerDiscoveryCleanupStats::default();
        let pending_ttl = self.config.pending_ttl.as_secs();
        let fail_ttl = self.config.fail_ttl.as_secs();

        // Collect addresses to remove from unified state
        let mut to_remove = Vec::new();

        // Check unified peer_states for expired entries
        for (addr, state) in self.peer_states.iter() {
            match state {
                PeerState::Pending { since } if pending_ttl > 0 => {
                    if now.saturating_sub(*since) > pending_ttl {
                        to_remove.push(*addr);
                        stats.pending_removed += 1;
                    }
                }
                PeerState::Failed { since, .. } if fail_ttl > 0 => {
                    if now.saturating_sub(*since) > fail_ttl {
                        to_remove.push(*addr);
                        stats.failed_removed += 1;
                    }
                }
                _ => {}
            }
        }

        // Remove expired entries from unified state
        for addr in &to_remove {
            self.peer_states.remove(addr);
        }

        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
    }

    fn loopback_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn link_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)), port)
    }

    fn private_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 100)), port)
    }

    fn public_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), port)
    }

    fn ipv6_loopback_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
    }

    fn ipv6_link_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)), port)
    }

    fn ipv6_unique_local_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)), port)
    }

    fn create_peer_gossip(addr: &str) -> PeerInfoGossip {
        PeerInfoGossip {
            address: addr.to_string(),
            peer_address: None,
            node_id: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
        }
    }

    fn state(discovery: &PeerDiscovery, addr: SocketAddr) -> Option<PeerState> {
        discovery.get_peer_state(&addr).copied()
    }

    fn assert_connected(discovery: &PeerDiscovery, addr: SocketAddr) {
        assert!(
            matches!(state(discovery, addr), Some(PeerState::Connected)),
            "expected {addr} to be connected"
        );
    }

    fn assert_not_connected(discovery: &PeerDiscovery, addr: SocketAddr) {
        assert!(
            !matches!(state(discovery, addr), Some(PeerState::Connected)),
            "expected {addr} to NOT be connected"
        );
    }

    fn assert_pending(discovery: &PeerDiscovery, addr: SocketAddr) {
        assert!(
            matches!(state(discovery, addr), Some(PeerState::Pending { .. })),
            "expected {addr} to be pending"
        );
    }

    fn assert_failed(discovery: &PeerDiscovery, addr: SocketAddr) {
        assert!(
            matches!(state(discovery, addr), Some(PeerState::Failed { .. })),
            "expected {addr} to be failed"
        );
    }

    #[test]
    fn test_filter_self() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peers = vec![
            create_peer_gossip("10.0.0.1:8080"), // self
            create_peer_gossip("10.0.0.2:8080"), // different peer
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return the different peer, not self
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0],
            test_addr(8080)
                .ip()
                .to_string()
                .replace("10.0.0.1", "10.0.0.2")
                .parse::<SocketAddr>()
                .unwrap_or(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                    8080
                ))
        );
    }

    #[test]
    fn test_filter_connected_peers() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        // Mark peer as connected
        let connected_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 8081);
        discovery.on_peer_connected(connected_peer);

        let peers = vec![
            create_peer_gossip("10.0.0.2:8081"), // already connected
            create_peer_gossip("10.0.0.3:8082"), // new peer
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return the new peer
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 8082)
        );
    }

    #[test]
    fn test_exponential_backoff() {
        let cases = [
            (1, 2),
            (2, 4),
            (3, 8),
            (5, 32),
            (10, 1024),
            (12, 3600),
            (20, 3600),
        ];

        for (attempts, expected) in cases {
            let state = PeerState::Failed { since: 0, attempts };
            assert_eq!(
                state.backoff_seconds(),
                expected,
                "attempts={attempts} should yield {expected}s backoff"
            );
        }
    }

    #[test]
    fn test_bogon_filtering_loopback() {
        let local = public_addr(8080);

        // With loopback disabled (default)
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&loopback_addr(8080)));

        // With loopback enabled
        let config = PeerDiscoveryConfig {
            allow_loopback_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&loopback_addr(8080)));
    }

    #[test]
    fn test_bogon_filtering_link_local() {
        let local = public_addr(8080);

        // With link-local disabled (default)
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&link_local_addr(8080)));

        // With link-local enabled
        let config = PeerDiscoveryConfig {
            allow_link_local_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&link_local_addr(8080)));
    }

    #[test]
    fn test_private_allowed_default() {
        let local = public_addr(8080);

        // Private IPs should be allowed by default
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(discovery.is_safe_to_dial(&private_addr(8080)));

        // With private disabled
        let config = PeerDiscoveryConfig {
            allow_private_discovery: false,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(!discovery.is_safe_to_dial(&private_addr(8080)));
    }

    #[test]
    fn test_ipv6_loopback_blocked_by_default() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&ipv6_loopback_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_loopback_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&ipv6_loopback_addr(8080)));
    }

    #[test]
    fn test_ipv6_link_local_blocked_by_default() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(!discovery.is_safe_to_dial(&ipv6_link_local_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_link_local_discovery: true,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(discovery.is_safe_to_dial(&ipv6_link_local_addr(8080)));
    }

    #[test]
    fn test_ipv6_unique_local_respects_private_flag() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);
        assert!(discovery.is_safe_to_dial(&ipv6_unique_local_addr(8080)));

        let config = PeerDiscoveryConfig {
            allow_private_discovery: false,
            ..Default::default()
        };
        let discovery = PeerDiscovery::new(local, config);
        assert!(!discovery.is_safe_to_dial(&ipv6_unique_local_addr(8080)));
    }

    #[test]
    fn test_max_failures_removal() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Apply failures up to MAX_PEER_FAILURES - 1
        for i in 1..MAX_PEER_FAILURES {
            let removed = discovery.on_peer_failure(peer);
            assert!(!removed, "peer should not be removed after {} failures", i);
            assert_failed(&discovery, peer);
        }

        // The MAX_PEER_FAILURES-th failure should remove the peer
        let removed = discovery.on_peer_failure(peer);
        assert!(
            removed,
            "peer should be removed after {} failures",
            MAX_PEER_FAILURES
        );
        assert!(
            state(&discovery, peer).is_none(),
            "peer should be removed from state after {} failures",
            MAX_PEER_FAILURES
        );
    }

    #[test]
    fn test_soft_cap_limiting() {
        let local = test_addr(8080);
        let config = PeerDiscoveryConfig {
            max_peers: 3,
            ..Default::default()
        };
        let mut discovery = PeerDiscovery::new(local, config);

        // Connect 2 peers (leaving 1 slot)
        discovery.on_peer_connected(test_addr(9001));
        discovery.on_peer_connected(test_addr(9002));

        assert_eq!(discovery.peer_count(), 2);
        assert_eq!(discovery.remaining_slots(), 1);

        // Try to add 5 more peers via gossip
        let peers = vec![
            create_peer_gossip("10.0.0.10:8000"),
            create_peer_gossip("10.0.0.11:8001"),
            create_peer_gossip("10.0.0.12:8002"),
            create_peer_gossip("10.0.0.13:8003"),
            create_peer_gossip("10.0.0.14:8004"),
        ];

        let candidates = discovery.on_peer_list_gossip(&peers);

        // Should only return 1 candidate (remaining slot based on connected + pending count)
        assert_eq!(candidates.len(), 1);
        assert_eq!(discovery.pending_peer_count(), 1);
        assert_pending(&discovery, candidates[0]);
        // remaining_slots now includes pending slots, so it should drop to zero
        assert_eq!(discovery.remaining_slots(), 0);
    }

    #[test]
    fn test_peer_connection_lifecycle() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Initially not connected
        assert_eq!(discovery.peer_count(), 0);
        assert!(!discovery.at_soft_cap());

        // Connect peer
        discovery.on_peer_connected(peer);
        assert_eq!(discovery.peer_count(), 1);
        assert_connected(&discovery, peer);

        // Disconnect peer
        discovery.on_peer_disconnected(peer);
        assert_eq!(discovery.peer_count(), 0);
        assert_not_connected(&discovery, peer);
    }

    #[test]
    fn test_should_retry_backoff() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // No failure record - should retry
        assert!(discovery.should_retry(&peer));

        // Add failure (backoff = 2 seconds)
        discovery.on_peer_failure(peer);

        // Immediately after failure - should NOT retry
        // (depends on timing, but backoff is at least 2 seconds)
        // We check the state directly
        let state = state(&discovery, peer).expect("peer state should exist");
        assert!(state.is_failed());
        assert_eq!(state.failure_info().unwrap().1, 1);
        assert_eq!(state.backoff_seconds(), 2);
    }

    #[test]
    fn test_unspecified_and_broadcast_blocked() {
        let local = public_addr(8080);
        let discovery = PeerDiscovery::with_defaults(local);

        // Unspecified (0.0.0.0) should be blocked
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);
        assert!(!discovery.is_safe_to_dial(&unspecified));

        // Broadcast (255.255.255.255) should be blocked
        let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)), 8080);
        assert!(!discovery.is_safe_to_dial(&broadcast));
    }

    #[test]
    fn test_clear_pending() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peers = vec![create_peer_gossip("10.0.0.10:8000")];
        let candidates = discovery.on_peer_list_gossip(&peers);

        assert_eq!(candidates.len(), 1);
        let peer = candidates[0];

        // Peer should be in pending
        assert_pending(&discovery, peer);

        // Clear pending
        discovery.clear_pending(&peer);
        assert!(
            state(&discovery, peer).is_none(),
            "peer should be cleared from pending"
        );
    }

    #[test]
    fn test_connection_clears_failure_state() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        let peer = test_addr(9000);

        // Add some failures
        discovery.on_peer_failure(peer);
        discovery.on_peer_failure(peer);
        assert_failed(&discovery, peer);

        // Connect the peer
        discovery.on_peer_connected(peer);

        // Failure state should be cleared
        assert_connected(&discovery, peer);
    }

    #[test]
    fn test_cleanup_expired_entries() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::new(
            local,
            PeerDiscoveryConfig {
                pending_ttl: Duration::from_secs(1),
                fail_ttl: Duration::from_secs(2),
                ..Default::default()
            },
        );

        let pending_peer = test_addr(9000);
        let failed_peer = test_addr(9001);

        // Insert into unified peer_states (primary source of truth)
        discovery
            .peer_states
            .insert(pending_peer, PeerState::Pending { since: 0 });
        discovery.peer_states.insert(
            failed_peer,
            PeerState::Failed {
                since: 1,
                attempts: 1,
            },
        );

        // Advance time beyond pending TTL but within failed TTL
        let now = 3;

        let stats = discovery.cleanup_expired(now);
        assert_eq!(stats.pending_removed, 1);
        assert_eq!(stats.failed_removed, 0);
        assert!(discovery.get_peer_state(&pending_peer).is_none());
        assert!(discovery.get_peer_state(&failed_peer).is_some());

        // Advance past fail_ttl as well
        let stats = discovery.cleanup_expired(now + 2);
        assert_eq!(stats.failed_removed, 1);
        assert!(discovery.get_peer_state(&failed_peer).is_none());
    }

    /// Test that slot calculation respects pending peers to avoid overcommitment
    #[test]
    fn test_on_peer_list_gossip_respects_pending_peers() {
        let local = test_addr(8080);
        let config = PeerDiscoveryConfig {
            max_peers: 5,
            ..Default::default()
        };
        let mut discovery = PeerDiscovery::new(local, config);

        // Add 2 peers to connected_peers
        discovery.on_peer_connected(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 20)),
            8001,
        ));
        discovery.on_peer_connected(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 21)),
            8002,
        ));

        assert_eq!(discovery.connected_peer_count(), 2);

        // First gossip call adds 2 peers to pending_peers (slots: 5-2=3, takes 2)
        let peers_first = vec![
            create_peer_gossip("10.0.0.30:8010"),
            create_peer_gossip("10.0.0.31:8011"),
        ];
        let candidates1 = discovery.on_peer_list_gossip(&peers_first);
        assert_eq!(candidates1.len(), 2);
        assert_eq!(discovery.pending_peer_count(), 2);

        // Now: 2 connected + 2 pending = 4, so only 1 slot should remain
        // Second gossip call with 3 new peers should only return 1 candidate
        let peers_second = vec![
            create_peer_gossip("10.0.0.40:8020"),
            create_peer_gossip("10.0.0.41:8021"),
            create_peer_gossip("10.0.0.42:8022"),
        ];
        let candidates2 = discovery.on_peer_list_gossip(&peers_second);

        // EXPECTED: 1 candidate (5 - 2 connected - 2 pending = 1 slot)
        // BUG: Current code returns 3 candidates (5 - 2 connected = 3 slots)
        assert_eq!(
            candidates2.len(),
            1,
            "should only return 1 candidate (5 max - 2 connected - 2 pending = 1 slot)"
        );
        assert_eq!(
            discovery.pending_peer_count(),
            3,
            "should have 3 pending peers total (2 from first + 1 from second)"
        );
    }

    /// Test that second gossip call with same peers skips already-pending ones
    /// Bug: After insert at line 218, if gossip runs again before connection
    /// completes, the same peer could be added as candidate again
    #[test]
    fn test_on_peer_list_gossip_skips_already_pending() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        // First gossip call with peers A and B
        let peers_first = vec![
            create_peer_gossip("10.0.0.50:8050"), // Peer A
            create_peer_gossip("10.0.0.51:8051"), // Peer B
        ];
        let candidates1 = discovery.on_peer_list_gossip(&peers_first);
        assert_eq!(candidates1.len(), 2);
        assert_eq!(discovery.pending_peer_count(), 2);

        // Second gossip call with A, B, and new peer C
        // A and B are already pending, so should be skipped
        let peers_second = vec![
            create_peer_gossip("10.0.0.50:8050"), // Peer A (already pending)
            create_peer_gossip("10.0.0.51:8051"), // Peer B (already pending)
            create_peer_gossip("10.0.0.52:8052"), // Peer C (new)
        ];
        let candidates2 = discovery.on_peer_list_gossip(&peers_second);

        // EXPECTED: Only peer C returned (A and B already pending)
        // Note: The current code DOES filter pending peers at line 183-186,
        // so this test should pass. But we add it to ensure the fix doesn't break this.
        assert_eq!(
            candidates2.len(),
            1,
            "should only return peer C (A and B already pending)"
        );

        let peer_c = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 52)), 8052);
        assert_eq!(
            candidates2[0], peer_c,
            "returned candidate should be peer C"
        );

        assert_eq!(
            discovery.pending_peer_count(),
            3,
            "should have 3 pending peers total"
        );
    }

    /// Test that peer state transitions are atomic using unified peer_states
    /// Issue #3: Non-atomic state transitions can leave peers in inconsistent states
    #[test]
    fn test_peer_state_transitions_atomic() {
        let local = test_addr(8080);
        let config = PeerDiscoveryConfig::default();
        let mut discovery = PeerDiscovery::new(local, config);
        let addr: SocketAddr = "10.0.0.100:8080".parse().unwrap();

        // Initially no state
        assert!(discovery.get_peer_state(&addr).is_none());

        // Add via gossip -> transitions to Pending
        let peers = vec![create_peer_gossip("10.0.0.100:8080")];
        let candidates = discovery.on_peer_list_gossip(&peers);
        assert_eq!(candidates.len(), 1);

        // Verify in Pending state atomically
        let state = discovery.get_peer_state(&addr).expect("should have state");
        assert!(state.is_pending(), "should be in Pending state");
        assert!(!state.is_connected(), "should not be Connected");
        assert!(!state.is_failed(), "should not be Failed");

        // Transition: Pending -> Connected (single atomic operation)
        discovery.on_peer_connected(addr);

        // Verify atomically in Connected state only
        let state = discovery.get_peer_state(&addr).expect("should have state");
        assert!(state.is_connected(), "should be in Connected state");
        assert!(!state.is_pending(), "should not be Pending");
        assert!(!state.is_failed(), "should not be Failed");

        // Verify counts are consistent
        assert_eq!(discovery.connected_count_unified(), 1);
        assert_eq!(discovery.pending_count_unified(), 0);
        assert_eq!(discovery.failed_count_unified(), 0);

        // Transition: Connected -> Removed (disconnect)
        discovery.on_peer_disconnected(addr);

        // Should be completely removed
        assert!(discovery.get_peer_state(&addr).is_none());
        assert_eq!(discovery.connected_count_unified(), 0);
    }

    /// Test that failure transitions are atomic and track attempts correctly
    #[test]
    fn test_peer_failure_transitions_atomic() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);
        let addr: SocketAddr = "10.0.0.101:8080".parse().unwrap();

        // Add via gossip to get into Pending state
        let peers = vec![create_peer_gossip("10.0.0.101:8080")];
        discovery.on_peer_list_gossip(&peers);
        assert!(discovery.get_peer_state(&addr).unwrap().is_pending());

        // First failure: Pending -> Failed{attempts=1}
        let removed = discovery.on_peer_failure(addr);
        assert!(!removed);

        let state = discovery.get_peer_state(&addr).expect("should have state");
        assert!(state.is_failed(), "should be in Failed state");
        assert_eq!(state.failure_info().unwrap().1, 1, "should have 1 attempt");

        // Second failure: Failed{1} -> Failed{2}
        discovery.on_peer_failure(addr);
        let state = discovery.get_peer_state(&addr).unwrap();
        assert_eq!(state.failure_info().unwrap().1, 2, "should have 2 attempts");

        // After successful connection: Failed -> Connected
        discovery.on_peer_connected(addr);
        let state = discovery.get_peer_state(&addr).unwrap();
        assert!(state.is_connected(), "should be Connected after success");
        assert!(!state.is_failed(), "failure state should be cleared");
    }

    /// Test that unified state counts match legacy counts during migration
    #[test]
    fn test_unified_and_legacy_counts_match() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::with_defaults(local);

        // Add some connected peers
        let conn1: SocketAddr = "10.0.0.200:8000".parse().unwrap();
        let conn2: SocketAddr = "10.0.0.201:8001".parse().unwrap();
        discovery.on_peer_connected(conn1);
        discovery.on_peer_connected(conn2);

        // Verify counts match
        assert_eq!(
            discovery.connected_count_unified(),
            discovery.connected_peer_count()
        );
        assert_eq!(discovery.connected_count_unified(), 2);

        // Add pending peers via gossip
        let peers = vec![
            create_peer_gossip("10.0.0.210:9000"),
            create_peer_gossip("10.0.0.211:9001"),
        ];
        discovery.on_peer_list_gossip(&peers);

        // Verify counts match
        assert_eq!(
            discovery.pending_count_unified(),
            discovery.pending_peer_count()
        );
        assert_eq!(discovery.pending_count_unified(), 2);

        // Add a failed peer
        let fail_peer: SocketAddr = "10.0.0.220:7000".parse().unwrap();
        let peers = vec![create_peer_gossip("10.0.0.220:7000")];
        discovery.on_peer_list_gossip(&peers);
        discovery.on_peer_failure(fail_peer);

        // Verify counts match
        assert_eq!(
            discovery.failed_count_unified(),
            discovery.failed_peer_count()
        );
        assert_eq!(discovery.failed_count_unified(), 1);
    }

    /// Test cleanup_expired works with unified peer_states
    #[test]
    fn test_cleanup_expired_uses_unified_state() {
        let local = test_addr(8080);
        let mut discovery = PeerDiscovery::new(
            local,
            PeerDiscoveryConfig {
                pending_ttl: Duration::from_secs(1),
                fail_ttl: Duration::from_secs(2),
                ..Default::default()
            },
        );

        // Manually insert into unified state for testing
        let pending_peer = test_addr(9100);
        let failed_peer = test_addr(9101);

        // Insert with old timestamps (will be expired)
        discovery
            .peer_states
            .insert(pending_peer, PeerState::Pending { since: 0 });
        discovery.peer_states.insert(
            failed_peer,
            PeerState::Failed {
                since: 1,
                attempts: 1,
            },
        );
        // Advance time beyond pending TTL but within failed TTL
        let now = 3;
        let stats = discovery.cleanup_expired(now);
        assert_eq!(stats.pending_removed, 1);
        assert_eq!(stats.failed_removed, 0);

        // Unified state should be cleaned
        assert!(discovery.get_peer_state(&pending_peer).is_none());
        assert!(discovery.get_peer_state(&failed_peer).is_some());

        // Advance past fail_ttl
        let stats = discovery.cleanup_expired(now + 2);
        assert_eq!(stats.failed_removed, 1);
        assert!(discovery.get_peer_state(&failed_peer).is_none());
    }
}
