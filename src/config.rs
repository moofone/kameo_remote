use std::time::Duration;

/// Default retry interval for failed peer connections
pub const DEFAULT_PEER_RETRY_SECONDS: u64 = 5;

/// Default maximum failed connection attempts before marking peer as failed
pub const DEFAULT_MAX_PEER_FAILURES: usize = 3;

/// Default gossip interval in seconds
pub const DEFAULT_GOSSIP_INTERVAL_SECS: u64 = 5;

/// Default cleanup interval in seconds  
pub const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 60;

/// Default dead peer timeout in seconds (15 minutes)
pub const DEFAULT_DEAD_PEER_TIMEOUT_SECS: u64 = 900;

/// Default small cluster threshold - clusters with this many nodes or fewer use full sync
/// Set to 0 to always use delta sync when possible
pub const DEFAULT_SMALL_CLUSTER_THRESHOLD: usize = 5;

/// Configuration for the gossip registry
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Node's key pair for identification (must be unique in the cluster)
    pub key_pair: Option<crate::KeyPair>,
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Maximum number of peers to gossip to in each round
    pub max_gossip_peers: usize,
    /// Time-to-live for actor entries
    pub actor_ttl: Duration,
    /// Cleanup interval for stale entries
    pub cleanup_interval: Duration,
    /// Connection timeout for outbound connections
    pub connection_timeout: Duration,
    /// Response timeout for gossip exchanges
    pub response_timeout: Duration,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Maximum number of failed connection attempts before marking peer as failed
    pub max_peer_failures: usize,
    /// Time to wait before retrying failed peers
    pub peer_retry_interval: Duration,
    /// Maximum number of deltas to keep in history
    pub max_delta_history: usize,
    /// Force full sync after this many delta exchanges
    pub full_sync_interval: u64,
    /// Maximum number of pooled connections
    pub max_pooled_connections: usize,
    /// Idle connection timeout for pool
    pub idle_connection_timeout: Duration,
    /// Timeout for connections checked out too long
    pub checkout_timeout: Duration,
    /// How often to run vector clock garbage collection  
    pub vector_clock_gc_frequency: Duration,
    /// How long to retain node entries in vector clocks after last seen
    pub vector_clock_retention_period: Duration,
    /// Maximum number of entries in a vector clock before compaction
    pub max_vector_clock_size: usize,
    /// Threshold for small clusters
    pub small_cluster_threshold: usize,
    /// Maximum time to wait for server to become ready before bootstrap
    pub bootstrap_readiness_timeout: Duration,
    /// Interval between readiness checks
    pub bootstrap_readiness_check_interval: Duration,
    /// Maximum bootstrap retry attempts
    pub bootstrap_max_retries: usize,
    /// Delay between bootstrap retry attempts
    pub bootstrap_retry_delay: Duration,
    /// Enable immediate propagation for urgent changes
    pub immediate_propagation_enabled: bool,
    /// Gossip fanout multiplier for urgent changes
    pub urgent_gossip_fanout: usize,
    /// Maximum retries for immediate propagation
    pub max_immediate_retries: usize,
    /// Timeout for causal consistency operations
    pub causal_consistency_timeout: Duration,
    /// How long to keep disconnected peers before removing them (default: 15 minutes)
    pub dead_peer_timeout: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            key_pair: None,
            gossip_interval: Duration::from_secs(DEFAULT_GOSSIP_INTERVAL_SECS),
            max_gossip_peers: 3,
            // Increase default actor TTL to avoid premature expiry of distributed actor discovery
            // This prevents cases where peers stop discovering actors after a few minutes of idle time.
            // If needed, this can still be overridden by callers providing a custom GossipConfig.
            actor_ttl: Duration::from_secs(86_400),
            cleanup_interval: Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS),
            connection_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(5),
            max_message_size: 10 * 1024 * 1024, // 10MB
            max_peer_failures: DEFAULT_MAX_PEER_FAILURES,
            peer_retry_interval: Duration::from_secs(DEFAULT_PEER_RETRY_SECONDS),
            max_delta_history: 100,
            full_sync_interval: 50,     // Force full sync every 50 deltas
            max_pooled_connections: 20, // Allow up to 20 pooled connections
            idle_connection_timeout: Duration::from_secs(300),
            checkout_timeout: Duration::from_secs(60),
            vector_clock_gc_frequency: Duration::from_secs(300), // 5 minutes
            vector_clock_retention_period: Duration::from_secs(7200), // 2 hours (was 1 hour)
            max_vector_clock_size: 1000,                         // Compact after 1000 entries
            small_cluster_threshold: DEFAULT_SMALL_CLUSTER_THRESHOLD,
            bootstrap_readiness_timeout: Duration::from_secs(30),
            bootstrap_readiness_check_interval: Duration::from_millis(100),
            bootstrap_max_retries: 5, // Increased from 3 to handle startup race conditions
            bootstrap_retry_delay: Duration::from_secs(5),
            immediate_propagation_enabled: true,
            urgent_gossip_fanout: 5,
            max_immediate_retries: 3,
            causal_consistency_timeout: Duration::from_millis(500),
            dead_peer_timeout: Duration::from_secs(DEFAULT_DEAD_PEER_TIMEOUT_SECS),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GossipConfig::default();

        assert_eq!(config.gossip_interval, Duration::from_secs(5));
        assert_eq!(config.max_gossip_peers, 3);
        assert_eq!(config.actor_ttl, Duration::from_secs(300));
        assert_eq!(config.cleanup_interval, Duration::from_secs(60));
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.response_timeout, Duration::from_secs(5));
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
        assert_eq!(config.max_peer_failures, 3);
        assert_eq!(
            config.peer_retry_interval,
            Duration::from_secs(DEFAULT_PEER_RETRY_SECONDS)
        );
        assert_eq!(config.max_delta_history, 100);
        assert_eq!(config.full_sync_interval, 50);
        assert_eq!(config.max_pooled_connections, 20);
        assert_eq!(config.idle_connection_timeout, Duration::from_secs(300));
        assert_eq!(config.checkout_timeout, Duration::from_secs(60));
        assert_eq!(config.vector_clock_gc_frequency, Duration::from_secs(300));
        assert_eq!(
            config.vector_clock_retention_period,
            Duration::from_secs(7200)
        );
        assert_eq!(config.small_cluster_threshold, 5);
        assert_eq!(config.bootstrap_readiness_timeout, Duration::from_secs(30));
        assert_eq!(
            config.bootstrap_readiness_check_interval,
            Duration::from_millis(100)
        );
        assert_eq!(config.bootstrap_max_retries, 5);
        assert_eq!(config.bootstrap_retry_delay, Duration::from_secs(5));
        assert!(config.immediate_propagation_enabled);
        assert_eq!(config.urgent_gossip_fanout, 5);
        assert_eq!(config.max_immediate_retries, 3);
        assert_eq!(
            config.causal_consistency_timeout,
            Duration::from_millis(500)
        );
        assert_eq!(config.dead_peer_timeout, Duration::from_secs(900));
    }

    #[test]
    fn test_config_clone() {
        let config = GossipConfig::default();
        let cloned = config.clone();

        assert_eq!(config.gossip_interval, cloned.gossip_interval);
        assert_eq!(config.max_gossip_peers, cloned.max_gossip_peers);
        assert_eq!(config.dead_peer_timeout, cloned.dead_peer_timeout);
    }

    #[test]
    fn test_config_debug() {
        let config = GossipConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("GossipConfig"));
        assert!(debug_str.contains("gossip_interval"));
        assert!(debug_str.contains("max_gossip_peers"));
    }

    #[test]
    fn test_custom_config() {
        let config = GossipConfig {
            gossip_interval: Duration::from_secs(10),
            max_gossip_peers: 5,
            peer_retry_interval: Duration::from_secs(2),
            ..Default::default()
        };

        assert_eq!(config.gossip_interval, Duration::from_secs(10));
        assert_eq!(config.max_gossip_peers, 5);
        assert_eq!(config.peer_retry_interval, Duration::from_secs(2));
        // Other fields should have default values
        assert_eq!(config.actor_ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_peer_retry_constant() {
        assert_eq!(DEFAULT_PEER_RETRY_SECONDS, 5);
    }
}
