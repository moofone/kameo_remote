use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::{
    connection_pool::ConnectionPool, current_timestamp, ActorLocation, GossipConfig, GossipError,
    RegistrationPriority, Result,
};

/// Registry change types for delta tracking
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryChange {
    /// Actor was added or updated
    ActorAdded {
        name: String,
        location: ActorLocation,
        priority: RegistrationPriority,
    },
    /// Actor was removed
    ActorRemoved {
        name: String,
        priority: RegistrationPriority,
    },
}

/// Delta representing changes since a specific sequence number
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryDelta {
    pub since_sequence: u64,
    pub current_sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub sender_addr: SocketAddr, // The bind address of the sending node
    pub wall_clock_time: u64,    // For debugging/monitoring only
}

/// Peer health status from a reporter's perspective
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerHealthStatus {
    /// Is the peer reachable from this reporter
    pub is_alive: bool,
    /// Last successful contact timestamp
    pub last_contact: u64,
    /// Number of failed connection attempts
    pub failure_count: u32,
}

/// Pending peer failure awaiting consensus
#[derive(Debug, Clone)]
pub struct PendingFailure {
    /// When we first detected the failure
    pub first_detected: u64,
    /// Timeout for collecting consensus
    pub consensus_deadline: u64,
    /// Have we queried other peers yet
    pub query_sent: bool,
}

/// Message types for the gossip protocol
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryMessage {
    /// Delta gossip message containing only changes
    DeltaGossip { delta: RegistryDelta },
    /// Response to delta gossip with our own delta
    DeltaGossipResponse { delta: RegistryDelta },
    /// Request for full sync (fallback when deltas are unavailable)
    FullSyncRequest {
        sender_addr: SocketAddr, // The bind address of the sending node
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Full synchronization message
    FullSync {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, ActorLocation>,
        sender_addr: SocketAddr, // The bind address of the sending node
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Response to full sync
    FullSyncResponse {
        local_actors: HashMap<String, ActorLocation>,
        known_actors: HashMap<String, ActorLocation>,
        sender_addr: SocketAddr, // The bind address of the sending node
        sequence: u64,
        wall_clock_time: u64,
    },
    /// Peer health status report
    PeerHealthReport {
        reporter: SocketAddr,
        peer_statuses: HashMap<SocketAddr, PeerHealthStatus>,
        timestamp: u64,
    },
    /// Query for peer health consensus
    PeerHealthQuery {
        sender: SocketAddr,
        target_peer: SocketAddr,
        timestamp: u64,
    },
}

/// Statistics about the gossip registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    pub local_actors: usize,
    pub known_actors: usize,
    pub active_peers: usize,
    pub failed_peers: usize,
    pub total_gossip_rounds: u64,
    pub current_sequence: u64,
    pub uptime_seconds: u64,
    pub last_gossip_timestamp: u64,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub delta_history_size: usize,
    pub avg_delta_size: f64,
}

/// Peer information with failure tracking and delta state
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub address: SocketAddr,              // Listening address
    pub peer_address: Option<SocketAddr>, // Actual connection address (may be NATed)
    pub failures: usize,
    pub last_attempt: u64,
    pub last_success: u64,
    pub last_sequence: u64,
    /// Last sequence we successfully sent to this peer
    pub last_sent_sequence: u64,
    /// Number of consecutive delta exchanges with this peer
    pub consecutive_deltas: u64,
    /// When this peer last failed (for tracking permanent failures)
    pub last_failure_time: Option<u64>,
}

/// Historical delta for efficient incremental updates
#[derive(Debug, Clone)]
pub struct HistoricalDelta {
    pub sequence: u64,
    pub changes: Vec<RegistryChange>,
    pub wall_clock_time: u64,
}

/// Data needed to perform gossip with a single peer
#[derive(Debug)]
pub struct GossipTask {
    pub peer_addr: SocketAddr,
    pub message: RegistryMessage,
    pub current_sequence: u64,
}

/// Result of a gossip operation
#[derive(Debug)]
pub struct GossipResult {
    pub peer_addr: SocketAddr,
    pub sent_sequence: u64,
    pub outcome: Result<Option<RegistryMessage>>,
}

/// Separated actor state for read-heavy operations (now with vector clocks)
#[derive(Debug)]
pub struct ActorState {
    pub local_actors: HashMap<String, ActorLocation>,
    pub known_actors: HashMap<String, ActorLocation>,
}

/// Gossip coordination state for write-heavy operations  
#[derive(Debug)]
pub struct GossipState {
    pub gossip_sequence: u64,
    pub pending_changes: Vec<RegistryChange>,
    pub urgent_changes: Vec<RegistryChange>, // High/Critical priority changes
    pub delta_history: Vec<HistoricalDelta>,
    pub peers: HashMap<SocketAddr, PeerInfo>,
    pub delta_exchanges: u64,
    pub full_sync_exchanges: u64,
    pub shutdown: bool,
    /// Track which actors are connected from which peer address
    pub peer_to_actors: HashMap<SocketAddr, HashSet<String>>,
    /// Track peer health reports from different observers
    pub peer_health_reports: HashMap<SocketAddr, HashMap<SocketAddr, PeerHealthStatus>>,
    /// Pending peer failures that need consensus
    pub pending_peer_failures: HashMap<SocketAddr, PendingFailure>,
}

/// Core gossip registry implementation with separated locks
#[derive(Clone)]
pub struct GossipRegistry {
    // Immutable config
    pub bind_addr: SocketAddr,
    pub config: GossipConfig,
    pub start_time: u64,

    // Separated lockable state
    pub actor_state: Arc<RwLock<ActorState>>,
    pub gossip_state: Arc<Mutex<GossipState>>,
    pub connection_pool: Arc<Mutex<ConnectionPool>>,
}

impl GossipRegistry {
    /// Create a new gossip registry
    pub fn new(bind_addr: SocketAddr, config: GossipConfig) -> Self {
        info!(
            bind_addr = %bind_addr,
            "creating new gossip registry"
        );

        let connection_pool =
            ConnectionPool::new(config.max_pooled_connections, config.connection_timeout);

        let registry = Self {
            bind_addr,
            config: config.clone(),
            start_time: current_timestamp(),
            actor_state: Arc::new(RwLock::new(ActorState {
                local_actors: HashMap::new(),
                known_actors: HashMap::new(),
            })),
            gossip_state: Arc::new(Mutex::new(GossipState {
                gossip_sequence: 0,
                pending_changes: Vec::new(),
                urgent_changes: Vec::new(),
                delta_history: Vec::new(),
                peers: HashMap::new(),
                delta_exchanges: 0,
                full_sync_exchanges: 0,
                shutdown: false,
                peer_to_actors: HashMap::new(),
                peer_health_reports: HashMap::new(),
                pending_peer_failures: HashMap::new(),
            })),
            connection_pool: Arc::new(Mutex::new(connection_pool)),
        };

        registry
    }

    /// Add bootstrap peers for initial connection
    pub async fn add_bootstrap_peers(&self, bootstrap_peers: Vec<SocketAddr>) {
        let mut gossip_state = self.gossip_state.lock().await;
        for peer in bootstrap_peers {
            if peer != self.bind_addr {
                gossip_state.peers.insert(
                    peer,
                    PeerInfo {
                        address: peer,
                        peer_address: None,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_failure_time: None,
                    },
                );
                debug!(peer = %peer, "added bootstrap peer");
            }
        }
        debug!(
            peer_count = gossip_state.peers.len(),
            "added bootstrap peers"
        );
    }

    /// Add a new peer (called when receiving connections)
    pub async fn add_peer(&self, peer_addr: SocketAddr) {
        debug!(peer = %peer_addr, self_addr = %self.bind_addr, "add_peer called");
        if peer_addr != self.bind_addr {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check if we already have this peer
            if gossip_state.peers.contains_key(&peer_addr) {
                debug!(peer = %peer_addr, "peer already tracked");
                return;
            }

            if let Entry::Vacant(e) = gossip_state.peers.entry(peer_addr) {
                let current_time = current_timestamp();
                e.insert(PeerInfo {
                    address: peer_addr,
                    peer_address: None,
                    failures: 0,
                    last_attempt: current_time,
                    last_success: current_time,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: None,
                });
                info!(peer = %peer_addr, peers_count = gossip_state.peers.len(), "📌 Added new peer (listening address)");

                // Pre-warm connection for immediate gossip (non-blocking)
                // if self.config.immediate_propagation_enabled {
                //     // Note: We'll create the connection lazily when first needed
                //     debug!(peer = %peer_addr, "will create persistent connection on first use");
                // }
            }
        } else {
            info!(peer = %peer_addr, "not adding peer - same as self");
        }
    }

    /// Register a local actor (fast path - minimal locking) with vector clock increment
    pub async fn register_actor(&self, name: String, location: ActorLocation) -> Result<()> {
        self.register_actor_with_priority(name, location, RegistrationPriority::Normal)
            .await
    }

    /// Register a local actor with specific priority
    pub async fn register_actor_with_priority(
        &self,
        name: String,
        mut location: ActorLocation,
        priority: RegistrationPriority,
    ) -> Result<()> {
        // Check if actor already exists
        {
            let actor_state = self.actor_state.read().await;
            if actor_state.local_actors.contains_key(&name)
                || actor_state.known_actors.contains_key(&name)
            {
                return Err(GossipError::ActorAlreadyExists(name));
            }
        }

        // Update the location with current wall time and priority
        location.wall_clock_time = current_timestamp();
        location.priority = priority;

        // Update actor state
        {
            let mut actor_state = self.actor_state.write().await;
            // Double-check after acquiring write lock
            if actor_state.local_actors.contains_key(&name)
                || actor_state.known_actors.contains_key(&name)
            {
                return Err(GossipError::ActorAlreadyExists(name));
            }
            actor_state
                .local_actors
                .insert(name.clone(), location.clone());
        }

        // Update gossip state with pending change - choose queue based on priority
        let should_trigger_immediate = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Check shutdown
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            let change = RegistryChange::ActorAdded {
                name: name.clone(),
                location,
                priority,
            };

            if priority.should_trigger_immediate_gossip() {
                gossip_state.urgent_changes.push(change);
                true
            } else {
                gossip_state.pending_changes.push(change);
                false
            }
        };

        if priority.should_trigger_immediate_gossip() {
            error!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                "🚀 REGISTERED_ACTOR_IMMEDIATE: Will trigger immediate propagation"
            );
        } else {
            info!(
                actor_name = %name,
                bind_addr = %self.bind_addr,
                priority = ?priority,
                "REGISTERED_ACTOR"
            );
        }

        // Trigger immediate gossip if this was an urgent change
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip");
            }
        }

        Ok(())
    }

    /// Unregister a local actor
    pub async fn unregister_actor(&self, name: &str) -> Result<Option<ActorLocation>> {
        // Remove from actor state
        let removed = {
            let mut actor_state = self.actor_state.write().await;
            actor_state.local_actors.remove(name)
        };

        if let Some(ref location) = removed {
            info!(actor_name = %name, "unregistered local actor");

            // Track this change for delta gossip - use the priority from the removed actor
            let should_trigger_immediate = {
                let mut gossip_state = self.gossip_state.lock().await;

                // Check shutdown
                if gossip_state.shutdown {
                    return Err(GossipError::Shutdown);
                }

                let change = RegistryChange::ActorRemoved {
                    name: name.to_string(),
                    priority: location.priority,
                };

                if location.priority.should_trigger_immediate_gossip() {
                    gossip_state.urgent_changes.push(change);
                    true
                } else {
                    gossip_state.pending_changes.push(change);
                    false
                }
            };

            // Trigger immediate gossip if this was an urgent change
            if should_trigger_immediate {
                if let Err(err) = self.trigger_immediate_gossip().await {
                    warn!(error = %err, "failed to trigger immediate gossip for actor removal");
                }
            }
        }
        Ok(removed)
    }

    /// Lookup an actor (read-only fast path)
    pub async fn lookup_actor(&self, name: &str) -> Option<ActorLocation> {
        let actor_state = self.actor_state.read().await;

        // Check local actors first
        if let Some(location) = actor_state.local_actors.get(name) {
            debug!(actor_name = %name, location = "local", "actor found");
            return Some(location.clone());
        }

        // Check known remote actors
        if let Some(location) = actor_state.known_actors.get(name) {
            let now = current_timestamp();
            let age_secs = now.saturating_sub(location.wall_clock_time);
            if age_secs < self.config.actor_ttl.as_secs() {
                debug!(
                    actor_name = %name,
                    location = "remote",
                    age_seconds = age_secs,
                    "actor found"
                );
                return Some(location.clone());
            }
        }

        debug!(actor_name = %name, "actor not found");
        None
    }

    /// Get registry statistics
    pub async fn get_stats(&self) -> RegistryStats {
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.len(),
                actor_state.known_actors.len(),
            )
        };

        let (
            gossip_sequence,
            active_peers,
            failed_peers,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
        ) = {
            let gossip_state = self.gossip_state.lock().await;
            let active_peers = gossip_state
                .peers
                .values()
                .filter(|p| p.failures < self.config.max_peer_failures)
                .count();
            let failed_peers = gossip_state.peers.len() - active_peers;

            (
                gossip_state.gossip_sequence,
                active_peers,
                failed_peers,
                gossip_state.delta_exchanges,
                gossip_state.full_sync_exchanges,
                gossip_state.delta_history.len(),
            )
        };

        let current_time = current_timestamp();
        let avg_delta_size = if delta_exchanges > 0 {
            // This is approximate since we don't hold the lock
            delta_history_size as f64
        } else {
            0.0
        };

        RegistryStats {
            local_actors,
            known_actors,
            active_peers,
            failed_peers,
            total_gossip_rounds: gossip_sequence,
            current_sequence: gossip_sequence,
            uptime_seconds: current_time.saturating_sub(self.start_time),
            last_gossip_timestamp: current_time,
            delta_exchanges,
            full_sync_exchanges,
            delta_history_size,
            avg_delta_size,
        }
    }

    /// Apply delta changes from a peer
    pub async fn apply_delta(&self, delta: RegistryDelta) -> Result<()> {
        let total_changes = delta.changes.len();
        let sender_addr = delta.sender_addr;

        // Check if any changes are immediate priority
        let has_immediate = delta.changes.iter().any(|change| match change {
            RegistryChange::ActorAdded { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
            RegistryChange::ActorRemoved { priority, .. } => {
                priority.should_trigger_immediate_gossip()
            }
        });

        if has_immediate {
            error!(
                "🎯 RECEIVING IMMEDIATE CHANGES: {} total changes from {}",
                total_changes, sender_addr
            );
        }

        let mut peer_actors = std::collections::HashSet::new();

        // Apply changes atomically under write lock
        let applied_count = {
            let mut actor_state = self.actor_state.write().await;
            let mut applied = 0;

            for change in delta.changes {
                match &change {
                    RegistryChange::ActorAdded {
                        name,
                        location,
                        priority: _,
                    } => {
                        // Don't override local actors
                        if actor_state.local_actors.contains_key(name) {
                            debug!(
                                actor_name = %name,
                                "skipping remote actor update - actor is local"
                            );
                            continue;
                        }

                        // Check if we already know about this actor
                        let should_apply = match actor_state.known_actors.get(name) {
                            Some(existing_location) => {
                                // Use wall clock time as the primary decision factor
                                // If times are equal, use address as tiebreaker for consistency
                                location.wall_clock_time > existing_location.wall_clock_time
                                    || (location.wall_clock_time
                                        == existing_location.wall_clock_time
                                        && location.address > existing_location.address)
                            }
                            None => {
                                debug!(
                                    actor_name = %name,
                                    "applying new actor"
                                );
                                true // New actor
                            }
                        };

                        if should_apply {
                            actor_state
                                .known_actors
                                .insert(name.clone(), location.clone());
                            applied += 1;

                            // Track this actor as belonging to the sender
                            peer_actors.insert(name.clone());

                            let received_timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_nanos();

                            let propagation_time_nanos =
                                received_timestamp - location.local_registration_time;
                            let propagation_time_ms = propagation_time_nanos as f64 / 1_000_000.0;

                            info!(
                                actor_name = %name,
                                priority = ?location.priority,
                                propagation_time_ms = propagation_time_ms,
                                "RECEIVED_ACTOR"
                            );
                        }
                    }
                    RegistryChange::ActorRemoved { name, priority: _ } => {
                        // Don't remove local actors
                        if actor_state.local_actors.contains_key(name) {
                            debug!(
                                actor_name = %name,
                                "skipping actor removal - actor is local"
                            );
                            continue;
                        }

                        // Simply remove the actor if it exists in known_actors
                        if actor_state.known_actors.remove(name).is_some() {
                            applied += 1;
                            debug!(actor_name = %name, "applied actor removal");
                        }
                    }
                }
            }

            applied
        };

        // Update peer-to-actors mapping for failure tracking
        if !peer_actors.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peer_to_actors
                .entry(sender_addr)
                .or_insert_with(std::collections::HashSet::new)
                .extend(peer_actors.clone());
        }

        debug!(
            sender = %sender_addr,
            total_changes,
            applied_changes = applied_count,
            peer_actor_count = peer_actors.len(),
            "completed delta application with vector clock conflict resolution"
        );

        Ok(())
    }

    /// Determine whether to use delta or full sync for a peer
    fn should_use_delta_state(&self, gossip_state: &GossipState, peer_info: &PeerInfo) -> bool {
        // For small clusters (≤ 5 total nodes), always use full sync to ensure
        // proper transitive propagation in star topologies
        let healthy_peers = gossip_state
            .peers
            .values()
            .filter(|p| p.failures < self.config.max_peer_failures)
            .count();
        let total_healthy_nodes = healthy_peers + 1;
        if total_healthy_nodes <= self.config.small_cluster_threshold {
            debug!(
                "using full sync for small cluster of {} healthy nodes",
                total_healthy_nodes
            );
            return false;
        }

        // Use full sync for new peers or if delta history is insufficient
        if peer_info.last_sequence == 0 {
            return false;
        }

        // Force full sync periodically
        if peer_info.consecutive_deltas >= self.config.full_sync_interval {
            return false;
        }

        // Check if we have the required delta history
        let oldest_available = gossip_state
            .delta_history
            .first()
            .map(|d| d.sequence)
            .unwrap_or(gossip_state.gossip_sequence);

        peer_info.last_sequence >= oldest_available
    }

    /// Create a delta containing changes since the specified sequence
    async fn create_delta_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryDelta> {
        let estimated_size =
            local_actors.len() + known_actors.len() + gossip_state.pending_changes.len();
        let mut changes = Vec::with_capacity(estimated_size);
        let current_time = current_timestamp();

        // If this is a brand new peer (since_sequence = 0), include all actors we know about
        if since_sequence == 0 {
            // Include all local actors as additions
            for (name, location) in local_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: location.priority,
                });
            }

            // Include all known remote actors as additions
            for (name, location) in known_actors {
                changes.push(RegistryChange::ActorAdded {
                    name: name.clone(),
                    location: location.clone(),
                    priority: location.priority,
                });
            }
        }

        // Include urgent changes first (they have higher priority)
        changes.extend(gossip_state.urgent_changes.clone());

        // Include pending changes from current round
        changes.extend(gossip_state.pending_changes.clone());

        // Include historical changes since the requested sequence
        for delta in &gossip_state.delta_history {
            if delta.sequence > since_sequence {
                changes.extend(delta.changes.clone());
            }
        }

        // Deduplicate changes to send only the most recent change for each actor
        let deduped_changes = Self::deduplicate_changes(changes);

        Ok(RegistryDelta {
            since_sequence,
            current_sequence: gossip_state.gossip_sequence,
            changes: deduped_changes,
            sender_addr: self.bind_addr,
            wall_clock_time: current_time,
        })
    }

    /// Create a full sync message from state
    async fn create_full_sync_message_from_state(
        &self,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        debug!(
            "Creating full sync message: {} local actors, {} known actors",
            local_actors.len(),
            known_actors.len()
        );
        RegistryMessage::FullSync {
            local_actors: local_actors.clone(),
            known_actors: known_actors.clone(),
            sender_addr: self.bind_addr,
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a full sync response message from state
    pub async fn create_full_sync_response_from_state(
        &self,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        sequence: u64,
    ) -> RegistryMessage {
        RegistryMessage::FullSyncResponse {
            local_actors: local_actors.clone(),
            known_actors: known_actors.clone(),
            sender_addr: self.bind_addr,
            sequence,
            wall_clock_time: current_timestamp(),
        }
    }

    /// Create a delta response for incoming gossip
    pub async fn create_delta_response_from_state(
        &self,
        gossip_state: &GossipState,
        local_actors: &HashMap<String, ActorLocation>,
        known_actors: &HashMap<String, ActorLocation>,
        since_sequence: u64,
    ) -> Result<RegistryMessage> {
        let delta = self
            .create_delta_from_state(gossip_state, local_actors, known_actors, since_sequence)
            .await?;
        Ok(RegistryMessage::DeltaGossipResponse { delta })
    }

    /// Prepare gossip round with consistent lock ordering to prevent deadlocks
    pub async fn prepare_gossip_round(&self) -> Result<Vec<GossipTask>> {
        // Step 1: Check shutdown status first
        {
            let gossip_state = self.gossip_state.lock().await;
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }
        }

        // Step 2: Atomically commit pending changes
        let (current_sequence, has_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Double-check shutdown after acquiring locks
            if gossip_state.shutdown {
                return Err(GossipError::Shutdown);
            }

            // Check if we have changes to commit
            let had_changes = !gossip_state.pending_changes.is_empty();

            // Increment sequence if we have changes
            if had_changes {
                // Increment sequence number
                gossip_state.gossip_sequence += 1;

                // Commit pending changes to history
                let delta = HistoricalDelta {
                    sequence: gossip_state.gossip_sequence,
                    changes: std::mem::take(&mut gossip_state.pending_changes),
                    wall_clock_time: current_timestamp(),
                };

                gossip_state.delta_history.push(delta);

                // Trim history if needed
                if gossip_state.delta_history.len() > self.config.max_delta_history {
                    gossip_state.delta_history.remove(0);
                }
            }

            (gossip_state.gossip_sequence, !gossip_state.peers.is_empty())
            // Lock is automatically released here
        };

        if !has_peers {
            return Ok(Vec::new());
        }

        // Step 2: Get actor state for message creation
        let (local_actors, known_actors) = {
            let actor_state = self.actor_state.read().await;
            (
                actor_state.local_actors.clone(),
                actor_state.known_actors.clone(),
            )
        };

        // Step 3: Select peers and create messages
        let tasks = {
            let gossip_state = self.gossip_state.lock().await;

            let current_time = current_timestamp();
            let available_peers: Vec<SocketAddr> = gossip_state
                .peers
                .values()
                .filter(|peer| {
                    peer.failures < self.config.max_peer_failures
                        || (current_time - peer.last_attempt)
                            > self.config.peer_retry_interval.as_secs()
                })
                .map(|peer| peer.address)
                .collect();

            if available_peers.is_empty() {
                debug!(
                    total_peers = gossip_state.peers.len(),
                    max_failures = self.config.max_peer_failures,
                    "no available peers for gossip round"
                );
                return Ok(Vec::new());
            }

            // Select peers using adaptive fanout
            let adaptive_fanout = std::cmp::min(
                std::cmp::max(3, (available_peers.len() as f64).log2().ceil() as usize),
                self.config.max_gossip_peers,
            );

            let selected_peers: Vec<SocketAddr> = {
                let mut rng = rand::rng();
                let mut peers = available_peers;
                peers.shuffle(&mut rng);
                peers.into_iter().take(adaptive_fanout).collect()
            };

            // Log if we're retrying any failed peers
            for peer in &selected_peers {
                if let Some(peer_info) = gossip_state.peers.get(peer) {
                    if peer_info.failures >= self.config.max_peer_failures {
                        let time_since_failure = peer_info.last_failure_time
                            .map(|t| current_time - t)
                            .unwrap_or(0);
                        info!(
                            peer = %peer,
                            failures = peer_info.failures,
                            time_since_failure = time_since_failure,
                            "🔄 Retrying connection to previously failed peer"
                        );
                    }
                }
            }

            let mut tasks = Vec::new();
            for peer_addr in selected_peers {
                let peer_info = gossip_state
                    .peers
                    .get(&peer_addr)
                    .cloned()
                    .unwrap_or(PeerInfo {
                        address: peer_addr,
                        peer_address: None,
                        failures: 0,
                        last_attempt: 0,
                        last_success: 0,
                        last_sequence: 0,
                        last_sent_sequence: 0,
                        consecutive_deltas: 0,
                        last_failure_time: None,
                    });

                let use_delta = self.should_use_delta_state(&gossip_state, &peer_info);

                let message = if use_delta {
                    match self
                        .create_delta_from_state(
                            &gossip_state,
                            &local_actors,
                            &known_actors,
                            peer_info.last_sequence,
                        )
                        .await
                    {
                        Ok(delta) => RegistryMessage::DeltaGossip { delta },
                        Err(err) => {
                            debug!(
                                peer = %peer_addr,
                                error = %err,
                                "failed to create delta, falling back to full sync"
                            );
                            self.create_full_sync_message_from_state(
                                &local_actors,
                                &known_actors,
                                current_sequence,
                            )
                            .await
                        }
                    }
                } else {
                    self.create_full_sync_message_from_state(
                        &local_actors,
                        &known_actors,
                        current_sequence,
                    )
                    .await
                };

                tasks.push(GossipTask {
                    peer_addr,
                    message,
                    current_sequence,
                });
            }

            tasks
        };

        debug!(
            task_count = tasks.len(),
            current_sequence = current_sequence,
            "prepared gossip round with atomic sequence/vector clock increment"
        );

        Ok(tasks)
    }

    /// Apply results from gossip tasks
    pub async fn apply_gossip_results(&self, results: Vec<GossipResult>) {
        let current_time = current_timestamp();

        for result in results {
            match result.outcome {
                Ok(response_opt) => {
                    // Success case - we successfully sent a message
                    // But with persistent connections, this doesn't mean the peer is alive
                    // We'll only reset failures when we receive messages from the peer
                    {
                        let mut gossip_state = self.gossip_state.lock().await;
                        if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                            // Don't reset failures here - only update attempt time
                            peer_info.last_attempt = current_time;
                            peer_info.last_sent_sequence = result.sent_sequence;

                            // Only update last_success if we're not in a failed state
                            if peer_info.failures < self.config.max_peer_failures {
                                peer_info.last_success = current_time;
                            }
                        }
                    }

                    // Record that this peer is active (we successfully communicated with it)
                    self.record_peer_activity(result.peer_addr).await;

                    // Process response if we got one
                    if let Some(response) = response_opt {
                        if let Err(err) = self
                            .handle_gossip_response(result.peer_addr, response)
                            .await
                        {
                            warn!(peer = %result.peer_addr, error = %err, "failed to handle gossip response");
                        }
                    }
                }
                Err(err) => {
                    // Failure case
                    warn!(peer = %result.peer_addr, error = %err, "failed to gossip to peer");
                    let mut gossip_state = self.gossip_state.lock().await;
                    if let Some(peer_info) = gossip_state.peers.get_mut(&result.peer_addr) {
                        peer_info.failures += 1;
                        peer_info.last_attempt = current_time;
                    }
                }
            }
        }
    }

    /// Handle gossip response with vector clock updates
    pub async fn handle_gossip_response(
        &self,
        addr: SocketAddr,
        response: RegistryMessage,
    ) -> Result<()> {
        match response {
            RegistryMessage::DeltaGossipResponse { delta } => {
                info!(
                    peer = %addr,
                    sender = %delta.sender_addr,
                    changes = delta.changes.len(),
                    "📥 GOSSIP: Received delta gossip response"
                );

                let delta_sequence = delta.current_sequence;

                self.apply_delta(delta).await?;
                // Don't add peer here - peers are managed through handle_connection

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.last_sequence = delta_sequence;
                    peer_info.consecutive_deltas += 1;
                }
                gossip_state.delta_exchanges += 1;
            }
            RegistryMessage::FullSyncResponse {
                local_actors,
                known_actors,
                sender_addr,
                sequence,
                wall_clock_time,
            } => {
                info!(
                    peer = %addr,
                    sender = %sender_addr,
                    sequence = sequence,
                    local_actors = local_actors.len(),
                    known_actors = known_actors.len(),
                    "📥 GOSSIP: Received full sync response"
                );

                // Add the sender as a peer if not already tracked
                info!(sender = %sender_addr, "Adding sender from full sync response as peer");
                self.add_peer(sender_addr).await;

                self.merge_full_sync(
                    local_actors,
                    known_actors,
                    sender_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

                let mut gossip_state = self.gossip_state.lock().await;
                if let Some(peer_info) = gossip_state.peers.get_mut(&addr) {
                    peer_info.consecutive_deltas = 0;
                    peer_info.last_sequence = sequence;
                }
                gossip_state.full_sync_exchanges += 1;
            }
            _ => {
                warn!(peer = %addr, "received unexpected message type in response");
            }
        }

        Ok(())
    }

    /// Merge incoming full sync data with vector clock-based conflict resolution
    pub async fn merge_full_sync(
        &self,
        remote_local: HashMap<String, ActorLocation>,
        remote_known: HashMap<String, ActorLocation>,
        sender_addr: SocketAddr,
        sequence: u64,
        _wall_clock_time: u64,
    ) {
        // Don't add peer here - peers are managed through handle_connection

        // Record comprehensive node activity

        // Check if we've already processed this or a newer sequence from this peer
        {
            let gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get(&sender_addr) {
                if sequence < peer_info.last_sequence {
                    debug!(
                        last_sequence = peer_info.last_sequence,
                        received_sequence = sequence,
                        "ignoring old gossip message"
                    );
                    return;
                }
            }
        }

        // Update peer sequence and vector clock
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                peer_info.last_sequence = std::cmp::max(peer_info.last_sequence, sequence);
            }
        }

        let mut new_actors = 0;
        let mut updated_actors = 0;
        let mut peer_actors = std::collections::HashSet::new();

        // Merge using vector clock-based conflict resolution
        {
            let mut actor_state = self.actor_state.write().await;

            // Merge remote local actors - these are owned by the sender
            for (name, location) in remote_local {
                peer_actors.insert(name.clone());
                if !actor_state.local_actors.contains_key(&name) {
                    match actor_state.known_actors.get(&name) {
                        Some(existing_location) => {
                            // Use wall clock time for conflict resolution
                            if location.wall_clock_time > existing_location.wall_clock_time {
                                actor_state.known_actors.insert(name.clone(), location);
                                updated_actors += 1;
                            } else if location.wall_clock_time == existing_location.wall_clock_time
                            {
                                // Use address as tiebreaker for concurrent updates
                                if location.address > existing_location.address {
                                    actor_state.known_actors.insert(name.clone(), location);
                                    updated_actors += 1;
                                }
                            }
                            // Otherwise keep existing
                        }
                        None => {
                            actor_state.known_actors.insert(name.clone(), location);
                            new_actors += 1;
                        }
                    }
                }
            }

            // Merge remote known actors
            for (name, location) in remote_known {
                if actor_state.local_actors.contains_key(&name) {
                    continue; // Don't override local actors
                }

                match actor_state.known_actors.get(&name) {
                    Some(existing_location) => {
                        // Use wall clock time for conflict resolution
                        if location.wall_clock_time > existing_location.wall_clock_time {
                            actor_state.known_actors.insert(name, location);
                            updated_actors += 1;
                        } else if location.wall_clock_time == existing_location.wall_clock_time {
                            // Use address as tiebreaker for concurrent updates
                            if location.address > existing_location.address {
                                actor_state.known_actors.insert(name, location);
                                updated_actors += 1;
                            }
                        }
                        // Otherwise keep existing
                    }
                    None => {
                        actor_state.known_actors.insert(name, location);
                        new_actors += 1;
                    }
                }
            }
        }

        // Update peer-to-actors mapping for failure tracking
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peer_to_actors
                .insert(sender_addr, peer_actors.clone());
        }

        debug!(
            new_actors = new_actors,
            updated_actors = updated_actors,
            peer = %sender_addr,
            peer_actor_count = peer_actors.len(),
            "merged gossip data using vector clock conflict resolution"
        );
    }

    /// Clean up stale actor entries (using wall clock for TTL)
    pub async fn cleanup_stale_actors(&self) {
        let now = current_timestamp();
        let ttl_secs = self.config.actor_ttl.as_secs();

        // Clean up stale known actors (using wall clock time for TTL)
        {
            let mut actor_state = self.actor_state.write().await;
            let before_count = actor_state.known_actors.len();
            actor_state
                .known_actors
                .retain(|_, location| now - location.wall_clock_time < ttl_secs);

            let removed = before_count - actor_state.known_actors.len();
            if removed > 0 {
                info!(removed_count = removed, "cleaned up stale actor entries");
            }
        }

        // Clean up old delta history (using wall clock)
        {
            let mut gossip_state = self.gossip_state.lock().await;
            let history_ttl = self.config.actor_ttl.as_secs() * 2;
            gossip_state
                .delta_history
                .retain(|delta| now - delta.wall_clock_time < history_ttl);
        }

        // Enforce bounds on data structures
        self.enforce_bounds().await;

        // Clean up connection pool
        {
            let mut connection_pool = self.connection_pool.lock().await;
            connection_pool.cleanup_stale_connections();
        }
    }

    /// Clean up actors from peers that have been disconnected for longer than dead_peer_timeout
    /// IMPORTANT: We keep the peer itself to allow reconnection, only clean up their actors
    pub async fn cleanup_dead_peers(&self) {
        let current_time = current_timestamp();
        let dead_peer_timeout_secs = self.config.dead_peer_timeout.as_secs();

        let peers_to_cleanup: Vec<SocketAddr> = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(_, info)| {
                    // Check if peer has been disconnected for too long
                    info.failures >= self.config.max_peer_failures
                        && info.last_failure_time.map_or(false, |failure_time| {
                            (current_time - failure_time) > dead_peer_timeout_secs
                        })
                })
                .map(|(addr, _)| *addr)
                .collect()
        };

        if !peers_to_cleanup.is_empty() {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut actor_state = self.actor_state.write().await;

            for peer_addr in &peers_to_cleanup {
                // IMPORTANT: We do NOT remove the peer itself - it stays in the peer list
                // This allows us to reconnect when the peer comes back online

                // Remove peer's actors from known_actors to free memory
                if let Some(actor_names) = gossip_state.peer_to_actors.get(peer_addr).cloned() {
                    for actor_name in &actor_names {
                        actor_state.known_actors.remove(actor_name);
                    }
                    gossip_state.peer_to_actors.remove(peer_addr);

                    info!(
                        peer = %peer_addr,
                        actors_removed = actor_names.len(),
                        timeout_minutes = dead_peer_timeout_secs / 60,
                        "cleaned up actors from long-disconnected peer (peer retained for reconnection)"
                    );
                }

                // Clean up health reports but keep the peer entry
                gossip_state.peer_health_reports.remove(peer_addr);
                gossip_state.pending_peer_failures.remove(peer_addr);
            }
        }
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        info!("shutting down gossip registry");

        // Set shutdown flag
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.shutdown = true;
        }

        // Close all connections in the pool
        {
            let mut connection_pool = self.connection_pool.lock().await;
            connection_pool.close_all_connections();
        }

        // Clear actor state
        {
            let mut actor_state = self.actor_state.write().await;
            actor_state.local_actors.clear();
            actor_state.known_actors.clear();
        }

        // Clear gossip state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            gossip_state.pending_changes.clear();
            gossip_state.urgent_changes.clear();
            gossip_state.delta_history.clear();
            gossip_state.peers.clear();
        }

        info!("gossip registry shutdown complete");
    }

    pub async fn is_shutdown(&self) -> bool {
        let gossip_state = self.gossip_state.lock().await;
        gossip_state.shutdown
    }

    /// Record that a peer (by address) is active
    pub async fn record_peer_activity(&self, peer_addr: SocketAddr) {
        let gossip_state = self.gossip_state.lock().await;

        // Peer activity is now tracked directly by address
        if let Some(_peer_info) = gossip_state.peers.get(&peer_addr) {
            // Activity is recorded through last_success/last_attempt timestamps
        }
    }

    /// Handle peer connection failure - start consensus process
    /// This is called for socket disconnections (not timeouts)
    pub async fn handle_peer_connection_failure(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(
            failed_peer = %failed_peer_addr,
            "socket disconnection detected, marking connection as failed (actors remain available)"
        );

        let current_time = current_timestamp();

        // IMMEDIATELY mark the connection as failed and remove from pool
        {
            let mut pool = self.connection_pool.lock().await;
            if pool.has_connection(&failed_peer_addr) {
                pool.remove_connection(failed_peer_addr);
                info!(addr = %failed_peer_addr, "removed disconnected connection from pool");
            }
        }

        // IMMEDIATELY mark peer as failed in our local state
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&failed_peer_addr) {
                peer_info.failures = self.config.max_peer_failures;
                peer_info.last_failure_time = Some(current_time);
                peer_info.last_attempt = current_time; // Update last_attempt so retry happens after interval
                info!(
                    peer = %failed_peer_addr,
                    retry_after_secs = self.config.peer_retry_interval.as_secs(),
                    "marked peer as disconnected in local state, will retry after interval"
                );
            }
        }

        // Now start consensus process for actor invalidation
        {
            let mut gossip_state = self.gossip_state.lock().await;

            // Record our own health report
            let our_report = PeerHealthStatus {
                is_alive: false,
                last_contact: current_time,
                failure_count: 1,
            };

            gossip_state
                .peer_health_reports
                .entry(failed_peer_addr)
                .or_insert_with(HashMap::new)
                .insert(self.bind_addr, our_report);

            // If we don't have a pending failure, create one
            if !gossip_state
                .pending_peer_failures
                .contains_key(&failed_peer_addr)
            {
                let pending = PendingFailure {
                    first_detected: current_time,
                    consensus_deadline: current_time + 5, // 5 second timeout
                    query_sent: false,
                };
                gossip_state
                    .pending_peer_failures
                    .insert(failed_peer_addr, pending);

                info!(
                    failed_peer = %failed_peer_addr,
                    deadline = current_time + 5,
                    "created pending failure record, waiting for consensus on actor invalidation"
                );
            }
        }

        // Don't query immediately - give other nodes time to detect their own disconnections
        // Schedule the query for 100ms later to avoid race conditions
        let registry = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            info!(
                failed_peer = %failed_peer_addr,
                "delayed query: now checking peer consensus after 100ms"
            );

            // Query other peers for their view of the failed peer
            // This is to determine if we should invalidate actors, not the connection status
            if let Err(e) = registry.query_peer_health_consensus(failed_peer_addr).await {
                warn!(error = %e, "failed to query peer health consensus");
            }
        });

        Ok(())
    }

    /// Query other peers for their view of a potentially failed peer
    async fn query_peer_health_consensus(&self, target_peer: SocketAddr) -> Result<()> {
        let query_msg = RegistryMessage::PeerHealthQuery {
            sender: self.bind_addr,
            target_peer,
            timestamp: current_timestamp(),
        };

        // Get list of healthy peers to query
        let peers_to_query = {
            let gossip_state = self.gossip_state.lock().await;
            gossip_state
                .peers
                .iter()
                .filter(|(addr, info)| {
                    **addr != target_peer && // Don't query the target
                    info.failures < self.config.max_peer_failures // Only query healthy peers
                })
                .map(|(addr, _)| *addr)
                .collect::<Vec<_>>()
        };

        info!(
            target_peer = %target_peer,
            querying_peers = peers_to_query.len(),
            "querying peers for health consensus"
        );

        // Send queries to all healthy peers
        for peer in peers_to_query {
            if let Ok(data) = bincode::serialize(&query_msg) {
                let len = data.len() as u32;
                let mut buffer = Vec::with_capacity(4 + data.len());
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(&data);

                // Try to send through existing connection
                let mut pool = self.connection_pool.lock().await;
                if let Ok(conn) = pool.get_connection(peer).await {
                    let _ = conn.send_data(buffer).await;
                }
            }
        }

        // Mark query as sent
        {
            let mut gossip_state = self.gossip_state.lock().await;
            if let Some(pending) = gossip_state.pending_peer_failures.get_mut(&target_peer) {
                pending.query_sent = true;
            }
        }

        Ok(())
    }

    /// Check if we have consensus about any pending peer failures
    pub async fn check_peer_consensus(&self) {
        let current_time = current_timestamp();

        {
            let mut gossip_state = self.gossip_state.lock().await;
            let mut completed_failures = Vec::new();

            for (peer_addr, pending) in &gossip_state.pending_peer_failures {
                // Check if we've reached the deadline or have enough reports
                let reports = gossip_state.peer_health_reports.get(peer_addr);
                let total_peers = gossip_state.peers.len();

                if let Some(reports) = reports {
                    let alive_count = reports.values().filter(|r| r.is_alive).count();
                    let dead_count = reports.values().filter(|r| !r.is_alive).count();
                    let total_reports = reports.len();

                    info!(
                        peer = %peer_addr,
                        alive_votes = alive_count,
                        dead_votes = dead_count,
                        total_reports = total_reports,
                        total_peers = total_peers,
                        "checking peer consensus"
                    );

                    // We NO LONGER remove actors even if consensus says the peer is dead
                    // The actors remain configured and available for when the node reconnects

                    if total_peers <= 1 {
                        // Only us and the failed peer
                        completed_failures.push(*peer_addr);
                        info!(
                            peer = %peer_addr,
                            "2-node cluster: peer is disconnected, keeping actors for potential reconnection"
                        );
                    } else {
                        // Multiple nodes - check consensus
                        let majority = (total_peers + 1) / 2;

                        if dead_count >= majority || current_time >= pending.consensus_deadline {
                            completed_failures.push(*peer_addr);

                            if dead_count > alive_count {
                                // Majority says dead
                                info!(
                                    peer = %peer_addr,
                                    dead_votes = dead_count,
                                    alive_votes = alive_count,
                                    "consensus: majority says peer is dead, but keeping actors for reconnection"
                                );
                            } else if alive_count > dead_count {
                                // Majority says alive
                                info!(
                                    peer = %peer_addr,
                                    alive_votes = alive_count,
                                    dead_votes = dead_count,
                                    "consensus: peer is alive elsewhere, keeping actors"
                                );
                            } else {
                                // Tie or timeout
                                info!(
                                    peer = %peer_addr,
                                    "consensus timeout or tie, keeping actors"
                                );
                            }
                        }
                    }
                }
            }

            // Remove completed failures
            for peer in completed_failures {
                gossip_state.pending_peer_failures.remove(&peer);
                gossip_state.peer_health_reports.remove(&peer);
            }
        }

        // We no longer invalidate actors based on consensus
        // The connection state is already updated, actors remain available
    }

    /// Deduplicate changes, keeping only the most recent change for each actor
    pub fn deduplicate_changes(changes: Vec<RegistryChange>) -> Vec<RegistryChange> {
        let mut actor_changes: HashMap<String, RegistryChange> = HashMap::new();

        for change in changes {
            let actor_name = Self::get_change_actor_name(&change);
            // Simply keep the last change for each actor
            actor_changes.insert(actor_name, change);
        }

        actor_changes.into_values().collect()
    }

    /// Extract the actor name from a registry change
    fn get_change_actor_name(change: &RegistryChange) -> String {
        match change {
            RegistryChange::ActorAdded { name, .. } | RegistryChange::ActorRemoved { name, .. } => {
                name.clone()
            }
        }
    }

    /// Enforce bounds on gossip state data structures to prevent unbounded growth
    async fn enforce_bounds(&self) {
        let mut gossip_state = self.gossip_state.lock().await;

        // Bound pending changes
        let max_pending = 1000;
        if gossip_state.pending_changes.len() > max_pending {
            debug!(
                "Trimming pending changes from {} to {}",
                gossip_state.pending_changes.len(),
                max_pending
            );
            gossip_state.pending_changes.truncate(max_pending);
        }

        // Bound urgent changes (smaller limit since these are high priority)
        let max_urgent = 100;
        if gossip_state.urgent_changes.len() > max_urgent {
            debug!(
                "Trimming urgent changes from {} to {}",
                gossip_state.urgent_changes.len(),
                max_urgent
            );
            gossip_state.urgent_changes.truncate(max_urgent);
        }

        // Bound delta history
        if gossip_state.delta_history.len() > self.config.max_delta_history {
            let excess = gossip_state.delta_history.len() - self.config.max_delta_history;
            debug!("Trimming delta history by {} entries", excess);
            gossip_state.delta_history.drain(0..excess);
        }

        // Bound peers list
        let max_peers = 1000;
        if gossip_state.peers.len() > max_peers {
            debug!(
                "Trimming peers from {} to {}",
                gossip_state.peers.len(),
                max_peers
            );
            let _current_time = current_timestamp();
            let mut peers_by_age: Vec<_> = gossip_state
                .peers
                .iter()
                .map(|(addr, peer)| (*addr, peer.last_success))
                .collect();
            peers_by_age.sort_by_key(|(_, last_success)| *last_success);

            let to_remove = gossip_state.peers.len() - max_peers;
            let addrs_to_remove: Vec<_> = peers_by_age
                .iter()
                .take(to_remove)
                .map(|(addr, _)| *addr)
                .collect();
            for addr in addrs_to_remove {
                gossip_state.peers.remove(&addr);
            }
        }
    }

    /// Trigger immediate gossip for urgent changes - optimized for speed
    pub async fn trigger_immediate_gossip(&self) -> Result<()> {
        if !self.config.immediate_propagation_enabled {
            return Ok(());
        }

        // Fast path: get urgent changes and peers in one go
        let (urgent_changes, critical_peers) = {
            let mut gossip_state = self.gossip_state.lock().await;

            if gossip_state.urgent_changes.is_empty() {
                return Ok(());
            }

            // Take all urgent changes for immediate propagation
            let changes = gossip_state.urgent_changes.clone();
            gossip_state.urgent_changes.clear();

            // Get healthy peers quickly - just take the first few healthy ones
            let peers: Vec<_> = gossip_state
                .peers
                .iter()
                .filter(|(_, peer)| peer.failures < self.config.max_peer_failures)
                .take(self.config.urgent_gossip_fanout)
                .map(|(addr, _)| *addr)
                .collect();

            (changes, peers)
        };

        if urgent_changes.is_empty() || critical_peers.is_empty() {
            return Ok(());
        }

        // Log immediate propagation
        error!(
            "🚀 IMMEDIATE GOSSIP: Broadcasting {} urgent changes to {} peers",
            urgent_changes.len(),
            critical_peers.len()
        );

        for change in &urgent_changes {
            match change {
                RegistryChange::ActorAdded {
                    name,
                    location,
                    priority,
                    ..
                } => {
                    error!(
                        "  ➕ IMMEDIATE: Adding actor {} at {} (priority: {:?})",
                        name, location.address, priority
                    );
                }
                RegistryChange::ActorRemoved { name, priority, .. } => {
                    error!(
                        "  ➖ IMMEDIATE: Removing actor {} (priority: {:?})",
                        name, priority
                    );
                }
            }
        }

        // Create delta once and reuse
        let delta = RegistryDelta {
            sender_addr: self.bind_addr,
            since_sequence: 0,   // Not used for immediate gossip
            current_sequence: 0, // Not used for immediate gossip
            changes: urgent_changes,
            wall_clock_time: current_timestamp(),
        };

        // Serialize message once
        let message = RegistryMessage::DeltaGossip { delta };
        let serialized_data = bincode::serialize(&message)?;

        if serialized_data.len() > self.config.max_message_size {
            return Err(GossipError::MessageTooLarge {
                size: serialized_data.len(),
                max: self.config.max_message_size,
            });
        }

        // Send to all peers concurrently with minimal overhead
        let mut join_handles = Vec::new();
        let connection_pool = self.connection_pool.clone();

        for peer_addr in critical_peers {
            let data = serialized_data.clone();
            let pool = connection_pool.clone();

            let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                // Get persistent connection
                let conn = {
                    let mut pool_guard = pool.lock().await;
                    pool_guard.get_connection(peer_addr).await?
                };

                // Send the pre-serialized data with length prefix
                let len = data.len() as u32;
                let mut buffer = Vec::with_capacity(4 + data.len());
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(&data);
                conn.send_data(buffer).await?;

                Ok(())
            });

            join_handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in join_handles {
            if let Err(e) = handle.await {
                warn!("immediate gossip task failed: {}", e);
            }
        }

        Ok(())
    }

    /// Immediately invalidate all actors from a failed peer
    pub async fn invalidate_peer_actors(&self, failed_peer_addr: SocketAddr) -> Result<()> {
        info!(failed_peer = %failed_peer_addr, "invalidating actors from failed peer");

        // Get the list of actors to invalidate from this peer
        let (actors_to_remove, should_trigger_immediate) = {
            let mut gossip_state = self.gossip_state.lock().await;

            // Get actors belonging to the failed peer
            let actors_to_remove = gossip_state
                .peer_to_actors
                .remove(&failed_peer_addr)
                .unwrap_or_default();

            if actors_to_remove.is_empty() {
                debug!(failed_peer = %failed_peer_addr, "no actors to invalidate");
                return Ok(());
            }

            // Create removal changes for each actor
            let mut removal_changes = Vec::new();
            for actor_name in &actors_to_remove {
                let change = RegistryChange::ActorRemoved {
                    name: actor_name.clone(),
                    priority: RegistrationPriority::Immediate, // Node failures are always immediate
                };
                removal_changes.push(change);
            }

            // Add all removal changes to urgent queue
            gossip_state.urgent_changes.extend(removal_changes);

            (actors_to_remove, !gossip_state.urgent_changes.is_empty())
        };

        // Remove actors from known_actors (they shouldn't be in local_actors if they're from a failed node)
        let removed_count = {
            let mut actor_state = self.actor_state.write().await;
            let mut removed = 0;

            for actor_name in &actors_to_remove {
                if actor_state.known_actors.remove(actor_name).is_some() {
                    removed += 1;
                }
                // Also remove from local_actors if somehow present (defensive)
                if actor_state.local_actors.remove(actor_name).is_some() {
                    removed += 1;
                }
            }
            removed
        };

        info!(
            failed_peer = %failed_peer_addr,
            actors_removed = removed_count,
            actors_invalidated = ?actors_to_remove,
            "PEER_FAILURE_INVALIDATION"
        );

        // Trigger immediate gossip to propagate the failures
        if should_trigger_immediate {
            if let Err(err) = self.trigger_immediate_gossip().await {
                warn!(error = %err, "failed to trigger immediate gossip for node failure");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn test_config() -> GossipConfig {
        GossipConfig {
            gossip_interval: Duration::from_millis(100),
            cleanup_interval: Duration::from_millis(200),
            peer_retry_interval: Duration::from_millis(50),
            immediate_propagation_enabled: true, // Enable for testing
            ..Default::default()
        }
    }

    #[test]
    fn test_registry_change_serialization() {
        let location = ActorLocation::new(test_addr(8080));
        let change = RegistryChange::ActorAdded {
            name: "test".to_string(),
            location,
            priority: RegistrationPriority::Immediate,
        };

        let serialized = bincode::serialize(&change).unwrap();
        let deserialized: RegistryChange = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            RegistryChange::ActorAdded { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Wrong change type"),
        }
    }

    #[test]
    fn test_registry_delta_serialization() {
        let delta = RegistryDelta {
            since_sequence: 10,
            current_sequence: 15,
            changes: vec![],
            sender_addr: test_addr(8080),
            wall_clock_time: 1000,
        };

        let serialized = bincode::serialize(&delta).unwrap();
        let deserialized: RegistryDelta = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.since_sequence, 10);
        assert_eq!(deserialized.current_sequence, 15);
        assert_eq!(deserialized.sender_addr, test_addr(8080));
    }

    #[test]
    fn test_peer_health_status() {
        let status = PeerHealthStatus {
            is_alive: true,
            last_contact: 1000,
            failure_count: 2,
        };

        assert!(status.is_alive);
        assert_eq!(status.last_contact, 1000);
        assert_eq!(status.failure_count, 2);
    }

    #[test]
    fn test_registry_message_variants() {
        // Test DeltaGossip
        let delta = RegistryDelta {
            since_sequence: 1,
            current_sequence: 2,
            changes: vec![],
            sender_addr: test_addr(8080),
            wall_clock_time: 1000,
        };
        let msg = RegistryMessage::DeltaGossip { delta };
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
        match deserialized {
            RegistryMessage::DeltaGossip { .. } => (),
            _ => panic!("Wrong message type"),
        }

        // Test FullSyncRequest
        let msg = RegistryMessage::FullSyncRequest {
            sender_addr: test_addr(8080),
            sequence: 10,
            wall_clock_time: 1000,
        };
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: RegistryMessage = bincode::deserialize(&serialized).unwrap();
        match deserialized {
            RegistryMessage::FullSyncRequest { sequence, .. } => {
                assert_eq!(sequence, 10);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_registry_stats() {
        let stats = RegistryStats {
            local_actors: 5,
            known_actors: 10,
            active_peers: 3,
            failed_peers: 1,
            total_gossip_rounds: 100,
            current_sequence: 100,
            uptime_seconds: 3600,
            last_gossip_timestamp: 1000,
            delta_exchanges: 50,
            full_sync_exchanges: 10,
            delta_history_size: 20,
            avg_delta_size: 5.5,
        };

        assert_eq!(stats.local_actors, 5);
        assert_eq!(stats.known_actors, 10);
        assert_eq!(stats.active_peers, 3);
        assert_eq!(stats.failed_peers, 1);
    }

    #[test]
    fn test_peer_info() {
        let mut peer = PeerInfo {
            address: test_addr(8080),
            peer_address: Some(test_addr(8081)),
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 3,
            last_failure_time: None,
        };

        assert_eq!(peer.address, test_addr(8080));
        assert_eq!(peer.failures, 0);

        peer.failures += 1;
        peer.last_failure_time = Some(200);
        assert_eq!(peer.failures, 1);
        assert_eq!(peer.last_failure_time, Some(200));
    }

    #[test]
    fn test_deduplicate_changes() {
        let location1 = ActorLocation::new(test_addr(8080));
        let location2 = ActorLocation::new(test_addr(8081));

        let changes = vec![
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location1.clone(),
                priority: RegistrationPriority::Normal,
            },
            RegistryChange::ActorAdded {
                name: "actor1".to_string(),
                location: location2,
                priority: RegistrationPriority::Immediate,
            },
            RegistryChange::ActorRemoved {
                name: "actor2".to_string(),
                priority: RegistrationPriority::Normal,
            },
        ];

        let deduped = GossipRegistry::deduplicate_changes(changes);
        assert_eq!(deduped.len(), 2); // Only one change per actor

        // Verify we kept the last change for actor1
        let actor1_changes: Vec<_> = deduped
            .iter()
            .filter(|c| match c {
                RegistryChange::ActorAdded { name, .. } => name == "actor1",
                _ => false,
            })
            .collect();
        assert_eq!(actor1_changes.len(), 1);
    }

    #[test]
    fn test_get_change_actor_name() {
        let location = ActorLocation::new(test_addr(8080));
        let add_change = RegistryChange::ActorAdded {
            name: "test_actor".to_string(),
            location,
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::get_change_actor_name(&add_change),
            "test_actor"
        );

        let remove_change = RegistryChange::ActorRemoved {
            name: "test_actor".to_string(),
            priority: RegistrationPriority::Normal,
        };
        assert_eq!(
            GossipRegistry::get_change_actor_name(&remove_change),
            "test_actor"
        );
    }

    #[tokio::test]
    async fn test_registry_creation() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());
        assert_eq!(registry.bind_addr, test_addr(8080));
        assert!(!registry.is_shutdown().await);
    }

    #[tokio::test]
    async fn test_add_bootstrap_peers() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());
        let peers = vec![test_addr(8081), test_addr(8082), test_addr(8080)]; // Including self

        registry.add_bootstrap_peers(peers).await;

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peers.len(), 2); // Should exclude self
        assert!(gossip_state.peers.contains_key(&test_addr(8081)));
        assert!(gossip_state.peers.contains_key(&test_addr(8082)));
        assert!(!gossip_state.peers.contains_key(&test_addr(8080))); // Self excluded
    }

    #[tokio::test]
    async fn test_add_peer() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8080)).await; // Try to add self
        registry.add_peer(test_addr(8081)).await; // Try to add duplicate

        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.peers.len(), 1);
        assert!(gossip_state.peers.contains_key(&test_addr(8081)));
    }

    #[tokio::test]
    async fn test_register_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = ActorLocation::new(test_addr(9001));
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(result.is_ok());

        // Verify actor is in local_actors
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.local_actors.contains_key("test_actor"));

        // Verify pending change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 1);
    }

    #[tokio::test]
    async fn test_register_actor_duplicate() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = ActorLocation::new(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location.clone())
            .await
            .unwrap();

        // Try to register again
        let result = registry
            .register_actor("test_actor".to_string(), location)
            .await;
        assert!(matches!(result, Err(GossipError::ActorAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_register_actor_with_priority() {
        let mut config = test_config();
        config.immediate_propagation_enabled = false; // Disable to test queuing
        let registry = GossipRegistry::new(test_addr(8080), config);

        let location = ActorLocation::new(test_addr(9001));
        let result = registry
            .register_actor_with_priority(
                "urgent_actor".to_string(),
                location,
                RegistrationPriority::Immediate,
            )
            .await;
        assert!(result.is_ok());

        // Verify urgent change was created (not cleared since immediate propagation is disabled)
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.urgent_changes.len(), 1);
        assert_eq!(gossip_state.pending_changes.len(), 0);
    }

    #[tokio::test]
    async fn test_unregister_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = ActorLocation::new(test_addr(9001));
        registry
            .register_actor("test_actor".to_string(), location)
            .await
            .unwrap();

        let removed = registry.unregister_actor("test_actor").await.unwrap();
        assert!(removed.is_some());

        // Verify actor is removed
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.local_actors.contains_key("test_actor"));

        // Verify removal change was created
        let gossip_state = registry.gossip_state.lock().await;
        assert_eq!(gossip_state.pending_changes.len(), 2); // Add + Remove
    }

    #[tokio::test]
    async fn test_lookup_actor() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Test local actor
        let location = ActorLocation::new(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), location.clone())
            .await
            .unwrap();

        let found = registry.lookup_actor("local_actor").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().address, test_addr(9001));

        // Test non-existent actor
        let not_found = registry.lookup_actor("missing_actor").await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_lookup_actor_ttl() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50); // Very short TTL for testing
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add a known actor with old timestamp
        let mut location = ActorLocation::new(test_addr(9001));
        location.wall_clock_time = current_timestamp() - 100; // Old timestamp

        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state
                .known_actors
                .insert("old_actor".to_string(), location);
        }

        // Should not find due to TTL
        let found = registry.lookup_actor("old_actor").await;
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_get_stats() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), ActorLocation::new(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        let stats = registry.get_stats().await;
        assert_eq!(stats.local_actors, 1);
        assert_eq!(stats.active_peers, 1);
        assert_eq!(stats.failed_peers, 0);
        assert_eq!(stats.uptime_seconds, 0); // Just created
    }

    #[tokio::test]
    async fn test_apply_delta() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let location = ActorLocation::new(test_addr(9001));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "remote_actor".to_string(),
                location,
                priority: RegistrationPriority::Normal,
            }],
            sender_addr: test_addr(8081),
            wall_clock_time: current_timestamp(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify actor was added to known_actors
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.known_actors.contains_key("remote_actor"));
    }

    #[tokio::test]
    async fn test_apply_delta_skip_local() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Register local actor
        let local_location = ActorLocation::new(test_addr(9001));
        registry
            .register_actor("local_actor".to_string(), local_location)
            .await
            .unwrap();

        // Try to override with remote update
        let remote_location = ActorLocation::new(test_addr(9002));
        let delta = RegistryDelta {
            since_sequence: 0,
            current_sequence: 1,
            changes: vec![RegistryChange::ActorAdded {
                name: "local_actor".to_string(),
                location: remote_location,
                priority: RegistrationPriority::Normal,
            }],
            sender_addr: test_addr(8081),
            wall_clock_time: current_timestamp(),
        };

        registry.apply_delta(delta).await.unwrap();

        // Verify local actor wasn't overridden
        let actor_state = registry.actor_state.read().await;
        let actor = actor_state.local_actors.get("local_actor").unwrap();
        assert_eq!(actor.address, test_addr(9001)); // Still local address
    }

    #[tokio::test]
    async fn test_should_use_delta_state() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let gossip_state = registry.gossip_state.lock().await;

        // New peer should use full sync
        let new_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            failures: 0,
            last_attempt: 0,
            last_success: 0,
            last_sequence: 0,
            last_sent_sequence: 0,
            consecutive_deltas: 0,
            last_failure_time: None,
        };
        assert!(!registry.should_use_delta_state(&gossip_state, &new_peer));

        // Peer with history should use delta
        let established_peer = PeerInfo {
            address: test_addr(8081),
            peer_address: None,
            failures: 0,
            last_attempt: 100,
            last_success: 100,
            last_sequence: 5,
            last_sent_sequence: 5,
            consecutive_deltas: 10,
            last_failure_time: None,
        };
        // Add some peers to make it not a small cluster
        drop(gossip_state);
        for i in 0..10 {
            registry.add_peer(test_addr(8090 + i)).await;
        }
        let gossip_state = registry.gossip_state.lock().await;
        assert!(registry.should_use_delta_state(&gossip_state, &established_peer));
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_no_peers() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn test_prepare_gossip_round_with_peers() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add peers
        registry.add_peer(test_addr(8081)).await;
        registry.add_peer(test_addr(8082)).await;

        // Add some changes
        registry
            .register_actor("actor1".to_string(), ActorLocation::new(test_addr(9001)))
            .await
            .unwrap();

        let tasks = registry.prepare_gossip_round().await.unwrap();
        assert!(!tasks.is_empty());
        assert!(tasks.len() <= 2); // Should gossip to available peers
    }

    #[tokio::test]
    async fn test_shutdown() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add some data
        registry
            .register_actor("actor1".to_string(), ActorLocation::new(test_addr(9001)))
            .await
            .unwrap();
        registry.add_peer(test_addr(8081)).await;

        assert!(!registry.is_shutdown().await);

        registry.shutdown().await;

        assert!(registry.is_shutdown().await);

        // Verify data was cleared
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.local_actors.is_empty());
        assert!(actor_state.known_actors.is_empty());

        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.is_empty());
    }

    #[tokio::test]
    async fn test_handle_peer_connection_failure() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add a peer
        registry.add_peer(test_addr(8081)).await;

        // Simulate failure
        registry
            .handle_peer_connection_failure(test_addr(8081))
            .await
            .unwrap();

        // Check peer is marked as failed
        let gossip_state = registry.gossip_state.lock().await;
        let peer = gossip_state.peers.get(&test_addr(8081)).unwrap();
        assert_eq!(peer.failures, registry.config.max_peer_failures);
        assert!(peer.last_failure_time.is_some());
    }

    #[tokio::test]
    async fn test_cleanup_stale_actors() {
        let mut config = test_config();
        config.actor_ttl = Duration::from_millis(50);
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add old actor
        let mut old_location = ActorLocation::new(test_addr(9001));
        old_location.wall_clock_time = current_timestamp() - 100;

        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state
                .known_actors
                .insert("old_actor".to_string(), old_location);
        }

        registry.cleanup_stale_actors().await;

        // Verify old actor was removed
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.known_actors.contains_key("old_actor"));
    }

    #[tokio::test]
    async fn test_cleanup_dead_peers() {
        let mut config = test_config();
        config.dead_peer_timeout = Duration::from_millis(50);
        config.max_peer_failures = 3;
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add a failed peer with old failure time
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.peers.insert(
                test_addr(8081),
                PeerInfo {
                    address: test_addr(8081),
                    peer_address: None,
                    failures: 3,
                    last_attempt: 0,
                    last_success: 0,
                    last_sequence: 0,
                    last_sent_sequence: 0,
                    consecutive_deltas: 0,
                    last_failure_time: Some(current_timestamp() - 100),
                },
            );
        }

        // Add some actors from the failed peer
        {
            let mut actor_state = registry.actor_state.write().await;
            actor_state.known_actors.insert(
                "peer_actor".to_string(),
                ActorLocation::new(test_addr(9001)),
            );

            let mut gossip_state = registry.gossip_state.lock().await;
            let mut actors = HashSet::new();
            actors.insert("peer_actor".to_string());
            gossip_state.peer_to_actors.insert(test_addr(8081), actors);
        }

        registry.cleanup_dead_peers().await;

        // Verify peer is KEPT but its actors were removed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.peers.contains_key(&test_addr(8081))); // Peer is still there!
        assert!(!gossip_state.peer_to_actors.contains_key(&test_addr(8081))); // But actors mapping is gone

        drop(gossip_state);
        let actor_state = registry.actor_state.read().await;
        assert!(!actor_state.known_actors.contains_key("peer_actor")); // Actors are cleaned up
    }

    #[tokio::test]
    async fn test_merge_full_sync() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        let mut remote_local = HashMap::new();
        remote_local.insert(
            "remote_actor1".to_string(),
            ActorLocation::new(test_addr(9001)),
        );

        let mut remote_known = HashMap::new();
        remote_known.insert(
            "remote_actor2".to_string(),
            ActorLocation::new(test_addr(9002)),
        );

        registry
            .merge_full_sync(
                remote_local,
                remote_known,
                test_addr(8081),
                1,
                current_timestamp(),
            )
            .await;

        // Verify actors were merged
        let actor_state = registry.actor_state.read().await;
        assert!(actor_state.known_actors.contains_key("remote_actor1"));
        assert!(actor_state.known_actors.contains_key("remote_actor2"));
    }

    #[tokio::test]
    async fn test_pending_failure() {
        let pending = PendingFailure {
            first_detected: 1000,
            consensus_deadline: 1005,
            query_sent: false,
        };

        assert_eq!(pending.first_detected, 1000);
        assert_eq!(pending.consensus_deadline, 1005);
        assert!(!pending.query_sent);
    }

    #[tokio::test]
    async fn test_historical_delta() {
        let delta = HistoricalDelta {
            sequence: 10,
            changes: vec![],
            wall_clock_time: 1000,
        };

        assert_eq!(delta.sequence, 10);
        assert!(delta.changes.is_empty());
        assert_eq!(delta.wall_clock_time, 1000);
    }

    #[tokio::test]
    async fn test_gossip_task() {
        let task = GossipTask {
            peer_addr: test_addr(8081),
            message: RegistryMessage::FullSyncRequest {
                sender_addr: test_addr(8080),
                sequence: 10,
                wall_clock_time: 1000,
            },
            current_sequence: 10,
        };

        assert_eq!(task.peer_addr, test_addr(8081));
        assert_eq!(task.current_sequence, 10);
    }

    #[tokio::test]
    async fn test_gossip_result() {
        let result = GossipResult {
            peer_addr: test_addr(8081),
            sent_sequence: 10,
            outcome: Ok(None),
        };

        assert_eq!(result.peer_addr, test_addr(8081));
        assert_eq!(result.sent_sequence, 10);
        assert!(result.outcome.is_ok());
    }

    #[tokio::test]
    async fn test_trigger_immediate_gossip() {
        let mut config = test_config();
        config.immediate_propagation_enabled = true;
        let registry = GossipRegistry::new(test_addr(8080), config);

        // Add peer
        registry.add_peer(test_addr(8081)).await;

        // Add urgent change
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state
                .urgent_changes
                .push(RegistryChange::ActorAdded {
                    name: "urgent".to_string(),
                    location: ActorLocation::new(test_addr(9001)),
                    priority: RegistrationPriority::Immediate,
                });
        }

        // Should not error even without actual connections
        let result = registry.trigger_immediate_gossip().await;
        assert!(result.is_ok());

        // Verify urgent changes were cleared
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.urgent_changes.is_empty());
    }

    #[tokio::test]
    async fn test_enforce_bounds() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add many pending changes
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            for i in 0..2000 {
                gossip_state
                    .pending_changes
                    .push(RegistryChange::ActorAdded {
                        name: format!("actor{}", i),
                        location: ActorLocation::new(test_addr(9000 + i as u16)),
                        priority: RegistrationPriority::Normal,
                    });
            }
        }

        registry.enforce_bounds().await;

        // Verify bounds were enforced
        let gossip_state = registry.gossip_state.lock().await;
        assert!(gossip_state.pending_changes.len() <= 1000);
    }

    #[tokio::test]
    async fn test_check_peer_consensus() {
        let registry = GossipRegistry::new(test_addr(8080), test_config());

        // Add a pending failure
        {
            let mut gossip_state = registry.gossip_state.lock().await;
            let pending = PendingFailure {
                first_detected: current_timestamp() - 10,
                consensus_deadline: current_timestamp() - 5, // Past deadline
                query_sent: true,
            };
            gossip_state
                .pending_peer_failures
                .insert(test_addr(8081), pending);

            // Add some health reports
            let mut reports = HashMap::new();
            reports.insert(
                test_addr(8080),
                PeerHealthStatus {
                    is_alive: false,
                    last_contact: current_timestamp(),
                    failure_count: 1,
                },
            );
            gossip_state
                .peer_health_reports
                .insert(test_addr(8081), reports);
        }

        registry.check_peer_consensus().await;

        // Verify pending failure was processed
        let gossip_state = registry.gossip_state.lock().await;
        assert!(!gossip_state
            .pending_peer_failures
            .contains_key(&test_addr(8081)));
    }
}
