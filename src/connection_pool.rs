use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    current_timestamp,
    registry::{GossipRegistry, RegistryMessage},
    GossipError, Result,
};

/// Connection pool for maintaining persistent TCP connections to peers
/// All connections are persistent - there is no checkout/checkin
pub struct ConnectionPool {
    /// Mapping: SocketAddr -> Connection
    connections: HashMap<SocketAddr, PersistentConnection>,
    max_connections: usize,
    connection_timeout: Duration,
    /// Registry reference for handling incoming messages
    registry: Option<std::sync::Weak<GossipRegistry>>,
}

struct PersistentConnection {
    sender: mpsc::Sender<Vec<u8>>,
    peer_addr: SocketAddr, // Actual connection address (may be NATed)
    last_used: u64,
    connected: bool,
}

/// Handle to send messages through a persistent connection
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    pub addr: SocketAddr,
    sender: mpsc::Sender<Vec<u8>>,
}

impl ConnectionHandle {
    /// Send pre-serialized data through this connection
    pub async fn send_data(&self, data: Vec<u8>) -> Result<()> {
        self.sender.send(data).await.map_err(|_| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "connection channel closed",
            ))
        })?;
        Ok(())
    }
}

impl ConnectionPool {
    pub fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        Self {
            connections: HashMap::new(),
            max_connections,
            connection_timeout,
            registry: None,
        }
    }

    /// Set the registry reference for handling incoming messages
    pub fn set_registry(&mut self, registry: std::sync::Arc<GossipRegistry>) {
        self.registry = Some(std::sync::Arc::downgrade(&registry));
    }

    /// Get or create a persistent connection to a peer
    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        let current_time = current_timestamp();

        // Check if we already have a connection to this address
        if let Some(conn) = self.connections.get_mut(&addr) {
            if conn.connected {
                conn.last_used = current_time;
                debug!(addr = %addr, "using existing persistent connection");
                return Ok(ConnectionHandle {
                    addr: conn.peer_addr,
                    sender: conn.sender.clone(),
                });
            } else {
                // Remove disconnected connection
                debug!(addr = %addr, "removing disconnected connection");
                self.connections.remove(&addr);
            }
        }

        // Create new persistent connection
        self.create_connection(addr).await
    }

    async fn create_connection(&mut self, addr: SocketAddr) -> Result<ConnectionHandle> {
        debug!(peer = %addr, "creating new persistent connection");

        // Make room if necessary
        if self.connections.len() >= self.max_connections {
            if let Some((oldest_addr, _)) = self.connections.iter().min_by_key(|(_, c)| c.last_used)
            {
                let oldest = *oldest_addr;
                self.connections.remove(&oldest);
                warn!(addr = %oldest, "removed oldest connection to make room");
            }
        }

        // Connect with timeout
        let stream = tokio::time::timeout(self.connection_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| GossipError::Timeout)?
            .map_err(GossipError::Network)?;

        // Configure socket
        stream.set_nodelay(true).map_err(GossipError::Network)?;

        // Create channel for sending messages
        let (tx, rx) = mpsc::channel(100);

        // Store connection info
        let conn = PersistentConnection {
            sender: tx.clone(),
            peer_addr: addr,
            last_used: current_timestamp(),
            connected: true,
        };

        self.connections.insert(addr, conn);

        // Start the connection handler directly
        let pool_addr = addr;
        let registry_weak = self.registry.clone();

        // Start handling the connection in background
        start_persistent_connection_handler(stream, rx, pool_addr, registry_weak);

        Ok(ConnectionHandle { addr, sender: tx })
    }

    /// Mark a connection as disconnected
    pub fn mark_disconnected(&mut self, addr: SocketAddr) {
        if let Some(conn) = self.connections.get_mut(&addr) {
            conn.connected = false;
            info!(peer = %addr, "marked connection as disconnected");
        }
    }

    /// Remove a connection from the pool by address
    pub fn remove_connection(&mut self, addr: SocketAddr) {
        if let Some(conn) = self.connections.remove(&addr) {
            info!(addr = %addr, "removed connection from pool");
            // Dropping the sender will cause the receiver to return None,
            // signaling the connection handler to shut down
            drop(conn.sender);
        }
    }

    /// Get number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.iter().filter(|(_, c)| c.connected).count()
    }

    /// Add a sender channel for an existing connection (used for incoming connections)
    /// This is temporary - we simply skip the check to allow both directions
    pub fn add_connection_sender(
        &mut self,
        listening_addr: SocketAddr,
        peer_addr: SocketAddr,
        sender: mpsc::Sender<Vec<u8>>,
    ) -> bool {
        // TEMPORARY: Comment out duplicate check to allow bidirectional connections
        // This causes the issue where both nodes reject each other's incoming connections
        // if self.connections.contains_key(&listening_addr) {
        //     return false; // Already have a connection to this address
        // }

        let conn = PersistentConnection {
            sender,
            peer_addr,
            last_used: current_timestamp(),
            connected: true,
        };

        // For incoming connections, we still use listening_addr as key
        // But we allow duplicates temporarily
        self.connections.insert(listening_addr, conn);
        info!(peer = %peer_addr, listening = %listening_addr, "added incoming connection sender to pool");
        true
    }

    /// Check if we have a connection to a peer by address
    pub fn has_connection(&self, addr: &SocketAddr) -> bool {
        self.connections
            .get(addr)
            .map(|c| c.connected)
            .unwrap_or(false)
    }

    /// Check health of all connections (for compatibility)
    pub async fn check_connection_health(&mut self) -> Vec<SocketAddr> {
        // Health checking is now done by the persistent connection handlers
        Vec::new()
    }

    /// Clean up stale connections
    pub fn cleanup_stale_connections(&mut self) {
        let to_remove: Vec<_> = self
            .connections
            .iter()
            .filter(|(_, conn)| !conn.connected)
            .map(|(addr, _)| *addr)
            .collect();

        for addr in to_remove {
            self.connections.remove(&addr);
            debug!(addr = %addr, "cleaned up disconnected connection");
        }
    }

    /// Close all connections (for shutdown)
    pub fn close_all_connections(&mut self) {
        let addrs: Vec<_> = self.connections.keys().cloned().collect();
        let count = addrs.len();
        for addr in addrs {
            self.remove_connection(addr);
        }
        info!("closed all {} connections", count);
    }
}

/// Start a persistent connection handler in the background
fn start_persistent_connection_handler(
    stream: TcpStream,
    rx: mpsc::Receiver<Vec<u8>>,
    peer_addr: SocketAddr,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    tokio::spawn(async move {
        handle_persistent_connection_impl(stream, rx, peer_addr, registry_weak).await;
    });
}

/// Handle an incoming persistent connection - processes ALL message types bidirectionally
pub(crate) async fn handle_incoming_persistent_connection(
    stream: TcpStream,
    rx: mpsc::Receiver<Vec<u8>>,
    _peer_addr: SocketAddr,
    listening_addr: SocketAddr, // The sender's listening address
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    handle_persistent_connection_impl(stream, rx, listening_addr, registry_weak).await
}

/// Implementation of persistent connection handling
async fn handle_persistent_connection_impl(
    stream: TcpStream,
    mut rx: mpsc::Receiver<Vec<u8>>,
    peer_addr: SocketAddr, // For outgoing: the peer's address. For incoming: the sender's listening address
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) {
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    info!(peer = %peer_addr, registry = ?registry_weak.is_some(), "persistent connection handler started");

    let (mut reader, mut writer) = stream.into_split();

    // Create a channel to signal EOF to write task
    let (eof_tx, mut eof_rx) = mpsc::channel::<()>(1);

    // Clone registry_weak for the read task
    let registry_weak_read = registry_weak.clone();

    // Spawn read task
    let read_task = tokio::spawn(async move {
        let mut partial_msg_buf = Vec::new();
        let mut read_buf = vec![0u8; 4096];

        loop {
            match reader.read(&mut read_buf).await {
                Ok(0) => {
                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: Peer closed write (EOF) - half-closed");
                    // Signal EOF to write task
                    let _ = eof_tx.send(()).await;
                    return io::Result::Ok(());
                }
                Ok(n) => {
                    // info!(peer = %peer_addr, bytes = n, "🔵 CONNECTION POOL: read {} bytes", n);
                    // Process the data we just read
                    partial_msg_buf.extend_from_slice(&read_buf[..n]);

                    // Try to process complete messages
                    while partial_msg_buf.len() >= 4 {
                        let len = u32::from_be_bytes([
                            partial_msg_buf[0],
                            partial_msg_buf[1],
                            partial_msg_buf[2],
                            partial_msg_buf[3],
                        ]) as usize;

                        if len > 10 * 1024 * 1024 {
                            warn!(peer = %peer_addr, "message too large: {}", len);
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "message too large",
                            ));
                        }

                        let total_len = 4 + len;
                        if partial_msg_buf.len() >= total_len {
                            // We have a complete message
                            let msg_data = partial_msg_buf[4..total_len].to_vec();

                            // Remove processed message from buffer
                            partial_msg_buf.drain(..total_len);

                            // Deserialize and handle the message
                            match bincode::deserialize::<RegistryMessage>(&msg_data) {
                                Ok(msg) => {
                                    // debug!(peer = %peer_addr, "📥 CONNECTION POOL: Received message");

                                    if let Some(ref registry_weak) = registry_weak_read {
                                        if let Some(registry) = registry_weak.upgrade() {
                                            // All connections are bidirectional - handle ALL message types
                                            // info!(peer = %peer_addr, "📥 Processing message on bidirectional connection");

                                            // Process the message using the existing handle_incoming_message logic
                                            if let Err(e) = handle_incoming_message(
                                                registry.clone(),
                                                peer_addr,
                                                msg,
                                            )
                                            .await
                                            {
                                                warn!(peer = %peer_addr, error = %e, "failed to handle incoming message");
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(peer = %peer_addr, error = %e, "failed to deserialize message");
                                }
                            }
                        } else {
                            // Need more data for complete message
                            break;
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!(peer = %peer_addr, error = %e, "error reading from socket");
                    return Err(e);
                }
            }
        }
    });

    // Spawn write task
    let write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Check for EOF signal
                _ = eof_rx.recv() => {
                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: EOF signal received, shutting down write task");
                    return io::Result::Ok(());
                }
                // Check for messages to send
                msg = rx.recv() => {
                    match msg {
                        Some(data) => {
                            match writer.write_all(&data).await {
                                Ok(_) => {
                                    debug!(peer = %peer_addr, bytes = data.len(), "📤 CONNECTION POOL: Sent message");
                                }
                                Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe
                                          || e.kind() == io::ErrorKind::ConnectionReset => {
                                    info!(peer = %peer_addr, "🔴 CONNECTION POOL: Peer closed read - write failed");
                                    return io::Result::Ok(());
                                }
                                Err(e) => {
                                    warn!(peer = %peer_addr, error = %e, "failed to write");
                                    return Err(e);
                                }
                            }
                        }
                        None => {
                            info!(peer = %peer_addr, "🔴 CONNECTION POOL: channel closed, shutting down write task");
                            return io::Result::Ok(());
                        }
                    }
                }
            }
        }
    });

    // Wait for both tasks to complete
    let (read_res, write_res) = tokio::join!(read_task, write_task);

    // Log results
    match read_res {
        Ok(Ok(_)) => info!(peer = %peer_addr, "read task completed successfully"),
        Ok(Err(e)) => warn!(peer = %peer_addr, error = %e, "read task failed"),
        Err(e) => warn!(peer = %peer_addr, error = %e, "read task panicked"),
    }

    match write_res {
        Ok(Ok(_)) => info!(peer = %peer_addr, "write task completed successfully"),
        Ok(Err(e)) => warn!(peer = %peer_addr, error = %e, "write task failed"),
        Err(e) => warn!(peer = %peer_addr, error = %e, "write task panicked"),
    }

    // Notify of disconnection
    info!(peer = %peer_addr, "persistent connection lost, triggering failure handling");

    // Mark connection as disconnected in the pool
    if let Some(ref registry_weak) = registry_weak {
        if let Some(registry) = registry_weak.upgrade() {
            // First mark disconnected in pool
            {
                let mut pool = registry.connection_pool.lock().await;
                pool.mark_disconnected(peer_addr);
            }

            // Then handle the failure in a separate task
            let failed_addr = peer_addr;
            tokio::spawn(async move {
                if let Err(e) = registry.handle_peer_connection_failure(failed_addr).await {
                    warn!(error = %e, peer = %failed_addr, "failed to handle peer connection failure");
                }
            });
        }
    }
}

/// Handle an incoming message on a bidirectional connection
pub(crate) async fn handle_incoming_message(
    registry: Arc<GossipRegistry>,
    _peer_addr: SocketAddr,
    msg: RegistryMessage,
) -> Result<()> {
    match msg {
        RegistryMessage::DeltaGossip { delta } => {
            debug!(
                sender = %delta.sender_addr,
                since_sequence = delta.since_sequence,
                changes = delta.changes.len(),
                "received delta gossip message on bidirectional connection"
            );

            // Add the sender as a peer
            registry.add_peer(delta.sender_addr).await;

            // Apply the delta
            if let Err(err) = registry.apply_delta(delta.clone()).await {
                warn!(error = %err, "failed to apply delta");
            }

            // Update peer info and handle reconnection
            {
                let mut gossip_state = registry.gossip_state.lock().await;

                // Check if this is a previously failed peer
                let was_failed = gossip_state
                    .peers
                    .get(&delta.sender_addr)
                    .map(|info| info.failures >= registry.config.max_peer_failures)
                    .unwrap_or(false);

                if was_failed {
                    info!(
                        peer = %delta.sender_addr,
                        "✅ Received delta from previously failed peer - connection restored!"
                    );

                    // Clear the pending failure record
                    gossip_state
                        .pending_peer_failures
                        .remove(&delta.sender_addr);
                }

                // Update peer info
                if let Some(peer_info) = gossip_state.peers.get_mut(&delta.sender_addr) {
                    // Reset failure state
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                    peer_info.last_success = crate::current_timestamp();

                    peer_info.last_sequence =
                        std::cmp::max(peer_info.last_sequence, delta.current_sequence);
                    peer_info.consecutive_deltas += 1;
                }
                gossip_state.delta_exchanges += 1;
            }

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        RegistryMessage::FullSync {
            local_actors,
            known_actors,
            sender_addr,
            sequence,
            wall_clock_time,
        } => {
            // Calculate failed peers count
            let failed_peers = {
                let gossip_state = registry.gossip_state.lock().await;
                let active_peers = gossip_state
                    .peers
                    .values()
                    .filter(|p| p.failures < registry.config.max_peer_failures)
                    .count();
                gossip_state.peers.len() - active_peers
            };

            info!(
                sender = %sender_addr,
                sequence = sequence,
                local_actors = local_actors.len(),
                known_actors = known_actors.len(),
                failed_peers = failed_peers,
                "📨 INCOMING: Received full sync message on bidirectional connection"
            );

            // Add the sender as a peer
            registry.add_peer(sender_addr).await;

            registry
                .merge_full_sync(
                    local_actors,
                    known_actors,
                    sender_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

            // Reset delta counter for this peer and handle reconnection
            {
                let mut gossip_state = registry.gossip_state.lock().await;

                // Check if this is a previously failed peer
                let was_failed = gossip_state
                    .peers
                    .get(&sender_addr)
                    .map(|info| info.failures >= registry.config.max_peer_failures)
                    .unwrap_or(false);

                if was_failed {
                    info!(
                        peer = %sender_addr,
                        "✅ Received full sync from previously failed peer - connection restored!"
                    );

                    // Clear the pending failure record
                    gossip_state.pending_peer_failures.remove(&sender_addr);
                }

                // Update peer info
                if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr) {
                    // Reset failure state
                    peer_info.failures = 0;
                    peer_info.last_failure_time = None;
                    peer_info.last_success = crate::current_timestamp();
                    peer_info.consecutive_deltas = 0;
                }
                gossip_state.full_sync_exchanges += 1;
            }

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        RegistryMessage::FullSyncRequest {
            sender_addr,
            sequence: _,
            wall_clock_time: _,
        } => {
            debug!(
                sender = %sender_addr,
                "received full sync request on bidirectional connection"
            );

            {
                let mut gossip_state = registry.gossip_state.lock().await;
                gossip_state.full_sync_exchanges += 1;
            }

            // Note: Response will be sent during regular gossip rounds
            Ok(())
        }
        // Handle response messages (these can arrive on incoming connections too)
        RegistryMessage::DeltaGossipResponse { delta } => {
            debug!(
                sender = %delta.sender_addr,
                changes = delta.changes.len(),
                "received delta gossip response on bidirectional connection"
            );

            if let Err(err) = registry.apply_delta(delta).await {
                warn!(error = %err, "failed to apply delta from response");
            } else {
                let mut gossip_state = registry.gossip_state.lock().await;
                gossip_state.delta_exchanges += 1;
            }
            Ok(())
        }
        RegistryMessage::FullSyncResponse {
            local_actors,
            known_actors,
            sender_addr,
            sequence,
            wall_clock_time,
        } => {
            debug!(
                sender = %sender_addr,
                "received full sync response on bidirectional connection"
            );

            registry
                .merge_full_sync(
                    local_actors,
                    known_actors,
                    sender_addr,
                    sequence,
                    wall_clock_time,
                )
                .await;

            let mut gossip_state = registry.gossip_state.lock().await;
            gossip_state.full_sync_exchanges += 1;
            Ok(())
        }
        RegistryMessage::PeerHealthQuery {
            sender,
            target_peer,
            timestamp: _,
        } => {
            debug!(
                sender = %sender,
                target = %target_peer,
                "received peer health query"
            );

            // Check our connection status to the target peer
            let is_alive = {
                let pool = registry.connection_pool.lock().await;
                pool.has_connection(&target_peer)
            };

            let last_contact = if is_alive {
                crate::current_timestamp()
            } else {
                // Check when we last had successful contact
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state
                    .peers
                    .get(&target_peer)
                    .map(|info| info.last_success)
                    .unwrap_or(0)
            };

            // Send our health report back
            let mut peer_statuses = HashMap::new();
            peer_statuses.insert(
                target_peer,
                crate::registry::PeerHealthStatus {
                    is_alive,
                    last_contact,
                    failure_count: 0, // TODO: track actual failure count
                },
            );

            let report = RegistryMessage::PeerHealthReport {
                reporter: registry.bind_addr,
                peer_statuses,
                timestamp: crate::current_timestamp(),
            };

            // Send report back to the querying peer
            if let Ok(data) = bincode::serialize(&report) {
                let len = data.len() as u32;
                let mut buffer = Vec::with_capacity(4 + data.len());
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(&data);

                let mut pool = registry.connection_pool.lock().await;
                if let Ok(conn) = pool.get_connection(sender).await {
                    let _ = conn.send_data(buffer).await;
                }
            }

            Ok(())
        }
        RegistryMessage::PeerHealthReport {
            reporter,
            peer_statuses,
            timestamp: _,
        } => {
            debug!(
                reporter = %reporter,
                peers = peer_statuses.len(),
                "received peer health report"
            );

            // Store the health reports
            {
                let mut gossip_state = registry.gossip_state.lock().await;
                for (peer, status) in peer_statuses {
                    gossip_state
                        .peer_health_reports
                        .entry(peer)
                        .or_insert_with(HashMap::new)
                        .insert(reporter, status);
                }
            }

            // Check if we have enough reports to make a decision
            registry.check_peer_consensus().await;

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::sleep;

    async fn create_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connections in background
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    // Simple echo server - just keep connection open
                    let mut buf = vec![0; 1024];
                    loop {
                        use tokio::io::AsyncReadExt;
                        match stream.read(&mut buf).await {
                            Ok(0) => break, // Connection closed
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        addr
    }

    #[test]
    fn test_connection_handle_debug() {
        // Compile-time test to ensure Debug is implemented
        use std::fmt::Debug;
        fn assert_debug<T: Debug>() {}
        assert_debug::<ConnectionHandle>();
    }

    #[tokio::test]
    async fn test_connection_pool_new() {
        let pool = ConnectionPool::new(10, Duration::from_secs(5));
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.max_connections, 10);
        assert_eq!(pool.connection_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_set_registry() {
        use crate::{registry::GossipRegistry, GossipConfig};
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let registry = Arc::new(GossipRegistry::new(
            "127.0.0.1:8080".parse().unwrap(),
            GossipConfig::default(),
        ));

        pool.set_registry(registry.clone());
        assert!(pool.registry.is_some());
    }

    #[tokio::test]
    async fn test_get_connection_new() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;

        // Wait for server to start
        sleep(Duration::from_millis(10)).await;

        // Get connection should create new one
        let handle = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(handle.addr, server_addr);
        assert_eq!(pool.connection_count(), 1);
        assert!(pool.has_connection(&server_addr));
    }

    #[tokio::test]
    async fn test_get_connection_reuse() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // First connection
        let handle1 = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        // Second get should reuse existing connection
        let handle2 = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1); // Still just one connection
        assert_eq!(handle1.addr, handle2.addr);
    }

    #[tokio::test]
    async fn test_get_connection_timeout() {
        let mut pool = ConnectionPool::new(10, Duration::from_millis(100));
        let nonexistent_addr = "127.0.0.1:1".parse().unwrap();

        // Should timeout or get connection refused
        let result = pool.get_connection(nonexistent_addr).await;
        match result {
            Err(GossipError::Timeout) => (),    // Expected
            Err(GossipError::Network(_)) => (), // Also acceptable (connection refused)
            _ => panic!("Expected timeout or network error, got: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_connection_pool_full() {
        let mut pool = ConnectionPool::new(2, Duration::from_secs(5));

        let server1 = create_test_server().await;
        let server2 = create_test_server().await;
        let server3 = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // Fill pool
        pool.get_connection(server1).await.unwrap();
        pool.get_connection(server2).await.unwrap();
        assert_eq!(pool.connection_count(), 2);

        // Third connection should evict oldest
        pool.get_connection(server3).await.unwrap();
        assert_eq!(pool.connection_count(), 2); // Still 2, one was evicted
    }

    #[tokio::test]
    async fn test_mark_disconnected() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        pool.mark_disconnected(server_addr);
        assert_eq!(pool.connection_count(), 0); // Disconnected connections don't count
        assert!(!pool.has_connection(&server_addr));
    }

    #[tokio::test]
    async fn test_remove_connection() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert!(pool.has_connection(&server_addr));

        pool.remove_connection(server_addr);
        assert!(!pool.has_connection(&server_addr));
        assert_eq!(pool.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_add_connection_sender() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let listening_addr = "127.0.0.1:8080".parse().unwrap();
        let peer_addr = "127.0.0.1:9090".parse().unwrap();
        let (tx, _rx) = mpsc::channel(100);

        let added = pool.add_connection_sender(listening_addr, peer_addr, tx);
        assert!(added);
        assert!(pool.has_connection(&listening_addr));
        assert_eq!(pool.connection_count(), 1);
    }

    #[tokio::test]
    async fn test_cleanup_stale_connections() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);

        // Mark as disconnected
        pool.mark_disconnected(server_addr);
        assert_eq!(pool.connection_count(), 0);

        // Cleanup should remove disconnected
        pool.cleanup_stale_connections();
        assert!(pool.connections.is_empty());
    }

    #[tokio::test]
    async fn test_close_all_connections() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));

        let server1 = create_test_server().await;
        let server2 = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        pool.get_connection(server1).await.unwrap();
        pool.get_connection(server2).await.unwrap();
        assert_eq!(pool.connection_count(), 2);

        pool.close_all_connections();
        assert_eq!(pool.connection_count(), 0);
        assert!(pool.connections.is_empty());
    }

    #[tokio::test]
    async fn test_connection_handle_send_data() {
        let (tx, mut rx) = mpsc::channel(100);
        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            sender: tx,
        };

        let data = vec![1, 2, 3, 4];
        handle.send_data(data.clone()).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn test_connection_handle_send_data_closed() {
        let (tx, rx) = mpsc::channel(100);
        drop(rx); // Close receiver

        let handle = ConnectionHandle {
            addr: "127.0.0.1:8080".parse().unwrap(),
            sender: tx,
        };

        let result = handle.send_data(vec![1, 2, 3]).await;
        assert!(matches!(result, Err(GossipError::Network(_))));
    }

    #[tokio::test]
    async fn test_check_connection_health() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let failed = pool.check_connection_health().await;
        assert!(failed.is_empty()); // Current implementation always returns empty
    }

    #[tokio::test]
    async fn test_persistent_connection() {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let last_used = current_timestamp();
        let (tx, _rx) = mpsc::channel(100);

        let conn = PersistentConnection {
            sender: tx,
            peer_addr: addr,
            last_used,
            connected: true,
        };

        assert_eq!(conn.peer_addr, addr);
        assert_eq!(conn.last_used, last_used);
        assert!(conn.connected);
    }

    #[tokio::test]
    async fn test_get_connection_disconnected_removal() {
        let mut pool = ConnectionPool::new(10, Duration::from_secs(5));
        let server_addr = create_test_server().await;
        sleep(Duration::from_millis(10)).await;

        // Create connection
        pool.get_connection(server_addr).await.unwrap();

        // Mark as disconnected
        pool.mark_disconnected(server_addr);

        // Getting connection again should create new one
        let handle = pool.get_connection(server_addr).await.unwrap();
        assert_eq!(pool.connection_count(), 1);
        assert_eq!(handle.addr, server_addr);
    }
}
