use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{interval, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    connection_pool::handle_incoming_message,
    registry::{GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryStats},
    GossipConfig, GossipError, RegistrationPriority, RemoteActorLocation, Result,
};

/// Per-connection streaming state for managing partial streams
#[derive(Debug)]
struct StreamingState {
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
    chunks: BTreeMap<u32, Bytes>, // chunk_index -> chunk_data
    received_size: usize,
}

impl StreamingState {
    fn new() -> Self {
        Self {
            active_streams: HashMap::new(),
            max_concurrent_streams: 16, // Reasonable limit
        }
    }

    fn start_stream(&mut self, header: crate::StreamHeader) -> Result<()> {
        if self.active_streams.len() >= self.max_concurrent_streams {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "Too many concurrent streams",
            )));
        }

        let stream = InProgressStream {
            stream_id: header.stream_id,
            total_size: header.total_size,
            type_hash: header.type_hash,
            actor_id: header.actor_id,
            chunks: BTreeMap::new(),
            received_size: 0,
        };

        self.active_streams.insert(header.stream_id, stream);
        Ok(())
    }

    fn add_chunk(
        &mut self,
        header: crate::StreamHeader,
        chunk_data: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let stream = self
            .active_streams
            .get_mut(&header.stream_id)
            .ok_or_else(|| {
                GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Received chunk for unknown stream_id={}", header.stream_id),
                ))
            })?;

        // Store chunk
        let chunk_bytes = Bytes::from(chunk_data);
        stream.received_size += chunk_bytes.len();
        stream.chunks.insert(header.chunk_index, chunk_bytes);

        // Check if we have all chunks (when total matches expected size)
        if stream.received_size >= stream.total_size as usize {
            self.assemble_complete_message(header.stream_id)
        } else {
            Ok(None)
        }
    }

    fn finalize_stream(&mut self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        // StreamEnd received - assemble the message
        self.assemble_complete_message(stream_id)
    }

    fn assemble_complete_message(&mut self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let stream = self.active_streams.remove(&stream_id).ok_or_else(|| {
            GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Cannot finalize unknown stream_id={}", stream_id),
            ))
        })?;

        // Assemble chunks in order
        let mut complete_data = BytesMut::with_capacity(stream.total_size as usize);
        for (_chunk_index, chunk_data) in stream.chunks {
            complete_data.put_slice(&chunk_data);
        }

        info!(
            "✅ STREAMING: Assembled complete message for stream_id={} ({} bytes for actor={}, type_hash=0x{:x})",
            stream.stream_id,
            complete_data.len(),
            stream.actor_id,
            stream.type_hash
        );

        Ok(Some(complete_data.to_vec()))
    }
}

/// Main API for the gossip registry with vector clocks and separated locks
pub struct GossipRegistryHandle {
    pub registry: Arc<GossipRegistry>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
    _monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry with TLS encryption
    ///
    /// This creates a secure gossip registry that uses TLS 1.3 for all connections.
    /// The secret_key is used to generate the node's identity certificate.
    pub async fn new_with_tls(
        bind_addr: SocketAddr,
        secret_key: crate::SecretKey,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();

        // Create the TCP listener first to get the actual bound address
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_bind_addr = listener.local_addr()?;

        // Create registry with TLS enabled
        let mut registry = GossipRegistry::new(actual_bind_addr, config.clone());
        registry.enable_tls(secret_key)?;

        let registry = Arc::new(registry);

        // Set the registry reference in the connection pool
        {
            let mut pool = registry.connection_pool.lock().await;
            pool.set_registry(registry.clone());
        }

        // Start the server with the existing listener
        let server_registry = registry.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(err) = start_gossip_server_with_listener(server_registry, listener).await {
                error!(error = %err, "TLS server error");
            }
        });

        // Start the gossip timer
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            start_gossip_timer(timer_registry).await;
        });

        // Connection monitoring is now done in the gossip timer
        let monitor_handle = None;

        info!(bind_addr = %actual_bind_addr, "TLS-enabled gossip registry started");

        Ok(Self {
            registry,
            _server_handle: server_handle,
            _timer_handle: timer_handle,
            _monitor_handle: monitor_handle,
        })
    }

    /// Create and start a new gossip registry
    ///
    /// Note: The peers parameter is deprecated. Always pass an empty Vec and use
    /// add_peer() after creation to ensure proper peer ID assignment.
    #[instrument(skip(peers, config))]
    pub async fn new(
        bind_addr: SocketAddr,
        peers: Vec<SocketAddr>,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let config = config.unwrap_or_default();

        // Create the TCP listener first to get the actual bound address
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_bind_addr = listener.local_addr()?;

        let registry = GossipRegistry::new(actual_bind_addr, config.clone());

        // Note: Peers should be added manually with add_peer() after creation
        // to ensure proper peer ID assignment. The peers parameter is kept
        // for backward compatibility but is now deprecated.
        if !peers.is_empty() {
            warn!(
                "Passing peer addresses to GossipRegistryHandle::new() is deprecated. \
                 Use handle.add_peer() after creation to set proper peer IDs."
            );
            // For backward compatibility, we still configure them but with a warning
            for (i, peer_addr) in peers.into_iter().enumerate() {
                let node_name = format!("peer_{}", i);
                warn!(
                    "Auto-generating peer name '{}' for {} - this is deprecated",
                    node_name, peer_addr
                );
                registry.configure_peer(node_name, peer_addr).await;
            }
        }

        let registry = Arc::new(registry);

        // Set the registry reference in the connection pool
        {
            let mut pool = registry.connection_pool.lock().await;
            pool.set_registry(registry.clone());
        }

        // Start the server with the existing listener
        let server_registry = registry.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(err) = start_gossip_server_with_listener(server_registry, listener).await {
                error!(error = %err, "server error");
            }
        });

        // Start the gossip timer
        debug!("🚀 About to spawn gossip timer task");
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            debug!("🚀 Gossip timer task spawned, calling start_gossip_timer");
            start_gossip_timer(timer_registry).await;
            warn!("⚠️ Gossip timer exited unexpectedly!");
        });
        debug!("🚀 Gossip timer spawn complete");

        // Connection monitoring is now done in the gossip timer
        let monitor_handle = None;

        info!(bind_addr = %actual_bind_addr, "gossip registry started");

        Ok(Self {
            registry,
            _server_handle: server_handle,
            _timer_handle: timer_handle,
            _monitor_handle: monitor_handle,
        })
    }

    /// Register a local actor
    pub async fn register(&self, name: String, address: SocketAddr) -> Result<()> {
        let location = RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with metadata
    pub async fn register_with_metadata(
        &self,
        name: String,
        address: SocketAddr,
        metadata: Vec<u8>,
    ) -> Result<()> {
        let location = RemoteActorLocation::new_with_metadata(
            address,
            self.registry.peer_id.clone(),
            metadata,
        );
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with high priority (faster propagation)
    pub async fn register_urgent(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Register a local actor with specified priority
    pub async fn register_with_priority(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let mut location =
            RemoteActorLocation::new_with_peer(address, self.registry.peer_id.clone());
        location.priority = priority;
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<RemoteActorLocation>> {
        self.registry.unregister_actor(name).await
    }

    /// Lookup an actor (now much faster - read-only lock)
    pub async fn lookup(&self, name: &str) -> Option<RemoteActorLocation> {
        self.registry.lookup_actor(name).await
    }

    /// Get registry statistics including vector clock metrics
    pub async fn stats(&self) -> RegistryStats {
        self.registry.get_stats().await
    }

    /// Add a peer to the gossip network
    pub async fn add_peer(&self, peer_id: &crate::PeerId) -> crate::Peer {
        // Pre-configure the peer as allowed (address will be set when connect() is called)
        {
            let pool = self.registry.connection_pool.lock().await;
            // Use a placeholder address - will be updated when connect() is called
            pool.peer_id_to_addr
                .insert(peer_id.clone(), "0.0.0.0:0".parse().unwrap());
        }

        crate::Peer {
            peer_id: peer_id.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Get a connection handle for direct communication (reuses existing pool connections)
    pub async fn get_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry.get_connection(addr).await
    }

    /// Bootstrap peer connections non-blocking (Phase 4)
    ///
    /// Dials seed peers asynchronously - doesn't block startup on gossip propagation.
    /// Failed connections are logged but don't prevent the node from starting.
    /// This is the recommended way to bootstrap a node with seed peers.
    pub async fn bootstrap_non_blocking(&self, seeds: Vec<SocketAddr>) {
        let seed_count = seeds.len();

        for seed in seeds {
            let registry = self.registry.clone();
            tokio::spawn(async move {
                match registry.get_connection(seed).await {
                    Ok(_conn) => {
                        debug!(seed = %seed, "bootstrap connection established");
                        // Mark peer as connected
                        registry.mark_peer_connected(seed).await;
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "bootstrap peer unavailable");
                        // Note: Don't penalize at startup - peer might be starting up too
                    }
                }
            });
        }

        debug!(
            seed_count = seed_count,
            "initiated non-blocking bootstrap for seed peers"
        );
    }

    /// Shutdown the registry
    pub async fn shutdown(&self) {
        self.registry.shutdown().await;

        // Cancel background tasks (they will terminate when they detect shutdown)
        self._server_handle.abort();
        self._timer_handle.abort();
        if let Some(monitor_handle) = &self._monitor_handle {
            monitor_handle.abort();
        }

        // No artificial delays - connections will close immediately
    }
}

/// Start the gossip registry server with an existing listener
#[instrument(skip(registry, listener))]
async fn start_gossip_server_with_listener(
    registry: Arc<GossipRegistry>,
    listener: TcpListener,
) -> Result<()> {
    let bind_addr = registry.bind_addr;
    info!(bind_addr = %bind_addr, "gossip server started");

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!(peer_addr = %peer_addr, "📥 ACCEPTED incoming connection");
                // Set TCP_NODELAY for low-latency communication
                let _ = stream.set_nodelay(true);

                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer_addr, registry_clone).await;
                });
            }
            Err(err) => {
                error!(error = %err, "failed to accept connection");
            }
        }
    }
}

/// Start the gossip timer with vector clock support
#[instrument(skip(registry))]
async fn start_gossip_timer(registry: Arc<GossipRegistry>) {
    debug!("start_gossip_timer function called");

    let gossip_interval = registry.config.gossip_interval;
    let cleanup_interval = registry.config.cleanup_interval;
    let vector_clock_gc_interval = registry.config.vector_clock_gc_frequency;
    let peer_gossip_interval = registry.config.peer_gossip_interval;

    let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
    let mut next_gossip_tick = Instant::now() + gossip_interval + jitter;
    let mut cleanup_timer = interval(cleanup_interval);
    let mut vector_clock_gc_timer = interval(vector_clock_gc_interval);

    // Peer gossip timer - only if peer discovery is enabled
    let mut peer_gossip_timer = peer_gossip_interval.map(|i| interval(i));

    debug!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
        vector_clock_gc_interval_secs = vector_clock_gc_interval.as_secs(),
        peer_gossip_interval_secs = peer_gossip_interval.map(|i| i.as_secs()),
        "gossip timer started with non-blocking I/O"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_gossip_tick) => {
                let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
                next_gossip_tick += gossip_interval + jitter;

                // Step 1: Prepare gossip tasks while holding the lock briefly
                let tasks = {
                    if registry.is_shutdown().await {
                        break;
                    }
                    match registry.prepare_gossip_round().await {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            error!(error = %err, "failed to prepare gossip round");
                            continue;
                        }
                    }
                };

                if tasks.is_empty() {
                    continue;
                }

                // Step 2: Execute all gossip tasks WITHOUT holding the registry lock
                // Use zero-copy optimized sending for each individual gossip message
                let results = {
                    let mut futures = Vec::new();

                    for task in tasks {
                        let registry_clone = registry.clone();
                        let peer_addr = task.peer_addr;
                        let sent_sequence = task.current_sequence;
                        let future = tokio::spawn(async move {
                            // Send the message using zero-copy persistent connections
                            let outcome = send_gossip_message_zero_copy(task, registry_clone).await;
                            GossipResult {
                                peer_addr,
                                sent_sequence,
                                outcome: outcome.map(|_| None),
                            }
                        });
                        futures.push(future);
                    }

                    // Wait for all gossip operations to complete concurrently
                    let mut results = Vec::new();
                    for future in futures {
                        match future.await {
                            Ok(result) => results.push(result),
                            Err(err) => {
                                error!(error = %err, "gossip task panicked");
                            }
                        }
                    }
                    results
                };

                // Step 3: Apply results while holding the lock briefly
                {
                    if !registry.is_shutdown().await {
                        registry.apply_gossip_results(results).await;
                    }
                }
            }
            _ = cleanup_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                registry.cleanup_stale_actors().await;
                // Also check for consensus timeouts
                registry.check_peer_consensus().await;
                // Clean up peers that have been dead for too long
                registry.cleanup_dead_peers().await;
                // Clean up stale peers from peer discovery (Phase 4)
                registry.prune_stale_peers().await;
            }
            _ = vector_clock_gc_timer.tick() => {
                if registry.is_shutdown().await {
                    break;
                }
                // Run vector clock garbage collection
                registry.run_vector_clock_gc().await;
            }
            // Peer gossip timer - for peer list gossip (Phase 4)
            _ = async {
                if let Some(ref mut timer) = peer_gossip_timer {
                    timer.tick().await
                } else {
                    // If peer gossip is disabled, wait forever (never fires)
                    std::future::pending::<tokio::time::Instant>().await
                }
            } => {
                if registry.is_shutdown().await {
                    break;
                }
                // Only gossip peer list if peer discovery is enabled
                if registry.config.enable_peer_discovery {
                    registry.gossip_peer_list().await;
                }
            }
        }
    }

    debug!("gossip timer stopped");
}

/// Handle incoming TCP connections - immediately set up bidirectional communication
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    debug!("🔌 HANDLE_CONNECTION: Starting to handle new incoming connection");

    // Check if TLS is enabled
    if let Some(tls_config) = &registry.tls_config {
        // TLS is enabled - perform TLS handshake
        info!(
            "🔐 TLS ENABLED: Performing TLS handshake with peer {}",
            peer_addr
        );

        let acceptor = tls_config.acceptor();
        match acceptor.accept(stream).await {
            Ok(tls_stream) => {
                info!("✅ TLS handshake successful with peer {}", peer_addr);
                handle_tls_connection(tls_stream, peer_addr, registry).await;
            }
            Err(e) => {
                error!(error = %e, peer = %peer_addr, "❌ TLS handshake failed - rejecting connection");
                // Connection failed, don't continue
                // This is correct - we should NOT fall back to plain TCP if TLS is enabled
            }
        }
    } else {
        // No TLS - panic for now to ensure we're using TLS
        panic!("⚠️ TLS DISABLED: Server attempted to accept plain TCP connection from {}. TLS is required!", peer_addr);

        // COMMENTED OUT: Non-TLS TCP path - migrating to TLS-only
        // // Split stream for direct TCP access
        // let (reader, writer) = stream.into_split();

        // // Get registry reference for the handler
        // let registry_weak = Some(Arc::downgrade(&registry));

        // // Start the incoming persistent connection handler immediately
        // // It will handle ALL messages including the first one
        // tokio::spawn(async move {
        //     debug!(peer = %peer_addr, "HANDLE.RS: Starting incoming direct TCP connection handler");
        //     let sender_node_id = handle_incoming_connection_direct_tcp(
        //         reader,
        //         writer,
        //         peer_addr,
        //         registry.clone(),
        //         registry_weak,
        //     )
        //     .await;
        //     debug!(peer = %peer_addr, "HANDLE.RS: Incoming direct TCP connection handler exited");
        //
        //     // Handle peer failure when connection is lost
        //     if let Some(failed_node_id) = sender_node_id {
        //         debug!(node_id = %failed_node_id, "HANDLE.RS: Triggering peer failure handling for node");
        //         if let Err(e) = registry.handle_peer_connection_failure_by_node_id(&failed_node_id).await {
        //             warn!(error = %e, node_id = %failed_node_id, "HANDLE.RS: Failed to handle peer connection failure");
        //         }
        //     } else {
        //         warn!(peer = %peer_addr, "HANDLE.RS: Cannot handle peer failure - sender node ID unknown");
        //     }
        // });
    }
}

/// Handle an incoming TLS connection
async fn handle_tls_connection(
    tls_stream: tokio_rustls::server::TlsStream<TcpStream>,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    // Split the TLS stream
    let (reader, writer) = tokio::io::split(tls_stream);

    // Get registry reference for the handler
    let registry_weak = Some(Arc::downgrade(&registry));

    // Start the incoming persistent connection handler
    tokio::spawn(async move {
        debug!(peer = %peer_addr, "HANDLE.RS: Starting incoming TLS connection handler");
        let sender_node_id = handle_incoming_connection_tls(
            reader,
            writer,
            peer_addr,
            registry.clone(),
            registry_weak,
        )
        .await;
        debug!(peer = %peer_addr, "HANDLE.RS: Incoming TLS connection handler exited");

        // Handle peer failure when connection is lost
        if let Some(failed_node_id) = sender_node_id {
            debug!(node_id = %failed_node_id, "HANDLE.RS: Triggering peer failure handling for node");
            if let Err(e) = registry
                .handle_peer_connection_failure_by_node_id(&failed_node_id)
                .await
            {
                warn!(error = %e, node_id = %failed_node_id, "HANDLE.RS: Failed to handle peer connection failure");
            }
        } else {
            warn!(peer = %peer_addr, "HANDLE.RS: Cannot handle peer failure - sender node ID unknown");
        }
    });
}

/// Handle an incoming TLS connection - processes all messages over encrypted stream
async fn handle_incoming_connection_tls<R, W>(
    mut reader: R,
    writer: W,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
    _registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) -> Option<String>
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let max_message_size = registry.config.max_message_size;

    // Initialize streaming state for this connection
    let mut streaming_state = StreamingState::new();

    // First, read the initial message to identify the sender
    let msg_result = read_message_from_tls_reader(&mut reader, max_message_size).await;

    let (sender_node_id, initial_correlation_id) = match &msg_result {
        Ok(MessageReadResult::Gossip(msg, correlation_id)) => {
            let node_id = match msg {
                RegistryMessage::DeltaGossip { delta } => delta.sender_peer_id.as_str().to_string(),
                RegistryMessage::FullSync { sender_peer_id, .. } => {
                    sender_peer_id.as_str().to_string()
                }
                RegistryMessage::FullSyncRequest { sender_peer_id, .. } => {
                    sender_peer_id.as_str().to_string()
                }
                RegistryMessage::FullSyncResponse { sender_peer_id, .. } => {
                    sender_peer_id.as_str().to_string()
                }
                RegistryMessage::DeltaGossipResponse { delta } => {
                    delta.sender_peer_id.as_str().to_string()
                }
                RegistryMessage::PeerHealthQuery { sender, .. } => sender.as_str().to_string(),
                RegistryMessage::PeerHealthReport { reporter, .. } => reporter.as_str().to_string(),
                RegistryMessage::ImmediateAck { .. } => {
                    warn!("Received ImmediateAck as first message - cannot identify sender");
                    return None;
                }
                RegistryMessage::ActorMessage { .. } => {
                    // For ActorMessage, we can't determine sender from the message
                    // But if it has a correlation_id, it's an Ask and we should handle it
                    if correlation_id.is_some() {
                        debug!("Received ActorMessage with Ask envelope as first message");
                        // We'll use a placeholder sender ID for now
                        "ask_sender".to_string()
                    } else {
                        warn!("Received ActorMessage as first message - cannot identify sender");
                        return None;
                    }
                }
                RegistryMessage::PeerListGossip { sender_addr, .. } => sender_addr.clone(),
            };
            (node_id, *correlation_id)
        }
        Ok(MessageReadResult::Actor { actor_id, .. }) => {
            // For actor messages received as the first message, we can't determine the sender
            // Use a placeholder identifier
            (format!("actor_sender_{}", actor_id), None)
        }
        Ok(MessageReadResult::Streaming { stream_header, .. }) => {
            // For streaming messages received as the first message, use the actor ID
            (format!("stream_sender_{}", stream_header.actor_id), None)
        }
        Err(e) => {
            warn!(error = %e, "Failed to read initial message from TLS stream");
            return None;
        }
    };

    debug!(peer_addr = %peer_addr, node_id = %sender_node_id, "Identified incoming TLS connection from node");

    // Update the gossip state with the NodeId for this peer
    // This is critical for bidirectional TLS connections
    {
        let node_id =
            crate::migration::migrate_peer_id_to_node_id(&crate::PeerId::new(&sender_node_id)).ok();
        if let Some(node_id) = node_id {
            registry
                .add_peer_with_node_id(peer_addr, Some(node_id))
                .await;
            debug!(peer_addr = %peer_addr, "Updated gossip state with NodeId for incoming TLS connection");
        }
    }

    // Process the initial message with correlation ID if present
    match msg_result {
        Ok(MessageReadResult::Gossip(msg, _correlation_id)) => {
            // For ActorMessage with correlation_id, ensure it's set
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
                    correlation_id: initial_correlation_id,
                }
            } else {
                msg
            };

            if let Err(e) =
                handle_incoming_message(registry.clone(), peer_addr, msg_to_handle).await
            {
                warn!(error = %e, "Failed to process initial TLS message");
            }
        }
        Ok(MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            payload,
        }) => {
            // Handle initial actor message directly
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
        Ok(MessageReadResult::Streaming {
            msg_type,
            stream_header,
            chunk_data,
        }) => {
            // Handle initial streaming message
            match msg_type {
                msg_type if msg_type == crate::MessageType::StreamStart as u8 => {
                    if let Err(e) = streaming_state.start_stream(stream_header) {
                        warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                    }
                }
                msg_type if msg_type == crate::MessageType::StreamData as u8 => {
                    // Data chunk - this should not be the first message typically, but handle it
                    if let Err(e) = streaming_state.start_stream(stream_header) {
                        debug!(error = %e, "Auto-starting stream for data chunk: stream_id={}", stream_header.stream_id);
                    }
                    if let Ok(Some(complete_data)) =
                        streaming_state.add_chunk(stream_header, chunk_data)
                    {
                        // Complete message assembled - route to actor
                        if let Some(handler) = &*registry.actor_message_handler.lock().await {
                            let actor_id_str = stream_header.actor_id.to_string();
                            let _ = handler
                                .handle_actor_message(
                                    &actor_id_str,
                                    stream_header.type_hash,
                                    &complete_data,
                                    None,
                                )
                                .await;
                        }
                    }
                }
                _ => {
                    warn!(
                        "Unexpected streaming message type as initial message: 0x{:02x}",
                        msg_type
                    );
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "Failed to read initial message - connection will be closed");
            return None;
        }
    }

    // Add the TLS writer to the connection pool for bidirectional communication
    {
        use std::pin::Pin;
        use tokio::io::AsyncWrite;

        // Box the writer as a trait object for heterogeneous storage
        let boxed_writer: Pin<Box<dyn AsyncWrite + Send>> = Box::pin(writer);

        // Create a LockFreeStreamHandle for the TLS writer
        let stream_handle = Arc::new(crate::connection_pool::LockFreeStreamHandle::new(
            boxed_writer,
            peer_addr,
            crate::connection_pool::ChannelId::Global,
            crate::connection_pool::BufferConfig::default(), // Use BufferConfig with 1MB buffer
        ));

        // Create a connection with the TLS stream handle
        let mut connection = crate::connection_pool::LockFreeConnection::new(peer_addr);
        connection.stream_handle = Some(stream_handle);
        connection.set_state(crate::connection_pool::ConnectionState::Connected);
        connection.update_last_used();

        // Add the connection to the pool by node ID
        let connection_arc = Arc::new(connection);

        // DUPLICATE CONNECTION TIE-BREAKER (Phase 4):
        // Check if we already have a connection to this peer
        let should_add = if registry.has_connection_to_node(&sender_node_id).await {
            // We have an existing connection - use tie-breaker
            // This is an INBOUND connection (is_outbound = false)
            if registry.should_keep_connection(&sender_node_id, false) {
                debug!(
                    node_id = %sender_node_id,
                    "tie-breaker: keeping new inbound connection, will close existing outbound"
                );
                true
            } else {
                debug!(
                    node_id = %sender_node_id,
                    "tie-breaker: keeping existing outbound connection, rejecting new inbound"
                );
                false
            }
        } else {
            true
        };

        if should_add {
            let pool = registry.connection_pool.lock().await;
            // Add the connection using the sender's node ID
            pool.add_connection_by_node_id(sender_node_id.clone(), peer_addr, connection_arc);

            debug!(node_id = %sender_node_id, peer_addr = %peer_addr,
                  "Added incoming TLS connection to pool for bidirectional communication");
        }
    }

    // Continue reading messages from the TLS stream
    // Note: writer has been moved to the connection pool, so we only have the reader
    loop {
        match read_message_from_tls_reader(&mut reader, max_message_size).await {
            Ok(MessageReadResult::Gossip(msg, correlation_id)) => {
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
                    handle_incoming_message(registry.clone(), peer_addr, msg_to_handle).await
                {
                    warn!(error = %e, "Failed to process TLS message");
                }
            }
            Ok(MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id,
                type_hash,
                payload,
            }) => {
                // Handle actor message directly
                // Call the actor message handler if available
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
            Ok(MessageReadResult::Streaming {
                msg_type,
                stream_header,
                chunk_data,
            }) => {
                // Handle streaming messages
                match msg_type {
                    msg_type if msg_type == crate::MessageType::StreamStart as u8 => {
                        if let Err(e) = streaming_state.start_stream(stream_header) {
                            warn!(error = %e, "Failed to start streaming for stream_id={}", stream_header.stream_id);
                        }
                    }
                    msg_type if msg_type == crate::MessageType::StreamData as u8 => {
                        if let Ok(Some(complete_data)) =
                            streaming_state.add_chunk(stream_header, chunk_data)
                        {
                            // Complete message assembled - route to actor
                            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                                let actor_id_str = stream_header.actor_id.to_string();
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        stream_header.type_hash,
                                        &complete_data,
                                        None,
                                    )
                                    .await;
                            }
                        }
                    }
                    msg_type if msg_type == crate::MessageType::StreamEnd as u8 => {
                        if let Ok(Some(complete_data)) =
                            streaming_state.finalize_stream(stream_header.stream_id)
                        {
                            // Complete message assembled - route to actor
                            if let Some(handler) = &*registry.actor_message_handler.lock().await {
                                let actor_id_str = stream_header.actor_id.to_string();
                                let _ = handler
                                    .handle_actor_message(
                                        &actor_id_str,
                                        stream_header.type_hash,
                                        &complete_data,
                                        None,
                                    )
                                    .await;
                            }
                        }
                    }
                    _ => {
                        warn!("Unknown streaming message type: 0x{:02x}", msg_type);
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, "TLS connection closed or error reading message");
                break;
            }
        }
    }

    Some(sender_node_id)
}

/// Result type for message reading that can handle gossip, actor, and streaming messages
enum MessageReadResult {
    Gossip(RegistryMessage, Option<u16>),
    Actor {
        msg_type: u8,
        correlation_id: u16,
        actor_id: u64,
        type_hash: u32,
        payload: Vec<u8>,
    },
    Streaming {
        msg_type: u8,
        stream_header: crate::StreamHeader,
        chunk_data: Vec<u8>,
    },
}

/// Read a message from a TLS reader
async fn read_message_from_tls_reader<R>(
    reader: &mut R,
    max_message_size: usize,
) -> Result<MessageReadResult>
where
    R: AsyncReadExt + Unpin,
{
    // Read the message length (4 bytes)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    if msg_len > max_message_size {
        return Err(crate::GossipError::MessageTooLarge {
            size: msg_len,
            max: max_message_size,
        });
    }

    // Read the message data - allocate with proper alignment for rkyv
    // Use Vec::with_capacity to ensure proper allocation alignment
    let mut msg_buf = Vec::with_capacity(msg_len);
    msg_buf.resize(msg_len, 0);
    reader.read_exact(&mut msg_buf).await?;

    // Check if this is an Ask message with envelope
    if msg_len >= 8 && msg_buf[0] == crate::MessageType::Ask as u8 {
        // This is an Ask message with envelope format:
        // [type:1][correlation_id:2][reserved:5][payload:N]

        // Extract correlation ID (bytes 1-2)
        let correlation_id = u16::from_be_bytes([msg_buf[1], msg_buf[2]]);

        // The actual RegistryMessage starts at byte 8
        // Create a properly aligned buffer for the payload
        let payload_len = msg_len - 8;
        let mut aligned_payload = Vec::with_capacity(payload_len);
        aligned_payload.extend_from_slice(&msg_buf[8..]);

        // Deserialize the RegistryMessage from the aligned payload
        let msg: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&aligned_payload)?;

        debug!(
            correlation_id = correlation_id,
            "Received Ask message with correlation ID"
        );

        Ok(MessageReadResult::Gossip(msg, Some(correlation_id)))
    } else {
        // Check if this is a Gossip message with type prefix
        if msg_len >= 1 {
            let first_byte = msg_buf[0];
            // Check if it's a known message type
            if let Some(msg_type) = crate::MessageType::from_byte(first_byte) {
                match msg_type {
                    crate::MessageType::Gossip => {
                        // This is a gossip message with type prefix, skip the type byte
                        if msg_buf.len() > 1 {
                            // Create a properly aligned buffer for the payload
                            let payload_len = msg_len - 1;
                            let mut aligned_payload = Vec::with_capacity(payload_len);
                            aligned_payload.extend_from_slice(&msg_buf[1..]);
                            let msg: RegistryMessage =
                                rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(
                                    &aligned_payload,
                                )?;
                            return Ok(MessageReadResult::Gossip(msg, None));
                        }
                    }
                    crate::MessageType::ActorTell | crate::MessageType::ActorAsk => {
                        // This is an actor message with envelope format:
                        // [type:1][correlation_id:2][reserved:5][actor_id:8][type_hash:4][payload_len:4][payload:N]
                        if msg_buf.len() < 24 {
                            // Need at least 24 bytes for header
                            return Err(crate::GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Actor message too small",
                            )));
                        }

                        // Parse the actor message envelope
                        let msg_type_byte = msg_buf[0];
                        let correlation_id = u16::from_be_bytes([msg_buf[1], msg_buf[2]]);
                        // Skip reserved bytes [3..8]
                        let actor_id = u64::from_be_bytes(msg_buf[8..16].try_into().unwrap());
                        let type_hash = u32::from_be_bytes(msg_buf[16..20].try_into().unwrap());
                        let payload_len =
                            u32::from_be_bytes(msg_buf[20..24].try_into().unwrap()) as usize;

                        if msg_buf.len() < 24 + payload_len {
                            return Err(crate::GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Actor message payload incomplete",
                            )));
                        }

                        let payload = msg_buf[24..24 + payload_len].to_vec();

                        return Ok(MessageReadResult::Actor {
                            msg_type: msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash,
                            payload,
                        });
                    }
                    crate::MessageType::StreamStart
                    | crate::MessageType::StreamData
                    | crate::MessageType::StreamEnd => {
                        // Handle streaming messages
                        // Message format: [type:1][correlation_id:2][reserved:5][stream_header:36][chunk_data:N]
                        if msg_buf.len() < 8 + crate::StreamHeader::SERIALIZED_SIZE {
                            return Err(crate::GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Streaming message header incomplete",
                            )));
                        }

                        // Parse the stream header (36 bytes starting at offset 8)
                        let header_bytes = &msg_buf[8..8 + crate::StreamHeader::SERIALIZED_SIZE];
                        let stream_header = crate::StreamHeader::from_bytes(header_bytes)
                            .ok_or_else(|| {
                                crate::GossipError::Network(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Invalid stream header",
                                ))
                            })?;

                        // Extract chunk data (everything after the header)
                        let chunk_data = if msg_buf.len() > 8 + crate::StreamHeader::SERIALIZED_SIZE
                        {
                            msg_buf[8 + crate::StreamHeader::SERIALIZED_SIZE..].to_vec()
                        } else {
                            Vec::new()
                        };

                        return Ok(MessageReadResult::Streaming {
                            msg_type: first_byte,
                            stream_header,
                            chunk_data,
                        });
                    }
                    _ => {
                        // This is not a recognized message type, return a custom error
                        return Err(crate::GossipError::Network(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Unknown message type: 0x{:02x}", first_byte),
                        )));
                    }
                }
            }
        }

        // Try to deserialize as regular gossip message (backwards compatibility)
        // Already have msg_buf allocated with proper alignment from above
        let msg: RegistryMessage =
            rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&msg_buf)?;
        Ok(MessageReadResult::Gossip(msg, None))
    }
}

/// Zero-copy gossip message sender - eliminates bottlenecks in serialization and connection handling
async fn send_gossip_message_zero_copy(
    mut task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> Result<()> {
    // Check if this is a retry attempt
    let is_retry = {
        let gossip_state = registry.gossip_state.lock().await;
        gossip_state
            .peers
            .get(&task.peer_addr)
            .map(|p| p.failures > 0)
            .unwrap_or(false)
    };

    if is_retry {
        info!(
            peer = %task.peer_addr,
            "🔄 GOSSIP RETRY: Attempting to reconnect to previously failed peer"
        );
    }

    // Get connection with minimal lock contention
    let conn = {
        let mut pool = registry.connection_pool.lock().await;
        debug!(
            "GOSSIP: Pool has {} connections before get_connection",
            pool.connection_count()
        );
        match pool.get_connection(task.peer_addr).await {
            Ok(conn) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        "✅ GOSSIP RETRY: Successfully reconnected to peer"
                    );
                }
                conn
            }
            Err(e) => {
                if is_retry {
                    info!(
                        peer = %task.peer_addr,
                        error = %e,
                        "❌ GOSSIP RETRY: Failed to reconnect to peer"
                    );
                }
                return Err(e);
            }
        }
    };

    // CRITICAL: Set precise timing RIGHT BEFORE TCP write to exclude all scheduling delays
    // Update wall_clock_time in delta changes to current time for accurate propagation measurement
    let _current_time_secs = crate::current_timestamp();
    let current_time_nanos = crate::current_timestamp_nanos();

    // Debug: Check if there's a delay in the task creation vs sending
    match &task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta } => {
            for change in &delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        let creation_time_nanos = location.wall_clock_time as u128 * 1_000_000_000;
                        let delay_nanos = current_time_nanos as u128 - creation_time_nanos;
                        let _delay_ms = delay_nanos as f64 / 1_000_000.0;
                        // eprintln!("🔍 DELTA_SEND_DELAY: {}ms between delta creation and sending", delay_ms);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    match &mut task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta } => {
            delta.precise_timing_nanos = current_time_nanos;
            // Update wall_clock_time in all changes to current time for accurate propagation measurement
            for change in &mut delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        // Set wall_clock_time to nanoseconds for consistent timing measurements
                        location.wall_clock_time = (current_time_nanos / 1_000_000_000) as u64;
                    }
                    crate::registry::RegistryChange::ActorRemoved { .. } => {
                        // No wall_clock_time to update
                    }
                }
            }
        }
        crate::registry::RegistryMessage::FullSync { .. } => {
            // Full sync doesn't use precise timing
        }
        _ => {}
    }

    // Serialize the message AFTER updating timing
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&task.message)?;

    // Create message with Gossip type prefix
    let mut msg_with_type = Vec::with_capacity(1 + data.len());
    msg_with_type.push(crate::MessageType::Gossip as u8);
    msg_with_type.extend_from_slice(&data);

    // Use zero-copy tell() which uses try_send() internally for max performance
    // This completely bypasses async overhead when the channel has capacity
    let tcp_start = std::time::Instant::now();
    conn.tell(msg_with_type.as_slice()).await?;
    let _tcp_elapsed = tcp_start.elapsed();
    // eprintln!("🔍 TCP_WRITE_TIME: {:?}", tcp_elapsed);
    Ok(())
}
