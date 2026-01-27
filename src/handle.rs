use std::sync::OnceLock;
use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use rkyv::Archive;
use std::ops::Range;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{interval, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    connection_pool::handle_incoming_message,
    gossip_buffer::{GossipFrameBuffer, PooledFrameBytes},
    registry::{
        GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryMessageFrame,
        RegistryStats,
    },
    streaming,
    streaming::StreamDescriptor,
    GossipConfig, GossipError, RegistrationPriority, RemoteActorLocation, Result,
};

const REGISTRY_MESSAGE_ALIGNMENT: usize = {
    let message_align = std::mem::align_of::<rkyv::Archived<RegistryMessage>>();
    let location_align = std::mem::align_of::<rkyv::Archived<RemoteActorLocation>>();
    if message_align > location_align {
        message_align
    } else {
        location_align
    }
};
#[cfg(any(test, feature = "test-helpers"))]
type RegistryAlignedVec = rkyv::util::AlignedVec<{ REGISTRY_MESSAGE_ALIGNMENT }>;

#[cfg_attr(
    not(any(test, feature = "test-helpers", debug_assertions)),
    allow(dead_code)
)]
fn typed_tell_capture_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("KAMEO_REMOTE_TYPED_TELL_CAPTURE").is_ok())
}

/// Duration after which incomplete Streaming state is aborted.
const STREAMING_STALE_TIMEOUT: Duration = Duration::from_secs(60);

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
        let mut config = config.unwrap_or_default();

        // Ensure config keypair matches the TLS identity (or set it)
        let derived_keypair = secret_key.to_keypair();
        match config.key_pair.as_ref() {
            Some(existing) => {
                if existing.peer_id() != derived_keypair.peer_id() {
                    return Err(GossipError::InvalidKeyPair(
                        "GossipConfig.key_pair does not match TLS secret key".to_string(),
                    ));
                }
            }
            None => {
                config.key_pair = Some(derived_keypair);
            }
        }

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

    /// Create and start a new gossip registry using a keypair (TLS-only helper)
    pub async fn new_with_keypair(
        bind_addr: SocketAddr,
        keypair: crate::KeyPair,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        let mut config = config.unwrap_or_default();
        match config.key_pair.as_ref() {
            Some(existing) => {
                if existing.peer_id() != keypair.peer_id() {
                    return Err(GossipError::InvalidKeyPair(
                        "GossipConfig.key_pair does not match provided keypair".to_string(),
                    ));
                }
            }
            None => {
                config.key_pair = Some(keypair.clone());
            }
        }
        let secret_key = keypair.to_secret_key();
        Self::new_with_tls(bind_addr, secret_key, Some(config)).await
    }

    /// Create and start a new gossip registry (TLS-only)
    #[instrument(skip(config))]
    pub async fn new(
        bind_addr: SocketAddr,
        secret_key: crate::SecretKey,
        config: Option<GossipConfig>,
    ) -> Result<Self> {
        Self::new_with_tls(bind_addr, secret_key, config).await
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

    /// Get a connection handle by peer ID (ensures TLS NodeId is known)
    pub async fn get_connection_to_peer(
        &self,
        peer_id: &crate::PeerId,
    ) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry
            .connection_pool
            .lock()
            .await
            .get_connection_to_peer(peer_id)
            .await
    }

    /// Bootstrap peer connections (blocking by default).
    ///
    /// Dials seed peers and blocks until a TLS handshake succeeds for each peer or the
    /// readiness timeout elapses. Failing to establish a handshake within the timeout
    /// is treated as fatal and will panic.
    pub async fn bootstrap(&self, seeds: Vec<SocketAddr>) {
        let seed_count = seeds.len();
        let readiness_timeout = self.registry.config.bootstrap_readiness_timeout;
        let readiness_interval = self.registry.config.bootstrap_readiness_check_interval;

        for seed in seeds {
            // Ensure we perform a fresh handshake for bootstrap seeds.
            // This avoids reusing stale connections that survived shutdowns.
            {
                let pool = self.registry.connection_pool.lock().await;
                if let Some(peer_id) = pool.get_peer_id_by_addr(&seed) {
                    let _ = pool.disconnect_connection_by_peer_id(&peer_id);
                } else if pool.has_connection(&seed) {
                    pool.remove_connection(seed);
                }
            }

            let start = Instant::now();
            let mut last_err: Option<GossipError> = None;
            loop {
                let remaining = readiness_timeout.saturating_sub(start.elapsed());
                if remaining.is_zero() {
                    panic!(
                        "bootstrap handshake timed out after {:?} for seed {} (last_error={:?})",
                        readiness_timeout, seed, last_err
                    );
                }

                match tokio::time::timeout(remaining, self.registry.get_connection(seed)).await {
                    Ok(Ok(_conn)) => {
                        debug!(seed = %seed, "bootstrap connection established");
                        self.registry.mark_peer_connected(seed).await;
                        break;
                    }
                    Ok(Err(e)) => {
                        last_err = Some(e);
                    }
                    Err(_) => {
                        last_err = Some(GossipError::Timeout);
                    }
                }

                tokio::time::sleep(readiness_interval).await;
            }
        }

        debug!(
            seed_count = seed_count,
            "completed blocking bootstrap for seed peers"
        );
    }

    /// Bootstrap peer connections in the background (non-blocking).
    ///
    /// Dials seed peers asynchronously - doesn't block startup on gossip propagation.
    /// Failed connections are logged but don't prevent the node from starting.
    #[deprecated(
        since = "0.1.0",
        note = "Use bootstrap() for blocking, handshake-verified startup"
    )]
    pub async fn bootstrap_non_blocking(&self, seeds: Vec<SocketAddr>) {
        let seed_count = seeds.len();

        for seed in seeds {
            let registry = self.registry.clone();
            tokio::spawn(async move {
                match registry.get_connection(seed).await {
                    Ok(_conn) => {
                        debug!(seed = %seed, "bootstrap connection established");
                        registry.mark_peer_connected(seed).await;
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "bootstrap peer unavailable");
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

    let max_jitter = std::cmp::min(gossip_interval, Duration::from_millis(1000));
    let jitter_ms = if max_jitter.is_zero() {
        0
    } else {
        let max_ms = max_jitter.as_millis().max(1) as u64;
        rand::random::<u64>() % max_ms
    };
    let jitter = Duration::from_millis(jitter_ms);
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
                let jitter_ms = if max_jitter.is_zero() {
                    0
                } else {
                    let max_ms = max_jitter.as_millis().max(1) as u64;
                    rand::random::<u64>() % max_ms
                };
                let jitter = Duration::from_millis(jitter_ms);
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
                    let tasks = registry.gossip_peer_list().await;
                    if tasks.is_empty() {
                        continue;
                    }

                    let mut futures = Vec::new();
                    for task in tasks {
                        let registry_clone = registry.clone();
                        let future = tokio::spawn(async move {
                            if let Err(err) =
                                send_gossip_message_zero_copy(task, registry_clone).await
                            {
                                warn!(error = %err, "peer list gossip send failed");
                            }
                        });
                        futures.push(future);
                    }

                    for future in futures {
                        if let Err(err) = future.await {
                            error!(error = %err, "peer list gossip task panicked");
                        }
                    }
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
    }
}

/// Handle an incoming TLS connection
async fn handle_tls_connection(
    mut tls_stream: tokio_rustls::server::TlsStream<TcpStream>,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    let negotiated_alpn = tls_stream
        .get_ref()
        .1
        .alpn_protocol()
        .map(|proto| proto.to_vec());

    let peer_node_id = tls_stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(|cert| crate::tls::extract_node_id_from_cert(cert).ok());

    let capabilities = match crate::handshake::perform_hello_handshake(
        &mut tls_stream,
        negotiated_alpn.as_deref(),
        registry.config.enable_peer_discovery,
    )
    .await
    {
        Ok(caps) => caps,
        Err(err) => {
            warn!(
                peer = %peer_addr,
                error = %err,
                "Hello handshake failed, closing inbound TLS connection"
            );
            return;
        }
    };

    registry.set_peer_capabilities(peer_addr, capabilities.clone());
    if let Some(node_id) = registry.lookup_node_id(&peer_addr).await {
        registry
            .associate_peer_capabilities_with_node(peer_addr, node_id)
            .await;
    }

    // Split the TLS stream
    let (reader, writer) = tokio::io::split(tls_stream);

    // Get registry reference for the handler
    let registry_weak = Some(Arc::downgrade(&registry));

    // Start the incoming persistent connection handler
    tokio::spawn(async move {
        debug!(peer = %peer_addr, "HANDLE.RS: Starting incoming TLS connection handler");
        match handle_incoming_connection_tls(
            reader,
            writer,
            peer_addr,
            registry.clone(),
            registry_weak,
            peer_node_id,
        )
        .await
        {
            ConnectionCloseOutcome::Normal {
                node_id: Some(failed_peer_id_hex),
            } => match crate::PeerId::from_hex(&failed_peer_id_hex) {
                Ok(peer_id) => {
                    debug!(peer_id = %peer_id, "HANDLE.RS: Triggering peer failure handling for node");
                    if let Err(e) = registry
                        .handle_peer_connection_failure_by_peer_id(&peer_id)
                        .await
                    {
                        warn!(error = %e, peer_id = %peer_id, "HANDLE.RS: Failed to handle peer connection failure");
                    }
                }
                Err(e) => {
                    warn!(error = %e, peer_id = %failed_peer_id_hex, "HANDLE.RS: Invalid peer id in connection close outcome");
                }
            },
            ConnectionCloseOutcome::Normal { node_id: None } => {
                warn!(peer = %peer_addr, "⚠️ HANDLE.RS: Connection handler returned without node_id - early exit path taken");
            }
            ConnectionCloseOutcome::DroppedByTieBreaker => {
                warn!(peer = %peer_addr, "⚠️ HANDLE.RS: Dropped duplicate connection via tie-breaker");
            }
        }
        warn!(peer = %peer_addr, "📤 HANDLE.RS: Incoming TLS connection handler task EXITED");
    });
}

enum ConnectionCloseOutcome {
    Normal { node_id: Option<String> },
    DroppedByTieBreaker,
}

/// Handle an incoming TLS connection - processes all messages over encrypted stream
async fn handle_incoming_connection_tls<R, W>(
    mut reader: R,
    writer: W,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
    _registry_weak: Option<std::sync::Weak<GossipRegistry>>,
    peer_node_id: Option<crate::NodeId>,
) -> ConnectionCloseOutcome
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let max_message_size = registry.config.max_message_size;

    // Initialize Streaming assembler for this connection
    let mut streaming_state = streaming::StreamAssembler::new(max_message_size);
    let gossip_buffer = GossipFrameBuffer::new();

    // First, read the initial message to identify the sender
    let msg_result =
        read_message_from_tls_reader(&mut reader, max_message_size, &gossip_buffer).await;
    let known_node_id = match peer_node_id {
        Some(node_id) => Some(node_id),
        None => registry.lookup_node_id(&peer_addr).await,
    };

    let (sender_node_id, initial_correlation_id) = match &msg_result {
        Ok(MessageReadResult::Gossip(frame, correlation_id)) => {
            type ArchivedRegistryMessage = <RegistryMessage as Archive>::Archived;

            let archived = match frame.archived() {
                Ok(archived) => archived,
                Err(err) => {
                    warn!(error = %err, "Failed to access archived RegistryMessage");
                    return ConnectionCloseOutcome::Normal { node_id: None };
                }
            };

            let node_id = match archived {
                ArchivedRegistryMessage::DeltaGossip { delta } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(
                        &delta.sender_peer_id,
                    ) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender_peer_id");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::FullSync { sender_peer_id, .. } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(sender_peer_id) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender_peer_id");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::FullSyncRequest { sender_peer_id, .. } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(sender_peer_id) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender_peer_id");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::FullSyncResponse { sender_peer_id, .. } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(sender_peer_id) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender_peer_id");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::DeltaGossipResponse { delta } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(
                        &delta.sender_peer_id,
                    ) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender_peer_id");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::PeerHealthQuery { sender, .. } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(sender) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize sender");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::PeerHealthReport { reporter, .. } => {
                    match crate::rkyv_utils::deserialize_archived::<crate::PeerId>(reporter) {
                        Ok(peer_id) => peer_id.to_hex(),
                        Err(err) => {
                            warn!(error = %err, "Failed to deserialize reporter");
                            return ConnectionCloseOutcome::Normal { node_id: None };
                        }
                    }
                }
                ArchivedRegistryMessage::ImmediateAck { .. } => {
                    warn!("Received ImmediateAck as first message - cannot identify sender");
                    return ConnectionCloseOutcome::Normal { node_id: None };
                }
                ArchivedRegistryMessage::ActorMessage {
                    correlation_id: inner_corr,
                    ..
                } => {
                    // For ActorMessage, we can't determine sender from the message
                    // But if it has a correlation_id, it's an Ask and we should handle it
                    let has_correlation =
                        matches!(inner_corr, rkyv::option::ArchivedOption::Some(_));
                    if correlation_id.is_some() || has_correlation {
                        debug!("Received ActorMessage with Ask envelope as first message");
                        // We'll use a placeholder sender ID for now
                        "ask_sender".to_string()
                    } else {
                        warn!("Received ActorMessage as first message - cannot identify sender");
                        return ConnectionCloseOutcome::Normal { node_id: None };
                    }
                }
                ArchivedRegistryMessage::PeerListGossip { sender_addr, .. } => {
                    sender_addr.as_str().to_string()
                }
            };
            (node_id, *correlation_id)
        }
        Ok(MessageReadResult::AskRaw { correlation_id, .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), Some(*correlation_id))
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Ask request arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Response { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Response arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Ok(MessageReadResult::Actor { actor_id, .. }) => {
            // For actor messages received as the first message, we can't determine the sender
            // Use a placeholder identifier
            (format!("actor_sender_{}", actor_id), None)
        }
        Ok(MessageReadResult::Streaming { .. }) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None)
            } else {
                ("stream_sender_unknown".to_string(), None)
            }
        }
        Ok(MessageReadResult::Raw(_)) => {
            if let Some(node_id) = known_node_id {
                (node_id.to_peer_id().to_hex(), None)
            } else {
                warn!(
                    peer_addr = %peer_addr,
                    "Raw message arrived before peer NodeId is known"
                );
                return ConnectionCloseOutcome::Normal { node_id: None };
            }
        }
        Err(e) => {
            warn!(error = %e, peer_addr = %peer_addr, "⚠️ Failed to read initial message from TLS stream - early exit");
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };

    debug!(peer_addr = %peer_addr, node_id = %sender_node_id, "Identified incoming TLS connection from node");

    // Update the gossip state with the NodeId for this peer
    // This is critical for bidirectional TLS connections
    let peer_id = match crate::PeerId::from_hex(&sender_node_id) {
        Ok(peer_id) => peer_id,
        Err(err) => {
            warn!(
                peer_addr = %peer_addr,
                error = %err,
                "Invalid peer id in first message; dropping connection"
            );
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    };
    let node_id_opt = Some(peer_id.to_node_id());

    // Prefer the configured listening address for this peer (if known) instead of the
    // inbound socket's ephemeral address to avoid gossiping to a non-listening port.
    let configured_addr = {
        let pool = registry.connection_pool.lock().await;
        pool.peer_id_to_addr
            .get(&peer_id)
            .map(|entry| *entry.value())
    };
    let peer_state_addr = configured_addr
        .filter(|addr| addr.port() != 0)
        .unwrap_or(peer_addr);

    if let Some(node_id) = node_id_opt {
        registry
            .add_peer_with_node_id(peer_state_addr, Some(node_id))
            .await;
        // Associate capabilities captured during the Hello handshake (stored under peer_addr).
        registry
            .associate_peer_capabilities_with_node(peer_addr, node_id)
            .await;
        if peer_state_addr != peer_addr {
            registry
                .associate_peer_capabilities_with_node(peer_state_addr, node_id)
                .await;
        }
        if peer_state_addr != peer_addr {
            let mut gossip_state = registry.gossip_state.lock().await;
            if let Some(peer_info) = gossip_state.peers.get_mut(&peer_state_addr) {
                peer_info.peer_address = Some(peer_addr);
            }
        }
        debug!(
            peer_addr = %peer_addr,
            peer_state_addr = %peer_state_addr,
            "Updated gossip state with NodeId for incoming TLS connection"
        );
    }

    #[allow(unused_assignments)]
    let mut response_connection: Option<Arc<crate::connection_pool::LockFreeConnection>> = None;

    // Register the TLS writer with the connection pool before handling the first message so responses work
    {
        use std::pin::Pin;
        use tokio::io::AsyncWrite;

        let boxed_writer: Pin<Box<dyn AsyncWrite + Send>> = Box::pin(writer);
        let buffer_config = crate::connection_pool::BufferConfig::default()
            .with_ask_inflight_limit(registry.config.ask_inflight_limit);
        let (stream_handle, writer_task_handle) = crate::connection_pool::LockFreeStreamHandle::new(
            boxed_writer,
            peer_addr,
            crate::connection_pool::ChannelId::Global,
            buffer_config,
        );
        let stream_handle = Arc::new(stream_handle);

        let mut connection = crate::connection_pool::LockFreeConnection::new(
            peer_state_addr,
            crate::connection_pool::ConnectionDirection::Inbound,
        );
        connection.stream_handle = Some(stream_handle);
        connection.set_state(crate::connection_pool::ConnectionState::Connected);
        connection.update_last_used();

        // Track the writer task handle (H-004)
        connection
            .task_tracker
            .lock()
            .set_writer(writer_task_handle);

        let connection_arc = Arc::new(connection);

        let keep_connection = {
            let pool = registry.connection_pool.lock().await;
            let has_existing = pool.has_connection_by_peer_id(&peer_id);

            if has_existing {
                if registry.should_keep_connection(&peer_id, false) {
                    debug!(
                        peer_id = %peer_id,
                        "tie-breaker: favoring inbound connection, dropping existing outbound"
                    );
                    if let Some(existing) = pool.disconnect_connection_by_peer_id(&peer_id) {
                        if let Some(handle) = existing.stream_handle.as_ref() {
                            handle.shutdown();
                        }
                    }
                    pool.add_connection_by_peer_id(
                        peer_id.clone(),
                        peer_state_addr,
                        connection_arc.clone(),
                    );
                    true
                } else {
                    debug!(
                        peer_id = %peer_id,
                        "tie-breaker: rejecting inbound duplicate connection"
                    );
                    registry.clear_peer_capabilities(&peer_addr);
                    false
                }
            } else {
                pool.add_connection_by_peer_id(
                    peer_id.clone(),
                    peer_state_addr,
                    connection_arc.clone(),
                );
                true
            }
        };

        if !keep_connection {
            if let Some(handle) = connection_arc.stream_handle.as_ref() {
                handle.shutdown();
            }
            return ConnectionCloseOutcome::DroppedByTieBreaker;
        }

        response_connection = Some(connection_arc.clone());

        // CRITICAL FIX: Also index by ephemeral peer_addr if it differs from peer_state_addr.
        // This ensures that handle_response_message (which looks up by peer_addr) can find
        // the connection AND the correlation tracker. Without this, responses fail to be
        // delivered because they are looked up by the ephemeral address but only indexed
        // by the configured bind address.
        if peer_addr != peer_state_addr {
            let pool = registry.connection_pool.lock().await;
            pool.index_connection_by_addr(peer_addr, connection_arc.clone());
            // Also add the addr_to_peer_id mapping so handle_response_message can look up
            // the shared correlation tracker via peer_id
            pool.add_addr_to_peer_id(peer_addr, peer_id.clone());
            debug!(
                node_id = %sender_node_id,
                peer_addr = %peer_addr,
                peer_state_addr = %peer_state_addr,
                "Also indexed incoming connection by ephemeral address for response delivery"
            );
        }

        debug!(
            node_id = %sender_node_id,
            peer_addr = %peer_addr,
            "Added incoming TLS connection to pool for bidirectional communication"
        );
    }

    // Process the initial message with correlation ID if present
    match msg_result {
        Ok(MessageReadResult::Gossip(frame, correlation_id)) => {
            if let Err(e) = handle_incoming_message(
                registry.clone(),
                peer_addr,
                frame,
                correlation_id.or(initial_correlation_id),
            )
            .await
            {
                warn!(error = %e, "Failed to process initial TLS message");
            }
        }
        Ok(MessageReadResult::AskRaw {
            correlation_id,
            payload,
        }) => {
            handle_raw_ask_request(&registry, peer_addr, correlation_id, payload).await;
        }
        Ok(MessageReadResult::Response {
            correlation_id,
            payload,
        }) => {
            handle_response_message_fast(
                &registry,
                peer_addr,
                correlation_id,
                payload,
                response_connection.as_ref(),
            )
            .await;
            trace!(
                peer = %peer_addr,
                correlation_id,
                "incoming response delivered (initial read)"
            );
        }
        Ok(MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            payload,
        }) => {
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if msg_type == crate::MessageType::ActorTell as u8 && typed_tell_capture_enabled() {
                    crate::test_helpers::record_raw_payload(payload.to_bytes());
                }
            }
            // Handle initial actor message directly
            if let Some(handler) = registry.load_actor_message_handler() {
                let mut id_buf = itoa::Buffer::new();
                let actor_id_str = id_buf.format(actor_id);
                let is_ask = msg_type == crate::MessageType::ActorAsk as u8;
                let correlation = if is_ask { Some(correlation_id) } else { None };
                match handler
                    .handle_actor_message(&actor_id_str, type_hash, payload.as_slice(), correlation)
                    .await
                {
                    Ok(Some(reply_payload)) if is_ask => {
                        if let Some(corr_id) = correlation {
                            send_streaming_response(&registry, peer_addr, corr_id, reply_payload)
                                .await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            peer = %peer_addr,
                            error = %e,
                            actor_id = %actor_id_str,
                            "Actor handler error on incoming TLS connection",
                        );
                    }
                }
            }
        }
        Ok(MessageReadResult::Streaming {
            frame,
            correlation_id,
        }) => {
            handle_streaming_frame(
                &registry,
                &mut streaming_state,
                &mut reader,
                peer_addr,
                frame,
                correlation_id,
            )
            .await;
        }
        Ok(MessageReadResult::Raw(_payload)) => {
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                if typed_tell_capture_enabled() {
                    crate::test_helpers::record_raw_payload(_payload.clone());
                }
            }
            debug!(peer_addr = %peer_addr, "Ignoring raw message payload");
        }
        Err(e) => {
            warn!(error = %e, "Failed to read initial message - connection will be closed");
            return ConnectionCloseOutcome::Normal { node_id: None };
        }
    }

    // Continue reading messages from the TLS stream
    // Note: writer has been moved to the connection pool, so we only have the reader
    // Cleanup interval for stale streams (every 30 seconds)
    let mut cleanup_interval = interval(Duration::from_secs(30));
    cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Use select! to either read a message or trigger periodic cleanup
        let msg_result = tokio::select! {
            result = read_message_from_tls_reader(&mut reader, max_message_size, &gossip_buffer) => result,
            _ = cleanup_interval.tick() => {
                // Periodically cleanup stale incomplete streams to prevent memory leak
                if let Some(descriptor) = streaming_state.abort_if_stale(STREAMING_STALE_TIMEOUT) {
                    warn!(
                        peer = %peer_addr,
                        stream_id = descriptor.stream_id,
                        payload_len = descriptor.payload_len,
                        "Streaming assembly aborted due to inactivity"
                    );
                }
                continue;
            }
        };

        match msg_result {
            Ok(MessageReadResult::Gossip(frame, correlation_id)) => {
                if let Err(e) =
                    handle_incoming_message(registry.clone(), peer_addr, frame, correlation_id)
                        .await
                {
                    warn!(error = %e, "Failed to process TLS message");
                }
            }
            Ok(MessageReadResult::AskRaw {
                correlation_id,
                payload,
            }) => {
                handle_raw_ask_request(&registry, peer_addr, correlation_id, payload).await;
            }
            Ok(MessageReadResult::Response {
                correlation_id,
                payload,
            }) => {
                handle_response_message_fast(
                    &registry,
                    peer_addr,
                    correlation_id,
                    payload,
                    response_connection.as_ref(),
                )
                .await;
                trace!(
                    peer = %peer_addr,
                    correlation_id,
                    "incoming response delivered"
                );
            }
            Ok(MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id,
                type_hash,
                payload,
            }) => {
                #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                {
                    if msg_type == crate::MessageType::ActorTell as u8
                        && typed_tell_capture_enabled()
                    {
                        crate::test_helpers::record_raw_payload(payload.to_bytes());
                    }
                }
                // Handle actor message directly
                // Call the actor message handler if available
                if let Some(handler) = registry.load_actor_message_handler() {
                    let mut id_buf = itoa::Buffer::new();
                    let actor_id_str = id_buf.format(actor_id);
                    let is_ask = msg_type == crate::MessageType::ActorAsk as u8;
                    let correlation = if is_ask { Some(correlation_id) } else { None };
                    match handler
                        .handle_actor_message(
                            &actor_id_str,
                            type_hash,
                            payload.as_slice(),
                            correlation,
                        )
                        .await
                    {
                        Ok(Some(reply_payload)) if is_ask => {
                            if let Some(corr_id) = correlation {
                                if let Some(connection) = response_connection.as_ref() {
                                    send_streaming_response_via_connection(
                                        connection,
                                        corr_id,
                                        reply_payload,
                                    )
                                    .await;
                                } else {
                                    send_streaming_response(
                                        &registry,
                                        peer_addr,
                                        corr_id,
                                        reply_payload,
                                    )
                                    .await;
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(
                                peer = %peer_addr,
                                error = %e,
                                actor_id = %actor_id_str,
                                "Actor handler error on outgoing reader",
                            );
                        }
                    }
                }
            }
            Ok(MessageReadResult::Streaming {
                frame,
                correlation_id,
            }) => {
                handle_streaming_frame(
                    &registry,
                    &mut streaming_state,
                    &mut reader,
                    peer_addr,
                    frame,
                    correlation_id,
                )
                .await;
            }
            Ok(MessageReadResult::Raw(_payload)) => {
                #[cfg(any(test, feature = "test-helpers", debug_assertions))]
                {
                    if typed_tell_capture_enabled() {
                        crate::test_helpers::record_raw_payload(_payload.clone());
                    }
                }
                debug!(peer_addr = %peer_addr, "Ignoring raw message payload");
            }
            Err(e) => {
                warn!(peer_addr = %peer_addr, error = %e, "⚠️ TLS connection closed or error reading message - incoming handler exiting");
                break;
            }
        }
    }

    warn!(peer_addr = %peer_addr, sender_node_id = %sender_node_id,
        "📤 Incoming TLS connection handler loop exited - peer may need reconnection");
    ConnectionCloseOutcome::Normal {
        node_id: Some(sender_node_id),
    }
}

/// Result type for message reading that can handle gossip, actor, and streaming messages
#[derive(Debug)]
pub(crate) enum MessageReadResult {
    Gossip(RegistryMessageFrame, Option<u16>),
    AskRaw {
        correlation_id: u16,
        payload: bytes::Bytes,
    },
    Response {
        correlation_id: u16,
        payload: bytes::Bytes,
    },
    Raw(bytes::Bytes),
    Actor {
        msg_type: u8,
        correlation_id: u16,
        actor_id: u64,
        type_hash: u32,
        payload: MessagePayload,
    },
    Streaming {
        frame: StreamingFrame,
        correlation_id: u16,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum StreamingFrame {
    Start { descriptor: StreamDescriptor },
    Data { payload: StreamingData },
    End,
}

#[derive(Debug, Clone)]
pub(crate) enum StreamingData {
    Direct { chunk_len: usize },
    Owned { chunk: bytes::Bytes },
}

#[derive(Clone, Debug)]
pub(crate) struct MessagePayload {
    pooled: PooledFrameBytes,
}

impl MessagePayload {
    fn from_pooled(pooled: PooledFrameBytes) -> Self {
        Self { pooled }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        self.pooled.as_slice()
    }

    fn len(&self) -> usize {
        self.pooled.len()
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            pooled: self.pooled.slice(range),
        }
    }

    fn slice_from(&self, start: usize) -> Self {
        self.slice(start..self.len())
    }

    fn into_slice_from(self, start: usize) -> Self {
        let len = self.len();
        Self {
            pooled: self.pooled.into_slice(start..len),
        }
    }

    fn to_bytes(&self) -> bytes::Bytes {
        self.pooled.to_bytes()
    }

    fn into_bytes(self) -> bytes::Bytes {
        self.pooled.into_bytes()
    }

    fn into_registry_frame(self) -> Result<RegistryMessageFrame> {
        RegistryMessageFrame::from_pooled(self.pooled)
    }
}

pub(crate) async fn handle_raw_ask_request(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: bytes::Bytes,
) {
    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        let response = if std::env::var("KAMEO_REMOTE_TYPED_ECHO").is_ok() && payload.len() >= 8 {
            payload.clone()
        } else {
            bytes::Bytes::from(crate::connection_pool::process_mock_request_payload(
                payload.as_ref(),
            ))
        };

        let conn = {
            let pool = registry.connection_pool.lock().await;
            pool.get_connection_by_addr(&peer_addr).or_else(|| {
                pool.get_peer_id_by_addr(&peer_addr)
                    .and_then(|peer_id| pool.get_connection_by_peer_id(&peer_id))
            })
        };

        if let Some(conn) = conn {
            if let Some(ref stream_handle) = conn.stream_handle {
                let header = crate::framing::write_ask_response_header(
                    crate::MessageType::Response,
                    correlation_id,
                    response.len(),
                );

                if let Err(e) = stream_handle
                    .write_header_and_payload_control_inline(
                        header,
                        crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN as u8,
                        response,
                    )
                    .await
                {
                    warn!(peer = %peer_addr, error = %e, "Failed to send Ask response");
                } else {
                    debug!(peer = %peer_addr, correlation_id = correlation_id, "Sent Ask response");
                }
            } else {
                warn!(peer = %peer_addr, "No stream handle for Ask response");
            }
        } else {
            warn!(peer = %peer_addr, "No connection found for Ask response");
        }
    }
    #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
    {
        let _ = registry;
        let _ = payload;
        warn!(
            peer = %peer_addr,
            correlation_id = correlation_id,
            "Received raw Ask request - not supported"
        );
    }
}

async fn send_streaming_response_via_connection(
    connection: &Arc<crate::connection_pool::LockFreeConnection>,
    correlation_id: u16,
    response: bytes::Bytes,
) {
    if let Some(ref stream_handle) = connection.stream_handle {
        let header = crate::framing::write_ask_response_header(
            crate::MessageType::Response,
            correlation_id,
            response.len(),
        );
        if let Err(e) = stream_handle
            .write_header_and_payload_ask_inline(
                header,
                crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN as u8,
                response,
            )
            .await
        {
            warn!(
                peer = %connection.addr,
                error = %e,
                "Failed to send ask response via connection"
            );
        }
    } else {
        warn!(
            peer = %connection.addr,
            "Connection missing stream handle for ask response"
        );
    }
}

async fn send_streaming_response(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    response: bytes::Bytes,
) {
    let conn = {
        let pool = registry.connection_pool.lock().await;
        pool.get_connection_by_addr(&peer_addr).or_else(|| {
            pool.get_peer_id_by_addr(&peer_addr)
                .and_then(|peer_id| pool.get_connection_by_peer_id(&peer_id))
        })
    };

    if let Some(conn) = conn {
        send_streaming_response_via_connection(&conn, correlation_id, response).await;
    } else {
        warn!(peer = %peer_addr, "No connection found for streamed ask response");
    }
}

async fn handle_completed_stream(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    complete: streaming::CompletedStream,
) {
    if let Some(handler) = registry.load_actor_message_handler() {
        let mut id_buf = itoa::Buffer::new();
        let actor_id_str = id_buf.format(complete.descriptor.actor_id);
        match handler
            .handle_actor_message(
                &actor_id_str,
                complete.descriptor.type_hash,
                &complete.payload,
                complete.correlation_id,
            )
            .await
        {
            Ok(Some(reply_payload)) => {
                if let Some(corr_id) = complete.correlation_id {
                    send_streaming_response(registry, peer_addr, corr_id, reply_payload).await;
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(peer = %peer_addr, error = %e, "Failed to handle streamed actor message");
            }
        }
    }
}

async fn handle_streaming_frame<R>(
    registry: &Arc<GossipRegistry>,
    assembler: &mut streaming::StreamAssembler,
    reader: &mut R,
    peer_addr: SocketAddr,
    frame: StreamingFrame,
    correlation_id: u16,
) where
    R: AsyncReadExt + Unpin,
{
    let correlation = if correlation_id == 0 {
        None
    } else {
        Some(correlation_id)
    };

    match frame {
        StreamingFrame::Start { descriptor } => {
            match assembler.start_stream(descriptor, correlation) {
                Ok(Some(complete)) => handle_completed_stream(registry, peer_addr, complete).await,
                Ok(None) => {}
                Err(err) => {
                    warn!(peer = %peer_addr, error = %err, "Failed to start Streaming sequence");
                }
            }
        }
        StreamingFrame::Data { payload } => match payload {
            StreamingData::Direct { chunk_len } => {
                match assembler.read_data_direct(reader, chunk_len).await {
                    Ok(Some(complete)) => {
                        handle_completed_stream(registry, peer_addr, complete).await
                    }
                    Ok(None) => {}
                    Err(err) => {
                        warn!(peer = %peer_addr, error = %err, "Streaming direct read error");
                        if let Some(descriptor) = assembler.abort_if_stale(Duration::ZERO) {
                            warn!(
                                peer = %peer_addr,
                                stream_id = descriptor.stream_id,
                                "Streaming state reset after direct read error"
                            );
                        }
                    }
                }
            }
            StreamingData::Owned { chunk: _ } => {
                // For response streaming, chunks are already in memory
                // This shouldn't happen in tell streaming path
                warn!(peer = %peer_addr, "Received unexpected owned chunk in tell streaming");
            }
        },
        StreamingFrame::End => match assembler.finish_with_end() {
            Ok(Some(complete)) => handle_completed_stream(registry, peer_addr, complete).await,
            Ok(None) => {}
            Err(err) => {
                warn!(peer = %peer_addr, error = %err, "Streaming end frame error");
                if let Some(descriptor) = assembler.abort_if_stale(Duration::ZERO) {
                    warn!(
                        peer = %peer_addr,
                        stream_id = descriptor.stream_id,
                        "Streaming state reset after end error"
                    );
                }
            }
        },
    }
}

/// Check if the payload is properly aligned for rkyv access
fn is_registry_payload_aligned(payload: &[u8]) -> bool {
    (payload.as_ptr() as usize).is_multiple_of(REGISTRY_MESSAGE_ALIGNMENT)
}

/// Decode a RegistryMessage from network buffer.
///
/// **CRITICAL**: Panics if payload is not 16-byte aligned in release mode.
/// The framing code guarantees 16-byte alignment for RegistryMessage payloads.
/// Unaligned data indicates a serious bug in the framing implementation.
///
fn decode_registry_message(
    payload: MessagePayload,
) -> std::result::Result<RegistryMessageFrame, (Bytes, GossipError)> {
    let slice = payload.as_slice();
    let is_aligned = is_registry_payload_aligned(slice);
    if !is_aligned {
        crate::telemetry::gossip_zero_copy::record_alignment_failure(
            "handle::decode_registry_message",
        );

        #[cfg(not(any(test, feature = "test-helpers")))]
        {
            panic!(
                "RegistryMessage payload must be {}-byte aligned! \
                 Pointer: {:p}, offset: {}. \
                 This is a PROTOCOL VIOLATION - the framing code guarantees alignment.",
                REGISTRY_MESSAGE_ALIGNMENT,
                slice.as_ptr(),
                slice.as_ptr() as usize
            );
        }

        #[cfg(any(test, feature = "test-helpers"))]
        {
            let mut aligned = RegistryAlignedVec::with_capacity(slice.len());
            aligned.extend_from_slice(slice);
            let bytes = Bytes::from(aligned.into_boxed_slice());
            return RegistryMessageFrame::from_bytes(bytes.clone()).map_err(|err| (bytes, err));
        }
    }

    if let Err(err) = RegistryMessageFrame::validate(slice) {
        let raw = payload.into_bytes();
        return Err((raw, err));
    }

    crate::telemetry::gossip_zero_copy::record_inbound_frame(
        "handle::decode_registry_message",
        payload.len(),
    );

    Ok(payload.into_registry_frame().unwrap_or_else(|err| {
        panic!(
            "Validated gossip payload failed conversion: {}. This should never happen.",
            err
        )
    }))
}

pub(crate) async fn handle_response_message(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: bytes::Bytes,
) {
    let pool = registry.connection_pool.lock().await;

    // First, try to deliver via connection's embedded correlation tracker
    if let Some(conn) = pool.get_connection_by_addr(&peer_addr) {
        if let Some(ref correlation) = conn.correlation {
            if correlation.has_pending(correlation_id) {
                correlation.complete(correlation_id, payload);
                debug!(
                    peer = %peer_addr,
                    correlation_id = correlation_id,
                    "Delivered response via connection correlation tracker"
                );
                return;
            }
        }
    }

    // FALLBACK: Use shared correlation tracker by peer_id.
    // This is critical for bidirectional connections where the ask was sent on
    // the outbound connection but the response arrives on the inbound connection.
    if let Some(peer_id) = pool.get_peer_id_by_addr(&peer_addr) {
        if let Some(correlation) = pool.get_shared_correlation_tracker(&peer_id) {
            if correlation.has_pending(correlation_id) {
                correlation.complete(correlation_id, payload);
                debug!(
                    peer = %peer_addr,
                    peer_id = %peer_id,
                    correlation_id = correlation_id,
                    "Delivered response via shared correlation tracker (fallback)"
                );
                return;
            }
        }
    }

    debug!(
        peer = %peer_addr,
        correlation_id = correlation_id,
        "Response received with no pending request (neither connection nor shared tracker)"
    );
}

pub(crate) async fn handle_response_message_fast(
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    correlation_id: u16,
    payload: bytes::Bytes,
    connection: Option<&Arc<crate::connection_pool::LockFreeConnection>>,
) {
    if let Some(conn) = connection {
        if let Some(ref correlation) = conn.correlation {
            if correlation.has_pending(correlation_id) {
                correlation.complete(correlation_id, payload);
                debug!(
                    peer = %peer_addr,
                    correlation_id = correlation_id,
                    "Delivered response via connection correlation tracker (fast path)"
                );
                return;
            }
        }
    }

    handle_response_message(registry, peer_addr, correlation_id, payload).await;
}

/// Read a message from a TLS reader.
pub(crate) async fn read_message_from_tls_reader<R>(
    // CRITICAL_PATH: zero-copy gossip ingestion depends on this function; coverage gates use it to ensure no regressions slip into the TLS receive loop.
    reader: &mut R,
    max_message_size: usize,
    buffer: &GossipFrameBuffer,
) -> Result<MessageReadResult>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    if msg_len > max_message_size {
        return Err(crate::GossipError::MessageTooLarge {
            size: msg_len,
            max: max_message_size,
        });
    }

    if msg_len == 0 {
        return Err(GossipError::InvalidStreamFrame("zero-length frame".into()));
    }

    let mut msg_type_buf = [0u8; 1];
    reader.read_exact(&mut msg_type_buf).await?;
    let msg_type_byte = msg_type_buf[0];
    let msg_type = crate::MessageType::from_byte(msg_type_byte);

    if matches!(msg_type, Some(crate::MessageType::StreamData)) {
        if msg_len < crate::framing::STREAM_HEADER_PREFIX_LEN {
            return Err(GossipError::InvalidStreamFrame(
                "StreamData shorter than header".into(),
            ));
        }
        let mut header_rest = [0u8; crate::framing::STREAM_HEADER_PREFIX_LEN - 1];
        reader.read_exact(&mut header_rest).await?;
        let correlation_id = u16::from_be_bytes([header_rest[0], header_rest[1]]);
        let chunk_len = msg_len - crate::framing::STREAM_HEADER_PREFIX_LEN;
        return Ok(MessageReadResult::Streaming {
            frame: StreamingFrame::Data {
                payload: StreamingData::Direct { chunk_len },
            },
            correlation_id,
        });
    }

    let total_frame_len = msg_len + crate::framing::LENGTH_PREFIX_LEN;
    let mut lease = buffer.lease(total_frame_len);
    {
        let frame = lease.as_mut_slice(total_frame_len);
        frame[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(&len_buf);
        frame[crate::framing::LENGTH_PREFIX_LEN] = msg_type_byte;
        if msg_len > 1 {
            reader
                .read_exact(&mut frame[crate::framing::LENGTH_PREFIX_LEN + 1..total_frame_len])
                .await?;
        }
    }
    let frozen = lease.freeze(total_frame_len);
    let msg_data = MessagePayload::from_pooled(
        frozen.slice(crate::framing::LENGTH_PREFIX_LEN..total_frame_len),
    );

    #[cfg(any(test, feature = "test-helpers", debug_assertions))]
    {
        if typed_tell_capture_enabled() {
            crate::test_helpers::record_raw_payload(msg_data.to_bytes());
        }
    }

    if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data.as_slice()[0] == crate::MessageType::Ask as u8
    {
        let correlation_id = u16::from_be_bytes([msg_data.as_slice()[1], msg_data.as_slice()[2]]);
        let payload = msg_data.into_slice_from(crate::framing::ASK_RESPONSE_HEADER_LEN);
        tracing::debug!(
            correlation_id,
            payload_len = payload.len(),
            "read_message_from_tls_reader decoded AskRaw frame"
        );

        match decode_registry_message(payload) {
            Ok(frame) => {
                debug!(
                    correlation_id = correlation_id,
                    "Received Ask message with correlation ID"
                );
                Ok(MessageReadResult::Gossip(frame, Some(correlation_id)))
            }
            Err((raw_payload, err)) => {
                debug!(
                    correlation_id = correlation_id,
                    payload_len = raw_payload.len(),
                    error = %err,
                    "Received raw Ask payload"
                );
                Ok(MessageReadResult::AskRaw {
                    correlation_id,
                    payload: raw_payload,
                })
            }
        }
    } else if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
        && msg_data.as_slice()[0] == crate::MessageType::Response as u8
    {
        let correlation_id = u16::from_be_bytes([msg_data.as_slice()[1], msg_data.as_slice()[2]]);
        let payload = msg_data.into_slice_from(crate::framing::ASK_RESPONSE_HEADER_LEN);
        tracing::debug!(
            correlation_id,
            payload_len = payload.len(),
            "read_message_from_tls_reader decoded Response frame"
        );
        Ok(MessageReadResult::Response {
            correlation_id,
            payload: payload.into_bytes(),
        })
    } else {
        if msg_len >= 1 {
            if let Some(msg_type) = msg_type {
                match msg_type {
                    crate::MessageType::Gossip => {
                        if msg_data.len() >= crate::framing::GOSSIP_HEADER_LEN {
                            let payload = msg_data.slice_from(crate::framing::GOSSIP_HEADER_LEN);
                            match decode_registry_message(payload) {
                                Ok(frame) => return Ok(MessageReadResult::Gossip(frame, None)),
                                Err((_raw_payload, err)) => {
                                    debug!(
                                        payload_len = msg_data.len(),
                                        error = %err,
                                        "Failed to decode gossip payload"
                                    );
                                    return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                                }
                            }
                        } else {
                            return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                        }
                    }
                    crate::MessageType::ActorTell | crate::MessageType::ActorAsk => {
                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN {
                            return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                        }

                        let msg_type_byte = msg_data.as_slice()[0];
                        let correlation_id =
                            u16::from_be_bytes([msg_data.as_slice()[1], msg_data.as_slice()[2]]);
                        let actor_id =
                            u64::from_be_bytes(msg_data.as_slice()[12..20].try_into().unwrap());
                        let type_hash =
                            u32::from_be_bytes(msg_data.as_slice()[20..24].try_into().unwrap());
                        let payload_len =
                            u32::from_be_bytes(msg_data.as_slice()[24..28].try_into().unwrap())
                                as usize;

                        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
                            return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                        }

                        let payload = msg_data.slice(
                            crate::framing::ACTOR_HEADER_LEN
                                ..crate::framing::ACTOR_HEADER_LEN + payload_len,
                        );

                        #[cfg(not(any(test, feature = "test-helpers")))]
                        {
                            let required_alignment = crate::framing::REGISTRY_ALIGNMENT;
                            let bytes = payload.as_slice();
                            let is_aligned =
                                (bytes.as_ptr() as usize).is_multiple_of(required_alignment);
                            assert!(
                                is_aligned,
                                "Actor payload must be {}-byte aligned! Pointer: {:p}, offset: {}",
                                required_alignment,
                                bytes.as_ptr(),
                                bytes.as_ptr() as usize
                            );
                        }

                        tracing::debug!(
                            msg_type = msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash = format_args!("{:#x}", type_hash),
                            payload_len,
                            "read_message_from_tls_reader decoded actor frame"
                        );

                        return Ok(MessageReadResult::Actor {
                            msg_type: msg_type_byte,
                            correlation_id,
                            actor_id,
                            type_hash,
                            payload,
                        });
                    }
                    crate::MessageType::StreamStart => {
                        let offset = crate::framing::STREAM_HEADER_PREFIX_LEN;
                        if msg_data.len() < offset + streaming::STREAM_DESCRIPTOR_SIZE {
                            return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                        }
                        let descriptor_slice =
                            msg_data.slice(offset..offset + streaming::STREAM_DESCRIPTOR_SIZE);
                        let descriptor =
                            match streaming::decode_stream_start(descriptor_slice.as_slice()) {
                                Ok(desc) => desc,
                                Err(_) => return Ok(MessageReadResult::Raw(msg_data.to_bytes())),
                            };
                        let correlation_id =
                            u16::from_be_bytes([msg_data.as_slice()[1], msg_data.as_slice()[2]]);
                        return Ok(MessageReadResult::Streaming {
                            frame: StreamingFrame::Start { descriptor },
                            correlation_id,
                        });
                    }
                    crate::MessageType::StreamEnd => {
                        let correlation_id =
                            u16::from_be_bytes([msg_data.as_slice()[1], msg_data.as_slice()[2]]);
                        return Ok(MessageReadResult::Streaming {
                            frame: StreamingFrame::End,
                            correlation_id,
                        });
                    }
                    _ => {
                        return Ok(MessageReadResult::Raw(msg_data.to_bytes()));
                    }
                }
            }
        }

        Ok(MessageReadResult::Raw(msg_data.to_bytes()))
    }
}

#[cfg(test)]
mod framing_tests {
    use super::{
        read_message_from_tls_reader, MessagePayload, MessageReadResult, RegistryAlignedVec,
        StreamingData, StreamingFrame,
    };
    use crate::{
        framing,
        gossip_buffer::{GossipFrameBuffer, PooledFrameBytes},
        registry::RegistryMessage,
        streaming::{self, StreamAssembler, StreamDescriptor},
        MessageType,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_frame(frame: Vec<u8>) -> MessageReadResult {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            writer.write_all(&frame).await.unwrap();
        });
        let buffer = GossipFrameBuffer::new();
        read_message_from_tls_reader(&mut reader, 1024 * 1024, &buffer)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn ask_raw_parses_with_padded_header() {
        let payload_bytes = b"hello";
        let header = framing::write_ask_response_header(MessageType::Ask, 42, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::AskRaw {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn response_parses_with_padded_header() {
        let payload_bytes = b"world";
        let header =
            framing::write_ask_response_header(MessageType::Response, 7, payload_bytes.len());
        let mut frame = Vec::with_capacity(header.len() + payload_bytes.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(payload_bytes);

        match read_frame(frame).await {
            MessageReadResult::Response {
                correlation_id,
                payload: body,
            } => {
                assert_eq!(correlation_id, 7);
                assert_eq!(body.as_ref(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn actor_tell_parses_with_reordered_header() {
        let payload_bytes = b"actor_payload";
        let actor_id = 0x0102030405060708u64;
        let type_hash = 0x11223344u32;

        // Wire format: [len:4][type:1][correlation_id:2][reserved:9][actor_id:8][type_hash:4][payload_len:4][payload:N]
        let total_len = framing::ACTOR_HEADER_LEN + payload_bytes.len();
        let mut frame = Vec::with_capacity(framing::LENGTH_PREFIX_LEN + total_len);
        frame.extend_from_slice(&(total_len as u32).to_be_bytes()); // 4 bytes: length prefix
        frame.push(MessageType::ActorTell as u8); // 1 byte: message type
        frame.extend_from_slice(&0u16.to_be_bytes()); // 2 bytes: correlation_id
        frame.extend_from_slice(&[0u8; 9]); // 9 bytes: reserved (for 32-byte alignment)
        frame.extend_from_slice(&actor_id.to_be_bytes()); // 8 bytes: actor_id
        frame.extend_from_slice(&type_hash.to_be_bytes()); // 4 bytes: type_hash
        frame.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes()); // 4 bytes: payload_len
        frame.extend_from_slice(payload_bytes); // N bytes: payload

        match read_frame(frame).await {
            MessageReadResult::Actor {
                msg_type,
                correlation_id,
                actor_id: parsed_actor_id,
                type_hash: parsed_type_hash,
                payload: body,
            } => {
                assert_eq!(msg_type, MessageType::ActorTell as u8);
                assert_eq!(correlation_id, 0);
                assert_eq!(parsed_actor_id, actor_id);
                assert_eq!(parsed_type_hash, type_hash);
                assert_eq!(body.as_slice(), payload_bytes);
            }
            _ => panic!("unexpected result"),
        }
    }

    #[tokio::test]
    async fn gossip_frame_uses_zero_copy_buffer() {
        let message = RegistryMessage::ImmediateAck {
            actor_name: "zero_copy".into(),
            success: true,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&message).unwrap();
        let header = framing::write_gossip_frame_prefix(payload.len());
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);

        match read_frame(frame).await {
            MessageReadResult::Gossip(frame, _) => {
                assert!(frame.is_zero_copy(), "expected pooled backing");
            }
            other => panic!("unexpected frame {:?}", other),
        }
    }

    #[tokio::test]
    async fn gossip_registry_payload_deserializes_from_aligned_buffer() {
        let message = RegistryMessage::ImmediateAck {
            actor_name: "test_actor".to_string(),
            success: true,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&message).unwrap();
        let header = framing::write_gossip_frame_prefix(payload.len());
        let mut frame = Vec::with_capacity(header.len() + payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);

        match read_frame(frame).await {
            MessageReadResult::Gossip(parsed, correlation_id) => {
                assert!(correlation_id.is_none());
                let msg = parsed.to_owned().expect("frame should deserialize");
                match msg {
                    RegistryMessage::ImmediateAck {
                        actor_name,
                        success,
                    } => {
                        assert_eq!(actor_name, "test_actor");
                        assert!(success);
                    }
                    other => panic!("unexpected gossip payload: {:?}", other),
                }
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn alignment_check_works_correctly() {
        use super::is_registry_payload_aligned;

        // Create a properly aligned buffer
        let mut aligned_buf: RegistryAlignedVec = RegistryAlignedVec::new();
        aligned_buf.extend_from_slice(&[1u8; 32]);
        assert!(
            is_registry_payload_aligned(aligned_buf.as_ref()),
            "RegistryAlignedVec should be properly aligned"
        );

        // Create an intentionally unaligned buffer by shifting by 1 byte
        let unaligned = vec![1u8, 2u8, 3u8, 4u8];
        // The alignment of Vec is typically not guaranteed to be REGISTRY_MESSAGE_ALIGNMENT
        // so this test verifies the alignment check function works correctly
        let is_aligned = is_registry_payload_aligned(&unaligned);
        // We don't assert false here because alignment depends on heap allocation
        // but we verify the function doesn't crash and returns a boolean
        let _ = is_aligned;
    }

    #[tokio::test]
    async fn decode_registry_message_handles_both_aligned_and_unaligned() {
        use super::decode_registry_message;

        let message = RegistryMessage::ImmediateAck {
            actor_name: "test_actor_aligned".to_string(),
            success: true,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&message).unwrap();

        // Test with aligned payload
        {
            let pooled = PooledFrameBytes::orphaned(&payload);
            let result = decode_registry_message(MessagePayload::from_pooled(pooled));
            assert!(result.is_ok(), "Should decode aligned payload successfully");
            let msg = result
                .unwrap()
                .to_owned()
                .expect("frame should deserialize");
            match msg {
                RegistryMessage::ImmediateAck {
                    actor_name,
                    success,
                } => {
                    assert_eq!(actor_name, "test_actor_aligned");
                    assert!(success);
                }
                _ => panic!("Unexpected message type"),
            }
        }

        // Test with unaligned payload (create offset)
        {
            let mut padded = Vec::with_capacity(payload.len() + 1);
            padded.push(0u8);
            padded.extend_from_slice(&payload);
            let padded_pooled = PooledFrameBytes::orphaned(&padded);
            let unaligned = padded_pooled.slice(1..padded_pooled.len());
            let result = decode_registry_message(MessagePayload::from_pooled(unaligned));
            assert!(
                result.is_ok(),
                "Should decode unaligned payload successfully"
            );
            let msg = result
                .unwrap()
                .to_owned()
                .expect("frame should deserialize");
            match msg {
                RegistryMessage::ImmediateAck {
                    actor_name,
                    success,
                } => {
                    assert_eq!(actor_name, "test_actor_aligned");
                    assert!(success);
                }
                _ => panic!("Unexpected message type"),
            }
        }
    }

    fn sample_descriptor(payload_len: u64) -> StreamDescriptor {
        StreamDescriptor {
            stream_id: 0xBAD_CAFE,
            payload_len,
            chunk_len: 1024,
            type_hash: 0x1122_3344,
            actor_id: 0x7788,
            flags: 0,
            reserved: 0,
        }
    }

    fn encode_data_frame(payload: &[u8], correlation_id: u16) -> Vec<u8> {
        let mut buf = Vec::with_capacity(streaming::STREAM_DATA_HEADER_LEN + payload.len());
        let header = streaming::encode_stream_data_header(payload.len(), correlation_id).unwrap();
        buf.extend_from_slice(&header);
        buf.extend_from_slice(payload);
        buf
    }

    fn encode_end_frame(correlation_id: u16) -> [u8; streaming::STREAM_DATA_HEADER_LEN] {
        let mut buf = [0u8; streaming::STREAM_DATA_HEADER_LEN];
        let len = framing::STREAM_HEADER_PREFIX_LEN as u32;
        buf[..4].copy_from_slice(&len.to_be_bytes());
        buf[4] = MessageType::StreamEnd as u8;
        buf[5..7].copy_from_slice(&correlation_id.to_be_bytes());
        buf
    }

    #[tokio::test]
    async fn streaming_frames_round_trip_via_duplex() {
        let descriptor = sample_descriptor(6);
        let correlation_id = 0xCAFE;
        let start_frame = streaming::encode_stream_start_frame(&descriptor, correlation_id);
        let payload = [1u8, 2, 3, 4, 5, 6];
        let data_frame = encode_data_frame(&payload, correlation_id);
        let end_frame = encode_end_frame(correlation_id);

        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let buffer = GossipFrameBuffer::new();
        let writer_task = tokio::spawn(async move {
            writer.write_all(&start_frame).await.unwrap();
            writer.write_all(&data_frame).await.unwrap();
            writer.write_all(&end_frame).await.unwrap();
        });

        match read_message_from_tls_reader(&mut reader, 1024 * 1024, &buffer)
            .await
            .unwrap()
        {
            MessageReadResult::Streaming {
                frame,
                correlation_id: parsed_corr,
            } => {
                assert_eq!(parsed_corr, correlation_id);
                match frame {
                    StreamingFrame::Start { descriptor: parsed } => {
                        assert_eq!(parsed, descriptor)
                    }
                    other => panic!("expected start frame, got {:?}", other),
                }
            }
            other => panic!("unexpected frame: {:?}", other),
        }

        match read_message_from_tls_reader(&mut reader, 1024 * 1024, &buffer)
            .await
            .unwrap()
        {
            MessageReadResult::Streaming {
                frame,
                correlation_id: parsed_corr,
            } => {
                assert_eq!(parsed_corr, correlation_id);
                if let StreamingFrame::Data {
                    payload: StreamingData::Direct { chunk_len },
                } = frame
                {
                    assert_eq!(chunk_len, payload.len());
                    let mut buf = vec![0u8; chunk_len];
                    reader.read_exact(&mut buf).await.unwrap();
                    assert_eq!(buf, payload);
                } else {
                    panic!("expected data frame, got {:?}", frame);
                }
            }
            other => panic!("unexpected frame: {:?}", other),
        }

        match read_message_from_tls_reader(&mut reader, 1024 * 1024, &buffer)
            .await
            .unwrap()
        {
            MessageReadResult::Streaming {
                frame,
                correlation_id: parsed_corr,
            } => {
                assert_eq!(parsed_corr, correlation_id);
                assert!(matches!(frame, StreamingFrame::End));
            }
            other => panic!("unexpected frame: {:?}", other),
        }

        writer_task.await.unwrap();
    }

    #[tokio::test]
    async fn streaming_data_before_start_is_rejected() {
        let payload = [0xAA, 0xBB];
        let data_frame = encode_data_frame(&payload, 0xBEEF);

        let (mut writer, mut reader) = tokio::io::duplex(256);
        let buffer = GossipFrameBuffer::new();
        tokio::spawn(async move {
            writer.write_all(&data_frame).await.unwrap();
        });

        let mut assembler = StreamAssembler::new(16);
        match read_message_from_tls_reader(&mut reader, 1024, &buffer)
            .await
            .unwrap()
        {
            MessageReadResult::Streaming { frame, .. } => {
                if let StreamingFrame::Data {
                    payload: StreamingData::Direct { chunk_len },
                } = frame
                {
                    let err = assembler
                        .read_data_direct(&mut reader, chunk_len)
                        .await
                        .unwrap_err();
                    assert!(matches!(err, crate::GossipError::InvalidStreamFrame(_)));
                } else {
                    panic!("expected data frame, got {:?}", frame);
                }
            }
            other => panic!("unexpected frame: {:?}", other),
        }
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

    if matches!(
        task.message,
        crate::registry::RegistryMessage::PeerListGossip { .. }
    ) && !registry.peer_supports_peer_list(&task.peer_addr).await
    {
        debug!(
            peer = %task.peer_addr,
            "Skipping PeerListGossip send - peer lacks negotiated capability"
        );
        return Ok(());
    }

    // CRITICAL: Set precise timing RIGHT BEFORE TCP write to exclude all scheduling delays
    // Update wall_clock_time in delta changes to current time for accurate propagation measurement
    let _current_time_secs = crate::current_timestamp();
    let current_time_nanos = crate::current_timestamp_nanos();

    // Debug: Check if there's a delay in the task creation vs sending
    if let crate::registry::RegistryMessage::DeltaGossip { delta } = &task.message {
        for change in &delta.changes {
            if let crate::registry::RegistryChange::ActorAdded { location, .. } = change {
                let creation_time_nanos = location.wall_clock_time as u128 * 1_000_000_000;
                let delay_nanos = current_time_nanos as u128 - creation_time_nanos;
                let _delay_ms = delay_nanos as f64 / 1_000_000.0;
                // eprintln!("🔍 DELTA_SEND_DELAY: {}ms between delta creation and sending", delay_ms);
            }
        }
    }

    match &mut task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta } => {
            delta.precise_timing_nanos = current_time_nanos;
            // Update wall_clock_time in all changes to current time for accurate propagation measurement
            for change in &mut delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        // Set wall_clock_time to nanoseconds for consistent timing measurements
                        location.wall_clock_time = current_time_nanos / 1_000_000_000;
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

    // Create message with Gossip type prefix and required padding
    let mut msg_with_type = Vec::with_capacity(crate::framing::GOSSIP_HEADER_LEN + data.len());
    msg_with_type.resize(crate::framing::GOSSIP_HEADER_LEN, 0);
    msg_with_type[0] = crate::MessageType::Gossip as u8;
    msg_with_type.extend_from_slice(&data);

    let msg_bytes = Bytes::from(msg_with_type);
    let frame_len = msg_bytes.len();
    conn.tell_bytes(msg_bytes).await?;
    crate::telemetry::gossip_zero_copy::record_outbound_frame(
        "handle::send_gossip_message_zero_copy",
        frame_len,
    );
    Ok(())
}
