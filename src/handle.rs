use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::{interval, Instant},
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    connection_pool::handle_incoming_message,
    current_timestamp,
    registry::{GossipRegistry, GossipResult, GossipTask, RegistryMessage, RegistryStats},
    ActorLocation, GossipConfig, GossipError, RegistrationPriority, Result,
};

/// Main API for the gossip registry with vector clocks and separated locks
pub struct GossipRegistryHandle {
    pub registry: Arc<GossipRegistry>,
    _server_handle: tokio::task::JoinHandle<()>,
    _timer_handle: tokio::task::JoinHandle<()>,
    _monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GossipRegistryHandle {
    /// Create and start a new gossip registry
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
        registry.add_bootstrap_peers(peers).await;

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
        let timer_registry = registry.clone();
        let timer_handle = tokio::spawn(async move {
            start_gossip_timer(timer_registry).await;
        });

        // Connection monitoring is now done in the gossip timer
        let monitor_handle = None;

        // Bootstrap after a brief delay to let server start
        // Bootstrap with readiness check - WAIT for completion to ensure connections are ready
        let bootstrap_registry = registry.clone();
        let bootstrap_config = config.clone();
        let bootstrap_handle = tokio::spawn(async move {
            if let Err(err) =
                wait_for_server_readiness_and_bootstrap(bootstrap_registry, bootstrap_config).await
            {
                error!(error = %err, "bootstrap failed after readiness check");
            }
        });
        
        // Wait for bootstrap to complete before returning, ensuring connections are ready
        // This prevents immediate gossip from happening before peers are connected
        if let Err(err) = bootstrap_handle.await {
            error!(error = %err, "bootstrap task failed");
        }

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
        let location = ActorLocation::new(address);
        self.registry.register_actor(name, location).await
    }

    /// Register a local actor with high priority (faster propagation)
    pub async fn register_urgent(
        &self,
        name: String,
        address: SocketAddr,
        priority: RegistrationPriority,
    ) -> Result<()> {
        let location = ActorLocation::new_with_priority(address, priority);
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
        let location = ActorLocation::new_with_priority(address, priority);
        self.registry
            .register_actor_with_priority(name, location, priority)
            .await
    }

    /// Unregister a local actor
    pub async fn unregister(&self, name: &str) -> Result<Option<ActorLocation>> {
        self.registry.unregister_actor(name).await
    }

    /// Lookup an actor (now much faster - read-only lock)
    pub async fn lookup(&self, name: &str) -> Option<ActorLocation> {
        self.registry.lookup_actor(name).await
    }

    /// Get registry statistics including vector clock metrics
    pub async fn stats(&self) -> RegistryStats {
        self.registry.get_stats().await
    }

    /// Add a peer to the gossip network
    pub async fn add_peer(&self, peer_addr: SocketAddr) -> Result<()> {
        self.registry.add_peer(peer_addr).await;
        Ok(())
    }
    
    /// Get a connection handle for direct communication (reuses existing pool connections)
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<crate::connection_pool::ConnectionHandle> {
        self.registry.get_connection(addr).await
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
    let gossip_interval = registry.config.gossip_interval;
    let cleanup_interval = registry.config.cleanup_interval;

    let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
    let mut next_gossip_tick = Instant::now() + gossip_interval + jitter;
    let mut cleanup_timer = interval(cleanup_interval);

    info!(
        gossip_interval_ms = gossip_interval.as_millis(),
        cleanup_interval_secs = cleanup_interval.as_secs(),
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
                            let _ = send_gossip_message_zero_copy(task, registry_clone).await;
                            GossipResult {
                                peer_addr,
                                sent_sequence,
                                outcome: Ok(None),
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
            }
        }
    }

    info!("gossip timer stopped");
}

/// Handle incoming TCP connections - immediately set up bidirectional communication
#[instrument(skip(stream, registry), fields(peer = %peer_addr))]
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
) {
    info!("🔌 HANDLE_CONNECTION: Starting to handle new incoming connection");

    // Split stream for direct TCP access
    let (reader, writer) = stream.into_split();

    // Get registry reference for the handler
    let registry_weak = Some(Arc::downgrade(&registry));

    // Start the incoming persistent connection handler immediately
    // It will handle ALL messages including the first one
    tokio::spawn(async move {
        info!(peer = %peer_addr, "HANDLE.RS: Starting incoming direct TCP connection handler");
        let sender_addr = handle_incoming_connection_direct_tcp(
            reader,
            writer,
            peer_addr,
            registry.clone(),
            registry_weak,
        )
        .await;
        info!(peer = %peer_addr, "HANDLE.RS: Incoming direct TCP connection handler exited");
        
        // Handle peer failure when connection is lost
        if let Some(failed_peer_addr) = sender_addr {
            info!(peer = %failed_peer_addr, "HANDLE.RS: Triggering peer failure handling");
            if let Err(e) = registry.handle_peer_connection_failure(failed_peer_addr).await {
                warn!(error = %e, peer = %failed_peer_addr, "HANDLE.RS: Failed to handle peer connection failure");
            }
        } else {
            warn!(peer = %peer_addr, "HANDLE.RS: Cannot handle peer failure - sender address unknown");
        }
    });
}

/// Handle an incoming direct TCP connection - processes all messages and manages the connection pool
async fn handle_incoming_connection_direct_tcp(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    peer_addr: SocketAddr,
    registry: Arc<GossipRegistry>,
    registry_weak: Option<std::sync::Weak<GossipRegistry>>,
) -> Option<SocketAddr> {
    let max_message_size = registry.config.max_message_size;

    // First, read the initial message to identify the sender
    let msg_result = read_message_from_reader(&mut reader, max_message_size).await;

    let sender_addr = match &msg_result {
        Ok(msg) => match msg {
            RegistryMessage::DeltaGossip { delta } => delta.sender_addr.clone(),
            RegistryMessage::FullSync { sender_addr, .. } => sender_addr.clone(),
            RegistryMessage::FullSyncRequest { sender_addr, .. } => sender_addr.clone(),
            RegistryMessage::FullSyncResponse { sender_addr, .. } => sender_addr.clone(),
            RegistryMessage::DeltaGossipResponse { delta } => delta.sender_addr.clone(),
            RegistryMessage::PeerHealthQuery { sender, .. } => sender.clone(),
            RegistryMessage::PeerHealthReport { reporter, .. } => reporter.clone(),
        },
        Err(err) => {
            warn!(error = %err, "failed to read initial message from incoming connection");
            return None;
        }
    };

    info!(listening_addr = %sender_addr, peer_addr = %peer_addr, "Identified incoming connection sender");

    // Process the first message
    if let Ok(msg) = msg_result {
        if let Err(e) = handle_incoming_message(registry.clone(), peer_addr, msg).await {
            warn!(error = %e, "failed to handle initial message");
        }
    }

    // Add this connection to the pool - LOCK-FREE
    {
        let mut pool = registry.connection_pool.lock().await;
        // Pass writer directly - no mutex needed
        if pool.add_connection_sender(sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()), peer_addr, writer) {
            info!(listening_addr = %sender_addr, peer_addr = %peer_addr,
                  "Added incoming connection to pool for bidirectional use");
        } else {
            warn!(listening_addr = %sender_addr, "Failed to add connection to pool");
            return None;
        }
    }

    // Trigger an immediate gossip round to establish bidirectional communication quickly
    {
        let registry_clone = registry.clone();
        let sender_addr_clone = sender_addr.clone();
        tokio::spawn(async move {
            debug!(peer = %sender_addr_clone, "Triggering immediate gossip round after incoming connection");

            // Prepare and execute a gossip round
            match registry_clone.prepare_gossip_round().await {
                Ok(tasks) => {
                    if !tasks.is_empty() {
                        // Execute all gossip tasks
                        let mut futures = Vec::new();

                        for task in tasks {
                            let registry_for_task = registry_clone.clone();
                            let peer_addr = task.peer_addr;
                            let sent_sequence = task.current_sequence;
                            let future = tokio::spawn(async move {
                                let _ =
                                    send_gossip_message_zero_copy(task, registry_for_task).await;
                                GossipResult {
                                    peer_addr,
                                    sent_sequence,
                                    outcome: Ok(None),
                                }
                            });
                            futures.push(future);
                        }

                        // Wait for all gossip operations to complete
                        let mut results = Vec::new();
                        for future in futures {
                            match future.await {
                                Ok(result) => results.push(result),
                                Err(err) => {
                                    warn!(error = %err, "gossip task panicked");
                                }
                            }
                        }

                        // Apply the results
                        registry_clone.apply_gossip_results(results).await;
                        info!(peer = %sender_addr_clone, "Completed immediate gossip round after incoming connection");
                    }
                }
                Err(err) => {
                    warn!(error = %err, peer = %sender_addr_clone, "Failed to trigger immediate gossip round");
                }
            }
        });
    }

    // Reset failure count for this peer since they successfully connected to us
    {
        let mut gossip_state = registry.gossip_state.lock().await;

        // Check if this is a previously failed peer
        let was_failed = gossip_state
            .peers
            .get(&sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
            .map(|info| info.failures >= registry.config.max_peer_failures)
            .unwrap_or(false);

        if was_failed {
            info!(
                peer = %sender_addr,
                "✅ Received incoming connection from previously failed peer - connection restored!"
            );

            // Clear the pending failure record
            gossip_state.pending_peer_failures.remove(&sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()));
        }

        // Reset failure state
        if let Some(peer_info) = gossip_state.peers.get_mut(&sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap())) {
            peer_info.failures = 0;
            peer_info.last_failure_time = None;
            peer_info.last_success = current_timestamp();
        }
    }

    // Now continue with the persistent connection reader for the rest of the connection
    crate::connection_pool::handle_persistent_connection_reader(
        reader,
        sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()), // Pass the sender's listening address
        registry_weak,
    )
    .await;
    
    // Return the sender address so the failure handler can use it
    Some(sender_addr.parse::<SocketAddr>().unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()))
}

async fn read_message(stream: &mut TcpStream, max_size: usize) -> Result<RegistryMessage> {
    // Read length header
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Check message size
    if len > max_size {
        return Err(GossipError::MessageTooLarge {
            size: len,
            max: max_size,
        });
    }

    // Read message data
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;

    let msg = rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&data)?;
    Ok(msg)
}

async fn read_message_from_reader(reader: &mut tokio::net::tcp::OwnedReadHalf, max_size: usize) -> Result<RegistryMessage> {
    // Read length header
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Check message size
    if len > max_size {
        return Err(GossipError::MessageTooLarge {
            size: len,
            max: max_size,
        });
    }

    // Read message data
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data).await?;

    let msg = rkyv::from_bytes::<RegistryMessage, rkyv::rancor::Error>(&data)?;
    Ok(msg)
}

/// Bootstrap by connecting to known peers and doing initial gossip with vector clocks
#[instrument(skip(registry))]
async fn bootstrap_peers(registry: Arc<GossipRegistry>) -> Result<()> {
    let bind_addr = registry.bind_addr;

    let peers = {
        let gossip_state = registry.gossip_state.lock().await;
        gossip_state.peers.keys().cloned().collect::<Vec<_>>()
    };

    info!(
        peer_count = peers.len(),
        "starting bootstrap with vector clock support"
    );

    for peer in peers {
        // Always use full sync for bootstrap
        let gossip_msg = {
            let (local_actors, known_actors) = {
                let actor_state = registry.actor_state.read().await;
                (
                    actor_state.local_actors.clone(),
                    actor_state.known_actors.clone(),
                )
            };

            let sequence = {
                let gossip_state = registry.gossip_state.lock().await;
                gossip_state.gossip_sequence
            };

            RegistryMessage::FullSync {
                local_actors: local_actors.into_iter().map(|(k, v)| (k, v)).collect(),
                known_actors: known_actors.into_iter().map(|(k, v)| (k, v)).collect(),
                sender_addr: bind_addr.to_string(), // This is the listening address
                sequence,
                wall_clock_time: current_timestamp(),
            }
        };

        // Send bootstrap message
        match rkyv::to_bytes::<rkyv::rancor::Error>(&gossip_msg) {
            Ok(data) => {
                // Get connection and create message buffer
                let (conn, buffer) = {
                    let mut pool = registry.connection_pool.lock().await;

                    info!(peer = %peer, "BOOTSTRAP: Attempting to get connection for bootstrap");

                    // If we already have a connection, use it
                    let conn = match pool.get_connection(peer).await {
                        Ok(c) => {
                            info!(peer = %peer, "BOOTSTRAP: Successfully got connection");
                            c
                        }
                        Err(e) => {
                            // During startup, connection refused is expected if peer's server isn't ready yet
                            // This will be retried by the bootstrap retry logic
                            match &e {
                                GossipError::Network(io_err)
                                    if io_err.kind() == std::io::ErrorKind::ConnectionRefused =>
                                {
                                    debug!(peer = %peer, "BOOTSTRAP: Peer not ready yet (connection refused)");
                                }
                                _ => {
                                    warn!(peer = %peer, error = %e, "BOOTSTRAP: Failed to connect to bootstrap peer");
                                }
                            }
                            continue;
                        }
                    };

                    // Create message buffer with length header using buffer pool
                    let buffer = pool.create_message_buffer(&data);

                    (conn, buffer)
                };

                debug!(peer = %peer, "connected to bootstrap peer");

                if let Err(err) = conn.send_data(buffer).await {
                    warn!(peer = %peer, error = %err, "failed to send bootstrap gossip");
                    continue;
                }

                debug!(peer = %peer, "sent bootstrap message through persistent connection");
                // Note: With persistent connections, we'll receive the response through
                // the incoming message handler, not here
            }
            Err(err) => {
                warn!(peer = %peer, error = %err, "failed to serialize bootstrap message");
            }
        }
    }

    info!("bootstrap completed with vector clock support");

    // Trigger an immediate gossip round after bootstrap to establish connections quickly
    tokio::spawn(async move {
        debug!("Triggering immediate gossip round after bootstrap");

        // Prepare and execute a gossip round
        match registry.prepare_gossip_round().await {
            Ok(tasks) => {
                if !tasks.is_empty() {
                    // Execute all gossip tasks
                    let mut futures = Vec::new();

                    for task in tasks {
                        let registry_for_task = registry.clone();
                        let peer_addr = task.peer_addr;
                        let sent_sequence = task.current_sequence;
                        let future = tokio::spawn(async move {
                            let _ = send_gossip_message_zero_copy(task, registry_for_task).await;
                            GossipResult {
                                peer_addr,
                                sent_sequence,
                                outcome: Ok(None),
                            }
                        });
                        futures.push(future);
                    }

                    // Wait for all gossip operations to complete
                    let mut results = Vec::new();
                    for future in futures {
                        match future.await {
                            Ok(result) => results.push(result),
                            Err(err) => {
                                warn!(error = %err, "gossip task panicked");
                            }
                        }
                    }

                    // Apply the results
                    registry.apply_gossip_results(results).await;
                    info!("Completed immediate gossip round after bootstrap");
                }
            }
            Err(err) => {
                warn!(error = %err, "Failed to trigger immediate gossip round after bootstrap");
            }
        }
    });

    Ok(())
}

/// Send a gossip message through persistent connection with zero-copy optimizations
async fn send_gossip_message_persistent(
    task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> Result<()> {
    // Pre-serialize the message (this could be optimized further with message pooling)
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&task.message)?;

    // Get persistent connection with minimal lock time
    let conn = {
        let mut pool = registry.connection_pool.lock().await;
        pool.get_connection(task.peer_addr).await?
    };

    // Use zero-copy tell() method for maximum performance
    conn.tell(data.as_slice()).await?;
    Ok(())
}

/// Zero-copy gossip message sender - eliminates bottlenecks in serialization and connection handling
async fn send_gossip_message_zero_copy(
    mut task: GossipTask,
    registry: Arc<GossipRegistry>,
) -> Result<()> {
    // Get connection with minimal lock contention
    let conn = {
        let mut pool = registry.connection_pool.lock().await;
        pool.get_connection(task.peer_addr).await?
    };

    // CRITICAL: Set precise timing RIGHT BEFORE TCP write to exclude all scheduling delays
    // Update wall_clock_time in delta changes to current time for accurate propagation measurement
    let current_time_secs = crate::current_timestamp();
    let current_time_nanos = crate::current_timestamp_nanos();
    
    // Debug: Check if there's a delay in the task creation vs sending
    match &task.message {
        crate::registry::RegistryMessage::DeltaGossip { delta } => {
            for change in &delta.changes {
                match change {
                    crate::registry::RegistryChange::ActorAdded { location, .. } => {
                        let creation_time_nanos = location.wall_clock_time as u128 * 1_000_000_000;
                        let delay_nanos = current_time_nanos as u128 - creation_time_nanos;
                        let delay_ms = delay_nanos as f64 / 1_000_000.0;
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

    // Use zero-copy tell() which uses try_send() internally for max performance
    // This completely bypasses async overhead when the channel has capacity
    let tcp_start = std::time::Instant::now();
    conn.tell(data.as_slice()).await?;
    let tcp_elapsed = tcp_start.elapsed();
    // eprintln!("🔍 TCP_WRITE_TIME: {:?}", tcp_elapsed);
    Ok(())
}

/// Wait for the server to be ready, then bootstrap
async fn wait_for_server_readiness_and_bootstrap(
    registry: Arc<GossipRegistry>,
    config: GossipConfig,
) -> Result<()> {
    let bind_addr = registry.bind_addr;

    // Wait for server to become ready
    let readiness_timeout = tokio::time::timeout(
        config.bootstrap_readiness_timeout,
        wait_for_server_ready(bind_addr, config.bootstrap_readiness_check_interval),
    );

    match readiness_timeout.await {
        Ok(Ok(())) => {
            // info!("server is ready, starting bootstrap");
        }
        Ok(Err(err)) => {
            warn!(error = %err, "readiness check failed, proceeding with bootstrap anyway");
        }
        Err(_) => {
            warn!("readiness check timeout, proceeding with bootstrap anyway");
        }
    }

    // Add a small initial delay to give peers a chance to start their servers
    // This helps avoid immediate connection failures during startup
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Attempt bootstrap with retries
    for attempt in 1..=config.bootstrap_max_retries {
        match bootstrap_peers(registry.clone()).await {
            Ok(()) => {
                info!(attempt = attempt, "bootstrap completed successfully");
                return Ok(());
            }
            Err(err) => {
                if attempt < config.bootstrap_max_retries {
                    // Use shorter retry delay for initial attempts during startup
                    let retry_delay = if attempt <= 2 {
                        // First two retries: use shorter delay (500ms)
                        Duration::from_millis(500)
                    } else {
                        // Later retries: use configured delay
                        config.bootstrap_retry_delay
                    };

                    warn!(
                        attempt = attempt,
                        max_attempts = config.bootstrap_max_retries,
                        retry_delay_ms = retry_delay.as_millis(),
                        error = %err,
                        "bootstrap attempt failed, retrying"
                    );
                    tokio::time::sleep(retry_delay).await;
                } else {
                    error!(
                        attempt = attempt,
                        error = %err,
                        "bootstrap failed after all retry attempts"
                    );
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

/// Check if the server is ready by attempting to connect to our own bind address
async fn wait_for_server_ready(bind_addr: SocketAddr, check_interval: Duration) -> Result<()> {
    loop {
        match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(bind_addr)).await
        {
            Ok(Ok(_stream)) => {
                debug!(bind_addr = %bind_addr, "server readiness confirmed");
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {
                // Server not ready yet, wait and retry
                tokio::time::sleep(check_interval).await;
            }
        }
    }
}
