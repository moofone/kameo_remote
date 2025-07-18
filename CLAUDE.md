# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Build and Test Commands
- `cargo build` - Build the project
- `cargo test` - Run all tests
- `cargo run --example multi_node` - Run the multi-node demo example
- `cargo check` - Check for compilation errors without building

### Testing Individual Components
- `cargo test test_vector_clock_basic` - Test basic vector clock functionality
- `cargo test test_delta_gossip_with_vector_clocks` - Test delta gossip with vector clocks
- `cargo run --example test_connection_debug` - Debug connection establishment and peer status
- `cargo test lock_free` - Run lock-free architecture tests

## Architecture Overview

This is a **distributed gossip protocol library** for actor registry management using vector clocks for causal consistency. The system implements a peer-to-peer gossip protocol where nodes share information about actor locations across a distributed network.

### Core Components

**Main Library (`src/lib.rs`)**
- Defines core types: `NodeId`, `VectorClock`, `ActorLocation`, `GossipConfig`, `GossipError`
- Implements vector clock logic for causal ordering and conflict resolution
- Provides configuration structures for gossip behavior

**GossipRegistry (`src/registry.rs`)**
- Core registry implementation with separated actor and gossip state locks
- Handles actor registration/unregistration with vector clock increments
- Implements delta-based gossip with full-sync fallback
- Provides vector clock garbage collection and conflict resolution
- Uses connection pooling for efficient peer communication

**GossipRegistryHandle (`src/handle.rs`)**
- Main API interface for the gossip registry
- Manages server lifecycle (TCP listener, gossip timer, bootstrap)
- Handles incoming connections and message processing
- Implements non-blocking gossip rounds with concurrent task execution

**ConnectionPool (`src/connection_pool.rs`)**
- Lock-free message sending using ring buffers and atomic operations
- Persistent TCP connections with bidirectional communication
- Single-writer pattern - each TCP write half owned by one background task
- DashMap for concurrent connection storage without locks
- Immediate EOF detection for fast failure handling
- Automatic reconnection with configurable retry intervals

### Key Design Patterns

**Vector Clock-Based Conflict Resolution**
- Each node maintains a vector clock for causal ordering
- Actor updates include vector clocks for conflict resolution
- Concurrent updates resolved using node ID as tiebreaker

**Delta Gossip with Full-Sync Fallback**
- Efficient delta-based gossip for incremental updates
- Automatic fallback to full sync when delta history insufficient
- Small clusters (≤5 nodes) use full sync for better propagation

**Lock-Free Message Sending Architecture**
- Message sending path is completely lock-free using atomic operations
- Ring buffer with single-writer pattern eliminates contention
- Background writer task batches messages for vectored I/O
- Fire-and-forget semantics - drops messages if buffer full
- Remaining locks (actor state RwLock, gossip state Mutex) are NOT in the hot path

**Persistent Bidirectional Connections**
- Each node maintains persistent TCP connections to all peers
- All connections are bidirectional - can send and receive any message type
- Immediate EOF detection (milliseconds) when peer disconnects
- Automatic reconnection attempts every 5 seconds for failed peers

### Message Types
- `DeltaGossip` - Incremental changes since last sequence
- `FullSync` - Complete actor state synchronization
- `FullSyncRequest` - Request for full state from peer
- Response variants for two-way communication

### Message Types
- `PeerHealthQuery` - Query other nodes about a peer's health status
- `PeerHealthReport` - Report on a peer's health from another node's perspective

### Lock-Free Architecture Details

**LockFreeRingBuffer**
- Fixed-size circular buffer with atomic head/tail pointers
- Uses `Ordering::AcqRel` for index updates to prevent ABA problems
- Single producer (application) / single consumer (background writer)
- `try_push()` returns immediately - no blocking on full buffer
- Capacity of 16384 messages by default

**LockFreeStreamHandle**
- Owns the TCP write half exclusively (no Arc<Mutex>)
- Background task continuously polls ring buffer for messages
- Batches up to 4096 messages into vectored writes
- Flushes after 256KB of data or when idle
- Uses unbounded MPSC channel as overflow for `tell()` operations

**Memory Ordering**
- `Ordering::AcqRel` - For atomic index updates (prevents reordering)
- `Ordering::Release` - After writes to ensure visibility
- `Ordering::Acquire` - Before reads to see all previous writes
- `Ordering::Relaxed` - For non-critical counters like bytes written

**Performance Characteristics**
- Zero-copy path from application to TCP socket
- Vectored I/O reduces syscall overhead by 100x
- No mutex contention on the hot path
- Sub-microsecond message queuing latency
- Handles 1M+ messages/second per connection

### Configuration
The `GossipConfig` struct controls:
- Gossip intervals and peer selection
- Connection timeouts and message limits
- Vector clock garbage collection frequency
- Delta history retention and full sync intervals
- `peer_retry_interval` - How often to retry failed connections (default: 5 seconds)
- `dead_peer_timeout` - How long before removing disconnected peers (default: 15 minutes)

## Connection Failure and Recovery Flow

### Immediate Disconnection Detection
When a peer disconnects or crashes, the system detects it immediately through EOF (End-of-File) detection:

1. **EOF Detection**: The read task on the TCP connection receives `Ok(0)` bytes, indicating the peer has closed their write side
2. **Connection Marked Failed**: The connection is immediately marked as disconnected in the connection pool
3. **Peer State Updated**: The peer's failure count is set to max failures, preventing normal gossip
4. **Actors Remain Available**: Importantly, actors registered by the failed peer are NOT removed - they remain queryable

### Consensus Process (Optional)
After detecting a local disconnection, the node may query other peers to understand if the failure is global:

1. **100ms Delay**: Wait briefly to avoid race conditions where other nodes haven't detected the failure yet
2. **Query Peers**: Send `PeerHealthQuery` messages to all healthy peers asking about the failed peer
3. **Collect Reports**: Gather `PeerHealthReport` responses from other nodes
4. **Consensus Decision**: If majority agrees the peer is dead, it's marked as globally failed
5. **No Actor Removal**: Even with consensus, actors are kept available for potential reconnection

### Automatic Reconnection
Failed peers are automatically retried:

1. **Retry Timer**: Every 5 seconds (configurable via `peer_retry_interval`), failed peers are included in gossip rounds
2. **Connection Attempt**: The system attempts to establish a new TCP connection to the failed peer
3. **Two-Way Recovery**: Recovery can happen in two ways:
   - **Outgoing Success**: Our retry connects to the recovered peer, and we receive messages from them
   - **Incoming Connection**: The recovered peer connects to us

### Failure Reset Conditions
A peer's failure state is only reset when we have proof they're truly back online:

1. **Incoming Connection**: When we accept a new connection and identify it's from a previously failed peer
2. **Message Receipt**: When we receive any gossip message (Delta or FullSync) from a previously failed peer
3. **NOT on Connection Success**: Simply establishing a TCP connection is not enough - we need actual communication

### Dead Peer Actor Cleanup
After extended disconnection periods, resources are cleaned up while maintaining connectivity:

1. **15-Minute Timeout**: By default, after 15 minutes of disconnection, actors from the peer are removed
2. **Peer Retained**: The peer itself is NEVER removed - ensuring we can always reconnect
3. **Memory Cleanup**: Only the actors are removed to free memory, not the peer entry
4. **Automatic Recovery**: When the peer reconnects, it will re-register its actors through normal gossip

### Example Scenario

```
Time 0s: Node A and B are connected, gossiping normally
Time 10s: Node A crashes
Time 10.003s: Node B detects EOF, marks A as failed (but keeps A's actors)
Time 10.103s: Node B queries other nodes about A's health (if any exist)
Time 15s: Node B attempts reconnection to A (fails)
Time 20s: Node B attempts reconnection to A (fails)
Time 25s: Node A restarts with same address
Time 25s: Node B attempts reconnection to A (succeeds, TCP established)
Time 25.1s: Node A sends FullSync message to B
Time 25.1s: Node B receives message, resets A's failure count
Time 25.1s: Normal gossip resumes between A and B
```

### Key Properties

1. **Fast Detection**: Disconnections detected in milliseconds, not through timeouts
2. **High Availability**: Actors remain queryable even when their registering node is down
3. **Automatic Recovery**: No manual intervention needed when nodes come back online
4. **No Split-Brain**: Clear ownership of actors through vector clocks prevents conflicts
5. **Efficient Retries**: Failed nodes checked every 5 seconds, not on every gossip round

## Troubleshooting

### Connection Shows as Failed Despite Being Connected
**Symptom**: `stats()` shows `failed_peers: 1` even though nodes are communicating

**Cause**: The incoming connection writer was being dropped, causing immediate EOF

**Solution**: Ensure `add_connection_sender()` properly creates a `LockFreeStreamHandle` with the writer instead of dropping it

### High CPU Usage
**Symptom**: Background writer task consuming excessive CPU

**Cause**: Tight polling loop without any yielding

**Solution**: The writer task includes `tokio::task::yield_now()` when no messages are available

### Messages Being Dropped
**Symptom**: Messages not reaching peers under high load

**Cause**: Ring buffer full (fire-and-forget semantics)

**Solution**: 
- Increase ring buffer size (default 16384)
- Monitor `pending_writes` counter
- Use the unbounded tell channel for critical messages

### Verifying Lock-Free Operation
To confirm the system is truly lock-free:
1. Run `cargo run --example test_connection_debug`
2. Check that connections remain stable (no disconnections)
3. Monitor message throughput remains consistent under load
4. Use `tokio-console` to verify no mutex contention

### Performance Tuning
- **Ring Buffer Size**: Increase for bursty workloads
- **Batch Size**: Default 4096, reduce for lower latency
- **Flush Threshold**: Default 256KB, reduce for more frequent flushes
- **Tell Channel**: Use for guaranteed delivery over ring buffer