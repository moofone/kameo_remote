# Kameo Remote Timing Behavior

This document explains the timing behavior of the kameo_remote gossip protocol for different scenarios.

## Key Constants

- `DEFAULT_GOSSIP_INTERVAL_SECS`: 5 seconds (configurable)
- `DEFAULT_MAX_PEER_FAILURES`: 3 attempts before marking as failed
- `DEFAULT_PEER_RETRY_SECONDS`: 5 seconds after failure
- `DEFAULT_DEAD_PEER_TIMEOUT_SECS`: 900 seconds (15 minutes)

## Scenario 1: Node A is Down, Node B is Starting

**Timeline:**
1. **T+0s**: Node B starts and tries to connect to Node A
2. **T+0-15s**: Node B attempts connection every gossip interval (5s default)
   - Attempt 1 at T+0s (during bootstrap)
   - Attempt 2 at T+5s (first gossip round)
   - Attempt 3 at T+10s (second gossip round)
3. **T+10s**: After 3 failures, Node A is marked as failed
4. **T+15s, T+20s, T+25s...**: Connection retries every 5 seconds (retry interval)
5. **T+15min**: If still failed, Node A's actors are removed (peer kept for reconnection)

## Scenario 2: Node A is Running, Node B Connects

**Timeline:**
1. **T+0s**: Node B starts and connects to Node A successfully
2. **T+0s**: Bidirectional persistent TCP connection established
3. **T+5s, T+10s, T+15s...**: Gossip exchanges every 5 seconds
4. **Continuous**: Messages flow immediately in both directions (no polling)

## Scenario 3: Node A Goes Down While Connected

**Timeline:**
1. **T+0s**: Node A shuts down, TCP connection drops
2. **T+0-20ms**: Node B detects EOF on socket (immediate detection)
3. **T+0ms**: Node B marks connection as failed locally
4. **T+100ms**: Node B queries other peers for consensus (if N > 2)
5. **T+5s, T+10s, T+15s...**: Reconnection attempts every 5 seconds
6. **T+15min**: If still down, Node A's actors removed (peer kept)

## Scenario 4: Node A Comes Back Up

**Timeline:**
1. **T+0s**: Node A restarts
2. **T+0-5s**: Node B's next retry attempt succeeds
3. **T+0s**: Connection restored, failure count reset
4. **T+5s, T+10s...**: Normal gossip resumes

## N-Node Network Behavior

### With 3+ Nodes (A, B, C)
- Each node maintains persistent connections to all known peers
- Gossip rounds are independent per node (not synchronized)
- When Node A fails:
  - B and C detect immediately (EOF on their connections)
  - B and C query each other for consensus after 100ms
  - If majority agree A is down, actors may be invalidated (configurable)
  - All nodes retry connection to A every 5 seconds

### Gossip Propagation Times
- **Normal priority**: Propagates during next gossip round (0-5s delay)
- **Immediate priority**: Propagates instantly to all connected peers (<100ms)

### Network Convergence
- **Actor discovery**: O(log N) gossip rounds due to random peer selection
- **Typical convergence**: 2-3 rounds (10-15 seconds) for full network
- **With immediate priority**: <1 second for connected components

### Connection Scaling
- Each node maintains up to `max_pooled_connections` (default: 20)
- Connections are persistent and bidirectional
- Memory usage: ~10KB per connection + message buffers
- Network usage: Minimal when idle (TCP keepalives only)

## Configuration Tips

### For Fast Detection (Gaming, Real-time)
```rust
GossipConfig {
    gossip_interval: Duration::from_millis(500),
    max_peer_failures: 2,
    peer_retry_interval: Duration::from_secs(1),
    immediate_propagation_enabled: true,
    ..Default::default()
}
```

### For Stable Networks (Microservices)
```rust
GossipConfig {
    gossip_interval: Duration::from_secs(10),
    max_peer_failures: 5,
    peer_retry_interval: Duration::from_secs(30),
    dead_peer_timeout: Duration::from_secs(3600), // 1 hour
    ..Default::default()
}
```

### For Large Networks (100+ nodes)
```rust
GossipConfig {
    max_gossip_peers: 5,  // Increase fanout
    max_pooled_connections: 50,  // More connections
    gossip_interval: Duration::from_secs(10),  // Less frequent
    ..Default::default()
}
```