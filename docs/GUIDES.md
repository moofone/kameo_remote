# Feature Guards in kameo_remote

## What is a Guard?

A **guard** is a Rust pattern that uses the type system to control access to operations. We use **feature guards** to prevent certain methods from being available in production code.

## RegistryMessageFrame::to_owned() Guard

The `to_owned()` method on `RegistryMessageFrame` is **guarded by the `test-helpers` feature**.

### Why This Guard Exists

- **Zero-Copy Philosophy**: Production code should use `archived()` for zero-copy access
- **Prevent Allocations**: Calling `to_owned()` allocates a new `RegistryMessage`, which defeats our zero-copy optimization
- **Catch Errors at Compile Time**: If you try to use `to_owned()` in production, **it won't compile**

### How to Use

#### In Tests (✅ Allowed)
```rust
#[cfg(feature = "test-helpers")]  // or just run cargo test --features test-helpers
fn test_something() {
    let frame = RegistryMessageFrame::new(bytes).unwrap();
    let owned = frame.to_owned().unwrap();  // ✅ OK in tests with feature
}
```

#### In Production (❌ Blocked)
```rust
fn production_code(frame: &RegistryMessageFrame) {
    // ❌ This won't compile!
    // let owned = frame.to_owned().unwrap();
    // Error: no method named `to_owned` found for struct `RegistryMessageFrame`

    // ✅ Use zero-copy access instead
    let archived = frame.archived().unwrap();
    let actor_id = archived.actor_id();
}
```

### Running Tests

```bash
# Tests that use to_owned() MUST enable the feature
cargo test --features test-helpers

# Regular tests can run without it
cargo test
```

### Compilation Errors

If you see this error:
```
error[E0599]: no method named `to_owned` found for struct `RegistryMessageFrame`
```

**Solution**: Either:
1. Use zero-copy `archived()` instead (preferred for production)
2. Enable `test-helpers` feature (only for tests)

### Adding New Guards

To guard a method with a feature:

```rust
#[cfg(feature = "your-feature-name")]
pub fn dangerous_method(&self) -> Result<Something> {
    // Only exists when feature is enabled
}
```

### See It In Action

Run the guard demo:
```bash
# Fails - guard blocks to_owned() without feature
cargo check --example test_guard_demo

# Passes - feature enables the method
cargo check --example test_guard_demo --features test-helpers
```

## Streaming Wire Contract

Streaming large actor payloads now relies on the `streaming` capability negotiated via the Hello handshake. All peers advertise `Feature::Streaming`, and the connection pool refuses to enter the streaming path if the capability intersection is missing.

### Frame Layout

- `StreamStart` frame: `[len:4][type:1][corr:2][pad:9][StreamDescriptor:40]`
- `StreamData` frame: `[len:4][type:1][corr:2][pad:9][payload:N]`
- Optional `StreamEnd` may follow, but completion is determined by `payload_len` from the descriptor.
- The descriptor is 40 bytes, aligned at offset 16, and uses big-endian fields: `stream_id`, `payload_len`, `chunk_len`, `type_hash`, `actor_id`, `flags`, `reserved`.

### Receiver Semantics

- Only one stream may be active per connection. `StreamData` is in-order and directly advances the write cursor; there is no chunk index.
- `streaming::StreamAssembler` owns the shared implementation used by both inbound (`handle.rs`) and outgoing (`connection_pool.rs`) readers. It enforces ordering, byte-count limits (via `GossipConfig.max_message_size`), and stale timeouts.
- Any attempt to send data before `StreamStart`, start a second stream, or overflow the declared `payload_len` results in `GossipError::InvalidStreamFrame`.

### Sender Semantics

- `ConnectionHandle::stream_large_message*` checks the negotiated capability before emitting StreamStart/Data/End frames.
- Legacy streaming handlers/assemblies have been removed; the zero-copy streaming path is the sole implementation going forward.

### Testing & Coverage

- Unit tests in `src/streaming.rs` cover descriptor layout, encode/decode helpers, assembler invariants, and ordering/overflow behavior.
- `tests/streaming_contract.rs` provides a wire-level contract test that snapshots the emitted frame bytes and validates assembler completion/error cases.
- Run `./scripts/coverage.sh` after adding new streaming code paths to keep the sprint modules at 100% coverage.

## Zero-Copy Tell APIs (0.2.0)

- The `legacy_tell_bytes*` gates, `TellMessage`, and `tell_msg!` macro have been deleted. Only `ConnectionHandle::tell_bytes`, `tell_bytes_batch`, `tell_typed`, and streaming helpers remain.
- These entry points are tagged as `// CRITICAL_PATH` and emit telemetry via `kameo_remote::telemetry::gossip_zero_copy::record_outbound_frame`, so `tests/gossip_zero_copy_observability.rs` can assert that every tell stays zero-copy.
- **Migration tip:** replace `conn.tell(TellMessage::batch(vec![...]))` with `conn.tell_bytes_batch(&[bytes.clone(), ...])` or `conn.tell_typed(&value)` depending on whether you have raw bytes or typed payloads.
- Batch ask helpers (`ask_batch*`) and delegation entry points (`ask_with_reply_to`, `send_binary_message`) now require owned `Bytes`. Build your request buffers upfront—`let requests: Vec<Bytes> = payloads.iter().cloned().collect();`—and pass slices of `Bytes` into the connection pool to avoid implicit copies.

```rust
use bytes::Bytes;
use kameo_remote::connection_pool::ConnectionHandle;

async fn send_price_update(conn: &ConnectionHandle, price_blob: &[u8]) -> kameo_remote::Result<()> {
    // Raw Bytes path (no copies)
    conn.tell_bytes(Bytes::copy_from_slice(price_blob)).await
}

async fn send_typed_update<T: kameo_remote::typed::WireEncode>(
    conn: &ConnectionHandle,
    value: &T,
) -> kameo_remote::Result<()> {
    conn.tell_typed(value).await
}
```

- Need batch semantics? Build a `Vec<Bytes>` once, pass it to `tell_bytes_batch`, and rely on the zero-copy tests (`connection_pool::tests::tell_bytes_batch_preserves_each_pointer`) to prove no extra allocations occur.
- When validating changes, run `cargo test tests::tell_bytes_increments_zero_copy_telemetry -- --nocapture` or execute the throughput suite (`cargo test --test streaming_vs_tell -- --nocapture`) to capture telemetry and performance artifacts for Sprint 2.
