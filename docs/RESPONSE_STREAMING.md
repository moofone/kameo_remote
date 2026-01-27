# Transparent Response Streaming - Architecture Summary

## Public API (Clean & Simple)

```rust
// From kameo actor calling a distributed actor:
let response = conn
    .ask(Bytes::from("my request"))
    .await?;
```

That's it! Just `ask()` - nothing else needed.

## What Happens Under the Hood

### Request Side (Not Streamed)
- Request sent via ring buffer (fast path)
- No streaming needed for requests

### Response Side (Automatically Streamed)
1. **Small response** (≤ threshold, typically ~256KB):
   - Sent via ring buffer
   - Direct correlation match
   - Zero-copy delivery

2. **Large response** (> threshold, e.g., 2MB):
   - **Sender**: `send_response_bytes()` detects large response
   - **Sender**: Automatically streams in chunks with correlation_id
   - **Receiver**: Detects correlation_id has pending ask
   - **Receiver**: Routes chunks to streaming accumulator
   - **Receiver**: Zero-copy reassembly into single `Bytes`
   - **Delivery**: `ask()` returns single `Bytes` - transparent!

## Implementation Details

### Sender Side (`send_response_bytes`)
```rust
pub async fn send_response_bytes(&self, correlation_id: u16, payload: Bytes) -> Result<()> {
    let threshold = self.streaming_threshold();

    if payload.len() > threshold {
        // Stream the response automatically
        self.stream_response_with_correlation(payload, correlation_id).await
    } else {
        // Send directly (ring buffer)
        write_header_and_payload_control_inline(header, 16, payload).await
    }
}
```

### Receiver Side (CorrelationTracker)
- `start_streaming()` - Initialize accumulator
- `add_streaming_chunk()` - Add chunks (zero-copy)
- `complete_streaming()` - Reassemble into single `Bytes`

### Message Routing
```rust
// In process_reader_streaming_frame():
let is_response_stream = correlation.has_pending(correlation_id);

if is_response_stream {
    // Route to correlation tracker for response streaming
    correlation.add_streaming_chunk(correlation_id, chunk);
} else {
    // Route to stream assembler for tell streaming
    assembler.read_data_direct(reader, chunk_len).await;
}
```

## Zero-Copy Guarantees

1. **Request**: No copying - sent as-is
2. **Response chunks**: Stored as `Bytes` slices (no copy)
3. **Reassembly**: `BytesMut::freeze()` - only allocates once for final buffer
4. **Delivery**: Single `Bytes` returned to caller

## Usage Examples

### Simple Ask (Automatic Streaming)
```rust
// Small request, large 2MB response
let response = conn
    .ask(Bytes::from("get_all_data"))
    .await?;

// ^^^ Returns 2MB response transparently
// No API changes needed!
```

### ReplyTo Handler (Automatic Streaming)
```rust
impl ActorMessageHandler for MyHandler {
    fn handle_actor_message(&self, ...) -> ActorMessageFuture<'_> {
        Box::pin(async move {
            // Large response - will be streamed automatically
            let large_response = vec![b'x'; 2 * 1024 * 1024];
            reply_to.reply(Bytes::from(large_response)).await?;
            Ok(())
        })
    }
}
```

## Key Features

✅ **Transparent** - No API changes needed
✅ **Zero-copy** - Minimal allocations
✅ **Automatic** - Threshold-based decision
✅ **Bidirectional awareness** - Routes correctly based on ask vs tell
✅ **Backwards compatible** - `ask_actor()` deprecated but works

## Migration Path

### Old Code (Still Works)
```rust
conn.ask_actor(actor_id, type_hash, payload, timeout).await?;
```

### New Code (Recommended)
```rust
conn.ask(payload).await?;
```

The old `ask_actor()` API is deprecated but still functional for backwards compatibility.
