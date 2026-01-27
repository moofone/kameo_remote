# Feature Guards Explained

## What is a Guard?

A **guard** in Rust is a pattern that uses the type system to control access to operations at compile time.

## Our Guard Implementation

```rust
#[cfg(any(test, feature = "test-helpers"))]
pub fn to_owned(&self) -> Result<RegistryMessage> {
    // Method only exists when:
    // 1. Running tests (#[cfg(test)] module)
    // 2. OR test-helpers feature is explicitly enabled
}
```

## Three-Tier Protection

### ✅ ALLOWED: In Test Modules
```rust
#[cfg(test)]  // <-- This enables the 'test' cfg flag
mod tests {
    use kameo_remote::registry::RegistryMessageFrame;

    #[tokio::test]
    async fn test_registry_message() {
        let frame = RegistryMessageFrame::new(bytes).unwrap();
        let owned = frame.to_owned().unwrap();  // ✅ WORKS!
    }
}
```

### ✅ ALLOWED: With Feature Flag
```bash
# Explicitly enable the feature
cargo test --features test-helpers
cargo build --features test-helpers
```

### ❌ BLOCKED: In Production Code
```rust
// src/some_module.rs (NOT in #[cfg(test)] module)
use kameo_remote::registry::RegistryMessageFrame;

pub fn production_handler(frame: &RegistryMessageFrame) {
    // ❌ COMPILER ERROR:
    // error[E0599]: no method named `to_owned` found for struct `RegistryMessageFrame`
    let owned = frame.to_owned().unwrap();

    // ✅ CORRECT: Use zero-copy access
    let archived = frame.archived().unwrap();
}
```

## How to Verify the Guard Works

### 1. Test it passes in tests
```bash
cargo test  # Tests work because they're in #[cfg(test)] modules
```

### 2. Test it's blocked in production
```bash
cargo check --example production_guard_test
# ERROR: no method named `to_owned` found
```

### 3. Test explicit feature flag
```bash
cargo check --example production_guard_test --features test-helpers
# Should work if the example was feature-gated
```

## Why This Matters

### Problem
Without guards, developers might accidentally use `to_owned()` in production:
```rust
// ❌ BAD: Allocates unnecessarily
let msg = frame.to_owned().unwrap();
let actor_id = msg.actor_id;
```

### Solution
Guard forces zero-copy patterns:
```rust
// ✅ GOOD: Zero allocation
let archived = frame.archived().unwrap();
let actor_id = archived.actor_id();
```

## Real-World Example

### Before Guard (Could Compile)
```rust
// src/registry.rs
impl RegistryMessageFrame {
    pub fn to_owned(&self) -> Result<RegistryMessage> { ... }
}

// src/production.rs
fn handle_frame(frame: &RegistryMessageFrame) {
    let msg = frame.to_owned().unwrap();  // ❌ Allocates! But compiles...
}
```

### After Guard (Cannot Compile)
```rust
// src/registry.rs
impl RegistryMessageFrame {
    #[cfg(any(test, feature = "test-helpers"))]  // Guard!
    pub fn to_owned(&self) -> Result<RegistryMessage> { ... }
}

// src/production.rs
fn handle_frame(frame: &RegistryMessageFrame) {
    let msg = frame.to_owned().unwrap();
    // ^^^ COMPILER ERROR: no method named `to_owned`

    // ✅ Forces correct pattern:
    let msg = frame.archived().unwrap();
}
```

## Other Examples of Guards

### 1. Platform-Specific Code
```rust
#[cfg(target_os = "linux")]
fn linux_only() { }

#[cfg(windows)]
fn windows_only() { }
```

### 2. Debug-Only Methods
```rust
#[cfg(debug_assertions)]
fn debug_logging(&self) { }
```

### 3. Feature-Gated APIs
```rust
#[cfg(feature = "advanced")]
fn advanced_feature(&self) { }
```

## Adding Your Own Guards

### Step 1: Define the feature in Cargo.toml
```toml
[features]
my-feature-name = []
```

### Step 2: Add the cfg attribute
```rust
#[cfg(feature = "my-feature-name")]
pub fn gated_method(&self) -> Result<()> {
    // Implementation
}
```

### Step 3: Document it
```rust
/// This method is only available with `my-feature-name` feature.
///
/// # Guard
/// Prevents accidental usage in production code.
#[cfg(feature = "my-feature-name")]
pub fn gated_method(&self) -> Result<()> {
    // Implementation
}
```

## Testing Your Guards

```bash
# 1. Verify it works with the feature
cargo build --features my-feature-name

# 2. Verify it's blocked without the feature
cargo check 2>&1 | grep "no method named"

# 3. Verify tests work
cargo test --features my-feature-name
```

## Gossip Zero-Copy Validation Runbook

Phase 3 of `spec/GOSSIP_ZERO_COPY.md` promotes the guard concept into our tooling:

- Run `./scripts/full_validation.sh` before merging any gossip changes. This script executes the allocator guard (`tests/body_allocation_baseline.rs`), TLS pointer identity proofs, `tests/gossip_zero_copy_observability.rs`, and both coverage gates with plan `sprints/GOSSIP_ZERO_COPY/sprint_3.md`.
- `./scripts/check_critical_coverage.sh sprints/GOSSIP_ZERO_COPY/sprint_3.md` fails CI if any `CRITICAL_PATH` annotation (e.g., `read_message_from_tls_reader`) drops below 100% coverage. Pair it with `./scripts/analyze_coverage_gaps.sh` to capture a Markdown report under `reports/`.
- Telemetry counters live under `kameo_remote::telemetry::gossip_zero_copy`. Operators (or tests) can call `snapshot()` to inspect `gossip_zero_copy_frames_total` / `gossip_zero_copy_alignment_failures_total`, and `reset()` keeps smoke tests deterministic.

These steps ensure the guard documentation matches our enforcement tooling—if the guard is bypassed, the validation suite turns red immediately.

## Zero-Copy Decode Only (2026-01-26)

- The former `allow-non-zero-copy` feature flag and the `decode_typed` / `deserialize_from_bytes` helpers have been removed.
- All typed payload consumers **must** use `decode_typed_archived` or `decode_typed_zero_copy`, which enforce type-hash validation (in debug builds) while keeping payloads zero-copy.
- Validation scripts (see `scripts/full_validation.sh`) now point to `sprints/LEGACY_FUNCTION_CLEANUP/sprint_3.md`; they fail if any CRITICAL_PATH decode helpers lose coverage or regress to copy-heavy patterns.

## Zero-Copy Tell Only (2026-01-26)

- Crate version `0.2.0` removes the `legacy_tell_bytes` / `_unlocked` feature gates, the `TellMessage` enum, the `tell_msg!` macro, and every legacy `tell_*`/`ask_*` helper. Cargo will now fail to build if downstream crates reference those APIs.
- The only supported send helpers are `ConnectionHandle::tell_bytes`, `tell_bytes_batch`, `tell_typed`, and the streaming variants (`stream_large_message*`). Each path is tagged with `// CRITICAL_PATH` and emits zero-copy telemetry via `kameo_remote::telemetry::gossip_zero_copy::record_outbound_frame`.
- Integration tests (`tests/gossip_zero_copy_observability.rs`) now include `tell_bytes_increments_zero_copy_telemetry`, which proves the telemetry counters advance without enabling any legacy features.
- Operators and QA can call `gossip_zero_copy::snapshot()` to validate `outbound_frames` increases after running `tell_bytes` workloads. Pair this with `tests/throughput_benchmarks.rs` or `tests/streaming_vs_tell.rs` to capture benchmark evidence before promoting changes.

## Key Takeaways

1. **Compile-Time Safety**: Guards prevent incorrect code from compiling
2. **Zero-Copy Enforcement**: Our guard ensures zero-copy patterns in production
3. **Clear Intent**: Guards make it obvious when code is test-only
4. **No Runtime Overhead**: `cfg` is purely compile-time - zero runtime cost

## Questions?

See:
- `docs/GUIDES.md` - Usage guide
- `examples/test_guard_demo.rs` - Working example
- `examples/production_guard_test.rs` - Demonstrates blocking
- `tests/guard_violation_test.rs` - Test showing guard behavior
