# 🛡️ Rust Feature Guards: Quick Start Guide

## What is a "Guard"?

A **guard** is a compile-time safety mechanism that **prevents code from compiling** unless certain conditions are met.

## Your Guard in Action

```rust
#[cfg(any(test, feature = "test-helpers"))]
pub fn to_owned(&self) -> Result<RegistryMessage> {
    // This method ONLY EXISTS in:
    // 1. Test modules (#[cfg(test)])
    // 2. When --features test-helpers is used
}
```

## See It Work

### ❌ Production Code BLOCKED
```bash
$ cargo check --example production_guard_test
error[E0599]: no method named `to_owned` found for struct `RegistryMessageFrame`
 --> examples/production_guard_test.rs:8:35
  |
8 |     let _owned = frame.to_owned().unwrap();
  |                                   ^^^^^^ method not found
```

### ✅ Test Code WORKS
```bash
$ cargo test
test result: ok. 152 passed
```

## Why Use Guards?

### Before: Can Make Mistakes
```rust
// ❌ Oops! I just allocated when I shouldn't have
let msg = frame.to_owned().unwrap();
let id = msg.actor_id;
```

### After: Compiler Catches Mistakes
```rust
// ❌ Won't even compile!
let msg = frame.to_owned().unwrap();
// ERROR: no method named `to_owned'

// ✅ Forces correct zero-copy pattern
let msg = frame.archived().unwrap();
let id = msg.actor_id();
```

## Quick Reference

| Scenario | to_owned() Available? | How to Enable |
|----------|---------------------|---------------|
| Production code | ❌ No | Cannot - use `archived()` instead |
| Unit tests | ✅ Yes | Automatic (in `#[cfg(test)]` modules) |
| Integration tests | ✅ Yes | Automatic (in `tests/` directory) |
| Examples | ❌ No | Use `--features test-helpers` |
| Benches | ❌ No | Use `--features test-helpers` |

## Gossip Zero-Copy Invariants (Sprint 2+)

- Zero-copy gossip buffers are **always** enabled in production builds. The legacy `Vec<u8>` receive path has been removed as of Sprint 2.
- Tests may still leverage `test_helpers::record_raw_payload` to capture frames, but there is no longer a runtime toggle to downgrade the buffer strategy.
- If allocator guards ever detect a regression, treat it as a blocker—not something to be bypassed with an env flag. File a sprint/bug immediately.

**Default:** Gossip receive enforces aligned pooled buffers and pointer identity proofs by default. No guard or feature flag exists to opt out.

### Validation workflow (Sprint 3)

| Task | Command |
| ---- | ------- |
| Run the entire validation playbook | `./scripts/full_validation.sh` |
| Enforce CRITICAL_PATH coverage | `./scripts/check_critical_coverage.sh sprints/GOSSIP_ZERO_COPY/sprint_3.md` |
| Produce a coverage gap report | `./scripts/analyze_coverage_gaps.sh sprints/GOSSIP_ZERO_COPY/sprint_3.md` |
| Inspect telemetry counters | `kameo_remote::telemetry::gossip_zero_copy::snapshot()` (from tests/tools) |

Counters exposed via `telemetry::gossip_zero_copy` include:
- `gossip_zero_copy_frames_total` (both inbound/outbound frames)
- `gossip_zero_copy_alignment_failures_total` (alerts when the guard had to copy)

Use `reset()` before deterministic smoke tests (e.g., `tests/gossip_zero_copy_observability.rs`) so concurrent test runs cannot skew assertions.

## Common Commands

```bash
# Run tests (to_owned() works in tests)
cargo test

# Build library (to_owned() blocked)
cargo build --lib

# Run specific test
cargo test test_name

# Enable feature for examples/benches
cargo build --features test-helpers

# Check compilation
cargo check
```

## Real Example: Your Codebase

### File: src/registry.rs (line 257)
```rust
/// Convert to an owned RegistryMessage.
///
/// # Guard
/// This method is only available with the `test-helpers` feature (or tests)
/// to prevent accidental allocations in production code.
#[cfg(any(test, feature = "test-helpers"))]
pub fn to_owned(&self) -> Result<RegistryMessage> {
    // ...
}
```

### File: src/handle.rs (line 1998) - In Test Module ✅
```rust
#[cfg(test)]  // <-- Enables 'test' cfg
mod tests {
    #[tokio::test]
    async fn test_gossip() {
        // ✅ This works! We're in a test module
        let msg = parsed.to_owned().expect("...");
    }
}
```

### File: examples/production_guard_test.rs - Production Code ❌
```rust
use kameo_remote::registry::RegistryMessageFrame;

fn production_function(frame: &RegistryMessageFrame) {
    // ❌ This won't compile!
    let msg = frame.to_owned().unwrap();

    // ✅ Correct way:
    let msg = frame.archived().unwrap();
}
```

## Learn More

- **Detailed Guide**: `docs/FEATURE_GUARDS_EXPLAINED.md`
- **Usage Guide**: `docs/GUIDES.md`
- **Working Examples**: `examples/test_guard_demo.rs`
- **Blocked Examples**: `examples/production_guard_test.rs`

## TL;DR

- **Guard** = Compile-time safety switch
- Your guard prevents `to_owned()` in production
- Tests automatically get access via `#[cfg(test)]`
- Production code **must** use zero-copy `archived()` instead
- **Result**: Zero accidental allocations! 🎯
