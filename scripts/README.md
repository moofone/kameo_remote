# Gossip Zero-Copy Validation Scripts

This directory contains the tooling referenced throughout `spec/GOSSIP_ZERO_COPY.md`
and `sprints/GOSSIP_ZERO_COPY/sprint_3.md`. The scripts prove that inbound gossip
traffic stays zero-copy, telemetry counters are emitted, and Critical Path sections
stay fully covered.

## Primary workflow

| Script | Purpose |
| ------ | ------- |
| `full_validation.sh` | Runs the end-to-end validation playbook (tests, allocation guards, telemetry smoke test, coverage gates). |
| `check_critical_coverage.sh <plan_path>` | Fails if any `CRITICAL_PATH` annotation lacks coverage. Requires `reports/coverage.lcov`. |
| `analyze_coverage_gaps.sh <plan_path>` | Generates `reports/coverage_gaps_<timestamp>.md` summarizing uncovered lines. |
| `coverage.sh` | Convenience wrapper around `cargo llvm-cov --workspace --lcov`. |
| `check_forbidden_copy_patterns.sh` | Fails zero-copy guard when forbidden buffering patterns appear in critical files. |
| `check_no_rkyv_from_bytes.sh` | Prevents accidental runtime calls to `rkyv::from_bytes` outside the annotated safe zones. |

### Running the full suite

```bash
./scripts/full_validation.sh
```

This script automatically:

1. Runs `cargo test --all`.
2. Enforces the `rkyv::from_bytes` guard.
3. Executes the gossip allocation guard in both debug and release profiles.
4. Re-runs the TLS pointer identity proofs (`gossip_frame_uses_zero_copy_buffer`, etc.).
5. Performs the telemetry smoke test (`tests/gossip_zero_copy_observability.rs`).
6. Invokes the Critical Path coverage gate and coverage gap report with plan `sprints/GOSSIP_ZERO_COPY/sprint_3.md`.

The log is written to `logs/validation_<timestamp>.txt`.

### Checking coverage gates manually

```bash
./scripts/check_critical_coverage.sh sprints/GOSSIP_ZERO_COPY/sprint_3.md
./scripts/analyze_coverage_gaps.sh sprints/GOSSIP_ZERO_COPY/sprint_3.md
```

Both scripts auto-run `scripts/coverage.sh` if `reports/coverage.lcov` is missing.

## Definition of done for Sprint 3 tooling

- Full validation fails on any gossip zero-copy regression.
- `CRITICAL_PATH` lines in `src/handle.rs` and `src/connection_pool.rs` are enforced via `check_critical_coverage.sh`.
- Coverage gap reports are attached to the sprint artifacts (see `reports/coverage_gaps_*.md`).
- Telemetry counters (`gossip_zero_copy_frames_total`, `gossip_zero_copy_alignment_failures_total`) are visible via tracing logs and the integration smoke test.
