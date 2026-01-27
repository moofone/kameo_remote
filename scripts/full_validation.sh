#!/bin/bash
# Full validation suite for the Gossip Zero-Copy rollout.
#
# This script wires together the zero-copy allocation guards, pointer identity
# proofs, telemetry smoke tests, and coverage gates required by
# sprints/GOSSIP_ZERO_COPY/sprint_3.md.
#
# Usage: ./scripts/full_validation.sh

set -euo pipefail


PLAN_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--plan)
            PLAN_PATH="$2"
            shift 2
            ;;
        *)
            if [[ -z "$PLAN_PATH" ]]; then
                PLAN_PATH="$1"
                shift
            else
                echo "Error: unexpected argument '$1'" >&2
                exit 1
            fi
            ;;
    esac
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║     ZERO-COPY VALIDATION (Legacy Cleanup Plan)               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

if [ ! -f "Cargo.toml" ]; then
    echo "Error: Must be run from the project root (Cargo.toml missing)"
    exit 1
fi

if [ -n "$PLAN_PATH" ] && [ ! -f "$PLAN_PATH" ]; then
    echo "Warning: plan file '$PLAN_PATH' not found; continuing without coverage gates"
    PLAN_PATH=""
fi

mkdir -p baselines reports logs
VALIDATION_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="logs/validation_${VALIDATION_DATE}.txt"

log_section() {
    local title="$1"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
    echo "${title}" | tee -a "${LOG_FILE}"
    echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
}

# Step 1: Run the full workspace test suite.
log_section "Step 1: cargo test --workspace"
if cargo test --all -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Workspace tests passed" | tee -a "${LOG_FILE}"
else
    echo "❌ Workspace tests failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

# Step 2: Guard against forbidden rkyv::from_bytes in runtime code.
log_section "Step 2: rkyv::from_bytes guard"
if ./scripts/check_no_rkyv_from_bytes.sh 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ No forbidden rkyv::from_bytes usage detected" | tee -a "${LOG_FILE}"
else
    echo "❌ Forbidden rkyv::from_bytes usage detected" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

# Step 3: Zero-copy allocation guards (debug profile).
log_section "Step 3: Forbidden copy guard"
if ./scripts/check_forbidden_copy_patterns.sh 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Forbidden copy patterns not detected" | tee -a "${LOG_FILE}"
else
    echo "❌ Forbidden copy guard failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

log_section "Step 4: Allocation guard (debug)"
if cargo test --test body_allocation_baseline -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Allocation guard (debug) passed" | tee -a "${LOG_FILE}"
else
    echo "❌ Allocation guard (debug) failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

# Step 4: Pointer identity proofs in the TLS reader.
log_section "Step 5: Pointer identity proofs"
POINTER_TESTS=(
    "gossip_frame_uses_zero_copy_buffer"
    "gossip_registry_payload_deserializes_from_aligned_buffer"
)
for test_name in "${POINTER_TESTS[@]}"; do
    if cargo test "${test_name}" -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ ${test_name} passed" | tee -a "${LOG_FILE}"
    else
        echo "❌ ${test_name} failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
done
echo "" | tee -a "${LOG_FILE}"

# Step 5: Telemetry smoke test to ensure counters wire through end-to-end.
log_section "Step 6: Telemetry observability smoke test"
if cargo test --test gossip_zero_copy_observability -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Telemetry smoke test passed" | tee -a "${LOG_FILE}"
else
    echo "❌ Telemetry smoke test failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

# Step 5.5: Response streaming edge case tests
log_section "Step 6.5: Response streaming edge case tests"
RESPONSE_STREAMING_TESTS=(
    "test_response_streaming_2mb"
    "test_small_response_no_streaming"
    "test_response_exactly_at_threshold"
    "test_response_just_above_threshold"
    "test_response_just_below_threshold"
    "test_multiple_concurrent_streaming_responses"
)
for test_name in "${RESPONSE_STREAMING_TESTS[@]}"; do
    if cargo test --test response_streaming "${test_name}" -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ ${test_name} passed" | tee -a "${LOG_FILE}"
    else
        echo "❌ ${test_name} failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
done
echo "" | tee -a "${LOG_FILE}"

# Step 6: Allocation guard (release profile) to catch optimizer-only regressions.
log_section "Step 7: Allocation guard (release)"
if cargo test --release --test body_allocation_baseline -- --nocapture 2>&1 | tee -a "${LOG_FILE}"; then
    echo "✅ Allocation guard (release) passed" | tee -a "${LOG_FILE}"
else
    echo "❌ Allocation guard (release) failed" | tee -a "${LOG_FILE}"
    exit 1
fi
echo "" | tee -a "${LOG_FILE}"

if [ -n "${PLAN_PATH}" ]; then
    # Step 7: Critical-path coverage gate.
    log_section "Step 8: Critical-path coverage gate"
    if ./scripts/check_critical_coverage.sh "${PLAN_PATH}" 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ CRITICAL_PATH coverage satisfied" | tee -a "${LOG_FILE}"
    else
        echo "❌ CRITICAL_PATH coverage failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
    echo "" | tee -a "${LOG_FILE}"

    # Step 8: Coverage gap analysis (produces Markdown report under reports/).
    log_section "Step 9: Coverage gap report"
    if ./scripts/analyze_coverage_gaps.sh "${PLAN_PATH}" 2>&1 | tee -a "${LOG_FILE}"; then
        echo "✅ Coverage gap report generated" | tee -a "${LOG_FILE}"
    else
        echo "❌ Coverage gap analysis failed" | tee -a "${LOG_FILE}"
        exit 1
    fi
    echo "" | tee -a "${LOG_FILE}"
else
    echo "Skipping coverage gate/report (no plan file)." | tee -a "${LOG_FILE}"
fi

echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
echo "VALIDATION COMPLETE: ${LOG_FILE}" | tee -a "${LOG_FILE}"
if [ -n "${PLAN_PATH}" ]; then
    echo "Plan reference: ${PLAN_PATH}" | tee -a "${LOG_FILE}"
else
    echo "Plan reference: <none>" | tee -a "${LOG_FILE}"
fi
echo "═══════════════════════════════════════════════════════════════" | tee -a "${LOG_FILE}"
echo ""
echo "Recommended follow-ups:"
echo "  1. Inspect ${LOG_FILE} for warnings (telemetry counters, allocation guard output)."
echo "  2. Review reports/coverage_gaps_* for actionable coverage work items."
echo "  3. Attach the validation log to the sprint report under ${PLAN_PATH}."
