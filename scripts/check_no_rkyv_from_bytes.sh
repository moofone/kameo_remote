#!/bin/bash
set -euo pipefail

if rg -n "rkyv::from_bytes" src; then
  echo "ERROR: rkyv::from_bytes found in src/ (runtime code)."
  exit 1
fi

echo "OK: no rkyv::from_bytes usage found in src/"
