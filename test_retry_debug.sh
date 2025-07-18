#!/bin/bash

echo "Testing peer retry behavior..."
echo "Starting Node A (will stay running)..."

# Start Node A in background
cargo run --example test_manual_a 2>&1 | tee node_a_debug.log &
NODE_A_PID=$!

echo "Waiting for Node A to start..."
sleep 3

echo "Starting Node B (will be killed after 3 seconds)..."
# Start Node B
cargo run --example test_manual_b 2>&1 | tee node_b_debug.log &
NODE_B_PID=$!

echo "Letting nodes communicate for 3 seconds..."
sleep 3

echo "Killing Node B to simulate disconnection..."
kill $NODE_B_PID

echo "Waiting to see retry attempts (should happen after 5 seconds)..."
echo "Watch the logs for retry attempts at ~5s intervals..."

# Keep Node A running to observe retry behavior
wait $NODE_A_PID