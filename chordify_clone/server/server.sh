#!/bin/bash

# Usage: ./server.sh [replication_factor] [consistency]
# Example: ./server.sh 3 l

REPLICATION_FACTOR="$1"
CONSISTENCY="$2"

# If not provided, fallback to defaults
if [ -z "$REPLICATION_FACTOR" ]; then
  REPLICATION_FACTOR=1
fi

if [ -z "$CONSISTENCY" ]; then
  CONSISTENCY="l"
fi

echo "Starting 10 servers with k=$REPLICATION_FACTOR and consistency=$CONSISTENCY..."
# Start the first server
python main.py \
  --port 5000 \
  --replication-factor "$REPLICATION_FACTOR" \
  --replication-consistency "$CONSISTENCY" \
  &> "logs/5000_${CONSISTENCY}_${REPLICATION_FACTOR}.log" &
# echo "Server 0 on port 5000 started..."
sleep 2  # Give the first server time to stabilize

# Start servers 1..9, bootstrap to first server
for i in {1..9}; do
  port=$((5000 + i))
  python main.py \
    --bootstrap-host 127.0.0.1 \
    --bootstrap-port 5000 \
    --port "$port" \
    --replication-factor "$REPLICATION_FACTOR" \
    --replication-consistency "$CONSISTENCY"  \
    &> "logs/${port}_${CONSISTENCY}_${REPLICATION_FACTOR}.log" &

#   echo "Server $i on port $port started..."
  sleep 1
done

echo "All servers started."
