#!/bin/bash

# Usage: ./run_queries.sh
# Make sure servers are already running.

START_TIME=$(date +%s%N) # Start time in nanoseconds

# Fire off 10 parallel processes (one for each node/file)
for i in {0..9}; do
  port=$((5000 + i))

  (
    while read -r song_title; do
      python3 cli.py --host 127.0.0.1 --port "$port" QUERY "$song_title"
    done < "queries/query_0${i}.txt"
  ) &

done

wait

END_TIME=$(date +%s%N)   # End time in nanoseconds
TOTAL_TIME=$((END_TIME - START_TIME)) # Total time in nanoseconds

# Convert nanoseconds to seconds for easier readability
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)
echo "All queries completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."

# Calculate throughput which is the time per 500 requests
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT requests/second."