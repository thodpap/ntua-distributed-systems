#!/bin/bash

# Usage: ./run_inserts.sh
# Make sure servers are already running.

START_TIME=$(date +%s%N) # Start time in nanoseconds

# Fire off 10 parallel processes (one for each node/file)
for i in {0..9}; do
  port=$((5000 + i))

  # For each file insert_0{i}.txt, we do line-by-line inserts
  (
    while read -r song_title; do
      # Example CLI usage:
      python3 cli.py --host 127.0.0.1 --port "$port" INSERT "$song_title"
    done < "insert/insert_0${i}_part.txt"
  ) &

done

# Wait for all background jobs to finish
wait

END_TIME=$(date +%s%N)   # End time in nanoseconds
TOTAL_TIME=$((END_TIME - START_TIME)) # Total time in nanoseconds

# Convert nanoseconds to seconds for easier readability
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)
echo "All inserts completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."

# Calculate throughput which is the time per 500 requests
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT requests/second."