#!/bin/bash

# Usage: ./run_requests.sh
# Make sure servers are running with k=3.
CONSISTENCY="$1"

echo "Running mixed insert/query requests (k=3)..."

START_TIME=$(date +%s%N) # Start time in nanoseconds

for i in {0..9}; do
  port=$((5000 + i))

  (
    while IFS=, read -r command value rest; do
      # Trim leading and trailing whitespaces
      command=$(echo "$command" | xargs -0)
      value=$(echo "$value" | xargs -0)
      rest=$(echo "$rest" | xargs -0)
      log_file="responses/requests_log_0${i}.txt"
      if [ "$CONSISTENCY" = "l" ]; then
        log_file="responses/linear/requests_log_0${i}.txt"
      else
        log_file="responses/eventual/requests_log_0${i}.txt"
      fi

      if [ "$command" = "insert" ]; then
        # Log INSERT command outputs
        {
          echo "Executing INSERT: $value, $rest"
          python3 cli.py --host 127.0.0.1 --port "$port" INSERT "$value" "$rest" --show-output
        } >> "$log_file" 2>&1
      elif [ "$command" = "query" ]; then
        # Log QUERY command outputs
        {
          echo "Executing QUERY: $value"
          RESPONSE=$(python3 cli.py --host 127.0.0.1 --port "$port" QUERY "$value" --show-output)
          echo "Query for '$value' returned: $RESPONSE"
        } >> "$log_file" 2>&1
      fi
    done < "requests/requests_0${i}.txt"
  ) &
done

wait

END_TIME=$(date +%s%N)   # End time in nanoseconds
TOTAL_TIME=$((END_TIME - START_TIME)) # Total time in nanoseconds

# Convert nanoseconds to seconds for easier readability
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)
echo "All mixed requests completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."

# Calculate throughput which is the time per 500 requests
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT requests/second."