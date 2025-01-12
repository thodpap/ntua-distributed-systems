#!/bin/bash

# Usage: ./run_requests.sh
# Make sure servers are running with k=3.

echo "Running mixed insert/query requests (k=3)..."

START_TIME=$(date +%s%N) # Start time in nanoseconds

for i in {0..9}; do
  port=$((5000 + i))

  (
    while read -r line; do
      # line is e.g. "INSERT, MySong" or "QUERY, MySong"
        # Convert any newline to a null char, then use xargs -0
        command=$(printf '%s\0' "$line" | cut -d',' -f1 | xargs -0)
        value=$(printf '%s\0' "$line"   | cut -d',' -f2- | xargs -0)


      if [ "$command" = "INSERT" ]; then
        python3 cli.py --host 127.0.0.1 --port "$port" INSERT "$value"
      elif [ "$command" = "QUERY" ]; then
        RESPONSE=$(python3 cli.py --host 127.0.0.1 --port "$port" QUERY "$value")
        echo "Query for '$value' returned: $RESPONSE" >> "responses_0${i}.log"
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

