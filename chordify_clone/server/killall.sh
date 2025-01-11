#!/bin/bash

# Find all processes that start with "python main" and terminate them
echo "Searching for processes that start with 'python main'..."

# Use pgrep to find the process IDs
PIDS=$(pgrep -f "^python main")

if [ -z "$PIDS" ]; then
    echo "No processes found matching 'python main'."
else
    echo "Found processes with PIDs: $PIDS"
    # Iterate over each PID and terminate
    for PID in $PIDS; do
        echo "Terminating process with PID: $PID"
        kill -9 $PID
    done
    echo "All matching processes terminated."
fi
