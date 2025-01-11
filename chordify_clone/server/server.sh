#!/bin/bash
# Start the first server in the background
python main.py --port 5000 &
echo "Starting the first server on port 5000..."

# Give the first server a moment to fully start
sleep 2

# Now launch the others one by one, waiting 1s between each
python main.py --bootstrap_host 127.0.0.1 --bootstrap_port 5000 --port 5001 &
sleep 1
python main.py --bootstrap_host 127.0.0.1 --bootstrap_port 5000 --port 5002 &
sleep 1
python main.py --bootstrap_host 127.0.0.1 --bootstrap_port 5000 --port 5003 &
sleep 1