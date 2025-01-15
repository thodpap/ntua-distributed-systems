#!/bin/bash
output=$(python3 cli.py --host 127.0.0.1 --port 5000 INSERT "Like a Rolling Stone")
echo "$output"
output=$(python3 cli.py --host 127.0.0.1 --port 5000 INSERT "Satisfaction")
echo "$output"

output=$(python3 cli.py --host 127.0.0.1 --port 5001 INSERT "The Message")
echo "$output"

output=$(python3 cli.py --host 127.0.0.1 --port 5001 INSERT "When Doves Cry")
echo "$output"

