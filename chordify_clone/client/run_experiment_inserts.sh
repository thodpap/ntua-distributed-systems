#!/bin/bash

# run_experiment_inserts.sh
# This will run the insertion experiment for (k=1,3,5) x (l,e).

replication_factors=(1 3 5)
consistencies=("l") # ("l" "e")

for rf in "${replication_factors[@]}"; do
  for c in "${consistencies[@]}"; do
    echo "==============================================="
    echo "Starting servers with k=$rf consistency=$c ..."
    pushd ../server/
    ./server.sh "$rf" "$c"
    sleep 5
    popd
    
    echo "Running insertion experiment..."
    ./run_inserts.sh
    
    echo "Killing all servers..."
    pkill -f "python main.py"
    echo "==============================================="
    sleep 5
  done
done
