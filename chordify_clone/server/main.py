# main.py

import sys
import time
import signal
import argparse
import logging
from chord_node_simple import ChordNode
from server import ChordServer
import os

# Ensure the logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure the root logger
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

def configure_logging(port):
    """
    Configure logging to write only to a file and suppress console output.
    """
    logger = logging.getLogger()
    # Setup file logging
    file_handler = logging.FileHandler(f"logs/{port}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    
    logger.addHandler(file_handler)  # Add the file handler to the logger
    logger.setLevel(logging.INFO)

def run_node(host, port, bootstrap_host=None, bootstrap_port=None, replication_factor=1, replication_consistency=None):
    """
    Instantiates a ChordNode and a ChordServer, then keeps it running.
    """
    node = ChordNode(host, port, bootstrap_host, bootstrap_port, replication_factor, replication_consistency)
    server = ChordServer(node)
    server.start()  # Start the background thread that accepts incoming connections

    def signal_handler(sig, frame):
        logging.info("[Main] Caught CTRL+C. Shutting down node...")
        node.depart()
        sys.exit(0)
        return

    signal.signal(signal.SIGINT, signal_handler)

    # Block main thread to keep the node alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--bootstrap-host", dest="bootstrap_host", type=str, default=None)
    parser.add_argument("--bootstrap-port", dest="bootstrap_port", type=int, default=None)
    parser.add_argument("--replication-factor", dest="replication_factor", type=int, default=3)
    parser.add_argument("--replication-consistency", dest="replication_consistency", type=str, default="l", help="l for linearizability or e for eventual consistency")

    args = parser.parse_args()
    
    # configure logging
    configure_logging(args.port)

    run_node(
        host=args.host,
        port=args.port,
        bootstrap_host=args.bootstrap_host,
        bootstrap_port=args.bootstrap_port,
        replication_factor=args.replication_factor,
        replication_consistency=args.replication_consistency
    )
