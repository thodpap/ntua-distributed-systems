import argparse
import json
import socket
from pprint import pprint

BUFF_SIZE = 1024

def send_request(host, port, request_dict):
    """
    Send a JSON request to the chord node and return the response dict.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        s.sendall(json.dumps(request_dict).encode('utf-8'))
        resp_data = s.recv(BUFF_SIZE)
        s.close()
        if resp_data:
            return json.loads(resp_data.decode('utf-8'))
        else:
            return {}
    except Exception as e:
        pprint(f"Error connecting to {host}:{port} -> {e}")
        return {}

def main():
    parser = argparse.ArgumentParser(description="CLI to interact with a Chord DHT node.")
    parser.add_argument("command", type=str, help="Command to run: PUT, GET, INFO, DELETE")
    parser.add_argument("key_or_value", type=str, nargs="?", help="Key (for GET/PUT), or unused for INFO")
    parser.add_argument("value", type=str, nargs="?", help="Value (for PUT)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Node host")
    parser.add_argument("--port", type=int, default=5000, help="Node port")

    args = parser.parse_args()
    
    if args.value is None:
        args.value = f"{args.host}:{args.port}"
    
    cmd = args.command.upper()
    if cmd == "PUT":
        if not args.key_or_value or not args.value:
            pprint(f"Usage: cli.py PUT <key> <value> [--host <host>] [--port <port>]")
            return
        request = {
            "cmd": "PUT",
            "key": args.key_or_value,
            "value": args.value
        }
        response = send_request(args.host, args.port, request)
        pprint(f"PUT response:")
        pprint(response)

    elif cmd == "GET":
        if not args.key_or_value:
            pprint(f"Usage: cli.py GET <key> [--host <host>] [--port <port>]")
            return
        request = {
            "cmd": "GET",
            "key": args.key_or_value
        }
        response = send_request(args.host, args.port, request)

    elif cmd == "INFO":
        # Show node info: ID, predecessor, successor, finger table
        request = {
            "cmd": "GET_NODE_INFO"
        }
        response = send_request(args.host, args.port, request)
        pprint(f"INFO response:")
        pprint(response)
    elif cmd == "DELETE":
        if not args.key_or_value:
            pprint(f"Usage: cli.py DELETE <key> [--host] [--port]")
            return
        request = {
            "cmd": "DELETE",
            "key": args.key_or_value,
            "value": args.value
        }
        response = send_request(args.host, args.port, request)
        pprint(f"DELETE response:")
        pprint(response)
    else:
        pprint(f"Unknown command: {cmd}")
if __name__ == "__main__":
    main()
