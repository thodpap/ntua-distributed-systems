import argparse
import json
import socket
from pprint import pprint

BUFF_SIZE = 1024

def send_request(host, port, message_dict):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))

        # 1) Convert message_dict to bytes
        data_bytes = json.dumps(message_dict).encode('utf-8')

        # 2) Send the length of the data (8 bytes, big-endian)
        data_length = len(data_bytes)
        s.sendall(data_length.to_bytes(8, byteorder='big'))

        # 3) Send the data in chunks
        bytes_sent = 0
        while bytes_sent < data_length:
            chunk = data_bytes[bytes_sent : bytes_sent + BUFF_SIZE]
            s.sendall(chunk)
            bytes_sent += len(chunk)

        # 4) Read the length of the response (8 bytes)
        response_length_bytes = s.recv(8)
        if not response_length_bytes:
            s.close()
            return {}
        
        response_length = int.from_bytes(response_length_bytes, byteorder='big')
        # Read the entire response
        response_data = b''
        while len(response_data) < response_length:
            chunk = s.recv(BUFF_SIZE)
            if not chunk:
                break
            response_data += chunk

        s.close()

        # 5) Deserialize the response and return
        return json.loads(response_data.decode('utf-8'))

    except Exception as e:
        print(f"[_send] Exception: {e}")
        return {}

def main():
    parser = argparse.ArgumentParser(description="CLI to interact with a Chord DHT node.")
    parser.add_argument("command", type=str, help="Command to run: insert, delete, query, depart, overlay, info, help")
    parser.add_argument("key_or_value", type=str, nargs="?", help="Key (for query, insert or delete), or unused for INFO")
    parser.add_argument("value", type=str, nargs="?", help="Value (for insert)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Node host")
    parser.add_argument("--port", type=int, default=5000, help="Node port")
    parser.add_argument("--remove-output", dest="remove_output", type=bool, default=True, help="Remove output (for speadup)")

    args = parser.parse_args()
    
    if args.value is None:
        args.value = f"{args.host}:{args.port}"
    
    cmd = args.command.upper()
    if cmd == "INSERT":
        if not args.key_or_value or not args.value:
            pprint(f"Usage: cli.py INSERT <key> <value> [--host <host>] [--port <port>]")
            return
        request = {
            "cmd": "PUT",
            "key": args.key_or_value,
            "value": args.value
        }
        response = send_request(args.host, args.port, request)
        if not args.remove_output:
            pprint(f"PUT response:")
            pprint(response)

    elif cmd == "QUERY":
        if not args.key_or_value:
            pprint(f"Usage: cli.py query <key> [--host <host>] [--port <port>]")
            return
        request = {
            "cmd": "GET",
            "key": args.key_or_value
        }
        response = send_request(args.host, args.port, request)
        if not args.remove_output:
            print(f"GET response:")
            pprint(response)
    elif cmd == "INFO":
        # Show node info: ID, predecessor, successor, finger table
        request = {
            "cmd": "GET_NODE_INFO"
        }
        response = send_request(args.host, args.port, request)
        if not args.remove_output:
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
        if not args.remove_output:
            pprint(f"DELETE response:")
            pprint(response)
    elif cmd == "OVERLAY":
        request = {
            "cmd": "GET_OVERLAY"
        }
        response = send_request(args.host, args.port, request)
        if not args.remove_output:
            pprint(f"OVERLAY response:")
            pprint(response)
    elif cmd == "DEPART":
        request = {
            "cmd": "DEPART"
        }
        response = send_request(args.host, args.port, request)
        if not args.remove_output:
            pprint(f"DEPART response:")
            pprint(response)
    elif cmd == "HELP":
        pprint("Commands:")
        pprint("  insert <key> <value> [--host <host>] [--port <port>]")
        pprint("  query <key> [--host <host>] [--port <port>]")
        pprint("  delete <key> [--host <host>] [--port <port>]")
        pprint("  overlay [--host <host>] [--port <port>]")
        pprint("  info [--host <host>] [--port <port>]")
        pprint("  depart [--host <host>] [--port <port>]")
        
    else:
        pprint(f"Unknown command: {cmd}")
if __name__ == "__main__":
    main()
