import socket
import threading
import json
from typing import Optional
from utils import chord_hash, M, in_interval
import sys
import signal
import time

BUFF_SIZE = 1024

class ChordNode:
    def __init__(self, host: str, port: int, bootstrap_host: Optional[str] = None, bootstrap_port: Optional[int] = None):
        self.host = host
        self.port = port

        # Each node has an ID in the range [0, 2^M)
        self.node_id = chord_hash(f"{host}:{port}")

        # Pointers
        self.successor = (self.node_id, self.host, self.port)
        self.predecessor = None

        # Finger table
        self.finger_table = [(None, None)] * M
        
        # Local data store: {key_id -> {
        #  key -> [values]
        # }
        self.data_store = {}

        # Start listening
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(5)

        print(f"[Node {self.node_id}] Listening on {self.host}:{self.port}...")

        # If we have a bootstrap node, join the ring
        if bootstrap_host and bootstrap_port:
            self.join(bootstrap_host, bootstrap_port)
        else:
            print("No bootstrap node provided, starting new ring.")
            self.successor = (self.node_id, self.host, self.port)
            self.predecessor = (self.node_id, self.host, self.port)

        # Start a background thread to periodically stabilize & fix fingers to fix broken nodes
        t_stabilize = threading.Thread(target=self._periodic_tasks, daemon=True)
        t_stabilize.start()

        # Start accepting connections
        t_accept = threading.Thread(target=self.accept_connections, daemon=True)
        t_accept.start()

    def _periodic_tasks(self):
        """ Periodically call stabilize and fix_fingers. """
        while True:
            # self.stabilize()
            self.fix_fingers()
            time.sleep(2)  # frequency of these checks

    def accept_connections(self):
        while True:
            try:
                client_sock, addr = self.server_sock.accept()
                t = threading.Thread(target=self.handle_connection, args=(client_sock, addr))
                t.daemon = True
                t.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

                
    def handle_connection(self, client_sock, addr):
        """
        Handle incoming messages (commands) from other nodes or a client.
        """
        try:
            data = client_sock.recv(BUFF_SIZE).decode('utf-8')
            if not data:
                client_sock.close()
                return
            try:
                request = json.loads(data)
            except json.JSONDecodeError:
                client_sock.close()
                return

            cmd = request.get("cmd")
            response = {}

            if cmd == "GET_NODE_INFO":
                response["node_id"] = self.node_id
                response["successor"] = self.successor
                response["predecessor"] = self.predecessor
                response["finger_table"] = self.finger_table

            elif cmd == "FIND_SUCCESSOR":
                key_id = request["key_id"]
                successor_info, predecessor_info = self.find_successor(key_id)
                response["successor"] = successor_info
                response["predecessor"] = predecessor_info

            elif cmd == "NOTIFY":
                # Another node calls 'notify' on us, claiming it might be our predecessor
                candidate = request["candidate"]
                self.notify(candidate)
                response["status"] = "OK"

            elif cmd == "PUT":
                key = request["key"]
                value = request["value"]
                self.chord_put(key, value)
                response["status"] = "OK"

            elif cmd == "GET":
                key = request["key"]
                if key == "*":
                    # Get all keys
                    start_node_id = request.get("start_node_id", self.node_id)
                    result = self.chord_get_all(start_node_id)
                else:
                    result = self.chord_get(key)
                response["value"] = result

            elif cmd == "DELETE":
                key = request.get("key", None)
                value = request.get("value", None)
                if not key or not value:
                    response["status"] = "WRONG_PARAMS"
                else:
                    response["status"] = self.chord_delete(key, value)

            elif cmd == "JOIN":
                new_node_host = request["host"]
                new_node_port = request["port"]
                print(f"[Node {self.node_id}] New node wants to join via {new_node_host}:{new_node_port}")
                succ, pred = self.chord_join(new_node_host, new_node_port)
                response["successor"] = succ
                response["predecessor"] = pred

            elif cmd == "DEPART":
                self.depart()
                response["status"] = "departing"

            elif cmd == "UPDATE_SUCCESSOR":
                print(f"[Node {self.node_id}] Updating successor to {request['new_succ_id']}, {request}")
                self._update_successor((request["new_succ_id"],
                                        request["new_succ_host"],
                                        request["new_succ_port"]))
                response["status"] = "OK"

            elif cmd == "UPDATE_PREDECESSOR":
                print(f"[Node {self.node_id}] Updating predecessor to {request['new_pred_id']}, {request}")
                self._update_predecessor((request["new_pred_id"],
                                        request["new_pred_host"],
                                        request["new_pred_port"]))
                response["status"] = "OK"

            elif cmd == "TRANSFER_KEYS":
                new_node_id = request["new_node_id"]
                keys_to_give = self._find_keys_for_node(new_node_id)
                response["keys"] = keys_to_give
                # Remove them from local store
                for k_str in keys_to_give.keys():
                    k_int = int(k_str)
                    self.data_store.pop(k_int, None)

            client_sock.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(e)
        finally:
            client_sock.close()

    def join(self, bootstrap_host: str, bootstrap_port: int):
        """Join the ring via a known bootstrap node."""
        successor_info = self._send(bootstrap_host, bootstrap_port, {
            "cmd": "JOIN",
            "host": self.host,
            "port": self.port
        })
        if "successor" in successor_info and "predecessor" in successor_info:
            print(f"[Node {self.node_id}] On join the ring, got info: {successor_info}") 
            self.successor = tuple(successor_info["successor"])
            self.predecessor = tuple(successor_info["predecessor"])
            
            print("notify predecessor and successor")
            # Notify predecessor and notify successor
            status = self._send(self.predecessor[1], self.predecessor[2], {
                "cmd": "UPDATE_SUCCESSOR",
                "new_succ_id": self.node_id,
                "new_succ_host": self.host,
                "new_succ_port": self.port
            })
            print(f"[Node {self.node_id}] Notified predecessor {self.predecessor} of new successor status: {status}")
            status = self._send(self.successor[1], self.successor[2], {
                "cmd": "UPDATE_PREDECESSOR",
                "new_pred_id": self.node_id,
                "new_pred_host": self.host,
                "new_pred_port": self.port
            })
            print(f"[Node {self.node_id}] Notified successor {self.successor} of new predecessor status: {status}")
            self.fix_fingers()
            
        else:
            # fallback
            self.successor = (self.node_id, self.host, self.port)
            self.predecessor = (self.node_id, self.host, self.port)

        print(f"[Node {self.node_id}] Joined ring via {bootstrap_host}:{bootstrap_port} "
              f"& got successor {self.successor}")
        print(f"[Node {self.node_id}] Predecessor: {self.predecessor}")

        # NEW: Immediately notify our successor, so it can update its predecessor if needed
        if self.successor[0] != self.node_id:
            self._send(self.successor[1], self.successor[2], {
                "cmd": "NOTIFY",
                "candidate": (self.node_id, self.host, self.port)
            })

    def depart(self):
        """
        Gracefully remove this node from the Chord ring.
        1. Transfer data to successor.
        2. Notify predecessor and successor to link each other.
        3. Close server socket.
        """
        print(f"[Node {self.node_id}] Departing the ring...")
        succ_id, succ_host, succ_port = self.successor
        pred_id, pred_host, pred_port = self.predecessor if self.predecessor else (None, None, None)

        # Transfer data to successor
        for key_id, value in self.data_store.items():
            for key, itm_value in value.items():
                ret = self._send(succ_host, succ_port, {
                    "cmd": "PUT",
                    "key": key,
                    "value": list(itm_value)
                })
        self.data_store.clear()

        # Notify predecessor & successor to link each other
        if pred_id is not None and (pred_id != self.node_id):
            print(f"Notifying predecessor {pred_id} for its new successor: {succ_id}")
            self._send(pred_host, pred_port, {
                "cmd": "UPDATE_SUCCESSOR",
                "new_succ_id": succ_id,
                "new_succ_host": succ_host,
                "new_succ_port": succ_port
            })

        if succ_id != self.node_id:
            print(f"Notifying successor {succ_id} for its new predecessor: {pred_id}")
            self._send(succ_host, succ_port, {
                "cmd": "UPDATE_PREDECESSOR",
                "new_pred_id": pred_id,
                "new_pred_host": pred_host,
                "new_pred_port": pred_port
            })

        print(f"[Node {self.node_id}] Closing socket and shutting down.")
        self.server_sock.close()
        sys.exit(0)

    def stabilize(self):
        """
        Periodically verify our immediate successor
        and tell the successor about ourselves.
        """
        if self.successor is None or self.successor[0] == self.node_id:
            return

        # 1. Ask successor for its predecessor
        resp = self._send(self.successor[1], self.successor[2], {
            "cmd": "GET_NODE_INFO"
        })
        if not resp:
            return

        x = resp.get("predecessor")  # x is (x_id, x_host, x_port)
        if x:
            x_id, x_host, x_port = x
            # 2. If x is "between" (self, successor), x might be a better successor
            if in_interval(x_id, self.node_id, self.successor[0]):
                self.successor = (x_id, x_host, x_port)

        # 3. Notify our successor that we might be its predecessor
        if self.successor[0] != self.node_id:
            self._send(self.successor[1], self.successor[2], {
                "cmd": "NOTIFY",
                "candidate": (self.node_id, self.host, self.port)
            })

    def notify(self, candidate):
        """
        Called by 'candidate' node that thinks it might be our predecessor.
        We update our predecessor if 'candidate' is indeed between our old
        predecessor and ourselves.
        """
        if candidate is None:
            return
        if self.predecessor is None:
            self.predecessor = candidate
        else:
            pred_id, _, _ = self.predecessor
            cand_id, _, _ = candidate
            if in_interval(cand_id, pred_id, self.node_id):
                self.predecessor = candidate

    def find_successor(self, key_id: int):
        succ_id, succ_host, succ_port = self.successor
        if in_interval(key_id, self.node_id, succ_id, inclusive=True):
            return self.successor, (self.node_id, self.host, self.port)
        else:
            next_node = self.closest_preceding_node(key_id)
            if next_node[0] == self.node_id:
                return self.successor, (self.node_id, self.host, self.port)
            resp = self._send(next_node[1], next_node[2], {
                "cmd": "FIND_SUCCESSOR",
                "key_id": key_id
            })
            if "successor" not in resp or "predecessor" not in resp:
                return self.successor, (self.node_id, self.host, self.port)
            else:
                return tuple(resp["successor"]), tuple(resp["predecessor"])

    def closest_preceding_node(self, key_id: int):
        """
        Find the highest node in our finger table that is between
        (self.node_id, key_id) in the ring.
        """
        for i in reversed(range(M)):
            node_info = self.finger_table[i][1]
            if node_info is not None:
                nid, nhost, nport = node_info
                if in_interval(nid, self.node_id, key_id):
                    return node_info
        return (self.node_id, self.host, self.port)

    def fix_fingers(self):
        # You may wish to reduce how verbose this is in production:
        # print(f"[Node {self.node_id}] Fixing fingers...")
        for i in range(M):
            start = (self.node_id + 2**i) % (2**M)
            succ_info, prev_info = self.find_successor(start)
            self.finger_table[i] = (start, succ_info)
        
    def chord_join(self, new_node_host: str, new_node_port: int):
        """
        A new node calls `JOIN` on us. We find its successor in our ring,
        then we might update our own successor to be consistent.
        """
        new_node_id = chord_hash(f"{new_node_host}:{new_node_port}")
        succ_info, pred_info = self.find_successor(new_node_id)
        
        return succ_info, pred_info

    def chord_put(self, key: str, value: str):
        key_id = chord_hash(key)
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if node_id == self.node_id:
            print(f"[Node {self.node_id}] PUT {key}[{key_id}] -> {value}")
            if key_id not in self.data_store:
                self.data_store[key_id] = {}
            if key not in self.data_store[key_id]:
                self.data_store[key_id][key] = set()
            self.data_store[key_id][key].add(value)
        else:
            print(f"[Node {self.node_id}] Forward PUT {key} -> {value} to {node_id}")
            self._send(node_host, node_port, {
                "cmd": "PUT",
                "key": key,
                "value": value
            })

    def chord_get(self, key: str):
        key_id = chord_hash(key)
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if node_id == self.node_id:
            if key_id in self.data_store and key in self.data_store[key_id]:
                return list(self.data_store[key_id][key])
            return []
        else:
            print(f"[Node {self.node_id}] Forward GET {key} to {node_id}")
            resp = self._send(node_host, node_port, {
                "cmd": "GET",
                "key": key
            })
            return resp.get("value", [])

    def chord_get_all(self, start_node_id):
        node_id, node_host, node_port = self.successor
        if node_id == start_node_id:
            print(f"[Node {self.node_id}] GET * local: {self.data_store}")
            return {
                self.node_id: str(self.data_store)
            }
        resp = self._send(node_host, node_port, {
            "cmd": "GET",
            "key": "*",
            "start_node_id": start_node_id
        })
        result = {
            **resp["value"],
            self.node_id: str(self.data_store)
        }
        return result

    def chord_delete(self, key: str, value: str):
        key_id = chord_hash(key)
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if node_id == self.node_id:
            if (key_id in self.data_store and
                key in self.data_store[key_id] and
                value in self.data_store[key_id][key]):
                self.data_store[key_id][key].remove(value)
                if not self.data_store[key_id][key]:
                    self.data_store[key_id].pop(key)
                if not self.data_store[key_id]:
                    self.data_store.pop(key_id)
                return "OK"
            return "NOT_FOUND"
        else:
            resp = self._send(node_host, node_port, {
                "cmd": "DELETE",
                "key": key,
                "value": value
            })
            return resp.get("status", "ERROR")

    def _send(self, host, port, message_dict):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.sendall(json.dumps(message_dict).encode('utf-8'))
            resp_data = s.recv(BUFF_SIZE)
            s.close()
            if resp_data:
                return json.loads(resp_data.decode('utf-8'))
            else:
                return {}
        except:
            return {}

    def _update_successor(self, new_successor):
        self.successor = tuple(new_successor)

    def _update_predecessor(self, new_predecessor):
        self.predecessor = tuple(new_predecessor)

    def _acquire_responsible_keys(self):
        """If you wanted to explicitly fetch keys from your successor."""
        succ_id, succ_host, succ_port = self.successor
        request = {
            "cmd": "TRANSFER_KEYS",
            "new_node_id": self.node_id
        }
        resp = self._send(succ_host, succ_port, request)
        keys_to_move = resp.get("keys", {})
        print(f"[Node {self.node_id}] Acquiring keys from {succ_id}: {keys_to_move}")
        for k_int, val in keys_to_move.items():
            self.data_store[k_int] = val

    def _find_keys_for_node(self, new_node_id: int) -> dict:
        """Return a dict of all keys that belong to new_node_id."""
        dispatch_data = {}
        for k_int, kv_dict in self.data_store.items():
            # If k_int is in (self.node_id, new_node_id)
            if in_interval(k_int, self.node_id, new_node_id):
                dispatch_data[k_int] = kv_dict
        return dispatch_data


def run_node(host, port, bootstrap_host=None, bootstrap_port=None):
    """
    Simple wrapper to instantiate ChordNode and keep it running.
    """
    node = ChordNode(host, port, bootstrap_host, bootstrap_port)

    def signal_handler(sig, frame):
        print('Shutting down node...')
        node.depart()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Block main thread to keep the node alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--bootstrap_host", type=str, default=None)
    parser.add_argument("--bootstrap_port", type=int, default=None)
    args = parser.parse_args()

    run_node(
        host=args.host,
        port=args.port,
        bootstrap_host=args.bootstrap_host,
        bootstrap_port=args.bootstrap_port
    )
