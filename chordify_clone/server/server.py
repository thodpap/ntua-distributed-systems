# server.py

import socket
import threading
import json
from chord_node_simple import ChordNode
from utils import BUFF_SIZE, _serialize_for_json
import sys

class ChordServer:
    def __init__(self, chord_node: ChordNode):
        """
        chord_node is an instance of ChordNode. We will listen on chord_node.host:chord_node.port
        and forward incoming requests to chord_node's logic.
        """
        self.node = chord_node
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.node.host, self.node.port))
        self.server_sock.listen(5)
        print(
            f"[ChordServer] Listening on {self.node.host}:{self.node.port} (NodeID={self.node.node_id})"
        )

    def start(self):
        """
        Start accepting incoming requests in a background thread.
        """
        t_accept = threading.Thread(target=self._accept_connections, daemon=True)
        t_accept.start()

    def _accept_connections(self):
        while True:
            try:
                client_sock, addr = self.server_sock.accept()
                t = threading.Thread(target=self._handle_connection, args=(client_sock, addr))
                t.daemon = True
                t.start()
            except Exception as e:
                print(f"[ChordServer] Error accepting connection: {e}")

    def _handle_connection(self, client_sock, addr):
        """
        Parse the request and forward to the chord_node for processing.
        """
        try:
            data = client_sock.recv(BUFF_SIZE).decode("utf-8")
            if not data:
                client_sock.close()
                return

            request = json.loads(data)
            # Decide which method on self.node to call
            response = self._dispatch(request)
            
            r_data = json.dumps(response).encode("utf-8")
            client_sock.sendall(r_data)
            
            if "status" in request and request["status"] == "departing":
                self.node.depart()
                print(f"[Node {self.node.node_id}] Closing socket and shutting down.")
                self.shutdown()
        except Exception as e:
            print("[ChordServer] Exception while handling connection:", e)
            client_sock.sendall(b"ERROR")

        finally:
            client_sock.close()

    def _dispatch(self, request):
        """
        A mapping of commands (from request) to chord_node methods.
        Return the response dictionary.
        """
        cmd = request.get("cmd")
        if cmd == "GET_NODE_INFO":
            return {
                "node_id": self.node.node_id,
                "successor": self.node.successor,
                "predecessor": self.node.predecessor
            }
        elif cmd == "FIND_SUCCESSOR":
            key_id = request["key_id"]
            successor_info, predecessor_info = self.node.find_successor(key_id)
            return {
                "successor": successor_info,
                "predecessor": predecessor_info,
            }
        elif cmd == "NOTIFY":
                # Another node calls 'notify' on us, claiming it might be our predecessor
            candidate = request["candidate"]
            self.node.notify(candidate)
            return {"status": "OK"}

        elif cmd == "PUT":
            key = request["key"]
            value = request["value"]
            start_node_id = request.get("start_node_id", self.node.node_id)
            ttl = request.get("ttl", None)
            self.node.chord_put(key, value, start_node_id, ttl)
            return {"status": "OK"}

        elif cmd == "GET":
            key = request["key"]
            if key == "*":
                # Get all keys
                start_node_id = request.get("start_node_id", self.node.node_id)
                result = self.node.chord_get_all(start_node_id)
                return {"value": result}
            
            start_node_id = request.get("start_node_id", self.node.node_id)
            ttl = request.get("ttl", None)
            result, id_ = self.node.chord_get(key, start_node_id, ttl)
            print(f"[Node {self.node.node_id}] GET {key} -> {result}")
            return {"id": id_, "value": result}

        elif cmd == "DELETE":
            key = request.get("key", None)
            value = request.get("value", None)
            if not key or not value:
                return {"status": "WRONG_PARAMS"}
            
            start_node_id = request.get("start_node_id", self.node.node_id)
            ttl = request.get("ttl", None)
            return {"status": self.node.chord_delete(key, value, start_node_id, ttl)}

        elif cmd == "JOIN":
            new_node_host = request["host"]
            new_node_port = request["port"]
            print(f"[Node {self.node.node_id}] New node wants to join via {new_node_host}:{new_node_port}")
            succ, pred = self.node.chord_join(new_node_host, new_node_port)
            return {"successor": succ, "predecessor": pred}

        elif cmd == "DEPART":
            self.node.depart()
            return {"status": "departing"}

        elif cmd == "UPDATE_SUCCESSOR":
            print(f"[Node {self.node.node_id}] Updating successor to {request['new_succ_id']}, {request}")
            self.node._update_successor((request["new_succ_id"],
                                    request["new_succ_host"],
                                    request["new_succ_port"]))
            return {"status": "OK"}

        elif cmd == "UPDATE_PREDECESSOR":
            print(f"[Node {self.node.node_id}] Updating predecessor to {request['new_pred_id']}, {request}")
            self.node._update_predecessor((request["new_pred_id"],
                                    request["new_pred_host"],
                                    request["new_pred_port"]))
            return {"status": "OK"}

        elif cmd == "TRANSFER_KEYS":
            new_node_id = request["new_node_id"]
            keys_to_give = self.node._find_keys_for_node(new_node_id)
            serialize_data = _serialize_for_json(keys_to_give)
            print(f"[Node {self.node.node_id}] Transferring keys to {new_node_id}: {serialize_data}")
            
            # Remove them from local store
            for k_str in keys_to_give.keys():
                k_int = int(k_str)
                self.node.data_store.pop(k_int, None)
            
            return {"keys": serialize_data}
        
        elif cmd == "GET_OVERLAY":
            if "start_node_id" not in request:
                start_node_id = self.node.node_id
            else:
                start_node_id = request["start_node_id"]
            
            return {"overlay": self.node.chord_overlay(start_node_id)}
            
        elif cmd == 'DEPART': 
            return {"status": "departing"}
            
        else:
            return {"error": f"Unknown command '{cmd}'"}

    def shutdown(self):
        """
        Close the server socket if you want to gracefully exit.
        """
        self.server_sock.close()
        sys.exit(0)
        
        # Stop all threads
        print("[ChordServer] Stopping all threads...")
        for thread in threading.enumerate():
            if thread is not threading.main_thread():
                try:
                    thread.join(timeout=1)
                except Exception as e:
                    print(f"[ChordServer] Error stopping thread: {e}")
