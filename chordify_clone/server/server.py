# server.py

import socket
import threading
import json
from chord_node_simple import ChordNode
from utils import BUFF_SIZE, _serialize_for_json, _deserialize_from_json
import sys
import logging

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
        try:
            logging.info(f"[ChordServer] Connection from {addr} {client_sock}")
            
            # 1) Read the request length prefix
            length_bytes = client_sock.recv(8)
            if not length_bytes:
                logging.info("[ChordServer] No size received. Closing connection.")
                return

            data_length = int.from_bytes(length_bytes, byteorder='big')
            # logging.info(f"[ChordServer] Receiving data of length {data_length}")

            # 2) Read the entire request data
            data = b''
            while len(data) < data_length:
                chunk = client_sock.recv(BUFF_SIZE)
                if not chunk:
                    break
                data += chunk

            if not data:
                logging.info("[ChordServer] No data received. Closing connection.")
                return

            # logging.info(f"[ChordServer] Received data: {data}")
            request = json.loads(data)

            # 3) Dispatch the request
            logging.info(f"[ChordServer] Dispatching request: {request}")
            response = self._dispatch(request)
            logging.info(f"[ChordServer] Response: {response}")

            # 4) Prepare response data
            r_data = json.dumps(response).encode("utf-8")

            # 5) If departing, do something special (just be sure to follow the protocol)
            if "status" in request and request["status"] == "departing":
                self.node.depart()
                print(f"[Node {self.node.node_id}] Closing socket and shutting down.")
                # Possibly still send a final response to follow the protocol?
                # Then shutdown:
                self.shutdown()

            # 6) Send response length + data
            data_length = len(r_data)
            # logging.info(f"[ChordServer] Sending response of length {data_length}")
            client_sock.sendall(data_length.to_bytes(8, byteorder='big'))

            bytes_sent = 0
            while bytes_sent < data_length:
                chunk = r_data[bytes_sent : bytes_sent + BUFF_SIZE]
                client_sock.sendall(chunk)
                bytes_sent += len(chunk)

        except Exception as e:
            print("[ChordServer] Exception while handling connection:", e)
            
            # Always send length prefix + error data
            error_msg = b"ERROR"
            client_sock.sendall(len(error_msg).to_bytes(8, byteorder='big'))
            client_sock.sendall(error_msg)

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
                "predecessor": self.node.predecessor,
                "data_store": _serialize_for_json(self.node.data_store),
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
            logging.error(f"HERE {key}, {value}, {start_node_id}, {ttl}")
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
            next_node_id = request.get("next_node_id", None)
            ttl = request.get("ttl", None)
            logging.info(f"[Node {self.node.node_id}] TTL TRANSFER_KEYS {ttl}")
            if ttl == 0:
                return {"keys": []}
            serialize_data = self.node.chord_transfer_keys(new_node_id, next_node_id, ttl)
            return {"keys": serialize_data}
        
        elif cmd == "MOVE_ALL_KEYS":
            # Our custom chain departure backward step:
            ttl = request.get("ttl", 1)
            data_store = _deserialize_from_json(request.get("data_store", None))
            logging.info(f"data store: {data_store}")
            self.node.chord_move_all_keys(data_store, ttl)
            return {"status": "OK"}
        
        elif cmd == "GET_OVERLAY":
            if "start_node_id" not in request:
                start_node_id = self.node.node_id
            else:
                start_node_id = request["start_node_id"]
            
            return {"overlay": self.node.chord_overlay(start_node_id)}
            
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
