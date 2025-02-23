import socket
import threading
import json
from typing import Optional
from utils import chord_hash, M, in_interval, _serialize_for_json, BUFF_SIZE
import logging
import sys
import signal
import time

class ChordNode:
    def __init__(
        self,
        host: str,
        port: int,
        bootstrap_host: Optional[str] = None,
        bootstrap_port: Optional[int] = None,
        replication_factor: int = 1, # No replication at all,
        replication_consistency: Optional[int] = None
    ):
        # Core state
        self.host = host
        self.port = port
        self.node_id = chord_hash(f"{host}:{port}")
        self.replication_factor = replication_factor
        self.replication_consistency = replication_consistency
        
        assert self.replication_consistency in [None, "l", "e"], "Invalid replication consistency"
        
        # Ring pointers
        self.successor = (self.node_id, self.host, self.port)
        self.predecessor = (self.node_id, self.host, self.port)

        # Data store and tracking
        self.uploaded_songs = []
        self.data_store = dict()

        # Possibly initialize ring if bootstrap is provided
        if bootstrap_host and bootstrap_port:
            self.join(bootstrap_host, bootstrap_port)
        else:
            logging.info("[ChordNode] No bootstrap node provided, creating a new ring.")

    def join(self, bootstrap_host: str, bootstrap_port: int):
        """Join the ring via a known bootstrap node."""
        successor_info = self._send(bootstrap_host, bootstrap_port, {
            "cmd": "JOIN",
            "host": self.host,
            "port": self.port
        })
        if "successor" in successor_info and "predecessor" in successor_info:
            logging.info(f"[Node {self.node_id}] On join the ring, got info: {successor_info}") 
            self.successor = tuple(successor_info["successor"])
            self.predecessor = tuple(successor_info["predecessor"])
            
            # If eventual consistency, we can asynchronously notify predecessor & successor
            update_succ_msg = {
                "cmd": "UPDATE_SUCCESSOR",
                "new_succ_id": self.node_id,
                "new_succ_host": self.host,
                "new_succ_port": self.port
            }
            update_pred_msg = {
                "cmd": "UPDATE_PREDECESSOR",
                "new_pred_id": self.node_id,
                "new_pred_host": self.host,
                "new_pred_port": self.port
            }
            # Notify predecessor and notify successor
            if self.replication_consistency == "e":
                # asynchronous updates
                self._send_async(self.predecessor[1], self.predecessor[2], update_succ_msg)
                self._send_async(self.successor[1], self.successor[2], update_pred_msg)
            
                threading.Thread(
                    target=self._acquire_keys,
                    args=(self.node_id, self.node_id, self.replication_factor+1 if self.replication_factor else self.replication_factor),
                    daemon=True
                ).start()
            else:
                print(f"[Node {self.node_id}] Notifying predecessor {self.predecessor}")
                # linearizable => synchronous
                status = self._send(self.predecessor[1], self.predecessor[2], update_succ_msg)
                logging.info(f"[Node {self.node_id}] Notified predecessor {self.predecessor} => {status}")
                status = self._send(self.successor[1], self.successor[2], update_pred_msg)
                logging.info(f"[Node {self.node_id}] Notified successor {self.successor} => {status}")
                self._acquire_keys(self.node_id, self.node_id, self.replication_factor+1 if self.replication_factor else self.replication_factor)
        else:
            # fallback
            self.successor = (self.node_id, self.host, self.port)
            self.predecessor = (self.node_id, self.host, self.port)

        logging.info(f"[Node {self.node_id}] Joined ring via {bootstrap_host}:{bootstrap_port} "
              f"& got successor {self.successor} & Predecessor: {self.predecessor}")

    def depart(self):
        """
        Gracefully remove this node from the Chord ring.
        1. Delete the songs that have been uploaded by this node.
        2. Transfer data to successor.
        3. Notify predecessor and successor to link each other.
        4. Close server socket.
        """
        ret = [self.chord_delete(song, f"{self.host}:{self.port}", self.node_id, self.replication_factor - 1 if self.replication_factor else self.replication_factor) for song in self.uploaded_songs]
        logging.info(f"Deleted songs' status: {list(zip(self.uploaded_songs, ret))}")

        # 3) Notify predecessor & successor to link each other
        succ_id, succ_host, succ_port = self.successor
        pred_id, pred_host, pred_port = self.predecessor if self.predecessor else (None, None, None)

        if pred_id is not None and (pred_id != self.node_id):
            logging.info(f"[Node {self.node_id}] Notifying predecessor {pred_id} to link successor {succ_id}")
            update_succ_msg = {
                "cmd": "UPDATE_SUCCESSOR",
                "new_succ_id": succ_id,
                "new_succ_host": succ_host,
                "new_succ_port": succ_port
            }
            if self.replication_consistency == "e":
                self._send_async(pred_host, pred_port, update_succ_msg)
            else:
                self._send(pred_host, pred_port, update_succ_msg)


        if succ_id != self.node_id:
            logging.info(f"[Node {self.node_id}] Notifying successor {succ_id} to link predecessor {pred_id}")
            update_pred_msg = {
                "cmd": "UPDATE_PREDECESSOR",
                "new_pred_id": pred_id,
                "new_pred_host": pred_host,
                "new_pred_port": pred_port
            }
            if self.replication_consistency == "e":
                self._send_async(succ_host, succ_port, update_pred_msg)
            else:
                self._send(succ_host, succ_port, update_pred_msg)       
        
        move_all_keys = {
            "cmd": "MOVE_ALL_KEYS",
            "ttl": self.replication_factor if self.replication_factor else 1,
            "data_store": _serialize_for_json(self.data_store)
        }
        if self.replication_consistency == "e":
            self._send_async(succ_host, succ_port, move_all_keys)
        else:
            resp = self._send(succ_host, succ_port, move_all_keys)
            logging.info(f"RESPONSE from MOVE_ALL_KEYS: {resp}")
        
        self.data_store.clear()


    def find_successor(self, key_id: int):
        succ_id, succ_host, succ_port = self.successor
        if in_interval(key_id, self.node_id, succ_id, inclusive=True):
            return self.successor, (self.node_id, self.host, self.port)
        else:
            next_node = self.successor # self.closest_preceding_node(key_id)
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
        # logging.info(f"[Node {self.node_id}] Fixing fingers...")
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

    def chord_put(self, key: str, value: str | list, start_node_id: int, ttl: int = None):
        if ttl == 0: return
        
        key_id = chord_hash(key)

        if self._chain_replicate_with_ttl(start_node_id, key_id, key, value, "PUT", ttl): return
        
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if self.node_id == start_node_id:
            self.uploaded_songs.append(key)

        if node_id == self.node_id:
            self._store_new_value(key_id, key, value)
            self._chain_replicate_without_ttl(self.node_id, key, value, "PUT")
            logging.info(f"[Node {self.node_id}] PUT {key}[{key_id}] -> {value}")
            return
            
        logging.info(f"[Node {self.node_id}] Forward PUT {key} -> {value} to {node_id} = {ttl}, {self.replication_factor}")
        msg = {
            "cmd": "PUT",
            "key": key,
            "value": value,
            "start_node_id": start_node_id
        }
        if self.replication_consistency == "e":
            self._send_async(node_host, node_port, msg)
        else:
            self._send(node_host, node_port, msg)

    def chord_get(self, key: str, start_node_id: int, ttl):
        key_id = chord_hash(key)
        
        if self.replication_consistency == "e":
            local_value, local_id = self._read_value(key_id, key)
            if local_id >= 0:
                return local_value, local_id
        
        ret_value = self._chain_replicate_with_ttl(start_node_id, key_id, key, None, "GET", ttl)
        if ret_value:
            return ret_value
        
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if node_id == self.node_id:
            if not self.replication_factor: # No replication at all
                return self._read_value(key_id, key)
            if self.replication_factor == 1:
                return self._read_value(key_id, key)
            return self._chain_replicate_without_ttl(self.node_id, key, None, "GET")        
        else:
            logging.info(f"[Node {self.node_id}] Forward GET {key} to {node_id}")
            resp = self._send(node_host, node_port, {
                "cmd": "GET",
                "key": key
            })
            return resp.get("value", []), resp.get("id", -1)

    def chord_get_all(self, start_node_id):
        node_id, node_host, node_port = self.successor
        if node_id == start_node_id:
            logging.info(f"[Node {self.node_id}] GET * local: {self.data_store}")
            return {
                self.node_id: _serialize_for_json(self.data_store)
            }
        resp = self._send(node_host, node_port, {
            "cmd": "GET",
            "key": "*",
            "start_node_id": start_node_id
        })
        result = {
            **resp.get("value", {}),
            self.node_id: _serialize_for_json(self.data_store)
        }
        return result

    def chord_delete(self, key: str, value: str, start_node_id: int, ttl):
        key_id = chord_hash(key)
        if self._chain_replicate_with_ttl(start_node_id, key_id, key, value, "DELETE", ttl): return
        (node_id, node_host, node_port), _ = self.find_successor(key_id)
        if node_id == self.node_id:
            self._delete_value(key_id, key, value)
            self._chain_replicate_without_ttl(self.node_id, key, value, "DELETE")
            logging.info(f"[Node {self.node_id}] PUT {key}[{key_id}] -> {value}")
            return "OK"
        
        msg = {
            "cmd": "DELETE",
            "key": key,
            "value": value,
            "start_node_id": start_node_id
        }
        if self.replication_consistency == "e":
            self._send_async(node_host, node_port, msg)
            return "OK"  # We don't wait for the real outcome
        else:
            resp = self._send(node_host, node_port, msg)
            return resp.get("status", "ERROR")


    def chord_overlay(self, start_node_id):
        """
        Return a list of dicts representing the current state of the Chord ring.
        Each dict contains information from a single node.
        """
        # Initialize the overlay with the current node's information
        overlay = [
            {
                "node_id": self.node_id,
                "successor": self.successor,
                "predecessor": self.predecessor,
                "data_store": _serialize_for_json(self.data_store),
                "uploaded_songs": _serialize_for_json(self.uploaded_songs)
            }
        ]

        # If the successor is the start node, we've completed the circle
        if self.successor[0] == start_node_id:
            return overlay

        # Otherwise, fetch the overlay information from the successor
        resp = self._send(self.successor[1], self.successor[2], {
            "cmd": "GET_OVERLAY",
            "start_node_id": start_node_id
        })

        # Add the received overlay data to our list
        overlay.extend(resp.get("overlay", []))
        return overlay

    def chord_transfer_keys(self, next_node_id, new_node_id, ttl=None):
        """
        Transfer keys that belong to new_node_id to the new node.
        """
        keys_to_give = self._find_keys_for_node(next_node_id)
        serialize_data = _serialize_for_json(keys_to_give)
        logging.info(f"[Node {self.node_id}] Transferring keys to {next_node_id}: {serialize_data}")
        
        # Remove them from local store
        logging.info(f"DELETING ? {new_node_id} != {self.node_id}")
        if new_node_id != self.node_id and ttl != 1:
            for k_int in keys_to_give.keys():
                self.data_store.pop(k_int)
            
        logging.info(f"[Node {self.node_id}] Transferring keys to {new_node_id}: {serialize_data}")
        
        self._chain_replicate_acquire_keys(new_node_id, new_node_id, ttl)
        return serialize_data

    def chord_move_all_keys(self, data_store, ttl=1):
        # Step 1: Get data from predecessor
        if ttl > 1:
            succ_id, succ_host, succ_port = self.successor
            keys_to_give = self._find_keys_for_successor_node(succ_id)
            move_all_keys = {
                "cmd": "MOVE_ALL_KEYS",
                "ttl": ttl - 1,
                "data_store": _serialize_for_json(keys_to_give)
            }
            if self.replication_consistency == "e":
                self._send_async(succ_host, succ_port, move_all_keys)
            else:
                resp = self._send(succ_host, succ_port, move_all_keys)
                logging.info(f"RESPONSE from MOVE_ALL_KEYS: {resp}")
            
        # We merge our data_store based on the data_store variable
        for k_int, kv_dict in data_store.items():
            for k, v in kv_dict.items():
                self._store_new_value(int(k_int), k, v)
    
    def _send_async(self, host, port, message_dict):
        """
        Fire-and-forget sending in a background thread (non-blocking).
        """
        def _bg_send():
            self._send(host, port, message_dict)
        t = threading.Thread(target=_bg_send, daemon=True)
        t.start()
        
    def _send(self, host, port, message_dict):
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
            logging.error(f"[_send] Exception: {e}")
            return {}

    def _update_successor(self, new_successor):
        self.successor = tuple(new_successor)

    def _update_predecessor(self, new_predecessor):
        self.predecessor = tuple(new_predecessor)

    def _acquire_keys(self, new_node_id: int = None, next_node_id: int = None, ttl: int = None):
        """
        If you wanted to explicitly fetch keys from your successor.
        Expects the successor to return something like:
            {
                "keys": {
                    "234": {
                        "Like a Rolling Stone": ["127.0.0.1:5000"]
                    }
                }
            }
        """
        if ttl == 0:
            logging.debug("REACHED THE END OF TTL ACQUIRE KEYS")
            return 
        
        succ_id, succ_host, succ_port = self.successor        
        request = {
            "cmd": "TRANSFER_KEYS",
            "new_node_id": new_node_id,
            "next_node_id": next_node_id,
        }
        if ttl:
            request["ttl"] = ttl - 1
        
        logging.info(f"[Node {self.node_id}] Requesting keys from {succ_id} with TTL {ttl}")
        resp = self._send(succ_host, succ_port, request)
        logging.info(f"RESPONSE from TRANSFER_KEYS: {resp}")

        keys_to_move = resp.get("keys", {})
        logging.info(f"[Node {self.node_id}] Acquiring keys from {succ_id}: {keys_to_move}")

        # Convert keys to integers and lists to sets (if needed):
        for k_int, nested_dict in keys_to_move.items():
            # Convert any list inside nested_dict back to a set (if that’s desired)
            # For example: { "Like a Rolling Stone": ["127.0.0.1:5000"] } -> set(...)[Node 88] REPLICATE PUT Like a Rolling Stone -> 127.0.0.1:5000 to 119 with TTL 1                                       │[Node 119] Forward PUT Like a Rolling Stone -> 127.0.0.1:5000 to 29
            self.data_store[int(k_int)] = nested_dict


    def _find_keys_for_node(self, new_node_id: int) -> dict:
        """Return a dict of all keys that belong to new_node_id."""
        dispatch_data = {}
        for k_int, kv_dict in self.data_store.items():
            # If k_int is in (self.node_id, new_node_id)
            if in_interval(int(k_int), self.node_id, new_node_id):
                dispatch_data[k_int] = kv_dict
        return dispatch_data
    
    def _find_keys_for_successor_node(self, succ_id: int) -> dict:
        """Return a dict of all keys that belong to succ_id."""
        dispatch_data = {}
        for k_int, kv_dict in self.data_store.items():
            # If k_int is in (self.node_id, new_node_id)
            if not in_interval(int(k_int), self.node_id, succ_id):
                dispatch_data[k_int] = kv_dict
        return dispatch_data

    def _store_new_value(self, key_id, key, value):
        if key_id not in self.data_store:
            self.data_store[key_id] = dict()
        if key not in self.data_store[key_id]:
            self.data_store[key_id][key] = set()
        
        if not isinstance(self.data_store[key_id][key], set):
            if isinstance(self.data_store[key_id][key], list):
                # Convert the list to a set of its elements
                self.data_store[key_id][key] = set(self.data_store[key_id][key])
            else:
                # Wrap the single value in a set
                self.data_store[key_id][key] = {self.data_store[key_id][key]}

            
        if isinstance(value, set):
            self.data_store[key_id][key].update(set(value))
        else:
            self.data_store[key_id][key].add(value)

    def _delete_value(self, key_id, key, value):
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
    
    def _read_value(self, key_id, key):
        if key_id in self.data_store and key in self.data_store[key_id]:
            value = self.data_store[key_id][key]
            return list(value), self.node_id
            # return list(self.data_store[key_id][key])
        return [], -1
    
    def _chain_replicate_with_ttl(self, start_node_id, key_id, key, value, cmd, ttl):
        if not self.replication_factor: return False
        if not ttl: return False
        if ttl == 0: return True
        ret_value = True
        # We need to update the successor node as well
        if cmd == "PUT":
            self._store_new_value(key_id, key, value)
        elif cmd == "DELETE":
            self._delete_value(key_id, key, value)
        elif cmd == "GET":
            ret_value = self._read_value(key_id, key)

        succ_id, succ_host, succ_port = self.successor
        
        logging.info(f"[Node {self.node_id}] REPLICATE {cmd} {key} -> {value} to {succ_id} with TTL {ttl}, {start_node_id}")
        if succ_id == start_node_id or ttl <= 1:
            logging.info(f"[Node {self.node_id}] TTL {ttl} for {key} reached")
            return ret_value
        
        if ttl == 1:
            logging.info(f"[Node {self.node_id}] TTL {ttl} for {key} reached")
            return ret_value
        
        chain_data = {
            "cmd": cmd,
            "key": key,
            "value": value,
            "start_node_id": start_node_id,
            "ttl": ttl - 1
        }
        if self.replication_consistency == "e" and cmd != "GET":
            self._send_async(succ_host, succ_port, chain_data)
            return True
        else:
            logging.info(f"[Node {self.node_id}] REPLICATE {cmd} {key} -> {value} to {succ_id}")
            ret = self._send(succ_host, succ_port, chain_data)
            if "value" in ret and "id" in ret: return ret["value"], ret["id"] # GET SPECIFIC
            return ret
    
    def _chain_replicate_without_ttl(self, start_node_id, key, value, cmd):
        if not self.replication_factor:
            # If replication factor is None or it's zero we are done.
            return
        
        # That was the first put, we need to replicate it
        # We need to update the successor node as well
        succ_id, succ_host, succ_port = self.successor

        logging.info(f"[Node {self.node_id}] REPLICATE {cmd} {key} -> {value} to {succ_id} without TTL, {start_node_id}")
        data = {
            "cmd": cmd,
            "key": key,
            "value": value,
            "start_node_id": start_node_id,
            "ttl": self.replication_factor - 1
        }
        if self.replication_consistency == "e" and cmd != "GET":
            self._send_async(succ_host, succ_port, data)
            return
        ret = self._send(succ_host, succ_port, data)
        if "value" in ret and "id" in ret: return ret["value"], ret["id"]
        return ret
    
    def _chain_replicate_acquire_keys(self, new_node_id, next_node_id, ttl):
        if not self.replication_factor:
            # If replication factor is None or it's zero we are done.
            return
        
        self._acquire_keys(new_node_id, self.node_id, ttl)