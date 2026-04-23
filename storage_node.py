"""
Storage Node - Distributed File System
Each node stores chunks and sends heartbeats to master
Run as: python storage_node.py <port>
"""

import os
import sys
import time
import pickle
import socket
import threading
from datetime import datetime
from pathlib import Path

MASTER_HOST = 'localhost'
MASTER_PORT = 9000
HEARTBEAT_INTERVAL = 4  # seconds

def log(msg, port, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [NODE-{port}] [{level}] {msg}")

class StorageNode:
    def __init__(self, port):
        self.port = port
        self.storage_dir = Path(f"node_storage_{port}")
        self.storage_dir.mkdir(exist_ok=True)
        self.running = True
        log(f"Storage initialized at ./{self.storage_dir}", self.port)

    def store_chunk(self, chunk_id, data):
        path = self.storage_dir / chunk_id
        with open(path, "wb") as f:
            f.write(data)
        log(f"Stored chunk: {chunk_id} ({len(data)} bytes)", self.port)
        return True

    def get_chunk(self, chunk_id):
        path = self.storage_dir / chunk_id
        if path.exists():
            with open(path, "rb") as f:
                return f.read()
        return None

    def delete_chunk(self, chunk_id):
        path = self.storage_dir / chunk_id
        if path.exists():
            path.unlink()
            log(f"Deleted chunk: {chunk_id}", self.port)
            return True
        return False

    def list_chunks(self):
        return [f.name for f in self.storage_dir.iterdir()]

    def get_storage_size(self):
        total = sum(f.stat().st_size for f in self.storage_dir.iterdir())
        return total

    def handle_request(self, conn):
        try:
            data = b""
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data += chunk

            msg = pickle.loads(data)
            cmd = msg.get("command")
            payload = msg.get("payload", {})
            response = {}

            if cmd == "STORE_CHUNK":
                ok = self.store_chunk(payload["chunk_id"], payload["data"])
                response = {"status": "ok" if ok else "error"}

            elif cmd == "GET_CHUNK":
                data = self.get_chunk(payload["chunk_id"])
                if data is not None:
                    response = {"status": "ok", "data": data}
                else:
                    response = {"status": "error", "msg": "Chunk not found"}

            elif cmd == "DELETE_CHUNK":
                ok = self.delete_chunk(payload["chunk_id"])
                response = {"status": "ok" if ok else "not_found"}

            elif cmd == "LIST_CHUNKS":
                response = {
                    "chunks": self.list_chunks(),
                    "storage_size": self.get_storage_size()
                }

            elif cmd == "PING":
                response = {"status": "alive", "port": self.port}

            conn.sendall(pickle.dumps(response))
            conn.shutdown(socket.SHUT_WR)
        except Exception as e:
            log(f"Request handler error: {e}", self.port, "ERROR")
        finally:
            conn.close()

    def send_heartbeat(self):
        """Continuously send heartbeats to master"""
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect((MASTER_HOST, MASTER_PORT))
                msg = {"command": "HEARTBEAT", "payload": {"port": self.port}}
                s.sendall(pickle.dumps(msg))
                s.shutdown(socket.SHUT_WR)
                s.recv(1024)
                s.close()
            except Exception as e:
                log(f"Heartbeat failed: {e}", self.port, "WARN")
            time.sleep(HEARTBEAT_INTERVAL)

    def start(self):
        # Start heartbeat thread
        hb_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        hb_thread.start()

        # Start server
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', self.port))
        server.listen(10)
        log(f"Node started, listening on port {self.port}", self.port)

        while self.running:
            try:
                conn, addr = server.accept()
                threading.Thread(
                    target=self.handle_request,
                    args=(conn,),
                    daemon=True
                ).start()
            except Exception as e:
                log(f"Server error: {e}", self.port, "ERROR")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python storage_node.py <port>")
        print("Example ports: 9001, 9002, 9003, 9004, 9005")
        sys.exit(1)

    port = int(sys.argv[1])
    node = StorageNode(port)
    node.start()