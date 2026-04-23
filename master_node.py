"""
Master Node - Distributed File System
Handles: metadata, replication coordination, fault detection
"""

import os
import json
import time
import math
import hashlib
import threading
import socket
import pickle
from pathlib import Path
from datetime import datetime

MASTER_HOST = 'localhost'
MASTER_PORT = 9000
REPLICATION_FACTOR = 3
CHUNK_SIZE = 512 * 1024  # 512KB per chunk
HEARTBEAT_INTERVAL = 5   # seconds
NODE_TIMEOUT = 15        # seconds before declared dead

# Node registry: port -> {status, last_seen, stored_chunks}
node_registry = {}
metadata_store = {}  # filename -> {size, chunks, created_at}
lock = threading.Lock()

METADATA_FILE = "master_metadata.json"

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [MASTER] [{level}] {msg}")

def save_metadata():
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata_store, f, indent=2)

def load_metadata():
    global metadata_store
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            metadata_store = json.load(f)
        log(f"Loaded metadata: {len(metadata_store)} files")

def get_alive_nodes():
    alive = []
    now = time.time()
    with lock:
        for port, info in node_registry.items():
            if now - info.get("last_seen", 0) < NODE_TIMEOUT:
                alive.append(port)
    return alive

def get_node_status():
    now = time.time()
    status = {}
    with lock:
        for port, info in node_registry.items():
            last = info.get("last_seen", 0)
            alive = (now - last) < NODE_TIMEOUT
            status[port] = {
                "alive": alive,
                "last_seen": last,
                "stored_chunks": info.get("stored_chunks", []),
                "chunks_count": len(info.get("stored_chunks", []))
            }
    return status

def send_to_node(port, command, payload=None):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(8)
        s.connect(('localhost', port))
        msg = {"command": command, "payload": payload}
        s.sendall(pickle.dumps(msg))
        s.shutdown(socket.SHUT_WR)  # Signal end of send
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
        s.close()
        if data:
            return pickle.loads(data)
    except Exception as e:
        log(f"Node {port} unreachable: {e}", "WARN")
        return None

def replicate_chunk(chunk_data, chunk_id, target_nodes):
    """Send a chunk to multiple nodes"""
    success_nodes = []
    for port in target_nodes:
        result = send_to_node(port, "STORE_CHUNK", {
            "chunk_id": chunk_id,
            "data": chunk_data
        })
        if result and result.get("status") == "ok":
            success_nodes.append(port)
            log(f"Chunk {chunk_id} stored on node {port}")
    return success_nodes

def upload_file(filename, file_data):
    """Split file into chunks and distribute across nodes"""
    alive_nodes = get_alive_nodes()
    if len(alive_nodes) < REPLICATION_FACTOR:
        return {"error": f"Need at least {REPLICATION_FACTOR} alive nodes, only {len(alive_nodes)} available"}

    file_size = len(file_data)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    chunks_info = []

    log(f"Uploading '{filename}' ({file_size} bytes, {total_chunks} chunks)")

    for i in range(total_chunks):
        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, file_size)
        chunk_data = file_data[start:end]

        chunk_id = hashlib.md5(f"{filename}_{i}".encode()).hexdigest()[:12]
        target_nodes = alive_nodes[:REPLICATION_FACTOR]  # pick first N alive nodes

        success_nodes = replicate_chunk(chunk_data, chunk_id, target_nodes)

        chunks_info.append({
            "chunk_id": chunk_id,
            "index": i,
            "size": len(chunk_data),
            "nodes": success_nodes
        })

        # Update node registry
        with lock:
            for port in success_nodes:
                if port in node_registry:
                    node_registry[port].setdefault("stored_chunks", []).append(chunk_id)

    metadata_store[filename] = {
        "size": file_size,
        "total_chunks": total_chunks,
        "chunks": chunks_info,
        "created_at": datetime.now().isoformat(),
        "checksum": hashlib.md5(file_data).hexdigest()
    }
    save_metadata()
    log(f"Upload complete: '{filename}'")
    return {"status": "ok", "chunks": total_chunks, "replicas": REPLICATION_FACTOR}

def download_file(filename):
    """Retrieve all chunks from nodes and reassemble"""
    if filename not in metadata_store:
        return {"error": "File not found"}

    meta = metadata_store[filename]
    file_data = b""
    alive_nodes = get_alive_nodes()

    for chunk_info in meta["chunks"]:
        chunk_id = chunk_info["chunk_id"]
        chunk_nodes = chunk_info["nodes"]
        chunk_retrieved = False

        for port in chunk_nodes:
            if port not in alive_nodes:
                continue
            result = send_to_node(port, "GET_CHUNK", {"chunk_id": chunk_id})
            if result and result.get("status") == "ok":
                file_data += result["data"]
                chunk_retrieved = True
                break

        if not chunk_retrieved:
            log(f"CRITICAL: Chunk {chunk_id} not retrievable!", "ERROR")
            return {"error": f"Chunk {chunk_id} lost — all replicas failed"}

    # Verify checksum
    if hashlib.md5(file_data).hexdigest() != meta["checksum"]:
        return {"error": "Checksum mismatch — data corrupted"}

    log(f"Download complete: '{filename}' ({len(file_data)} bytes)")
    return {"status": "ok", "data": file_data, "size": len(file_data)}

def delete_file(filename):
    """Delete file from all nodes"""
    if filename not in metadata_store:
        return {"error": "File not found"}

    meta = metadata_store[filename]
    for chunk_info in meta["chunks"]:
        for port in chunk_info["nodes"]:
            send_to_node(port, "DELETE_CHUNK", {"chunk_id": chunk_info["chunk_id"]})

    del metadata_store[filename]
    save_metadata()
    log(f"Deleted: '{filename}'")
    return {"status": "ok"}

def check_and_rereplicate():
    """Background: detect node failures and rereplicate lost chunks"""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        alive_nodes = get_alive_nodes()

        for filename, meta in list(metadata_store.items()):
            for chunk_info in meta["chunks"]:
                alive_replicas = [n for n in chunk_info["nodes"] if n in alive_nodes]

                if len(alive_replicas) < REPLICATION_FACTOR and alive_replicas:
                    needed = REPLICATION_FACTOR - len(alive_replicas)
                    candidates = [n for n in alive_nodes if n not in alive_replicas][:needed]

                    if candidates:
                        # Fetch chunk from alive replica
                        result = send_to_node(alive_replicas[0], "GET_CHUNK",
                                              {"chunk_id": chunk_info["chunk_id"]})
                        if result and result.get("status") == "ok":
                            new_nodes = replicate_chunk(
                                result["data"], chunk_info["chunk_id"], candidates
                            )
                            chunk_info["nodes"].extend(new_nodes)
                            if new_nodes:
                                log(f"Re-replicated chunk {chunk_info['chunk_id']} to {new_nodes}", "RECOVERY")
                            save_metadata()

def handle_client(conn, addr):
    """Handle API requests from dashboard"""
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

        if cmd == "HEARTBEAT":
            port = payload.get("port")
            with lock:
                if port not in node_registry:
                    node_registry[port] = {"stored_chunks": []}
                    log(f"New node registered: {port}")
                node_registry[port]["last_seen"] = time.time()
            response = {"status": "ok"}

        elif cmd == "UPLOAD":
            response = upload_file(payload["filename"], payload["data"])

        elif cmd == "DOWNLOAD":
            response = download_file(payload["filename"])

        elif cmd == "DELETE":
            response = delete_file(payload["filename"])

        elif cmd == "LIST_FILES":
            files = []
            for fname, meta in metadata_store.items():
                files.append({
                    "name": fname,
                    "size": meta["size"],
                    "chunks": meta["total_chunks"],
                    "created_at": meta["created_at"]
                })
            response = {"files": files}

        elif cmd == "NODE_STATUS":
            response = {"nodes": get_node_status(), "alive_count": len(get_alive_nodes())}

        elif cmd == "GET_STATS":
            alive = get_alive_nodes()
            response = {
                "total_files": len(metadata_store),
                "alive_nodes": len(alive),
                "total_nodes": len(node_registry),
                "replication_factor": REPLICATION_FACTOR
            }

        conn.sendall(pickle.dumps(response))
        conn.shutdown(socket.SHUT_WR)
    except Exception as e:
        log(f"Client handler error: {e}", "ERROR")
    finally:
        conn.close()

def start_master():
    load_metadata()
    threading.Thread(target=check_and_rereplicate, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(20)
    log(f"Master Node started on port {MASTER_PORT}")
    log(f"Replication Factor: {REPLICATION_FACTOR} | Chunk Size: {CHUNK_SIZE//1024}KB")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()