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

node_registry = {}
metadata_store = {}
lock = threading.Lock()

METADATA_FILE = "master_metadata.json"

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    symbols = {"INFO": "·", "WARN": "⚠", "ERROR": "✕", "RECOVERY": "↻"}
    sym = symbols.get(level, "·")
    print(f"[{ts}] [MASTER] {sym} {msg}")

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
        s.shutdown(socket.SHUT_WR)
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
    success_nodes = []
    for port in target_nodes:
        result = send_to_node(port, "STORE_CHUNK", {
            "chunk_id": chunk_id,
            "data": chunk_data
        })
        if result and result.get("status") == "ok":
            success_nodes.append(port)
            log(f"Chunk {chunk_id} → node :{port}")
    return success_nodes

def upload_file(filename, file_data):
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
        target_nodes = alive_nodes[:REPLICATION_FACTOR]
        success_nodes = replicate_chunk(chunk_data, chunk_id, target_nodes)

        chunks_info.append({
            "chunk_id": chunk_id,
            "index": i,
            "size": len(chunk_data),
            "nodes": success_nodes
        })

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
    log(f"Upload complete: '{filename}' — {total_chunks} chunks × RF={REPLICATION_FACTOR}")
    return {"status": "ok", "chunks": total_chunks, "replicas": REPLICATION_FACTOR}

def download_file(filename):
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

    if hashlib.md5(file_data).hexdigest() != meta["checksum"]:
        return {"error": "Checksum mismatch — data corrupted"}

    log(f"Download complete: '{filename}' ({len(file_data)} bytes)")
    return {"status": "ok", "data": file_data, "size": len(file_data)}

def delete_file(filename):
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
                        result = send_to_node(alive_replicas[0], "GET_CHUNK",
                                              {"chunk_id": chunk_info["chunk_id"]})
                        if result and result.get("status") == "ok":
                            new_nodes = replicate_chunk(
                                result["data"], chunk_info["chunk_id"], candidates
                            )
                            chunk_info["nodes"].extend(new_nodes)
                            if new_nodes:
                                log(f"Re-replicated chunk {chunk_info['chunk_id']} → {new_nodes}", "RECOVERY")
                            save_metadata()

def handle_client(conn, addr):
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
                    log(f"New node registered: :{port}")
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
                alive_nodes = get_alive_nodes()
                chunks_health = []
                for c in meta["chunks"]:
                    alive = [n for n in c["nodes"] if n in alive_nodes]
                    chunks_health.append({
                        "chunk_id": c["chunk_id"],
                        "index": c["index"],
                        "size": c["size"],
                        "nodes": c["nodes"],
                        "alive_replicas": len(alive),
                        "total_replicas": len(c["nodes"])
                    })
                files.append({
                    "name": fname,
                    "size": meta["size"],
                    "chunks": meta["total_chunks"],
                    "created_at": meta["created_at"],
                    "checksum": meta["checksum"],
                    "chunks_detail": chunks_health
                })
            response = {"files": files}

        elif cmd == "NODE_STATUS":
            response = {"nodes": get_node_status(), "alive_count": len(get_alive_nodes())}

        elif cmd == "GET_STATS":
            alive = get_alive_nodes()
            total_chunks = sum(len(m["chunks"]) for m in metadata_store.values())
            total_size = sum(m["size"] for m in metadata_store.values())
            response = {
                "total_files": len(metadata_store),
                "alive_nodes": len(alive),
                "total_nodes": len(node_registry),
                "replication_factor": REPLICATION_FACTOR,
                "chunk_size": CHUNK_SIZE,
                "total_chunks": total_chunks,
                "total_size": total_size,
                "node_ports": [9001, 9002, 9003, 9004, 9005]
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

    print("\n" + "="*55)
    print("  ⬡  MASTER NODE ONLINE")
    print(f"     Port          : {MASTER_PORT}")
    print(f"     Replication   : RF={REPLICATION_FACTOR}")
    print(f"     Chunk size    : {CHUNK_SIZE//1024} KB")
    print(f"     Node timeout  : {NODE_TIMEOUT}s")
    print("="*55 + "\n")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_master()
