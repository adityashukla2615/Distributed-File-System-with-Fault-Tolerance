"""
Web Dashboard - Distributed File System
Flask app serving the monitoring UI and API endpoints
"""

import os
import pickle
import socket
import time
from flask import Flask, render_template, request, jsonify, send_file
import io

app = Flask(__name__)
MASTER_PORT = 9000

NODE_PORTS = [9001, 9002, 9003, 9004, 9005]

def send_to_master(command, payload=None):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect(('localhost', MASTER_PORT))
        msg = {"command": command, "payload": payload or {}}
        s.sendall(pickle.dumps(msg))
        s.shutdown(socket.SHUT_WR)  # Signal end of send
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
        s.close()
        return pickle.loads(data) if data else {}
    except Exception as e:
        return {"error": str(e)}

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/stats")
def api_stats():
    stats = send_to_master("GET_STATS")
    nodes = send_to_master("NODE_STATUS")
    files = send_to_master("LIST_FILES")

    node_list = []
    if "nodes" in nodes:
        for port, info in nodes["nodes"].items():
            node_list.append({
                "port": int(port),
                "alive": info["alive"],
                "chunks": info["chunks_count"],
                "last_seen": round(time.time() - info["last_seen"], 1) if info["last_seen"] else None
            })

    return jsonify({
        "total_files": stats.get("total_files", 0),
        "alive_nodes": stats.get("alive_nodes", 0),
        "total_nodes": stats.get("total_nodes", 0),
        "replication_factor": stats.get("replication_factor", 3),
        "nodes": sorted(node_list, key=lambda x: x["port"]),
        "files": files.get("files", [])
    })

@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "No file selected"}), 400

    file_data = f.read()
    result = send_to_master("UPLOAD", {
        "filename": f.filename,
        "data": file_data
    })
    return jsonify(result)

@app.route("/api/download/<filename>")
def api_download(filename):
    result = send_to_master("DOWNLOAD", {"filename": filename})
    if "error" in result:
        return jsonify(result), 404
    return send_file(
        io.BytesIO(result["data"]),
        download_name=filename,
        as_attachment=True
    )

@app.route("/api/delete/<filename>", methods=["DELETE"])
def api_delete(filename):
    result = send_to_master("DELETE", {"filename": filename})
    return jsonify(result)

@app.route("/api/files")
def api_files():
    result = send_to_master("LIST_FILES")
    return jsonify(result)

if __name__ == "__main__":
    print("\n" + "="*50)
    print("  DFS Dashboard running at http://localhost:5000")
    print("="*50 + "\n")
    app.run(debug=False, port=5000)