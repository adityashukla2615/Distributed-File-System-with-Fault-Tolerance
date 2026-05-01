"""
DFS Launcher — Starts entire system with one command
Launches: Master Node + 5 Storage Nodes + Web Dashboard
"""

import subprocess
import sys
import time
import os
import signal
import atexit

processes = []

def kill_all():
    print("\n[LAUNCHER] Shutting down all processes...")
    for p in processes:
        try:
            p.terminate()
        except:
            pass

atexit.register(kill_all)
signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

PYTHON = sys.executable
NODE_PORTS = [9001, 9002, 9003, 9004, 9005]

def start_process(cmd, label):
    p = subprocess.Popen(cmd)
    processes.append(p)
    print(f"  ✓ Started: {label}  (PID {p.pid})")
    return p

print("\n" + "="*55)
print("  ⬡  Distributed File System — Starting Up")
print("="*55)

print("\n[1/3] Starting Master Node (:9000)...")
start_process([PYTHON, "master_node.py"], "Master Node :9000")
time.sleep(1.5)

print("\n[2/3] Starting 5 Storage Nodes...")
for port in NODE_PORTS:
    start_process([PYTHON, "storage_node.py", str(port)], f"Storage Node :{port}")
    time.sleep(0.3)

time.sleep(2)

print("\n[3/3] Starting Web Dashboard...")
start_process([PYTHON, "dashboard.py"], "Dashboard :5000")
time.sleep(1)

print("\n" + "="*55)
print("  ✅  All systems running!")
print("  📊  Dashboard → http://localhost:5000")
print("  🖥   Nodes    : 9001, 9002, 9003, 9004, 9005")
print("  🔑  Master   : 9000")
print("="*55)
print("\n  Press Ctrl+C to stop all processes\n")

try:
    while True:
        time.sleep(2)
        dead = [p for p in processes if p.poll() is not None]
        if dead:
            print(f"[WARN] {len(dead)} process(es) exited unexpectedly")
except KeyboardInterrupt:
    pass
