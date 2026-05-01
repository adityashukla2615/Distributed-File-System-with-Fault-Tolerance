# ⬡ Distributed File System with Fault Tolerance
### OS Course Project

---

## Quick Start (one command)

```bash
# 1. Install dependencies
pip install flask

# 2. Start everything
python launch.py
```

Then open → **http://localhost:5000**

---

## Architecture

```
                    ┌─────────────────┐
                    │   Master Node   │
                    │    :9000        │
                    │  metadata store │
                    │  fault detector │
                    └────────┬────────┘
                             │ heartbeats + commands
          ┌──────────────────┼──────────────────┐
          │          │       │       │           │
     ┌────┴┐    ┌────┴┐  ┌───┴─┐  ┌─┴───┐  ┌───┴─┐
     │:9001│    │:9002│  │:9003│  │:9004│  │:9005│
     │chunk│    │chunk│  │chunk│  │chunk│  │chunk│
     │store│    │store│  │store│  │store│  │store│
     └─────┘    └─────┘  └─────┘  └─────┘  └─────┘

     Web Dashboard :5000 talks to Master via socket API
```

## Key Concepts Demonstrated

| Feature | How it works |
|---|---|
| **Chunking** | Files split into 512 KB chunks |
| **Replication** | Each chunk stored on 3 nodes (RF=3) |
| **Fault Tolerance** | System survives 2 node failures with RF=3 |
| **Auto Re-replication** | Master detects dead nodes, restores RF |
| **Checksum** | MD5 verified on every download |
| **Heartbeat** | Nodes ping master every 4s; timeout=15s |

## File Structure

```
dfs_project/
├── launch.py          ← Start everything here
├── master_node.py     ← Metadata + coordination
├── storage_node.py    ← Chunk storage
├── dashboard.py       ← Flask web server
├── requirements.txt   ← pip install flask
├── master_metadata.json  ← Auto-generated
└── templates/
    └── dashboard.html ← Full interactive UI
```

## Manual start (separate terminals)

```bash
python master_node.py               # Terminal 1
python storage_node.py 9001         # Terminal 2
python storage_node.py 9002         # Terminal 3
python storage_node.py 9003         # Terminal 4
python storage_node.py 9004         # Terminal 5
python storage_node.py 9005         # Terminal 6
python dashboard.py                 # Terminal 7
```

## Dashboard Features

- **Live cluster topology** — SVG visualization of master + 5 nodes
- **Real file upload/download** — actual chunking & replication
- **Kill/Recover nodes** — click any node to simulate failure
- **Chunk replica matrix** — see which chunks live on which nodes
- **Demo buttons** — one-click demos for presentations
- **System log** — real-time heartbeat & operation log
- **Auto-refresh** — polls master every 3 seconds

## Fault Tolerance Math

With RF=3 across 5 nodes:
- **0 failures** → all chunks at 3× replication ✓
- **1 failure**  → chunks at 2× → master re-replicates ✓
- **2 failures** → chunks at 1× → still readable ✓
- **3 failures** → some chunks at 0× → DATA LOSS ✗

---
*OS Course Project | Python | Flask | Raw Sockets | Threading*
