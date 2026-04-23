📂 Distributed File System (DFS)
🚀 Overview

This project is a Distributed File System (DFS) built using Python that ensures:

✅ High data availability
✅ Strong consistency
✅ Robust fault tolerance

The system distributes files across multiple storage nodes, replicates data, and automatically handles node failures while maintaining reliable access.

🧠 Core Idea

Files are split into chunks and distributed across multiple nodes.
Each chunk is replicated to ensure reliability and recovery during failures.

Example metadata structure:
📄

🏗️ Architecture
Client (Web Dashboard)
        ↓
   Master Node (Port 9000)
        ↓
Storage Nodes (Ports 9001–9005)
⚙️ Components
🧩 1. Master Node

📄

Handles:

Metadata management
File chunking (512 KB chunks)
Replication (default = 3 copies)
Node health monitoring (heartbeat system)
Failure recovery (re-replication)
💾 2. Storage Nodes

📄

Responsibilities:

Store file chunks
Serve read/write requests
Send heartbeat signals to master
Manage local storage
🌐 3. Web Dashboard

📄

Features:

Upload / Download / Delete files
View system stats
Monitor node health
Track file distribution

Runs at:
👉 http://localhost:5000

🚀 4. Launcher Script

📄

Starts everything with one command:

Master node
5 storage nodes
Dashboard
🔥 Key Features
🔁 Replication Factor = 3
📦 Chunk-based storage (512KB per chunk)
❤️ Heartbeat-based failure detection
🔄 Automatic re-replication on node failure
🔐 Checksum verification for data integrity
📊 Live monitoring dashboard
⚙️ Installation
1. Clone Repository
git clone https://github.com/your-username/dfs-project.git
cd dfs-project
2. Install Dependencies
pip install -r requirements.txt

📄 Requirements:


▶️ Running the Project
✅ One Command Setup (Recommended)
python launch.py

This will start:

Master Node → 9000
Storage Nodes → 9001–9005
Dashboard → 5000
📊 Usage
Upload File
Use dashboard UI
Or API: /api/upload
Download File
From dashboard
Or API: /api/download/<filename>
Delete File
From dashboard
Or API: /api/delete/<filename>
🧪 Fault Tolerance Mechanism
Nodes send heartbeat every 4 seconds
If node inactive for 15 seconds → marked dead
System automatically:
Detects missing replicas
Re-replicates chunks to healthy nodes
📁 File Handling Flow
File uploaded
Split into chunks
Chunks distributed across nodes
Metadata stored in master
Replicas created (3 copies)
📊 Example
File → 2 chunks

Each chunk stored on:

Node 9001, 9002, 9003
🔐 Data Integrity
Uses MD5 checksum
Validates file during download
Prevents corrupted data delivery
📈 Future Improvements
☁️ Cloud deployment (AWS / GCP)
🔐 Encryption support
⚖️ Load balancing strategies
📡 Distributed master (remove single point of failure)
📊 Advanced analytics dashboard
👨‍💻 Author

Aditya Shukla

⭐ Show Your Support

If you like this project, give it a ⭐ on GitHub!

💡 Why This Project Matters

This project demonstrates real-world concepts used in systems like:

Google File System (GFS)
Hadoop Distributed File System (HDFS)
