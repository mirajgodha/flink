# ⚡ Apache Flink – Stream & Batch Data Processing

Welcome to the **Apache Flink Learning & Project Repository** — created as part of the **QuantumRoot Data Engineering curriculum**.  
This repo contains hands-on examples, tutorials, and explanations of key **Flink concepts**, including real-world pipelines, optimizations, and performance tuning.

---

## 🌐 About QuantumRoot

**[QuantumRoot](https://quantumroot.in/)** is a learning platform focused on **Data Engineering**, **Big Data Systems**, and **Generative AI**.  
We help professionals master modern data and AI platforms through **hands-on, architecture-driven training**.

### 🎓 Courses Offered at QuantumRoot
| Category | Course Title | Key Topics |
|-----------|---------------|-------------|
| 🧩 **Data Engineering** | [Data Engineering using Databricks on AWS & Azure](https://quantumroot.in/) | Delta Lake, Spark SQL, ETL pipelines |
| ⚙️ **Big Data** | Advanced Apache Spark | Catalyst Optimizer, Tungsten, Shuffle Optimization |
| 🌊 **Streaming Systems** | Apache Flink – Real-Time Stream Processing | Stateful stream ops, time windows, joins |
| ☁️ **Cloud & DevOps** | Azure DevOps Fundamentals | Pipelines, Boards, Artifacts |
| 🧠 **Generative AI & LLMs** | Building LLM-Powered Applications | Prompt Engineering, RAG, LangChain, OpenAI APIs |
| 📊 **Analytics & Visualization** | Power BI & Modern Dashboarding | Data modeling, DAX, visual storytelling |

Learn more at 👉 [quantumroot.in](https://quantumroot.in/)

---

## 🚀 Repository Overview

This repository demonstrates **end-to-end Apache Flink concepts** with practical code, diagrams, and job examples.  
Each folder focuses on a specific concept with ready-to-run jobs and datasets.

### 📂 Directory Structure

---

## 🔍 **Key Apache Flink Concepts Covered**

### 1️⃣ **Architecture Overview**
- **JobManager** → Coordinates tasks, checkpoints, recovery
- **TaskManagers** → Execute subtasks in **task slots**
- **Dispatcher & ResourceManager** → Handle resource allocation and job submission
- **Execution Graph** → Logical DAG converted into physical subtasks

### 2️⃣ **Programming Model**
- **StreamExecutionEnvironment**
- **DataStream / Table API**
- **Operators** → `map()`, `filter()`, `keyBy()`, `window()`, `aggregate()`
- **State Management** and **Checkpointing**
- **Watermarks** & **Event Time Semantics**

### 3️⃣ **Execution Flow**

User Code → JobGraph → ExecutionGraph → TaskManagers → Slots → Parallel Tasks

### 4️⃣ **Slot Sharing**
- Operators from the same job share slots for better resource utilization
- Chained operators (e.g., `map + filter`) run in the same thread → less serialization

### 5️⃣ **Join Optimizations**
| Strategy | Hint | Description |
|-----------|------|-------------|
| **Broadcast Hash Join** | `BROADCAST(t2)` | Broadcast small table to all tasks |
| **Shuffle Hash Join** | `SHUFFLE_HASH(t1,t2)` | Partition both sides by join key |
| **Sort-Merge Join** | `SHUFFLE_MERGE(t1,t2)` | Sort inputs before join |
| **Nested Loop Join** | `NEST_LOOP(t2)` | Used for non-equi joins |
| **Temporal / Lookup Joins** | – | Stream enrichment with bounded tables |

👉 Includes examples with `/*+ BROADCAST(dim) */` and `.hints("BROADCAST")`.

### 6️⃣ **Optimizations**
- **Join Hints** for controlling physical plan
- **State Backend Tuning** (RocksDB, Memory)
- **Watermarking and Backpressure management**
- **Checkpoints & Savepoints** for recovery
- **Parallelism tuning** and **slot sharing groups**

### 7️⃣ **Cluster Deployment**
- **Local / Standalone**
- **YARN Mode**
- **Kubernetes Mode**
- **Docker Setup**
- Includes `start-cluster.sh`, Docker Compose templates, and YAML manifests

---

## 🧠 **Hands-On Jobs Included**

| Job | Description |
|------|--------------|
| `WordCount` | Classic example for stream and batch modes |
| `WordCountFilter` | Demonstrates operator chaining (`map + filter`) |
| `WindowJoinExample` | Sliding and tumbling window joins |
| `BroadcastJoinExample` | Broadcast join optimization demo |
| `TemporalJoinExample` | Stream enrichment with bounded dimension table |
| `StatefulProcessingExample` | Keyed state and timer-based processing |

---

## 🧩 **Installation & Usage**

### Prerequisites
- Java 8+
- Maven 3.6+
- Flink 1.20+

### Build JAR
```bash
mvn clean package
```
### Run Locally
```bash

~/Downloads/flink/flink/bin/start-cluster.sh
~/Downloads/flink/flink/bin/flink run \
-c quantumroot.example.WordCountFilter \
target/quantumroot-0.0.1-SNAPSHOT.jar \
--input ~/code/flink/data/input \
--output ~/Downloads/flink/output.csv
```
### Flink UI
Access Flink Dashboard → http://localhost:8081
