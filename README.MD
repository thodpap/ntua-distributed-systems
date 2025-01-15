# Distributed Systems - Chordify Project

### **Author**
Thodoris Paparrigopoulos  
PhD Student  
tpaparrigopoulos - 03003199  

---

## **Table of Contents**
1. [Introduction](#introduction)
2. [Code Architecture](#code-architecture)
   - [Client](#client)
   - [Server](#server)
3. [Operations](#operations)
   - [Join](#join)
   - [Put](#put)
   - [Delete](#delete)
   - [Get](#get)
   - [Depart](#depart)
4. [Experiments](#experiments)
   - [Insert & Query Experiment](#insert--query-experiment)
   - [Requests Experiments](#requests-experiments)
5. [Explanation of Results](#explanation-of-results)
6. [Observations](#observations)

---

## **Introduction**
The Chordify project focuses on the design and implementation of a peer-to-peer (P2P) file-sharing application leveraging the Chord Distributed Hash Table (DHT) protocol. The goal is to create a scalable and efficient system that allows users to store and locate song metadata across a distributed network of nodes. The project emphasizes core functionalities, including:
- Node management
- Data replication
- Efficient routing
- Exploration of consistency models: **Linearizability** and **Eventual Consistency**

This work provides practical insights into the operation and challenges of distributed systems, emphasizing the balance between performance and consistency.

---

## **Code Architecture**
The codebase is structured into two primary components: the **Client** and the **Server**.

### **Client**
#### **CLI**
The client provides a command-line interface (CLI) for interacting with the server. Supported commands include:
1. **Insert**:  
   `insert <key> <value> [--host <host>] [--port <port>]`  
   Adds a new `<key, value>` pair. By default, `value` is set to `<host>:<port>`.
2. **Query**:  
   `query <key> [--host <host>] [--port <port>]`  
   Retrieves the value for the given `<key>` or returns all pairs with `*`.
3. **Delete**:  
   `delete <key> [--host <host>] [--port <port>]`  
   Deletes the `<key, value>` pair.
4. **Overlay**:  
   Displays the current network topology.
5. **Info**:  
   Provides details about the node, such as ID and neighbors.
6. **Depart**:  
   Removes the node from the network gracefully.
7. **Help**:  
   Prints a summary of all commands.

#### **Supporting Files**
Shell scripts automate experiments and evaluate system performance:
- `run_inserts.sh`: Automates insertions.
- `run_queries.sh`: Automates key queries.
- `run_requests.sh`: Executes mixed operations.
- `run_experiment.sh`: Combines scripts into a single workflow.

---

### **Server**
The server handles the core logic of the Chord DHT and communicates with clients and nodes in the network.

#### **Main Components**
1. **main.py**  
   - Entry point for the application.
   - Handles server startup and graceful shutdown.
2. **server.py**  
   - Manages socket creation and communication.
   - Implements a custom protocol for large data transmission.
   - Dispatches commands like `PUT`, `GET`, `DELETE`, and `TRANSFER_KEYS`.
3. **chord_node_simple.py**  
   - Core logic for the DHT.
   - Manages node operations, routing, and consistency.

---

## **Operations**
### **Join**
A new node contacts the bootstrap node, retrieves its successor and predecessor, and acquires keys in its range. Replication updates occur either asynchronously (eventual consistency) or synchronously (linearizability).

### **Put**
Handles data insertion. The **primary node** stores the key, introduces TTL, and replicates to successors.

### **Delete**
Deletes data. TTL ensures that replicas across the network are updated.

### **Get**
Retrieves data. Eventual consistency prioritizes local reads for faster responses. Linearization queries the tail of the replication chain for the most consistent result.

### **Depart**
Gracefully removes the node:
1. Deletes uploaded data.
2. Notifies successor and predecessor.
3. Transfers keys to successor.
4. Clears local data store.

---

## **Experiments**

### **Insert & Query Experiment**
| **Consistency**       | **K=1 Insert (req/s)** | **K=3 Insert (req/s)** | **K=5 Insert (req/s)** | **K=1 Query (req/s)** | **K=3 Query (req/s)** | **K=5 Query (req/s)** |
|------------------------|------------------------|------------------------|------------------------|-----------------------|-----------------------|-----------------------|
| **Linearization**      | 181.87                | 178.11                | 168.47                | 182.58               | 176.47               | 171.10               |
| **Eventual Consistency**| 188.10                | 181.85                | 169.77                | 203.38               | 207.00               | 183.99               |

---

### **Requests Experiments**
| **Command**           | **Linearization**                                | **Eventual Consistency**                       |
|------------------------|------------------------------------------------|-----------------------------------------------|
| query, Hey Jude        | {'id': -1, 'value': []}                        | {'id': -1, 'value': []}                       |
| insert, Respect, 528   | {'status': 'OK'}                               | {'status': 'OK'}                              |
| query, Hey Jude        | {'id': 9, 'value': [' 524']}                   | {'id': 247, 'value': [' 524']}               |
| query, Like a Rolling Stone | {'id': 9, 'value': [' 583']}             | {'id': 247, 'value': [' 583']}               |
| query, Respect         | {'id': 9, 'value': [' 528', ' 553']}           | {'id': 247, 'value': [' 528', ' 553']}       |
| insert, Respect, 522   | {'status': 'OK'}                               | {'status': 'OK'}                              |
| insert, Hey Jude, 542  | {'status': 'OK'}                               | {'status': 'OK'}                              |
| query, Hey Jude        | {'id': 9, 'value': [' 539', ' 542', ' 523']}   | {'id': 247, 'value': [' 542', ' 561']}       |

---

## **Explanation of Results**
1. **Linearization**:  
   - Ensures strong consistency with synchronized operations.
   - Queries always return the most consistent data.

2. **Eventual Consistency**:  
   - Prioritizes performance by performing asynchronous operations.
   - May return stale or incomplete results due to delayed updates.

---

## **Observations**
- **Trade-offs**:  
  - Linearization provides strong consistency but higher latency.
  - Eventual consistency offers better performance but weaker guarantees.

- **Impact of Replication (K)**:  
  - Higher \(K\) slows down insertions due to increased overhead.
  - Queries are faster under eventual consistency as more nodes hold data.

- **Use Cases**:  
  - **Linearization**: Suitable for critical systems requiring accuracy (e.g., banking).  
  - **Eventual Consistency**: Ideal for scenarios prioritizing speed (e.g., social media feeds).

--- 

