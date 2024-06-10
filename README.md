# CMU Storage Engine

A disk-oriented storage manager with SQL query support.

## Table of Contents

1. [Motive](#motive)
2. [Future Changes](#future-changes)
3. [Components](#components)
   - [Distributed](#distributed)
   - [Query Engine](#query-engine)
   - [Buffer Pool Manager](#buffer-pool-manager)
   - [Replacer](#replacer)
   - [Disk Manager](#disk-manager)
   - [Disk Scheduler](#disk-scheduler)
4. [Pages Layout](#pages-layout)
   - [Directory Page](#directory-page)
   - [Row Pages](#row-pages)
5. [How to Run the Project](#how-to-run-the-project)

## Motive

This project serves as a practical application of the knowledge gained from the "Intro To Database Systems - CMU" course, inspiring the decision to build a custom implementation from scratch.

## Future Changes

- [x] Create a query engine, adding SQL support to the storage engine, implementing concepts learned in query optimization and execution.
- [x] Learn distributed systems by implementing data replication.

## Components

### Distributed

- **Paxos for Leader Election**: Utilized Paxos for leader election, ensuring agreement on values by a quorum of nodes.
- **Heartbeats**: Established Heartbeats to enable any node to detect leader failures and initiate an election.
- **Multi-Paxos and Total Order Broadcasting**: Deployed Multi-Paxos and total order broadcasting to maintain consistency and query order.
- **Split Brain Scenarios**: Addressed split brain scenarios by implementing a quorum system to ensure consistency.
- **gRPC**: Employed gRPC for executing queries across all nodes in the system.
- **Custom TCP Server**: Developed a custom TCP server to ensure node awareness and keep clients updated on the leader node.

### Query Engine

- **SQL Parsing**: Parsed SQL queries efficiently.
- **Rule-based Planner**: Developed a rule-based planner.
- **Join Algorithm Optimization**: Optimized join algorithm to enhance query execution.
- **Graceful Termination Handling**: Implemented graceful handling of process termination to prevent data loss or corruption in the buffer pool.
- **Thread-safe Code**: Ensured thread-safe code for manipulating numerous pages with diverse queries.

### Buffer Pool Manager

Implemented a robust buffer pool capable of efficiently managing 4GB of pages, approximately one billion pages. The buffer pool seamlessly handled numerous query requests, ensuring data consistency and eliminating locking overhead. It achieved reading all one billion pages in just 4 minutes.

### Replacer

Implemented LRU-K, selecting pages for eviction based on their recent accesses and frequency, ensuring that only essential pages remain in the buffer pool.

### Disk Manager

Facilitates interactions between the buffer pool and the disk, managing the Directory page, Row Pages, and headers stored on the disk.

### Disk Scheduler

Optimizes the order of disk operations to minimize seek times and enhance overall disk I/O performance, efficiently scheduling disk access requests to reduce latency.

## Pages Layout

### Directory Page

The design of the directory page was changed from EXTENDIBLE HASH INDEX to a B+ Tree, compressing storage and allowing for range searches.

### Row Pages

Store actual data records within the database. A hashmap was used as each page could only hold 50 entries of JSON data. If storing as bytes, a different data structure would be utilized.

## How to Run the Project

Simply download the source code and run it in the terminal:

```bash
go mod tidy
go run main.go
```

This will showcase pages being created, accessed, and evicted to the DB-file, along with the system's nodes' activity, leader election, and fault detection.
