# CMU Storage Engine

## Motive
This project serves as a practical application of the knowledge I gained from the "Intro To Database Systems - CMU" course.

## Future Changes
- [x] create a query engine, adding SQL support to the storage engine, implementing the concepts I learned when it comes to query optimization and execution.

## Components

### Query Engine
The most exciting part of the project:
- [x] parsing SQL queries.
- [x] creating a rule-based planner.
- [x] optimizing algorithms for better query execution.
- [x] gracefully handling process termination so all changes made to the buffer pool don't get lost.
- [x] creating thread-safe code for manipulating hundreds of pages with many different queries.

### Buffer Pool Manager

The Buffer Pool Manager manages the memory used for caching data pages. It ensures efficient utilization of memory resources and optimizes data retrieval operations.

### Replacer

The Replacer component is responsible for managing the replacement strategy within the buffer pool. It determines which pages should be evicted from memory when additional space is required, I chose to use the LRU-K algorithm taking into consideration both past access timestamps and the frequency of page accesses.

### Disk Manager

The Disk Manager facilitates interactions between the buffer pool and the disk, It manages the Directory page, Row Pages, and headers stored in the disk.

### Disk Scheduler

The Disk Scheduler optimizes the order of disk operations to minimize seek times and enhance overall disk I/O performance. It aims to efficiently schedule disk access requests to reduce latency.

## Pages Layout

### Directory Page

I changed the design of the directory page from EXTENDIBLE HASH INDEX to a B+ Tree, which compressed storage and allowed for range searches.

### Row Pages

Row pages store actual data records within the database. I used a hashmap for it since each page could only hold 50 entries of JSON data, if I was to store it as bytes I would use a different data structure.

## How to Run the Project
just download the source code and run it on the terminal
- go run main.go

you will see pages getting created, accessed and evicted to the DB-file.
