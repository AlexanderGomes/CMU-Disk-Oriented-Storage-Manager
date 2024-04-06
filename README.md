# CMU Storage Engine

## Motive
This project serves as a practical application of the knowledge I gained from the "Intro To Database Systems - CMU" course.

## Future Changes
- [ ] create a query engine in C++, adding SQL support to the storage engine, implementing the concepts I learned when it comes to query optimization and execution.

- [ ] Do the one billion rows challenge and analyze what changes need to be made to my storage and query engine to efficiently store and retrieve such a quantity of rows.

=======
## Components

### Buffer Pool Manager

The Buffer Pool Manager plays a crucial role in managing the memory used for caching data pages. It ensures efficient utilization of memory resources and optimizes data retrieval operations.

### Replacer

The Replacer component is responsible for managing the replacement strategy within the buffer pool. It determines which pages should be evicted from memory when additional space is required, I chose to use the LRU-K algorithm taking into consideration both past access timestamps and the frequency of pages.

### Disk Manager

The Disk Manager facilitates interactions between the buffer pool and the disk, It manages the Directory page, Row Pages, and headers stored in the disk.
=======
The Replacer component is responsible for managing the replacement strategy within the buffer pool. It determines which pages should be evicted from memory when additional space is required.

### Disk Manager

The Disk Manager facilitates interactions between the database system and the physical storage devices. It oversees tasks such as reading and writing data pages to and from disk.
>>>>>>> a2571c6 (Initializing README.md)

### Disk Scheduler

The Disk Scheduler optimizes the order of disk operations to minimize seek times and enhance overall disk I/O performance. It aims to efficiently schedule disk access requests to reduce latency.

## Pages Layout

### Directory Page

I changed the design of the directory page from EXTENDIBLE HASH INDEX to a B+ Tree, which compressed storage and allowed for range searches.

### Row Pages

Row pages store actual data records within the database. I used a hashmap for it since each page could only hold 50 entries of JSON data, if I was to store it as bytes I would use a different data structure.

## How to Run the Project
just download the source code and run the following on the terminal
- go run main.go

you will see pages getting created, accessed and evicted to the DB-file.
=======
Directory pages maintain a hierarchical structure for efficiently locating data within the database. They store metadata about the organization of data pages, facilitating quick access to relevant information.

### Row Pages

Row pages store actual data records within the database. They organize data in a format suitable for efficient retrieval and manipulation operations.

=======
### How to Run the Project
just download the source code and run it on the terminal
- go run main.go

you will see pages getting created, accessed and evicted to the DB-file.
