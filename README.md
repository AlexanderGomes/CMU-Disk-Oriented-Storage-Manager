# CMU Storage Engine

## Motive
This project serves as a practical application of the knowledge I gained from the "Intro To Database Systems - CMU" course.

## Components

### Buffer Pool Manager

The Buffer Pool Manager plays a crucial role in managing the memory used for caching data pages. It ensures efficient utilization of memory resources and optimizes data retrieval operations.

### Replacer

The Replacer component is responsible for managing the replacement strategy within the buffer pool. It determines which pages should be evicted from memory when additional space is required.

### Disk Manager

The Disk Manager facilitates interactions between the database system and the physical storage devices. It oversees tasks such as reading and writing data pages to and from disk.

### Disk Scheduler

The Disk Scheduler optimizes the order of disk operations to minimize seek times and enhance overall disk I/O performance. It aims to efficiently schedule disk access requests to reduce latency.

## Pages Layout

### Directory Page

Directory pages maintain a hierarchical structure for efficiently locating data within the database. They store metadata about the organization of data pages, facilitating quick access to relevant information.

### Row Pages

Row pages store actual data records within the database. They organize data in a format suitable for efficient retrieval and manipulation operations.
