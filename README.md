# rocksdb-rs
rust version of rocksdb

## Why we need to build a rust version of rocksdb

### Clean and simple architecture

RocksDB is a common data engine for multiple kinds of database,
and one of the most important application among them is MyRocks, which is the kernel
engine to replace InnoDB in MySQL. Obviously, most users in RocksDB community do not need
a transaction engine for MySQL, we just want a simple but well-perform KV engine.
The RocksDB has merged so many features which we may never enable them and they made this project
hard to maintain. I want to build a simple database which is easy to maintain for simple KV application. 

### Better support for asynchronous IO

RocksDB does not support asynchronous IO. Not only for IO, but also other method such as `Ingest` and `CreateColumnFamily` are 
also synchronous. It means that every method may block the user thread for a long time. In cloud environment, this problem may
be worse because the latency of cloud disk is much higher than local NVMe SSD.

## Development Guide

### Model And Architecture

Our engine has five main modules, which are `WAL`, `MANIFEST`, `Version`, `Compaction`, `Table`.

* `WAL` module will assign sequence for every writes and then write them into a write-ahead-log file. It will run as an
independent future task, and some other jobs may also be processed in this module, such as ingest. 
You can think of him as a combination of `write_thread` and `WriteToWAL` in RocksDB.
The format of file is compatible with RocksDB, so that we can start this engine at the RocksDB directory. 
* `MANIFEST` will persist changes for SST files, include the result of compaction and flush jobs.
* The most important structure of `Version` module are `VersionSet` and `KernelNumberContext`. I split them from `VersionSet` of RocksDB. If one operation can convert to an atomic operation,
I store it in `KernelNumberContext`, otherwise it must acquire a lock guard for `Arc<Mutex<VersionSet>>`. `VersionSet` will manage the info of `ColumnFamily` and every `ColumnFamily` will 
own a `SuperVersion`, which include the collection of `Memtable` and the collection of `SSTable`. `SuperVersion` consists of `MemtableList` and `Version`, 
every time we switch memtable for one `ColumnFamily`, we will create a new `SuperVersion` with the new `Memtable` and the old `Version`. Every time we finish a compaction job or a flush job,
we will create a new `SuperVersion` with the old `Memtable` and the new `Version`.
* `Compaction` module consists all code for `Compaction` and `Flush`.
* `Table` module consists the SSTable format and the read operation or write operation.

## TODO List

### DB

* Compact files from Level0 to other level.
* Support create iterator.
* Support snapshot isolation for get and iterator.

### Table
* Support LZ4 and ZSTD compression algorithm.
* Support hash-index for small data block.
* Support block-cache.

### IO
* Support AIO for real asynchronous IO.
