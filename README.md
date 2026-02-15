# lsm-engine

A crash-safe key-value storage engine built from scratch in Go. Implements write-ahead logging, memtables, SSTables, bloom filters, and compaction in ~1,000 lines with zero external dependencies.

Built as a companion to the blog post: [Building a Storage Engine: WAL, LSM Trees, and SSTables from Scratch](https://deveshshetty.com/blog/lsm-storage-engine/)

## What's inside

| File | Purpose |
|------|---------|
| `wal.go` | Write-ahead log with CRC32 checksums and fsync per write |
| `memtable.go` | In-memory sorted buffer using binary search insertion |
| `bloom.go` | Bloom filter with FNV-1a double hashing |
| `sstable.go` | SSTable writer (data + index + bloom + footer) |
| `sstable_reader.go` | SSTable reader with bloom filter check and binary search |
| `compaction.go` | K-way merge of sorted SSTables |
| `db.go` | Public API: Open, Put, Get, Delete, Close |
| `db_internal.go` | Flush, compaction trigger, SSTable loading |

## Quick start

Requires Go 1.21+. No external dependencies.

```bash
git clone https://github.com/devesh-shetty/lsm-engine.git
cd lsm-engine
go test ./... -v
```

Run the demo:

```bash
go run ./cmd/demo/
```

Run the crash recovery test (writes 500 keys, exits without Close, reopens and verifies):

```bash
go run ./cmd/crashtest/
```

## Performance

Measured on Apple M3 Pro, macOS 26.1:

| Operation | Throughput |
|-----------|------------|
| Sequential writes | ~240 ops/sec (fsync per write) |
| Random reads | ~4M ops/sec (in-memory memtable) |
| Mixed 50/50 | ~450 ops/sec |

Write throughput is dominated by fsync. Production engines batch writes to amortize this cost.

## Architecture

```
Put(k, v)                                Get(k)
    |                                       |
    v                                       v
 WAL append + fsync                    Memtable lookup
    |                                       |
    v                                   not found
 Memtable insert                            |
    |                                       v
    v                              SSTables (newest first)
 Full? --> flush to SSTable          bloom filter -> index -> disk
    |
    v
 Compact if needed
```

## What this doesn't do

This is an educational implementation. Production engines like RocksDB add leveled compaction, block compression, block caches, concurrent compaction, MANIFEST tracking, write batching, MVCC, and more. See the [blog post](https://deveshshetty.com/blog/lsm-storage-engine/) for details on what's missing and why.

## License

MIT
