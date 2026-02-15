package lsm

import (
	"fmt"
	"os"
	"path/filepath"
)

// DB is the top-level LSM tree database. It provides a simple
// key-value interface backed by a write-ahead log, an in-memory
// sorted buffer (memtable), and sorted string tables (SSTables)
// on disk.
type DB struct {
	dir      string
	wal      *WAL
	mem      *Memtable
	sstables []*SSTableReader // newest first
	nextSeq  int              // next SSTable sequence number
}

// ErrKeyNotFound is returned when a key doesn't exist.
var ErrKeyNotFound = fmt.Errorf("key not found")

// Open opens or creates a database at the given directory path.
// On startup it replays the WAL to recover any writes that weren't
// flushed to SSTables, and loads existing SSTables.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("db mkdir: %w", err)
	}

	db := &DB{
		dir:     dir,
		mem:     NewMemtable(DefaultMemtableSize),
		nextSeq: 1,
	}

	// Load existing SSTables
	if err := db.loadSSTables(); err != nil {
		return nil, fmt.Errorf("db load sstables: %w", err)
	}

	// Replay WAL into memtable for crash recovery
	walPath := filepath.Join(dir, "wal")
	entries, err := Replay(walPath)
	if err != nil {
		return nil, fmt.Errorf("db replay wal: %w", err)
	}
	for _, e := range entries {
		switch e.Op {
		case OpPut:
			db.mem.Put(string(e.Key), e.Value)
		case OpDelete:
			db.mem.Delete(string(e.Key))
		}
	}

	// Open WAL for new writes
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("db open wal: %w", err)
	}
	db.wal = wal

	return db, nil
}

// Put writes a key-value pair to the database.
// The write is durable as soon as this returns — it's in the WAL.
func (db *DB) Put(key string, value []byte) error {
	if err := db.wal.Append(WALEntry{Op: OpPut, Key: []byte(key), Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	if db.mem.IsFull() {
		return db.flush()
	}
	return nil
}

// Get reads a value by key. Returns ErrKeyNotFound if the key
// doesn't exist or was deleted.
func (db *DB) Get(key string) ([]byte, error) {
	// Check memtable first (most recent data)
	if val, found := db.mem.Get(key); found {
		if val == nil {
			return nil, ErrKeyNotFound // tombstone
		}
		return val, nil
	}

	// Check SSTables from newest to oldest
	for _, sst := range db.sstables {
		val, tombstone, found := sst.Get(key)
		if found {
			if tombstone {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Delete removes a key by writing a tombstone marker.
func (db *DB) Delete(key string) error {
	if err := db.wal.Append(WALEntry{Op: OpDelete, Key: []byte(key)}); err != nil {
		return err
	}
	db.mem.Delete(key)
	if db.mem.IsFull() {
		return db.flush()
	}
	return nil
}

// Close flushes the memtable and closes all resources.
func (db *DB) Close() error {
	if db.mem.Len() > 0 {
		if err := db.flush(); err != nil {
			return err
		}
	}
	for _, sst := range db.sstables {
		sst.Close()
	}
	return db.wal.Close()
}

// Stats returns diagnostic information about the database.
func (db *DB) Stats() DBStats {
	return DBStats{
		NumSSTables:   len(db.sstables),
		MemtableSize:  db.mem.Size(),
		MemtableCount: db.mem.Len(),
		WALSize:       db.wal.Size(),
	}
}

// DBStats holds database diagnostic information.
type DBStats struct {
	NumSSTables   int
	MemtableSize  int
	MemtableCount int
	WALSize       int64
}
