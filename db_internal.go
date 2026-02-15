package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// flush writes the current memtable to a new level-0 SSTable,
// resets the WAL, and triggers compaction if needed.
func (db *DB) flush() error {
	// Convert memtable entries to SSTable entries
	memEntries := db.mem.Entries()
	sstEntries := make([]SSTableEntry, len(memEntries))
	for i, e := range memEntries {
		sstEntries[i] = SSTableEntry{
			Key:       e.key,
			Value:     e.value,
			Tombstone: e.tombstone,
		}
	}

	// Write the new SSTable at level 0
	path := db.sstPath(0, db.nextSeq)
	if err := WriteSSTable(path, sstEntries); err != nil {
		return fmt.Errorf("db flush: %w", err)
	}

	// Open it for reading
	reader, err := OpenSSTable(path)
	if err != nil {
		return fmt.Errorf("db open flushed sst: %w", err)
	}

	// Prepend to the list (newest first)
	db.sstables = append([]*SSTableReader{reader}, db.sstables...)
	db.nextSeq++

	// Reset memtable and WAL
	db.mem = NewMemtable(DefaultMemtableSize)
	db.wal.Close()
	os.Remove(filepath.Join(db.dir, "wal"))
	wal, err := OpenWAL(filepath.Join(db.dir, "wal"))
	if err != nil {
		return fmt.Errorf("db reset wal: %w", err)
	}
	db.wal = wal

	return db.maybeCompact()
}

// maybeCompact triggers compaction when level-0 has too many SSTables.
// We compact ALL SSTables (L0 + L1) into a single new file. This is
// simple and makes tombstone removal safe: there are no older files
// that could still hold a deleted key.
func (db *DB) maybeCompact() error {
	level0 := db.level0SSTables()
	if len(level0) < CompactionThreshold {
		return nil
	}

	// Collect paths for ALL existing SSTables, newest first by sequence.
	// kWayMerge treats the lowest index as newest, so this ordering
	// ensures the most recent write wins when duplicate keys exist.
	allPaths := db.allSSTables()

	readers := make([]*SSTableReader, len(allPaths))
	for i, path := range allPaths {
		r, err := OpenSSTable(path)
		if err != nil {
			return fmt.Errorf("compaction open: %w", err)
		}
		readers[i] = r
	}

	// Merge everything into one output SSTable
	outputPath := db.sstPath(1, db.nextSeq)
	if err := Compact(readers, outputPath); err != nil {
		for _, r := range readers {
			r.Close()
		}
		return fmt.Errorf("compaction: %w", err)
	}
	for _, r := range readers {
		r.Close()
	}

	// Close existing readers and remove ALL old SSTable files
	for _, sst := range db.sstables {
		sst.Close()
	}
	for _, path := range allPaths {
		os.Remove(path)
	}
	db.nextSeq++

	// Reload from disk (just the one new file)
	db.sstables = nil
	return db.loadSSTables()
}

// level0SSTables returns paths of all level-0 SSTable files.
func (db *DB) level0SSTables() []string {
	entries, _ := os.ReadDir(db.dir)
	var paths []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "0-") && strings.HasSuffix(e.Name(), ".sst") {
			paths = append(paths, filepath.Join(db.dir, e.Name()))
		}
	}
	return paths
}

// allSSTables returns paths of ALL .sst files sorted newest-first
// by sequence number. This ordering is critical: kWayMerge treats
// the lowest index as newest, so the most recent write wins.
func (db *DB) allSSTables() []string {
	entries, _ := os.ReadDir(db.dir)

	type sstInfo struct {
		path string
		seq  int
	}
	var ssts []sstInfo
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(e.Name(), ".sst"), "-")
		if len(parts) != 2 {
			continue
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		ssts = append(ssts, sstInfo{
			path: filepath.Join(db.dir, e.Name()),
			seq:  seq,
		})
	}

	// Newest first
	sort.Slice(ssts, func(i, j int) bool {
		return ssts[i].seq > ssts[j].seq
	})

	paths := make([]string, len(ssts))
	for i, s := range ssts {
		paths[i] = s.path
	}
	return paths
}

// loadSSTables scans the directory for .sst files and opens them,
// sorted newest-first by sequence number.
func (db *DB) loadSSTables() error {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	type sstInfo struct {
		path string
		seq  int
	}

	var ssts []sstInfo
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(e.Name(), ".sst"), "-")
		if len(parts) != 2 {
			continue
		}
		seq, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		ssts = append(ssts, sstInfo{
			path: filepath.Join(db.dir, e.Name()),
			seq:  seq,
		})
		if seq >= db.nextSeq {
			db.nextSeq = seq + 1
		}
	}

	// Sort newest first
	sort.Slice(ssts, func(i, j int) bool {
		return ssts[i].seq > ssts[j].seq
	})

	for _, info := range ssts {
		reader, err := OpenSSTable(info.path)
		if err != nil {
			return fmt.Errorf("load sst %s: %w", info.path, err)
		}
		db.sstables = append(db.sstables, reader)
	}
	return nil
}

// sstPath returns the file path for an SSTable.
func (db *DB) sstPath(level, seq int) string {
	return filepath.Join(db.dir, fmt.Sprintf("%d-%06d.sst", level, seq))
}
