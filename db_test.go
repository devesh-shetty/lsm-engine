package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// --- Basic DB operations ---

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestGetMissing(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put("key1", []byte("val1"))
	db.Delete("key1")

	_, err = db.Get("key1")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put("key", []byte("first"))
	db.Put("key", []byte("second"))

	val, err := db.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "second" {
		t.Fatalf("expected 'second', got %q", val)
	}
}

// --- Persistence: close and reopen ---

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	// Write some data and close
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		db.Put(key, []byte(val))
	}
	db.Close()

	// Reopen and verify
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expected := fmt.Sprintf("val-%04d", i)
		val, err := db2.Get(key)
		if err != nil {
			t.Fatalf("key %s not found after reopen: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("key %s: expected %q, got %q", key, expected, val)
		}
	}
}

// --- Crash recovery: write to WAL, don't flush, simulate recovery ---

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write directly to WAL without flushing memtable to SSTable
	walPath := filepath.Join(dir, "wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Append(WALEntry{Op: OpPut, Key: []byte("crash-key"), Value: []byte("crash-val")})
	wal.Append(WALEntry{Op: OpPut, Key: []byte("another"), Value: []byte("entry")})
	wal.Close()

	// Open the database — it should replay the WAL
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val, err := db.Get("crash-key")
	if err != nil {
		t.Fatalf("crash-key not recovered: %v", err)
	}
	if string(val) != "crash-val" {
		t.Fatalf("expected 'crash-val', got %q", val)
	}

	val, err = db.Get("another")
	if err != nil {
		t.Fatalf("another not recovered: %v", err)
	}
	if string(val) != "entry" {
		t.Fatalf("expected 'entry', got %q", val)
	}
}

// --- WAL: corrupted entries are skipped ---

func TestWALCorruptedTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Write valid entries
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Append(WALEntry{Op: OpPut, Key: []byte("good"), Value: []byte("data")})
	wal.Close()

	// Append garbage to simulate a partial write
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF, 0xFF})
	f.Close()

	// Replay should return only the valid entry
	entries, err := Replay(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if string(entries[0].Key) != "good" {
		t.Fatalf("expected key 'good', got %q", entries[0].Key)
	}
}

// --- Bloom filter false positive rate ---

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	n := 10000
	bf := NewBloomFilter(n, 0.01)

	// Add n items
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	// All added items must be found (no false negatives)
	for i := 0; i < n; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative for key-%d", i)
		}
	}

	// Check false positive rate on items NOT in the set
	fp := 0
	tests := 100000
	for i := n; i < n+tests; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			fp++
		}
	}

	rate := float64(fp) / float64(tests)
	t.Logf("Bloom filter false positive rate: %.4f%% (target: 1%%)", rate*100)
	if rate > 0.02 { // allow some margin
		t.Fatalf("false positive rate too high: %.4f%%", rate*100)
	}
}

// --- Bloom filter serialization roundtrip ---

func TestBloomSerialize(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add([]byte("alpha"))
	bf.Add([]byte("beta"))

	data := bf.Serialize()
	bf2 := DeserializeBloom(data)

	if !bf2.MayContain([]byte("alpha")) {
		t.Fatal("alpha not found after deserialize")
	}
	if !bf2.MayContain([]byte("beta")) {
		t.Fatal("beta not found after deserialize")
	}
}

// --- SSTable read/write roundtrip ---

func TestSSTableRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := []SSTableEntry{
		{Key: "apple", Value: []byte("red")},
		{Key: "banana", Value: []byte("yellow")},
		{Key: "cherry", Value: []byte("red"), Tombstone: true},
		{Key: "date", Value: []byte("brown")},
	}

	if err := WriteSSTable(path, entries); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Lookup existing keys
	val, tomb, found := reader.Get("apple")
	if !found || tomb || string(val) != "red" {
		t.Fatalf("apple: got val=%q tomb=%v found=%v", val, tomb, found)
	}

	val, tomb, found = reader.Get("banana")
	if !found || tomb || string(val) != "yellow" {
		t.Fatalf("banana: got val=%q tomb=%v found=%v", val, tomb, found)
	}

	// Tombstone entry
	_, tomb, found = reader.Get("cherry")
	if !found || !tomb {
		t.Fatalf("cherry should be a tombstone: tomb=%v found=%v", tomb, found)
	}

	// Missing key
	_, _, found = reader.Get("elderberry")
	if found {
		t.Fatal("elderberry should not be found")
	}
}

// --- Compaction correctness ---

func TestCompaction(t *testing.T) {
	dir := t.TempDir()

	// Create two SSTables with overlapping keys
	sst1 := filepath.Join(dir, "0-000001.sst")
	WriteSSTable(sst1, []SSTableEntry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("old-b")},
		{Key: "c", Value: []byte("1")},
	})

	sst2 := filepath.Join(dir, "0-000002.sst")
	WriteSSTable(sst2, []SSTableEntry{
		{Key: "b", Value: []byte("new-b")},  // newer value
		{Key: "c", Tombstone: true},           // delete c
		{Key: "d", Value: []byte("2")},
	})

	r1, _ := OpenSSTable(sst1)
	r2, _ := OpenSSTable(sst2)

	output := filepath.Join(dir, "1-000003.sst")
	// Newer SSTable first in the readers slice
	if err := Compact([]*SSTableReader{r2, r1}, output); err != nil {
		t.Fatal(err)
	}
	r1.Close()
	r2.Close()

	// Read compacted SSTable
	reader, err := OpenSSTable(output)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// "a" should survive
	val, _, found := reader.Get("a")
	if !found || string(val) != "1" {
		t.Fatalf("a: expected '1', got %q (found=%v)", val, found)
	}

	// "b" should have the newer value
	val, _, found = reader.Get("b")
	if !found || string(val) != "new-b" {
		t.Fatalf("b: expected 'new-b', got %q (found=%v)", val, found)
	}

	// "c" should be gone (tombstone removed during compaction)
	_, _, found = reader.Get("c")
	if found {
		t.Fatal("c should have been removed by compaction")
	}

	// "d" should survive
	val, _, found = reader.Get("d")
	if !found || string(val) != "2" {
		t.Fatalf("d: expected '2', got %q (found=%v)", val, found)
	}
}

// --- Large workload: 10,000+ keys ---

func TestLargeWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large workload test")
	}

	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	n := 10000

	// Write n keys
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%08d", i)
		val := fmt.Sprintf("val-%08d", i)
		if err := db.Put(key, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}

	// Read all back
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%08d", i)
		expected := fmt.Sprintf("val-%08d", i)
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("missing key %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("key %s: expected %q, got %q", key, expected, val)
		}
	}

	// Delete half
	for i := 0; i < n; i += 2 {
		key := fmt.Sprintf("key-%08d", i)
		if err := db.Delete(key); err != nil {
			t.Fatal(err)
		}
	}

	// Verify deletes and remaining keys
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%08d", i)
		val, err := db.Get(key)
		if i%2 == 0 {
			if err != ErrKeyNotFound {
				t.Fatalf("key %s should be deleted", key)
			}
		} else {
			if err != nil {
				t.Fatalf("key %s should exist: %v", key, err)
			}
			expected := fmt.Sprintf("val-%08d", i)
			if string(val) != expected {
				t.Fatalf("key %s: expected %q, got %q", key, expected, val)
			}
		}
	}

	db.Close()

	// Reopen and verify persistence
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for i := 1; i < n; i += 2 {
		key := fmt.Sprintf("key-%08d", i)
		expected := fmt.Sprintf("val-%08d", i)
		val, err := db2.Get(key)
		if err != nil {
			t.Fatalf("key %s not found after reopen: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("key %s: expected %q, got %q", key, expected, val)
		}
	}

	stats := db2.Stats()
	t.Logf("Stats after 10k workload: %+v", stats)
}
