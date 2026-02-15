package lsm

import "sort"

const DefaultMemtableSize = 4 * 1024 * 1024 // 4 MB

// memEntry is a single key-value pair stored in the memtable.
// A nil value represents a tombstone (deleted key).
type memEntry struct {
	key       string
	value     []byte
	tombstone bool
}

// Memtable is an in-memory sorted buffer of key-value pairs.
// It uses a sorted slice with binary search for lookups and
// insertions. Once the approximate size exceeds the threshold,
// the caller should flush it to an SSTable.
type Memtable struct {
	entries   []memEntry
	size      int // approximate memory usage in bytes
	threshold int
}

// NewMemtable creates a memtable with the given size threshold.
func NewMemtable(threshold int) *Memtable {
	return &Memtable{
		threshold: threshold,
	}
}

// Put inserts or updates a key-value pair.
func (m *Memtable) Put(key string, value []byte) {
	idx := m.search(key)

	if idx < len(m.entries) && m.entries[idx].key == key {
		// Update existing entry — adjust size tracking
		m.size -= len(m.entries[idx].value)
		m.entries[idx].value = value
		m.entries[idx].tombstone = false
		m.size += len(value)
		return
	}

	// Insert new entry at the correct sorted position
	entry := memEntry{key: key, value: value}
	m.entries = append(m.entries, memEntry{}) // grow by one
	copy(m.entries[idx+1:], m.entries[idx:])
	m.entries[idx] = entry
	m.size += len(key) + len(value) + 1 // +1 for tombstone flag overhead
}

// Get retrieves the value for a key. Returns (value, true) if found,
// (nil, true) if the key was deleted (tombstone), or (nil, false) if
// the key was never written.
func (m *Memtable) Get(key string) ([]byte, bool) {
	idx := m.search(key)
	if idx < len(m.entries) && m.entries[idx].key == key {
		if m.entries[idx].tombstone {
			return nil, true // deleted
		}
		return m.entries[idx].value, true
	}
	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone.
func (m *Memtable) Delete(key string) {
	idx := m.search(key)

	if idx < len(m.entries) && m.entries[idx].key == key {
		m.size -= len(m.entries[idx].value)
		m.entries[idx].value = nil
		m.entries[idx].tombstone = true
		return
	}

	// Key doesn't exist yet — insert a tombstone
	entry := memEntry{key: key, tombstone: true}
	m.entries = append(m.entries, memEntry{})
	copy(m.entries[idx+1:], m.entries[idx:])
	m.entries[idx] = entry
	m.size += len(key) + 1
}

// IsFull returns true when the memtable has reached its size threshold.
func (m *Memtable) IsFull() bool {
	return m.size >= m.threshold
}

// Len returns the number of entries (including tombstones).
func (m *Memtable) Len() int {
	return len(m.entries)
}

// Size returns the approximate memory usage in bytes.
func (m *Memtable) Size() int {
	return m.size
}

// Entries returns all entries in sorted key order.
// This is used when flushing the memtable to an SSTable.
func (m *Memtable) Entries() []memEntry {
	return m.entries
}

// search returns the index where key would be inserted to keep
// the slice sorted. If the key exists, it returns its index.
func (m *Memtable) search(key string) int {
	return sort.Search(len(m.entries), func(i int) bool {
		return m.entries[i].key >= key
	})
}
