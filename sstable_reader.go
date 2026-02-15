package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// SSTableReader provides read access to an SSTable file on disk.
// It loads the index and bloom filter into memory on open, then
// uses binary search and random reads to serve point lookups.
type SSTableReader struct {
	file  *os.File
	index []indexEntry
	bloom *BloomFilter
}

// OpenSSTable opens an SSTable file and loads its index and bloom filter.
func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable open: %w", err)
	}

	// Read footer from end of file
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable stat: %w", err)
	}
	if info.Size() < int64(footerSize) {
		f.Close()
		return nil, fmt.Errorf("sstable too small")
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, info.Size()-int64(footerSize)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable read footer: %w", err)
	}

	magic := binary.LittleEndian.Uint32(footer[24:28])
	if magic != sstMagic {
		f.Close()
		return nil, fmt.Errorf("sstable bad magic: %x", magic)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexCount := binary.LittleEndian.Uint32(footer[8:12])
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomSize := binary.LittleEndian.Uint32(footer[20:24])

	// Load bloom filter
	bloomData := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomData, bloomOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable read bloom: %w", err)
	}
	bloom := DeserializeBloom(bloomData)

	// Load index
	indexData := make([]byte, bloomOffset-indexOffset)
	if _, err := f.ReadAt(indexData, indexOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable read index: %w", err)
	}

	index := make([]indexEntry, 0, indexCount)
	pos := 0
	for i := uint32(0); i < indexCount; i++ {
		keyLen := binary.LittleEndian.Uint32(indexData[pos : pos+4])
		pos += 4
		key := string(indexData[pos : pos+int(keyLen)])
		pos += int(keyLen)
		offset := int64(binary.LittleEndian.Uint64(indexData[pos : pos+8]))
		pos += 8
		index = append(index, indexEntry{Key: key, Offset: offset})
	}

	return &SSTableReader{file: f, index: index, bloom: bloom}, nil
}

// Get looks up a key in the SSTable.
// Returns (value, tombstone, found).
func (r *SSTableReader) Get(key string) ([]byte, bool, bool) {
	// Fast path: check bloom filter first
	if !r.bloom.MayContain([]byte(key)) {
		return nil, false, false
	}

	// Binary search the in-memory index
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].Key >= key
	})
	if idx >= len(r.index) || r.index[idx].Key != key {
		return nil, false, false // bloom filter false positive
	}

	return r.readEntry(r.index[idx].Offset)
}

// readEntry reads a single data entry from disk at the given offset.
func (r *SSTableReader) readEntry(offset int64) ([]byte, bool, bool) {
	buf4 := make([]byte, 4)
	if _, err := r.file.ReadAt(buf4, offset); err != nil {
		return nil, false, false
	}
	keyLen := binary.LittleEndian.Uint32(buf4)

	// Skip past key, read value length
	valLenOff := offset + 4 + int64(keyLen)
	if _, err := r.file.ReadAt(buf4, valLenOff); err != nil {
		return nil, false, false
	}
	valLen := binary.LittleEndian.Uint32(buf4)

	// Read value + tombstone byte
	valOff := valLenOff + 4
	data := make([]byte, valLen+1)
	if _, err := r.file.ReadAt(data, valOff); err != nil {
		return nil, false, false
	}

	value := data[:valLen]
	tombstone := data[valLen] == 1
	return value, tombstone, true
}

// ReadAll reads all entries from the SSTable in sorted order.
// Used during compaction to merge SSTables.
func (r *SSTableReader) ReadAll() []SSTableEntry {
	entries := make([]SSTableEntry, 0, len(r.index))
	for _, idx := range r.index {
		val, tomb, ok := r.readEntry(idx.Offset)
		if !ok {
			continue
		}
		valueCopy := make([]byte, len(val))
		copy(valueCopy, val)
		entries = append(entries, SSTableEntry{
			Key:       idx.Key,
			Value:     valueCopy,
			Tombstone: tomb,
		})
	}
	return entries
}

// Close closes the underlying SSTable file.
func (r *SSTableReader) Close() error {
	return r.file.Close()
}
