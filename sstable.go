package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
)

// SSTable on-disk format:
//
//	[data entries...][index entries...][bloom filter bytes][footer]
//
// Data entry:  [key_len(4)][key][value_len(4)][value][tombstone(1)]
// Index entry: [key_len(4)][key][offset(8)]
// Footer:      [index_offset(8)][index_count(4)][bloom_offset(8)][bloom_size(4)][magic(4)]
//
// Magic number: 0x4C534D54 ("LSMT")
const sstMagic uint32 = 0x4C534D54
const footerSize = 8 + 4 + 8 + 4 + 4 // 28 bytes

// SSTableEntry represents a key-value pair written to an SSTable.
type SSTableEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// indexEntry maps a key to its byte offset in the data section.
type indexEntry struct {
	Key    string
	Offset int64
}

// WriteSSTable writes a sorted slice of entries to an SSTable file.
// The caller must ensure entries are sorted by key.
func WriteSSTable(path string, entries []SSTableEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("sstable create: %w", err)
	}
	defer f.Close()

	// Build bloom filter from keys
	bloom := NewBloomFilter(len(entries), 0.01)
	for _, e := range entries {
		bloom.Add([]byte(e.Key))
	}

	// Write data entries, collecting index as we go
	var index []indexEntry
	offset := int64(0)

	for _, e := range entries {
		index = append(index, indexEntry{Key: e.Key, Offset: offset})

		keyBytes := []byte(e.Key)
		entrySize := 4 + len(keyBytes) + 4 + len(e.Value) + 1
		buf := make([]byte, entrySize)

		off := 0
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(keyBytes)))
		off += 4
		copy(buf[off:], keyBytes)
		off += len(keyBytes)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Value)))
		off += 4
		copy(buf[off:], e.Value)
		off += len(e.Value)
		if e.Tombstone {
			buf[off] = 1
		}

		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("sstable write data: %w", err)
		}
		offset += int64(entrySize)
	}

	// Write index entries
	indexOffset := offset
	for _, idx := range index {
		keyBytes := []byte(idx.Key)
		buf := make([]byte, 4+len(keyBytes)+8)
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		copy(buf[4:], keyBytes)
		binary.LittleEndian.PutUint64(buf[4+len(keyBytes):], uint64(idx.Offset))
		if _, err := f.Write(buf); err != nil {
			return fmt.Errorf("sstable write index: %w", err)
		}
		offset += int64(len(buf))
	}

	// Write bloom filter
	bloomBytes := bloom.Serialize()
	bloomOffset := offset
	if _, err := f.Write(bloomBytes); err != nil {
		return fmt.Errorf("sstable write bloom: %w", err)
	}

	// Write footer
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(len(index)))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(len(bloomBytes)))
	binary.LittleEndian.PutUint32(footer[24:28], sstMagic)
	if _, err := f.Write(footer); err != nil {
		return fmt.Errorf("sstable write footer: %w", err)
	}

	return f.Sync()
}
