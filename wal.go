package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// OpType represents the type of WAL operation.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// WALEntry is a single operation recorded in the write-ahead log.
type WALEntry struct {
	Op    OpType
	Key   []byte
	Value []byte // empty for deletes
}

// WAL is an append-only write-ahead log that survives crashes.
// Every write is fsync'd before returning, so committed entries
// are guaranteed to be on disk.
type WAL struct {
	file *os.File
}

// OpenWAL opens (or creates) a write-ahead log at the given path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	return &WAL{file: f}, nil
}

// Append writes an entry to the log and fsyncs it to disk.
//
// On-disk format per entry:
//
//	[4 bytes total length][4 bytes CRC32][1 byte op][4 bytes key len][key][4 bytes value len][value]
//
// The CRC32 covers everything after the CRC field (op + key len + key + value len + value).
func (w *WAL) Append(entry WALEntry) error {
	// Build the payload: op + key_len + key + val_len + val
	payloadSize := 1 + 4 + len(entry.Key) + 4 + len(entry.Value)
	payload := make([]byte, payloadSize)

	off := 0
	payload[off] = byte(entry.Op)
	off++
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(entry.Key)))
	off += 4
	copy(payload[off:], entry.Key)
	off += len(entry.Key)
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(entry.Value)))
	off += 4
	copy(payload[off:], entry.Value)

	// Compute CRC over the payload
	checksum := crc32.ChecksumIEEE(payload)

	// Build the full record: length + CRC + payload
	record := make([]byte, 4+4+payloadSize)
	binary.LittleEndian.PutUint32(record[0:4], uint32(payloadSize))
	binary.LittleEndian.PutUint32(record[4:8], checksum)
	copy(record[8:], payload)

	n, err := w.file.Write(record)
	if err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	if n != len(record) {
		return fmt.Errorf("wal short write: wrote %d of %d bytes", n, len(record))
	}
	// fsync ensures durability. On macOS this uses F_FULLFSYNC
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return nil
}

// Replay reads all valid entries from the WAL file. Partial or
// corrupted entries at the tail are silently skipped — they
// represent writes that weren't fsync'd before a crash.
func Replay(path string) ([]WALEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal replay open: %w", err)
	}
	defer f.Close()

	var entries []WALEntry
	header := make([]byte, 8) // length + CRC

	for {
		// Read the 8-byte header
		if _, err := io.ReadFull(f, header); err != nil {
			break // EOF or partial header — done
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		storedCRC := binary.LittleEndian.Uint32(header[4:8])

		// Sanity check: reject absurdly large entries
		if length > 64*1024*1024 {
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // partial payload — entry wasn't fully written
		}

		// Verify checksum
		if crc32.ChecksumIEEE(payload) != storedCRC {
			break // corrupted entry — stop here
		}

		entry, err := decodePayload(payload)
		if err != nil {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// decodePayload parses the op + key + value from a WAL payload.
func decodePayload(payload []byte) (WALEntry, error) {
	if len(payload) < 9 { // 1 op + 4 key_len + at least 0 key + 4 val_len
		return WALEntry{}, fmt.Errorf("payload too short")
	}
	op := OpType(payload[0])
	keyLen := binary.LittleEndian.Uint32(payload[1:5])
	if uint32(len(payload)) < 5+keyLen+4 {
		return WALEntry{}, fmt.Errorf("payload truncated at key")
	}
	key := make([]byte, keyLen)
	copy(key, payload[5:5+keyLen])

	valOff := 5 + keyLen
	valLen := binary.LittleEndian.Uint32(payload[valOff : valOff+4])
	if uint32(len(payload)) < valOff+4+valLen {
		return WALEntry{}, fmt.Errorf("payload truncated at value")
	}
	value := make([]byte, valLen)
	copy(value, payload[valOff+4:valOff+4+valLen])

	return WALEntry{Op: op, Key: key, Value: value}, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// Size returns the current size of the WAL file in bytes.
func (w *WAL) Size() int64 {
	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
