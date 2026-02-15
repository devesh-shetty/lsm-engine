package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure that
// tests whether an element is a member of a set. False positives are
// possible, but false negatives are not.
//
// We use double hashing to derive k hash functions from a single
// FNV-1a hash: h(i) = h1 + i*h2 (mod m).
type BloomFilter struct {
	bits    []byte // bit array packed into bytes
	numBits uint32 // total number of bits (m)
	numHash uint32 // number of hash functions (k)
}

// NewBloomFilter creates a bloom filter sized for the expected number
// of items and desired false positive rate.
//
// Optimal parameters:
//
//	m = -n * ln(p) / (ln2)^2   (number of bits)
//	k = (m/n) * ln2             (number of hash functions)
func NewBloomFilter(expectedItems int, fpRate float64) *BloomFilter {
	if expectedItems < 1 {
		expectedItems = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}

	n := float64(expectedItems)
	m := -n * math.Log(fpRate) / (math.Ln2 * math.Ln2)
	k := (m / n) * math.Ln2

	numBits := uint32(math.Ceil(m))
	if numBits < 8 {
		numBits = 8
	}
	numHash := uint32(math.Ceil(k))
	if numHash < 1 {
		numHash = 1
	}

	return &BloomFilter{
		bits:    make([]byte, (numBits+7)/8),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set.
// A false result guarantees the key is not in the set.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// hash computes two independent hash values from a key using FNV-1a.
// We split the 64-bit FNV hash into two 32-bit halves.
func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	h1 := uint32(sum)        // lower 32 bits
	h2 := uint32(sum >> 32)  // upper 32 bits
	if h2 == 0 {
		h2 = 1 // avoid degenerate case
	}
	return h1, h2
}

// Serialize encodes the bloom filter to bytes for storage in an SSTable.
// Format: [4 bytes numBits][4 bytes numHash][bits...]
func (bf *BloomFilter) Serialize() []byte {
	buf := make([]byte, 8+len(bf.bits))
	binary.LittleEndian.PutUint32(buf[0:4], bf.numBits)
	binary.LittleEndian.PutUint32(buf[4:8], bf.numHash)
	copy(buf[8:], bf.bits)
	return buf
}

// DeserializeBloom reconstructs a bloom filter from its serialized form.
func DeserializeBloom(data []byte) *BloomFilter {
	if len(data) < 8 {
		return NewBloomFilter(1, 0.01)
	}
	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHash := binary.LittleEndian.Uint32(data[4:8])
	bits := make([]byte, len(data)-8)
	copy(bits, data[8:])

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: numHash,
	}
}
