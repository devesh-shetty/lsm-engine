package lsm

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkSequentialWrites(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%08d", i)
		val := fmt.Sprintf("bench-val-%08d", i)
		if err := db.Put(key, []byte(val)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRandomReads(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate with 1000 keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench-key-%08d", i)
		val := fmt.Sprintf("bench-val-%08d", i)
		db.Put(key, []byte(val))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%08d", rng.Intn(1000))
		db.Get(key)
	}
}

func BenchmarkMixedWorkload(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate with 500 keys
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("bench-key-%08d", i)
		val := fmt.Sprintf("bench-val-%08d", i)
		db.Put(key, []byte(val))
	}

	rng := rand.New(rand.NewSource(42))
	writeCounter := 500
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Intn(2) == 0 {
			// Read
			key := fmt.Sprintf("bench-key-%08d", rng.Intn(writeCounter))
			db.Get(key)
		} else {
			// Write
			key := fmt.Sprintf("bench-key-%08d", writeCounter)
			val := fmt.Sprintf("bench-val-%08d", writeCounter)
			db.Put(key, []byte(val))
			writeCounter++
		}
	}
}
