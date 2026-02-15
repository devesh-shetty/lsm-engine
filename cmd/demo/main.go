package main

import (
	"fmt"
	"os"
	"time"

	lsm "github.com/devesh-shetty/lsm-engine"
)

func main() {
	// Create a temporary directory for our database
	dir, err := os.MkdirTemp("", "lsm-demo-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	fmt.Printf("Database directory: %s\n\n", dir)

	// Open the database
	db, err := lsm.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
		os.Exit(1)
	}

	// --- Write 1000 key-value pairs ---
	fmt.Println("Writing 1000 key-value pairs...")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("value-%06d-payload-data-here", i)
		if err := db.Put(key, []byte(val)); err != nil {
			fmt.Fprintf(os.Stderr, "put failed: %v\n", err)
			os.Exit(1)
		}
	}
	writeTime := time.Since(start)
	fmt.Printf("  Done in %v\n\n", writeTime)

	// --- Read them all back and verify ---
	fmt.Println("Reading 1000 keys and verifying...")
	start = time.Now()
	errors := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		expected := fmt.Sprintf("value-%06d-payload-data-here", i)
		val, err := db.Get(key)
		if err != nil {
			errors++
			continue
		}
		if string(val) != expected {
			errors++
		}
	}
	readTime := time.Since(start)
	fmt.Printf("  Done in %v (%d errors)\n\n", readTime, errors)

	// --- Delete every other key ---
	fmt.Println("Deleting 500 keys (even-numbered)...")
	start = time.Now()
	for i := 0; i < 1000; i += 2 {
		key := fmt.Sprintf("key-%06d", i)
		if err := db.Delete(key); err != nil {
			fmt.Fprintf(os.Stderr, "delete failed: %v\n", err)
			os.Exit(1)
		}
	}
	deleteTime := time.Since(start)
	fmt.Printf("  Done in %v\n\n", deleteTime)

	// --- Verify deletes ---
	fmt.Println("Verifying deletes...")
	deletedOK, presentOK := 0, 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		_, err := db.Get(key)
		if i%2 == 0 {
			if err == lsm.ErrKeyNotFound {
				deletedOK++
			}
		} else {
			if err == nil {
				presentOK++
			}
		}
	}
	fmt.Printf("  Deleted keys confirmed gone: %d/500\n", deletedOK)
	fmt.Printf("  Present keys confirmed:      %d/500\n\n", presentOK)

	// --- Print stats ---
	stats := db.Stats()
	fmt.Println("Database Stats:")
	fmt.Printf("  SSTables:         %d\n", stats.NumSSTables)
	fmt.Printf("  WAL size:         %d bytes\n", stats.WALSize)
	fmt.Printf("  Memtable entries: %d\n", stats.MemtableCount)
	fmt.Printf("  Memtable size:    %d bytes\n\n", stats.MemtableSize)

	// --- Timing summary ---
	fmt.Println("Timing Summary:")
	fmt.Printf("  Write 1000 keys:  %v (%.0f ops/sec)\n", writeTime, 1000/writeTime.Seconds())
	fmt.Printf("  Read 1000 keys:   %v (%.0f ops/sec)\n", readTime, 1000/readTime.Seconds())
	fmt.Printf("  Delete 500 keys:  %v (%.0f ops/sec)\n", deleteTime, 500/deleteTime.Seconds())

	// --- Close and reopen to test recovery ---
	fmt.Println("\nClosing and reopening database to test recovery...")
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close failed: %v\n", err)
		os.Exit(1)
	}

	db2, err := lsm.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reopen failed: %v\n", err)
		os.Exit(1)
	}

	// Verify keys survived
	surviving := 0
	for i := 1; i < 1000; i += 2 {
		key := fmt.Sprintf("key-%06d", i)
		if _, err := db2.Get(key); err == nil {
			surviving++
		}
	}
	fmt.Printf("  Keys surviving reopen: %d/500\n", surviving)

	db2.Close()
	fmt.Println("\nDone!")
}
