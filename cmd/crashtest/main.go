package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	lsm "github.com/devesh-shetty/lsm-engine"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "write-and-crash" {
		// Phase 1: Write 500 key-value pairs then crash (os.Exit(1), no Close())
		dir := os.Args[2]
		db, err := lsm.Open(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open failed: %v\n", err)
			os.Exit(2)
		}

		for i := 0; i < 500; i++ {
			key := fmt.Sprintf("crash-key-%06d", i)
			val := fmt.Sprintf("crash-val-%06d", i)
			if err := db.Put(key, []byte(val)); err != nil {
				fmt.Fprintf(os.Stderr, "put failed at %d: %v\n", i, err)
				os.Exit(2)
			}
		}

		fmt.Println("Wrote 500 keys. Crashing NOW (os.Exit(1), no Close)...")
		_ = db // intentionally NOT calling db.Close()
		os.Exit(1)
	}

	// Phase 0: orchestrator
	dir, err := os.MkdirTemp("", "lsm-crash-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	fmt.Println("=== LSM Crash Recovery Test ===")
	fmt.Printf("Database directory: %s\n\n", dir)

	// Run ourselves in write-and-crash mode as a subprocess
	fmt.Println("Phase 1: Writing 500 keys then crashing (no clean shutdown)...")
	moduleRoot := findModuleRoot()

	cmd := exec.Command("go", "run", "./cmd/crashtest/", "write-and-crash", dir)
	cmd.Dir = moduleRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err == nil {
		fmt.Println("ERROR: child process should have exited with code 1")
		os.Exit(1)
	}
	fmt.Printf("  Child exited with: %v (expected)\n\n", err)

	// Phase 2: Reopen and verify all 500 keys
	fmt.Println("Phase 2: Reopening database and verifying all 500 keys...")
	db, err := lsm.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reopen failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	recovered := 0
	missing := 0
	corrupt := 0
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("crash-key-%06d", i)
		expected := fmt.Sprintf("crash-val-%06d", i)
		val, err := db.Get(key)
		if err != nil {
			missing++
			continue
		}
		if string(val) != expected {
			corrupt++
			continue
		}
		recovered++
	}

	fmt.Printf("  Recovered: %d/500\n", recovered)
	fmt.Printf("  Missing:   %d/500\n", missing)
	fmt.Printf("  Corrupt:   %d/500\n\n", corrupt)

	if recovered == 500 {
		fmt.Println("PASS: All 500 keys recovered after crash!")
	} else {
		fmt.Printf("FAIL: Only %d/500 keys recovered\n", recovered)
		os.Exit(1)
	}
}

func findModuleRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}
