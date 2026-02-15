package lsm

// CompactionThreshold is the number of SSTables at a level that
// triggers compaction into the next level.
const CompactionThreshold = 4

// Compact merges multiple SSTables into a single new SSTable.
// Entries are merged in sorted order; when duplicate keys exist,
// the entry from the SSTable with the higher sequence number
// (earlier in the readers slice) wins — it's the most recent write.
//
// Tombstones are removed during compaction since all SSTables
// containing the key are being merged together.
func Compact(readers []*SSTableReader, outputPath string) error {
	// Read all entries from each SSTable
	allSets := make([][]SSTableEntry, len(readers))
	for i, r := range readers {
		allSets[i] = r.ReadAll()
	}

	// Merge: k-way merge of sorted inputs
	merged := kWayMerge(allSets)

	// Remove tombstones — during compaction we can safely discard them
	// because we're merging all SSTables that could contain these keys
	live := make([]SSTableEntry, 0, len(merged))
	for _, e := range merged {
		if !e.Tombstone {
			live = append(live, e)
		}
	}

	// Write the merged entries if there are any
	if len(live) == 0 {
		// Even with no entries, create an empty SSTable for consistency.
		// In practice we could skip this, but it simplifies the caller.
		return WriteSSTable(outputPath, live)
	}

	return WriteSSTable(outputPath, live)
}

// kWayMerge merges k sorted slices into one sorted slice.
// When the same key appears in multiple slices, the entry from the
// slice with the lowest index wins (that's the newest SSTable).
func kWayMerge(sets [][]SSTableEntry) []SSTableEntry {
	// Track current position in each set
	positions := make([]int, len(sets))
	var result []SSTableEntry

	for {
		// Find the smallest key across all sets
		minKey := ""
		minSet := -1
		for i, pos := range positions {
			if pos >= len(sets[i]) {
				continue // this set is exhausted
			}
			key := sets[i][pos].Key
			if minSet == -1 || key < minKey {
				minKey = key
				minSet = i
			}
		}

		if minSet == -1 {
			break // all sets exhausted
		}

		// Collect the entry from the newest set (lowest index) that
		// has this key, and advance all sets past this key
		var winner SSTableEntry
		winnerIdx := -1
		for i, pos := range positions {
			if pos >= len(sets[i]) {
				continue
			}
			if sets[i][pos].Key == minKey {
				if winnerIdx == -1 || i < winnerIdx {
					winner = sets[i][pos]
					winnerIdx = i
				}
				positions[i]++ // advance past this key in every set
			}
		}

		result = append(result, winner)
	}

	return result
}
