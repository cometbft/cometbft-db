package db

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*PebbleDB)
	assert.True(t, ok)
}

// TODO: Add tests for pebble

func TestPebbleDBStats(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	stats := db.Stats()
	assert.NotEmpty(t, stats)

	assert.Contains(t, stats, "BlockCacheSize")
	assert.Contains(t, stats, "BlockCacheHits")
	assert.Contains(t, stats, "BlockCacheMisses")
	assert.Contains(t, stats, "MemTableSize")
	assert.Contains(t, stats, "Flushes")
	assert.Contains(t, stats, "Compactions")
}

type BenchmarkCase struct {
	Name      string
	Backend   BackendType
	BenchFunc func(b *testing.B, db DB)
}

type BenchmarkVariationsCase struct {
	Name      string
	Backend   BackendType
	NumKeys   int
	ValueSize int
}

func BenchmarkDB(b *testing.B) {
	var variationCases []BenchmarkVariationsCase

	// Define the range of keys and values
	keys := []int{10000, 25000, 50000, 75000, 100000} // 10k, 100k - things we figure would happen in prod
	values := []int{512, 1024, 2048, 4096}            // 2KB, 4KB, 8KB - things we figure would happen in prod

	// Define the backends
	backends := []BackendType{PebbleDBBackend, GoLevelDBBackend}

	// Generate the test cases
	for _, key := range keys {
		for _, value := range values {
			if value > 1<<19 { // Limit the value size to 500KB
				break
			}
			for _, backend := range backends {
				variationCases = append(variationCases, BenchmarkVariationsCase{
					Name:      fmt.Sprintf("Keys_%d_Values_%d", key, value),
					Backend:   backend,
					NumKeys:   key,
					ValueSize: value,
				})
			}
		}
	}

	// Create a map to store the benchmark results
	results := make(map[string]BenchmarkResult)

	for _, vc := range variationCases {
		b.Run(fmt.Sprintf("%s_%s", vc.Name, vc.Backend), func(b *testing.B) {
			name := fmt.Sprintf("test_%x", randStr(12))
			dir := os.TempDir()
			db, err := NewDB(name, vc.Backend, dir)
			require.NoError(b, err)
			defer cleanupDBDir(dir, name)

			result := benchmarkDBVariations(b, db, vc.NumKeys, vc.ValueSize)

			// Store the benchmark result
			results[fmt.Sprintf("%s_%s", vc.Name, vc.Backend)] = result
		})
	}

	// Print out the comparison table
	fmt.Println("Test Case\tBackend\tSetTime\tReadTime\tWriteTime\tConcurrentTime")
	for _, vc := range variationCases {
		var resultsArr []BenchmarkResult
		for _, backend := range backends {
			result := results[fmt.Sprintf("%s_%s", vc.Name, backend)]
			resultsArr = append(resultsArr, result)
		}

		// Calculate total time for each backend
		totalTime0 := resultsArr[0].SetTime.Seconds() + resultsArr[0].ReadTime.Seconds() + resultsArr[0].WriteTime.Seconds()
		totalTime1 := resultsArr[1].SetTime.Seconds() + resultsArr[1].ReadTime.Seconds() + resultsArr[1].WriteTime.Seconds()

		// Determine which backend is faster
		fasterBackend := backends[0]
		if totalTime0 > totalTime1 {
			fasterBackend = backends[1]
		}

		// Print total times and faster backend
		fmt.Printf("Test Case: %s\n", vc.Name)
		fmt.Printf("Total time for %s: %.2f seconds\n", backends[0], totalTime0)
		fmt.Printf("Total time for %s: %.2f seconds\n", backends[1], totalTime1)
		fmt.Printf("Faster Backend: %s\n", fasterBackend)
		fmt.Println("-----------------------------")
	}
}

func benchmarkDBVariations(b *testing.B, db DB, numKeys int, valueSize int) BenchmarkResult {
	b.Helper()
	// Generate a large value
	largeValue := make([]byte, valueSize)
	for i := range largeValue {
		largeValue[i] = 'a'
	}

	var wg sync.WaitGroup
	const numRoutines = 100
	numOpsPerRoutine := numKeys / numRoutines

	// Set keys and values
	setTime := time.Now()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOpsPerRoutine; j++ {
				key := fmt.Sprintf("key_%d", j)
				err := db.Set([]byte(key), largeValue)
				require.NoError(b, err)
			}
		}()
	}
	wg.Wait()
	setDuration := time.Since(setTime)

	b.ResetTimer()

	// Random reads
	readTime := time.Now()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOpsPerRoutine; j++ {
				key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
				_, err := db.Get([]byte(key))
				require.NoError(b, err)
			}
		}()
	}
	wg.Wait()
	readDuration := time.Since(readTime)

	// Random writes
	writeTime := time.Now()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOpsPerRoutine; j++ {
				key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
				err := db.Set([]byte(key), largeValue)
				require.NoError(b, err)
			}
		}()
	}
	wg.Wait()
	writeDuration := time.Since(writeTime)

	// Return the durations
	return BenchmarkResult{
		SetTime:   setDuration,
		ReadTime:  readDuration,
		WriteTime: writeDuration,
	}
}

type BenchmarkResult struct {
	SetTime   time.Duration
	ReadTime  time.Duration
	WriteTime time.Duration
}
