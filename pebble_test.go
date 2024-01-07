package db

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fatih/color"
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
	keys := []int{1e3, 1e4}
	values := []int{1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15}

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
					Name:      fmt.Sprintf("Keys_%de3_Values_%d", key, value),
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
			result := benchmarkDBVariations(b, db, vc.NumKeys, vc.ValueSize)

			// Store the benchmark result
			results[fmt.Sprintf("%s_%s", vc.Name, vc.Backend)] = result
			cleanupDBDir(dir, name)
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

		// Calculate percentage difference
		setTimeDiff := (resultsArr[0].SetTime.Seconds() - resultsArr[1].SetTime.Seconds()) / resultsArr[0].SetTime.Seconds() * 100
		readTimeDiff := (resultsArr[0].ReadTime.Seconds() - resultsArr[1].ReadTime.Seconds()) / resultsArr[0].ReadTime.Seconds() * 100
		writeTimeDiff := (resultsArr[0].WriteTime.Seconds() - resultsArr[1].WriteTime.Seconds()) / resultsArr[0].WriteTime.Seconds() * 100
		concurrentTimeDiff := (resultsArr[0].ConcurrentTime.Seconds() - resultsArr[1].ConcurrentTime.Seconds()) / resultsArr[0].ConcurrentTime.Seconds() * 100

		// Print results with color
		for i, backend := range backends {
			result := results[fmt.Sprintf("%s_%s", vc.Name, backend)]
			if i == 0 {
				color.Set(color.FgGreen)
			} else {
				if resultsArr[0].SetTime.Seconds() < resultsArr[1].SetTime.Seconds() {
					color.Set(color.FgRed)
				} else {
					color.Set(color.FgGreen)
				}
			}
			fmt.Printf("%s\t%s\t", vc.Name, backend)

			if resultsArr[0].SetTime.Seconds() < resultsArr[1].SetTime.Seconds() {
				color.Set(color.FgGreen)
			} else {
				color.Set(color.FgRed)
			}
			fmt.Printf("%s\t", result.SetTime)

			if resultsArr[0].ReadTime.Seconds() < resultsArr[1].ReadTime.Seconds() {
				color.Set(color.FgGreen)
			} else {
				color.Set(color.FgRed)
			}
			fmt.Printf("%s\t", result.ReadTime)

			if resultsArr[0].WriteTime.Seconds() < resultsArr[1].WriteTime.Seconds() {
				color.Set(color.FgGreen)
			} else {
				color.Set(color.FgRed)
			}
			fmt.Printf("%s\t", result.WriteTime)

			if resultsArr[0].ConcurrentTime.Seconds() < resultsArr[1].ConcurrentTime.Seconds() {
				color.Set(color.FgGreen)
			} else {
				color.Set(color.FgRed)
			}
			fmt.Printf("%s\n", result.ConcurrentTime)

			color.Unset() // Reset to default color

			// Calculate percentage difference and print results
			setTimeDiff := (resultsArr[0].SetTime.Seconds() - resultsArr[1].SetTime.Seconds()) / resultsArr[0].SetTime.Seconds() * 100
			readTimeDiff := (resultsArr[0].ReadTime.Seconds() - resultsArr[1].ReadTime.Seconds()) / resultsArr[0].ReadTime.Seconds() * 100
			writeTimeDiff := (resultsArr[0].WriteTime.Seconds() - resultsArr[1].WriteTime.Seconds()) / resultsArr[0].WriteTime.Seconds() * 100
			concurrentTimeDiff := (resultsArr[0].ConcurrentTime.Seconds() - resultsArr[1].ConcurrentTime.Seconds()) / resultsArr[0].ConcurrentTime.Seconds() * 100

			if setTimeDiff > 0 {
				fmt.Printf("PebbleDB is %.2f%% faster when setting values.\n", setTimeDiff)
			} else {
				fmt.Printf("GoLevelDB is %.2f%% faster when setting values.\n", -setTimeDiff)
			}

			if readTimeDiff > 0 {
				fmt.Printf("PebbleDB is %.2f%% faster when reading values.\n", readTimeDiff)
			} else {
				fmt.Printf("GoLevelDB is %.2f%% faster when reading values.\n", -readTimeDiff)
			}

			if writeTimeDiff > 0 {
				fmt.Printf("PebbleDB is %.2f%% faster when writing values.\n", writeTimeDiff)
			} else {
				fmt.Printf("GoLevelDB is %.2f%% faster when writing values.\n", -writeTimeDiff)
			}

			if concurrentTimeDiff > 0 {
				fmt.Printf("PebbleDB is %.2f%% faster when reading and writing values concurrently.\n", concurrentTimeDiff)
			} else {
				fmt.Printf("GoLevelDB is %.2f%% faster when reading and writing values concurrently.\n", -concurrentTimeDiff)
			}
		}

		// Print percentage difference
		fmt.Printf("SetTime difference: %.2f%%\n", setTimeDiff)
		fmt.Printf("ReadTime difference: %.2f%%\n", readTimeDiff)
		fmt.Printf("WriteTime difference: %.2f%%\n", writeTimeDiff)
		fmt.Printf("ConcurrentTime difference: %.2f%%\n", concurrentTimeDiff)
	}

}

func benchmarkDBVariations(b *testing.B, db DB, numKeys int, valueSize int) BenchmarkResult {
	b.Helper()
	// Generate a large value
	largeValue := make([]byte, valueSize)
	for i := range largeValue {
		largeValue[i] = 'a'
	}

	// Set keys and values
	setTime := time.Now()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		err := db.Set([]byte(key), largeValue)
		require.NoError(b, err)
	}
	setDuration := time.Since(setTime)

	b.ResetTimer()

	// Random reads
	readTime := time.Now()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
		_, err := db.Get([]byte(key))
		require.NoError(b, err)
	}
	readDuration := time.Since(readTime)

	// Random writes
	writeTime := time.Now()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
		err := db.Set([]byte(key), largeValue)
		require.NoError(b, err)
	}
	writeDuration := time.Since(writeTime)

	// Random concurrent reads and writes
	var wg sync.WaitGroup
	const numRoutines = 100
	numOpsPerRoutine := numKeys / numRoutines

	concurrentTime := time.Now()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOpsPerRoutine; j++ {
				key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
				value := fmt.Sprintf("value_%d", rand.Intn(numKeys))

				err := db.Set([]byte(key), []byte(value))
				require.NoError(b, err)

				_, err = db.Get([]byte(key))
				require.NoError(b, err)
			}
		}()
	}
	// Wait for all goroutines to finish
	wg.Wait()
	concurrentDuration := time.Since(concurrentTime)

	// Return the durations
	return BenchmarkResult{
		SetTime:        setDuration,
		ReadTime:       readDuration,
		WriteTime:      writeDuration,
		ConcurrentTime: concurrentDuration,
	}
}

type BenchmarkResult struct {
	SetTime        time.Duration
	ReadTime       time.Duration
	WriteTime      time.Duration
	ConcurrentTime time.Duration
}
