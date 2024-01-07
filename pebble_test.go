package db

import (
	"fmt"
	"os"
	"sync"
	"testing"

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

// func TestPebbleDBStats(t *testing.T) {
// 	name := fmt.Sprintf("test_%x", randStr(12))
// 	dir := os.TempDir()
// 	db, err := NewDB(name, PebbleDBBackend, dir)
// 	require.NoError(t, err)
// 	defer cleanupDBDir(dir, name)

// 	assert.NotEmpty(t, db.Stats())
// }

func BenchmarkPebbleDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = db.Close()
		require.NoError(b, err)
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
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

func TestPebbleDBConcurrentReadsWrites(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	var wg sync.WaitGroup
	const numRoutines = 100
	const numOpsPerRoutine = 1000

	// Start numRoutines goroutines, each performing read and write operations
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < numOpsPerRoutine; j++ {
				key := fmt.Sprintf("key_%d_%d", i, j)
				value := fmt.Sprintf("value_%d_%d", i, j)

				err := db.Set([]byte(key), []byte(value))
				require.NoError(t, err)

				readValue, err := db.Get([]byte(key))
				require.NoError(t, err)
				assert.Equal(t, value, string(readValue))
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
