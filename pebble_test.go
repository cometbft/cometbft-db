package db

import (
	"fmt"
	"os"
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

func TestPebbleDBNewPebbleDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := NewPebbleDB(name, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, wr1.Close())
	})
	_, err = NewPebbleDB(name, "")
	require.Error(t, err, "should not be able to open db twice")
}

func TestPebbleDBCompact(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	}()

	// Write some data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		err = db.Set(key, value)
		require.NoError(t, err)
	}

	// Test compaction with nil start and end
	err = db.Compact(nil, nil)
	require.NoError(t, err)

	// Test compaction with specific range
	err = db.Compact([]byte("key000"), []byte("key050"))
	require.NoError(t, err)

	// Verify data is still accessible after compaction
	value, err := db.Get([]byte("key025"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value025"), value)
}
