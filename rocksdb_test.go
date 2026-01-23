//go:build rocksdb
// +build rocksdb

package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRocksDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*RocksDB)
	assert.True(t, ok)
}

func TestRocksDBStats(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	assert.NotEmpty(t, db.Stats())
}

func TestRocksDBNewRocksDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := NewRocksDB(name, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, wr1.Close())
	})
	_, err = NewRocksDB(name, "")
	require.Error(t, err, "should not be able to open db twice")
}

func TestRocksDBCompact(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, RocksDBBackend, dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
		cleanupDBDir(dir, name)
	}()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		err = db.Set(key, value)
		require.NoError(t, err)
	}

	err = db.Compact(nil, nil)
	require.NoError(t, err)

	err = db.Compact([]byte("key000"), []byte("key050"))
	require.NoError(t, err)

	value, err := db.Get([]byte("key025"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value025"), value)
}

func BenchmarkRocksDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewRocksDB(name, "")
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
