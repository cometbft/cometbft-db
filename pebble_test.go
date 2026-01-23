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
	_, err = NewPebbleDB(name, "")
	require.Error(t, err, "should not be able to open db twice")
	err = wr1.Close()
	require.NoError(t, err)
}

func TestPebbleDBCompact(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

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

func TestPebbleDBDeleteSync(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	// Set a key
	key := []byte("testkey")
	value := []byte("testvalue")
	err = db.SetSync(key, value)
	require.NoError(t, err)

	// Verify it exists
	got, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, got)

	// Delete it synchronously
	err = db.DeleteSync(key)
	require.NoError(t, err)

	// Verify it's gone
	got, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPebbleDBBatch(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	// Create a batch
	batch := db.NewBatch()
	require.NotNil(t, batch)

	// Add operations to batch
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("batchkey%d", i))
		value := []byte(fmt.Sprintf("batchvalue%d", i))
		err = batch.Set(key, value)
		require.NoError(t, err)
	}

	// Write batch
	err = batch.WriteSync()
	require.NoError(t, err)

	// Verify all keys were written
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("batchkey%d", i))
		expectedValue := []byte(fmt.Sprintf("batchvalue%d", i))
		got, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, got)
	}
}

func TestPebbleDBIterator(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	// Write test data
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		err = db.Set([]byte(k), []byte("value_"+k))
		require.NoError(t, err)
	}

	// Test forward iteration
	itr, err := db.Iterator([]byte("b"), []byte("e"))
	require.NoError(t, err)
	defer itr.Close()

	expected := []string{"b", "c", "d"}
	i := 0
	for ; itr.Valid(); itr.Next() {
		assert.Equal(t, []byte(expected[i]), itr.Key())
		i++
	}
	assert.Equal(t, len(expected), i)

	// Test reverse iteration
	ritr, err := db.ReverseIterator([]byte("b"), []byte("e"))
	require.NoError(t, err)
	defer ritr.Close()

	expectedReverse := []string{"d", "c", "b"}
	i = 0
	for ; ritr.Valid(); ritr.Next() {
		assert.Equal(t, []byte(expectedReverse[i]), ritr.Key())
		i++
	}
	assert.Equal(t, len(expectedReverse), i)
}
